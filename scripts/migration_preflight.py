"""Read-only Docker PostgreSQL audit and external backup before reviewed migrations.

Never applies migrations or starts/stops collectors. Default is an offline plan.
The backup can contain credentials and must remain outside the repository.
"""
import argparse
import hashlib
import json
import shutil
import subprocess
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path


def docker(*args, **kwargs):
    return subprocess.check_output(['docker', *args], timeout=120, **kwargs)


def query(container, sql):
    # Credentials remain inside the container; psql accepts only SELECT in this transaction.
    command = 'exec psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    raw = docker('exec', '-i', container, 'sh', '-c', command,
                 input=('BEGIN READ ONLY; SET LOCAL statement_timeout=\'10s\'; '
                        + sql + '; ROLLBACK;').encode())
    return json.loads(raw)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--container', default='product-data-collector-postgres')
    parser.add_argument('--backup-dir', type=Path)
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute': False, 'read_only_audit': True, 'apply_migrations': False}))
        return
    root = Path(__file__).resolve().parents[1]
    backup = args.backup_dir.resolve() if args.backup_dir else None
    if backup is None or backup == root or root in backup.parents:
        parser.error('--backup-dir must be explicitly outside the repository')
    backup.mkdir(parents=True, exist_ok=False)
    result = {'utc': datetime.now(timezone.utc).isoformat(), 'migration_applied': False,
              'backup_path': str(backup), 'container': args.container}
    checks = {
        'identity': "SELECT json_build_object('database',current_database(),'role',current_user,'version',version(),'search_path',current_setting('search_path'),'database_bytes',pg_database_size(current_database()))",
        'ledger': "SELECT coalesce(json_agg(name ORDER BY name),'[]') FROM schema_migrations",
        'tables': "SELECT json_agg(t) FROM (SELECT schemaname,tablename,tableowner FROM pg_tables WHERE schemaname NOT IN ('pg_catalog','information_schema') ORDER BY 1,2) t",
        'columns': "SELECT json_agg(t) FROM (SELECT table_schema,table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position) t",
        'indexes': "SELECT json_agg(t) FROM (SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname) t",
        'roles': "SELECT json_agg(t) FROM (SELECT rolname,rolsuper,rolinherit,rolcreaterole,rolcreatedb,rolcanlogin,rolreplication,rolbypassrls FROM pg_roles ORDER BY rolname) t",
        'memberships': "SELECT coalesce(json_agg(t),'[]') FROM (SELECT pg_get_userbyid(roleid) AS role,pg_get_userbyid(member) AS member,admin_option FROM pg_auth_members) t",
        'acl': "SELECT json_build_object('database',(SELECT datacl FROM pg_database WHERE datname=current_database()),'schema',(SELECT nspacl FROM pg_namespace WHERE nspname='public'),'relations',(SELECT json_agg(t) FROM (SELECT relname,relkind,relacl FROM pg_class JOIN pg_namespace n ON n.oid=relnamespace WHERE n.nspname='public' ORDER BY relname) t))",
        'sessions': "SELECT coalesce(json_agg(t),'[]') FROM (SELECT pid,usename,application_name,client_addr,state,xact_start,wait_event_type,wait_event,backend_type FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()) t",
        'locks': "SELECT coalesce(json_agg(t),'[]') FROM (SELECT l.pid,locktype,mode,granted,relation::regclass::text AS relation FROM pg_locks l JOIN pg_stat_activity a USING(pid) WHERE a.datname=current_database() AND l.pid<>pg_backend_pid()) t",
        'settings': "SELECT json_agg(t) FROM (SELECT name,setting,unit FROM pg_settings WHERE name IN ('data_directory','max_wal_size','min_wal_size','archive_mode','wal_level','max_replication_slots','max_connections')) t",
        'queue': "SELECT json_agg(t) FROM (SELECT platform,count(*) AS pending FROM product_discovery_targets WHERE requested_version>completed_version GROUP BY platform) t",
        'relation_sizes': "SELECT json_agg(t) FROM (SELECT relname,pg_relation_size(c.oid) AS heap_bytes,pg_indexes_size(c.oid) AS index_bytes,pg_total_relation_size(c.oid) AS total_bytes,c.reltuples::bigint AS estimated_rows FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND c.relkind='r' ORDER BY pg_total_relation_size(c.oid) DESC) t",
    }
    for name, sql in checks.items():
        result[name] = query(args.container, sql)
    result['host_free_bytes'] = shutil.disk_usage(root).free
    result['docker_free'] = docker('exec', args.container, 'df', '-B1', '/var/lib/postgresql/data', text=True)
    result['wal_bytes'] = query(args.container, 'SELECT coalesce(sum(size),0)::text::json FROM pg_ls_waldir()')
    info = json.loads(docker('inspect', 'product-data-collector-app'))[0]
    result['collector'] = {'running': info['State']['Running'], 'command': info['Config']['Cmd']}
    start = time.perf_counter()
    dump = backup / 'collector.dump'
    with dump.open('xb') as output:
        subprocess.run(['docker', 'exec', args.container, 'sh', '-c',
                        'exec pg_dump -Fc -U "$POSTGRES_USER" -d "$POSTGRES_DB"'],
                       stdout=output, check=True, timeout=120)
    result['dump_seconds'] = time.perf_counter() - start
    globals_file = backup / 'roles.sql'
    globals_file.write_bytes(docker('exec', args.container, 'sh', '-c',
                                   'exec pg_dumpall --roles-only --no-role-passwords -U "$POSTGRES_USER"'))
    with zipfile.ZipFile(backup / 'resume-state.zip', 'x', zipfile.ZIP_DEFLATED) as archive:
        files = [root / '.env']
        for supplier in ('ownerclan_API', 'domeggook_API', 'coupang_API', 'naver_API'):
            for folder in ('data', 'config'):
                path = root / supplier / folder
                if path.exists():
                    files.extend(p for p in path.rglob('*') if p.is_file() and '__pycache__' not in p.parts)
        for path in files:
            if path.is_file():
                archive.write(path, path.relative_to(root).as_posix())
    result['state_archive_current_differences'] = []
    with zipfile.ZipFile(backup / 'resume-state.zip') as archive:
        for entry in archive.infolist():
            path = root / entry.filename
            if not path.exists() or hashlib.sha256(path.read_bytes()).digest() != hashlib.sha256(archive.read(entry)).digest():
                result['state_archive_current_differences'].append(entry.filename)
    result['files'] = []
    for path in sorted(backup.iterdir()):
        if path.is_file():
            with path.open('rb') as source:
                digest = hashlib.file_digest(source, 'sha256').hexdigest()
            result['files'].append({'name': path.name, 'bytes': path.stat().st_size, 'sha256': digest})
    result['limitations'] = ['Logical backup; no PITR/failure drill.',
                             'Role passwords excluded; restore cannot recreate login secrets.',
                             'Role/ACL audit does not establish intended Collector/Spring role assignments.',
                             'Read-only audit is a point-in-time observation, not a writer fence.']
    (backup / 'manifest.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    target = root / 'artifacts/validation/migration-preflight.json'
    target.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf8')
    print(json.dumps({'backup': str(backup), 'ledger': result['ledger'], 'queue': result['queue'],
                      'sessions': result['sessions'], 'collector': result['collector'], 'files': result['files']}))


if __name__ == '__main__':
    main()
