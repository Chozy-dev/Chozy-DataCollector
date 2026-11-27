"""Thin adapters to supplier workflows; no alternate collection logic."""
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from psycopg.types.json import Jsonb
from .budget import BudgetSession, shared_budget_required
from .fence import scoring_boundary,certify_collection,source_collection_boundary


def handlers(config,connect,stop=lambda:False):
    root=Path(__file__).resolve().parents[2]
    settings=config['sources']

    shared_required = shared_budget_required() or config.get('require_platform_budget', True)

    def wrap(client,source,attr='_session',api='default',routes=None,parents=None):
        spec=settings[source]
        setattr(client,attr,BudgetSession(getattr(client,attr),connect,spec['account'],api,
            shared_required=shared_required,routes=routes,parents=parents))
        return client

    def collect(source,mode='daily'):
        def perform(conn,job):
            spec=settings[source]
            if source=='domeggook':
                from domeggook_API.api.client import PrivateDetailClient
                from domeggook_API.workflows.product_sync import incremental, catalogue
                from domeggook_API.api.private_sync import PrivateSyncClient, private_api_key
                from domeggook_API.api.rate_limiter import RateLimiter
                from domeggook_API.persistence.storage import load_state

                state = Path(spec['state_dir'])
                state.mkdir(parents=True, exist_ok=True)
                key = private_api_key()
                limiter = RateLimiter(spec['private_rpm'])
                private = wrap(PrivateSyncClient(key, rate_limiter=limiter, request_budget=None),
                               source, 'session', 'private')
                details = wrap(PrivateDetailClient(key, limiter), source, api='private')
                if mode != 'daily':
                    return catalogue(conn, private, details, state / (mode + '.json'), mode=mode)
                sync_path = state / 'sync.json'
                first_range = {}
                if not sync_path.exists():
                    bootstrap = load_state(state / 'bootstrap.json')
                    private_start = bootstrap.get('startedAt') if bootstrap.get('source') == 'private-search' else None
                    first_range['start'] = private_start or spec.get('initial_start')
                return incremental(conn, private, details, sync_path,
                                   provider_timezone=spec['provider_timezone'],
                                   **first_range)
            from ownerclan_API.config import load_config
            from ownerclan_API.api.factory import make_client
            from ownerclan_API.workflows.history_sync import collect_history,refresh_details
            from ownerclan_API.workflows.bootstrap import bootstrap
            cfg=load_config(Path(spec['config']),root)
            if mode!='daily':
                from product_data_collector.persistence import postgres_storage as pg
                legacy=pg.load_postgres_config(root)
                if (legacy.host,legacy.port,legacy.database,legacy.user)!=(conn.info.host,conn.info.port,conn.info.dbname,conn.info.user):
                    raise ValueError('Legacy storage target differs from operations DSN')
            client=wrap(make_client(root,cfg),source)
            # Authentication HTTP uses the same account contract.
            if hasattr(client._jwt_provider,'_session'):
                client._jwt_provider._session=BudgetSession(client._jwt_provider._session,connect,
                    spec['account'],'auth',shared_required=shared_required)
            result=(collect_history(root,cfg,client=client,connection=conn) if mode=='daily' else
                    bootstrap(root,cfg,client=client,mode=mode))
            if result.get('status')!='completed': return result
            return refresh_details(root,cfg,client=client,connection=conn)
        def execute(conn,job):
            if mode!='daily': return perform(conn,job)
            with source_collection_boundary(conn,source):
                outcome=perform(conn,job)
                if outcome.get('status')=='completed':
                    conn.commit()
                    certify_collection(conn,source,job['job_id'])
                return outcome
        return execute

    def score(source):
        def execute(conn,job):
            from product_data_collector.persistence.scoring import run_scoring
            rid=job['job_id']+':score'
            with scoring_boundary(conn,rid,source):
                pending=conn.execute("SELECT 1 FROM ops_jobs WHERE task LIKE %s AND job_id<>%s AND status<>'completed' AND superseded_by IS NULL LIMIT 1",
                                     (source+'-%',job['job_id'])).fetchone()
                if pending: raise RuntimeError('Source catalogue incomplete')
                # Fence acquired before the DB clock and retained across all chunks.
                if 'as_of' not in job['metrics']:
                    job['metrics']['as_of']=conn.execute('SELECT clock_timestamp() AS t').fetchone()['t'].isoformat()
                    conn.execute('UPDATE ops_jobs SET metrics=%s WHERE job_id=%s',(Jsonb(job['metrics']),job['job_id']))
                    conn.commit()
                conn.commit()
                result=run_scoring(conn,run_id=rid,platform=source,mode=job['scope'].get('scoring_mode','incremental'),
                    as_of=job['metrics']['as_of'],chunk_size=config.get('chunk_size',500),stop_requested=stop)
                return {'status':('paused' if result['status']=='running' else result['status']),'scored':result['processed']}
        return execute

    def market(source):
        def execute(conn,job):
            from .runner import KST
            day=job['scheduled_at'].astimezone(KST).date()
            conn.commit()
            if source=='coupang':
                from coupang_API.config.best import load_best_config
                from coupang_API.config.settings import load_credentials
                from coupang_API.api.client import CoupangPartnersClient
                from coupang_API.api.rate_limiter import RateLimiter
                from coupang_API.workflows.best import collect_best
                cfg=load_best_config(Path(settings[source]['config']))
                client=wrap(CoupangPartnersClient(*load_credentials(root),rate_limiter=RateLimiter(cfg.requests_per_minute,60)),
                    source,api='best',parents={'best':'default'})
                return collect_best(conn,client,cfg,day)
            from naver_API.config import load_config
            from naver_API.client import NaverClient
            from naver_API.collector import collect_season
            cfg=load_config(Path(settings[source]['config']),day)
            client=wrap(NaverClient.from_env(cfg),source,'session','ad',{'search-trend':'trend'},
                parents={'trend':'hub'} if shared_required else None)
            return collect_season(conn,client,cfg,day)
        return execute

    def comparison(conn,job):
        from naver_API.queries import read_compare
        from .runner import KST
        result=read_compare(conn,config['comparison_theme'])
        conn.commit()
        available=bool(result and result.get('comparisonStatus')=='available'
                       and result['snapshotDate']==job['scheduled_at'].astimezone(KST).date())
        return {'status':'completed' if available else 'paused','comparisonAvailable':available}

    result={'domeggook':[collect('domeggook'),score('domeggook')],
            'ownerclan':[collect('ownerclan'),score('ownerclan')],
            'coupang':[market('coupang')],'naver':[market('naver')],'comparison':[comparison]}
    for source in ('domeggook','ownerclan'):
        for mode in ('bootstrap','discovery','recovery'):
            result[source+'-'+mode]=[collect(source,mode)]
    def guarded(handler,task):
        def execute(conn,job):
            if job['scope'].get('config_hash')!=execution_fingerprint(config,task):
                raise ValueError('Execution configuration changed; restore original config or start a new job')
            return handler(conn,job)
        return execute
    result={task:[guarded(h,task) for h in steps] for task,steps in result.items()}
    return result


def execution_fingerprint(config,task=None):
    frozen={k:config.get(k) for k in ('sources','scoring_mode','comparison_theme','chunk_size','require_platform_budget')}
    if task:
        names=('naver','coupang') if task=='comparison' else (task.split('-')[0],)
        frozen['sources']={s:v for s,v in config['sources'].items() if s in names}
    # Also freeze the contents of provider scope files, not credentials.
    frozen['files']={}
    for source,spec in frozen['sources'].items():
        try: frozen['files'][source]=Path(spec['config']).read_text(encoding='utf8')
        except OSError as exc: frozen['files'][source]={'unavailable':type(exc).__name__}
    return hashlib.sha256(json.dumps(frozen,sort_keys=True).encode()).hexdigest()
