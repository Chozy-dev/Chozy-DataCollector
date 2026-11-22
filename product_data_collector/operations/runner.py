"""Durable sequential steps with independent task failure domains."""
from datetime import datetime, timedelta, timezone, time
from zoneinfo import ZoneInfo
from psycopg.types.json import Jsonb

KST=ZoneInfo('Asia/Seoul')
TASKS=('domeggook','ownerclan','coupang','naver','comparison',
       'domeggook-bootstrap','ownerclan-bootstrap','domeggook-discovery','ownerclan-discovery','domeggook-recovery','ownerclan-recovery')


def slots(config, now):
    """Bounded catch-up. Stable slots also deduplicate simultaneous schedulers."""
    local=now.astimezone(KST)
    horizon=int(config.get('catchup_days',1))
    if not 0<=horizon<=31: raise ValueError('catchup_days must be between 0 and 31')
    for task,spec in config.get('schedules',{}).items():
        if task not in TASKS: raise ValueError('Unknown task')
        interval=int(spec.get('every_days',1))
        if interval<1: raise ValueError('Invalid schedule interval')
        at=time.fromisoformat(spec['at'])
        if at.tzinfo: raise ValueError('Schedule time must be local KST')
        for ago in range(horizon,-1,-1):
            day=local.date()-timedelta(days=ago)
            if (day-datetime(1970,1,1).date()).days % interval: continue
            due=datetime.combine(day,at,KST)
            if due<=local: yield task,due.astimezone(timezone.utc)


class Runner:
    def __init__(self, connect, handlers, max_attempts=3, retry_seconds=300, stop=lambda:False):
        self.connect,self.handlers=connect,handlers
        self.max_attempts,self.retry_seconds,self.stop=max_attempts,retry_seconds,stop

    def run(self,task,scheduled_at,scope,job_id=None):
        if task not in TASKS: raise ValueError('Unknown task')
        if scheduled_at.tzinfo is None: raise ValueError('Timezone-aware schedule required')
        scheduled_at=scheduled_at.astimezone(timezone.utc)
        job_id=job_id or task+':'+scheduled_at.isoformat()
        with self.connect() as conn:
            key='ops-task:'+task.split('-')[0]
            locked=conn.execute('SELECT pg_try_advisory_lock(hashtextextended(%s,0)) AS ok',(key,)).fetchone()['ok']
            conn.commit()
            if not locked: return 'paused'
            try:
                conn.execute('INSERT INTO ops_jobs(job_id,task,scheduled_at,scope) VALUES(%s,%s,%s,%s) ON CONFLICT DO NOTHING',
                             (job_id,task,scheduled_at,Jsonb(scope)))
                job=conn.execute('SELECT * FROM ops_jobs WHERE job_id=%s',(job_id,)).fetchone()
                if (job['task'],job['scheduled_at'],job['scope'])!=(task,scheduled_at,scope):
                    raise ValueError('Job scope frozen')
                if job['status']=='completed': return 'completed'
                if job['superseded_by']: return 'failed'
                # Old unfinished runs block newer scheduled work for the same task.
                older=conn.execute("SELECT 1 FROM ops_jobs WHERE task=%s AND scheduled_at<%s AND status<>'completed' AND superseded_by IS NULL LIMIT 1",(task,scheduled_at)).fetchone()
                if older: return 'paused'
                if job['attempts']>=self.max_attempts:
                    self.finish(conn,job_id,'failed','attempts_exhausted')
                    return 'failed'
                if job['attempts'] and job['status']!='running' and datetime.now(timezone.utc)<job['updated_at']+timedelta(seconds=self.retry_seconds): return 'paused'
                conn.execute("UPDATE ops_jobs SET status='running',attempts=attempts+1,started_at=COALESCE(started_at,clock_timestamp()),updated_at=clock_timestamp(),error=NULL WHERE job_id=%s",(job_id,))
                conn.commit()
                for index,handler in enumerate(self.handlers[task]):
                    if index<job['step']: continue
                    if self.stop():
                        self.finish(conn,job_id,'paused','shutdown')
                        return 'paused'
                    result=handler(conn,job)
                    if isinstance(result,str): result={'status':result}
                    if result.get('status')!='completed':
                        self.finish(conn,job_id,result.get('status','failed'),'step_incomplete',result)
                        return result.get('status','failed')
                    job['metrics'].update(result)
                    conn.execute('UPDATE ops_jobs SET step=%s,metrics=%s,updated_at=clock_timestamp() WHERE job_id=%s',
                                 (index+1,Jsonb(job['metrics']),job_id))
                    conn.commit()
                self.finish(conn,job_id,'completed',None,job['metrics'])
                return 'completed'
            except Exception as exc:
                conn.rollback()
                self.finish(conn,job_id,'failed',type(exc).__name__)
                return 'failed'
            finally:
                conn.rollback()
                conn.execute('SELECT pg_advisory_unlock(hashtextextended(%s,0))',(key,))
                conn.commit()

    @staticmethod
    def finish(conn,job_id,status,error,metrics=None):
        conn.execute("""UPDATE ops_jobs SET status=%s,error=%s,updated_at=clock_timestamp(),
            completed_at=CASE WHEN %s='completed' THEN clock_timestamp() ELSE NULL END,
            metrics=metrics || %s WHERE job_id=%s AND status<>'completed'""",(status,error,status,Jsonb(metrics or {}),job_id))
        if status!='completed':
            conn.execute('INSERT INTO ops_alerts(job_id,kind) VALUES(%s,%s) ON CONFLICT DO NOTHING',(job_id,error or status))
        conn.commit()
