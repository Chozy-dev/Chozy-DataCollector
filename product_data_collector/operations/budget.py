"""Reserve on a separate committed connection BEFORE every HTTP attempt."""
import time


class BudgetStopped(RuntimeError):
    pass


class BudgetSession:
    def __init__(self, session, connect, account, api, *, shared_required=True, routes=None):
        self.session, self.connect = session, connect
        self.account, self.api = account, api
        self.shared_required, self.routes = shared_required, routes or {}

    def request(self, method, url, **kwargs):
        api=next((v for k,v in self.routes.items() if k in url),self.api)
        # Bounded pacing retries; failures and process death never refund usage.
        for _ in range(61):
            with self.connect() as conn:
                result=conn.execute("""SELECT CASE
                    WHEN to_regprocedure('block_api_account(text,text)') IS NULL
                    THEN 'runtime_budget_guard_missing'
                    ELSE reserve_api_call(%s,%s,%s) END AS result""",
                    (self.account,api,self.shared_required)).fetchone()['result']
                conn.commit()
            if result!='interval': break
            time.sleep(1)
        if result!='reserved':
            with self.connect() as conn:
                conn.execute('INSERT INTO ops_alerts(job_id,kind) VALUES(%s,%s) ON CONFLICT DO NOTHING',
                    ('api:'+self.account+':'+api,result))
                conn.commit()
            raise BudgetStopped(result)
        kwargs['allow_redirects']=False
        response=self.session.request(method,url,**kwargs)
        if response.status_code in (403,429):
            with self.connect() as conn:
                conn.execute('SELECT block_api_account(%s,%s)',
                             (self.account,api))
                conn.execute("INSERT INTO ops_alerts(job_id,kind) VALUES(%s,'api_restricted') ON CONFLICT DO NOTHING",
                             ('api:'+self.account+':'+api,))
                conn.commit()
            raise BudgetStopped('provider_restricted')
        return response

    def get(self,url,**kwargs): return self.request('GET',url,**kwargs)
    def post(self,url,**kwargs): return self.request('POST',url,**kwargs)
