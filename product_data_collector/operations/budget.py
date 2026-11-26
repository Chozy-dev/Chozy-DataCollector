"""Reserve on a separate committed connection BEFORE every HTTP attempt."""
import os
import time
from urllib.parse import parse_qs, urlsplit

import requests


class BudgetStopped(RuntimeError):
    pass


class BudgetSession:
    def __init__(self, session, connect, account, api, *, shared_required=True, routes=None,
                 parents=None):
        self.session, self.connect = session, connect
        self.account, self.api = account, api
        self.shared_required, self.routes = shared_required, routes or {}
        self.parents = parents or {}
        if isinstance(session, SupplierSession):
            session.managed_by_operations = True

    def reserve(self, api):
        parent = self.parents.get(api)
        # Bounded pacing retries; failures and process death never refund usage.
        for attempt in range(61):
            with self.connect() as conn:
                configured = True
                if parent is not None:
                    # Serialize the required mapping check with policy updates and reservation.
                    conn.execute("""SELECT pg_advisory_xact_lock(
                        hashtextextended('chozy-api-budget:' || %s, 0))""", (self.account,))
                    configured = conn.execute("""SELECT 1 FROM api_budget_policy leaf
                        JOIN api_budget_policy total ON (total.account,total.api)=(leaf.account,leaf.shared_api)
                        WHERE leaf.account=%s AND leaf.api=%s AND total.api=%s
                          AND (leaf.api <> 'best' OR total.api <> 'default' OR NOT EXISTS (
                            SELECT 1 FROM jsonb_each_text(total.limits) AS quota(period,value)
                            WHERE (leaf.limits->>quota.period)::bigint IS NULL
                               OR (leaf.limits->>quota.period)::bigint >= quota.value::bigint
                          ))""", (self.account, api, parent)).fetchone()
                result = 'shared_policy_missing'
                if configured:
                    result = conn.execute("""SELECT CASE
                        WHEN to_regprocedure('block_api_account(text,text)') IS NULL
                        THEN 'runtime_budget_guard_missing'
                        ELSE reserve_api_call(%s,%s,%s) END AS result""",
                        (self.account, api, self.shared_required)).fetchone()['result']
                conn.commit()
            if result != 'interval':
                break
            if attempt < 60:
                time.sleep(1)
        if result != 'reserved':
            with self.connect() as conn:
                conn.execute('INSERT INTO ops_alerts(job_id,kind) VALUES(%s,%s) ON CONFLICT DO NOTHING',
                    ('api:'+self.account+':'+api,result))
                conn.commit()
            raise BudgetStopped(result)

    def check_response(self, api, url, response):
        if response.status_code in (403, 429) or provider_body_restricted(url, response):
            try:
                with self.connect() as conn:
                    conn.execute('SELECT block_api_account(%s,%s)', (self.account, api))
                    conn.execute("INSERT INTO ops_alerts(job_id,kind) VALUES(%s,'api_restricted') ON CONFLICT DO NOTHING",
                                 ('api:'+self.account+':'+api,))
                    conn.commit()
            finally:
                response.close()
            raise BudgetStopped('provider_restricted')

    def request(self, method, url, **kwargs):
        api = next((value for path, value in self.routes.items() if path in url), self.api)
        self.reserve(api)
        kwargs['allow_redirects'] = False
        response = self.session.request(method, url, **kwargs)
        self.check_response(api, url, response)
        return response

    def get(self,url,**kwargs): return self.request('GET',url,**kwargs)
    def post(self,url,**kwargs): return self.request('POST',url,**kwargs)


def provider_body_restricted(url, response):
    host = urlsplit(url).hostname
    if response.status_code != 200 or host not in {
        'api-gateway.coupang.com', 'api.ownerclan.com', 'api-sandbox.ownerclan.com',
    }:
        return False
    try:
        body = response.json()
    except ValueError:
        return False
    if not isinstance(body, dict):
        return False
    if host == 'api-gateway.coupang.com':
        return str(body.get('rCode')) in {'403', '429'}
    errors = body.get('errors')
    return bool(errors) and any(
        marker in str(errors).lower() for marker in ('too many requests', 'rate limit', 'quota')
    )


def shared_budget_required():
    return os.environ.get('COLLECTOR_REQUIRE_SHARED_BUDGET', '').lower() in {'1', 'true'}


def supplier_route(url, body=None):
    endpoint = urlsplit(url)
    host = endpoint.hostname or ''
    if host == 'api-gateway.coupang.com':
        return 'COUPANG', 'best' if '/products/bestcategories/' in endpoint.path else 'default'
    if host == 'api.searchad.naver.com':
        return 'NAVER', 'ad'
    if host == 'naverapihub.apigw.ntruss.com':
        if endpoint.path.startswith('/search-trend/'):
            return 'NAVER', 'trend'
        if endpoint.path.startswith('/shopping/'):
            return 'NAVER', 'shopping'
        raise BudgetStopped('unknown_hub_route')
    if host in {'api.ownerclan.com', 'api-sandbox.ownerclan.com'}:
        return 'OWNERCLAN', 'default'
    if host in {'auth.ownerclan.com', 'auth-sandbox.ownerclan.com'}:
        return 'OWNERCLAN', 'auth'
    if host == 'product.domeggook.com':
        return 'DOMEGGOOK', 'private'
    if host in {'domeggook.com', 'www.domeggook.com', 'domeme.domeggook.com'}:
        form = body.decode('ascii', errors='replace') if isinstance(body, bytes) else body
        modes = parse_qs(endpoint.query).get('mode', []) + parse_qs(form or '').get('mode', [])
        private = any(mode in {'setLogin', 'getAllSupplyChk', 'getItemViewES'} for mode in modes)
        return 'DOMEGGOOK', 'private' if private else 'open'
    return None


class SupplierSession(requests.Session):
    """Standalone CLI/probe transport. Use the shared DB when configured or required."""

    managed_by_operations = False

    def send(self, request, **kwargs):
        dsn = os.environ.get('COLLECTOR_SHARED_BUDGET_DSN')
        if self.managed_by_operations or not (dsn or shared_budget_required()):
            return super().send(request, **kwargs)
        route = supplier_route(request.url, request.body)
        if route is None:
            return super().send(request, **kwargs)
        provider, api = route
        account = os.environ.get(f'COLLECTOR_{provider}_BUDGET_ACCOUNT')
        if not (dsn and account):
            raise BudgetStopped('shared_budget_configuration_missing')
        from psycopg import connect
        from psycopg.rows import dict_row

        parents = {'best': 'default'} if provider == 'COUPANG' else {'trend': 'hub', 'shopping': 'hub'}
        budget = BudgetSession(None, lambda: connect(dsn, row_factory=dict_row, connect_timeout=5),
                               account, api, parents=parents)
        budget.reserve(api)
        kwargs['allow_redirects'] = False
        response = super().send(request, **kwargs)
        budget.check_response(api, request.url, response)
        return response
