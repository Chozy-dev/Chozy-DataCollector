"""Administrator configuration for shared database call quotas."""
import math

from psycopg.types.json import Jsonb


def install_policies(conn, policies):
    names = set()
    for policy in policies:
        key = policy['account'], policy['api']
        limits = policy['limits']
        interval = policy.get('min_interval', 0)
        shared_api = policy.get('shared_api')
        if not all(isinstance(name, str) and name.strip() for name in key) or key in names:
            raise ValueError('Budget account and API must be unique nonblank names')
        if not isinstance(limits, dict) or not limits or any(
            period not in {'second', 'minute', 'hour', 'day', 'month'}
            or type(value) is not int or not 1 <= value <= 2**63 - 1
            for period, value in limits.items()
        ):
            raise ValueError('Invalid budget limits')
        if type(interval) not in (int, float) or not math.isfinite(interval) or interval < 0:
            raise ValueError('Budget min_interval must be finite and nonnegative')
        if shared_api is not None and (not isinstance(shared_api, str) or not shared_api.strip()
                                      or shared_api == policy['api']):
            raise ValueError('Invalid shared_api')
        if type(policy.get('platform_integrated', False)) is not bool:
            raise ValueError('platform_integrated must be boolean')
        names.add(key)

    accounts = sorted({account for account, _ in names})
    for account in accounts:
        conn.execute("""SELECT pg_advisory_xact_lock(
            hashtextextended('chozy-api-budget:' || %s, 0))""", (account,))
    for policy in policies:
        conn.execute("""INSERT INTO api_budget_policy(account,api,limits,min_interval,platform_integrated,shared_api)
            VALUES(%s,%s,%s,%s,%s,%s) ON CONFLICT(account,api) DO UPDATE SET
            limits=EXCLUDED.limits,min_interval=EXCLUDED.min_interval,
            platform_integrated=EXCLUDED.platform_integrated,shared_api=EXCLUDED.shared_api""",
            (policy['account'], policy['api'], Jsonb(policy['limits']), policy.get('min_interval', 0),
             policy.get('platform_integrated', False), policy.get('shared_api')))
    if conn.execute("""SELECT 1 FROM api_budget_policy leaf
        LEFT JOIN api_budget_policy parent ON (parent.account,parent.api)=(leaf.account,leaf.shared_api)
        WHERE leaf.account=ANY(%s) AND leaf.shared_api IS NOT NULL
          AND (parent.api IS NULL OR parent.shared_api IS NOT NULL)
        LIMIT 1""", (accounts,)).fetchone():
        raise ValueError('shared_api must name a root budget in the same account')
