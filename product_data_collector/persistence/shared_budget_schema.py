"""Extend existing budget functions without dropping usage, grants, or owners."""


def replace_budget_function(signature, definition):
    # CREATE OR REPLACE keeps ownership and ACLs but resets function attributes.
    return f"""DO $migration$
    DECLARE namespace text := current_schema();
            qualified_signature text := format('%I.{signature}', namespace);
            was_definer boolean;
    BEGIN
      SELECT prosecdef INTO STRICT was_definer FROM pg_proc
        WHERE oid = to_regprocedure(qualified_signature);
      EXECUTE $definition${definition}$definition$;
      IF was_definer THEN
        EXECUTE 'ALTER FUNCTION ' || qualified_signature || ' SECURITY DEFINER';
      END IF;
      EXECUTE format('ALTER FUNCTION %s SET search_path TO pg_catalog,%I,pg_temp',
                     qualified_signature, namespace);
    END
    $migration$"""


DDL = (
    """ALTER TABLE api_budget_policy ADD COLUMN shared_api TEXT,
      ADD CONSTRAINT api_budget_distinct_parent CHECK (api <> shared_api),
      ADD CONSTRAINT api_budget_parent FOREIGN KEY (account, shared_api)
        REFERENCES api_budget_policy(account, api) DEFERRABLE INITIALLY DEFERRED""",
    replace_budget_function(
        "reserve_api_call(text,text,boolean)",
        """CREATE OR REPLACE FUNCTION reserve_api_call(acct TEXT, endpoint TEXT, shared_required BOOLEAN DEFAULT false)
        RETURNS TEXT LANGUAGE plpgsql AS $function$
        DECLARE leaf api_budget_policy; bucket api_budget_policy;
                routes text[]; t timestamptz; k text; n bigint; period timestamptz;
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtextextended('chozy-api-budget:' || acct, 0));
          SELECT * INTO leaf FROM api_budget_policy WHERE account=acct AND api=endpoint FOR UPDATE;
          IF NOT FOUND THEN RETURN 'unconfigured'; END IF;
          routes := array_remove(ARRAY[endpoint, leaf.shared_api], NULL);
          t := clock_timestamp();
          FOR bucket IN SELECT * FROM api_budget_policy WHERE account=acct AND api=ANY(routes) FOR UPDATE LOOP
            IF bucket.api <> endpoint AND bucket.shared_api IS NOT NULL THEN RETURN 'invalid_shared_policy'; END IF;
            IF shared_required AND NOT bucket.platform_integrated THEN RETURN 'platform_unintegrated'; END IF;
            IF bucket.blocked THEN RETURN 'blocked'; END IF;
            IF bucket.limits = '{}'::jsonb THEN RETURN 'unconfigured'; END IF;
            FOR k,n IN SELECT key,value::text::bigint FROM jsonb_each(bucket.limits) LOOP
              IF k NOT IN ('second','minute','hour','day','month') OR n<1 THEN
                RAISE EXCEPTION 'Invalid budget';
              END IF;
              period := date_trunc(k,t AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul';
              IF COALESCE((SELECT reserved FROM api_budget_usage WHERE account=acct AND api=bucket.api
                           AND kind=k AND period_start=period), 0) >= n THEN
                RETURN 'exhausted:' || k;
              END IF;
            END LOOP;
            IF bucket.last_reserved_at IS NOT NULL
               AND t < bucket.last_reserved_at + bucket.min_interval * interval '1 second' THEN
              RETURN 'interval';
            END IF;
          END LOOP;
          FOR bucket IN SELECT * FROM api_budget_policy WHERE account=acct AND api=ANY(routes) LOOP
            FOR k IN SELECT jsonb_object_keys(bucket.limits) LOOP
              period := date_trunc(k,t AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul';
              INSERT INTO api_budget_usage VALUES(acct,bucket.api,k,period,1)
              ON CONFLICT(account,api,kind,period_start) DO UPDATE SET reserved=api_budget_usage.reserved+1;
            END LOOP;
          END LOOP;
          UPDATE api_budget_policy SET last_reserved_at=t WHERE account=acct AND api=ANY(routes);
          RETURN 'reserved';
        END
        $function$""",
    ),
    replace_budget_function(
        "block_api_account(text,text)",
        """CREATE OR REPLACE FUNCTION block_api_account(acct TEXT, endpoint TEXT)
        RETURNS VOID LANGUAGE plpgsql AS $function$
        DECLARE parent text;
        BEGIN
          PERFORM pg_advisory_xact_lock(hashtextextended('chozy-api-budget:' || acct, 0));
          SELECT shared_api INTO parent FROM api_budget_policy WHERE account=acct AND api=endpoint;
          UPDATE api_budget_policy SET blocked=true
            WHERE account=acct AND api=coalesce(parent,endpoint);
        END
        $function$""",
    ),
)
