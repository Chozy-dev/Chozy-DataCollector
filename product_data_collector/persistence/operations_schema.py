"""Additive operations ledger, input fences, and shared account budget contract."""
DDL = (
    "CREATE INDEX IF NOT EXISTS idx_ops_observation ON products(platform,last_collected_at DESC)",
    "CREATE SEQUENCE IF NOT EXISTS ops_domeggook_revision",
    "CREATE SEQUENCE IF NOT EXISTS ops_ownerclan_revision",
    """CREATE TABLE IF NOT EXISTS ops_collection_receipts (
        platform TEXT PRIMARY KEY, revision BIGINT NOT NULL, job_id TEXT NOT NULL,
        completed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())""",
    """CREATE TABLE IF NOT EXISTS ops_score_inputs (
        run_id TEXT PRIMARY KEY, platform TEXT NOT NULL, revision BIGINT NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS ops_jobs (
        job_id TEXT PRIMARY KEY, task TEXT NOT NULL, scheduled_at TIMESTAMPTZ NOT NULL,
        scope JSONB NOT NULL, status TEXT NOT NULL DEFAULT 'paused'
        CHECK(status IN ('running','completed','failed','paused')),
        attempts INT NOT NULL DEFAULT 0, step INT NOT NULL DEFAULT 0,
        started_at TIMESTAMPTZ, updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        completed_at TIMESTAMPTZ, superseded_by TEXT, error TEXT, metrics JSONB NOT NULL DEFAULT '{}')""",
    "CREATE INDEX IF NOT EXISTS idx_ops_jobs_pending ON ops_jobs(task,scheduled_at) WHERE status<>'completed'",
    """CREATE TABLE IF NOT EXISTS ops_alerts (
        id BIGSERIAL PRIMARY KEY, job_id TEXT, kind TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(), acknowledged_at TIMESTAMPTZ,
        UNIQUE(job_id,kind))""",
    """CREATE TABLE IF NOT EXISTS api_budget_policy (
        account TEXT NOT NULL, api TEXT NOT NULL, limits JSONB NOT NULL,
        min_interval DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK(min_interval>=0),
        platform_integrated BOOLEAN NOT NULL DEFAULT false,
        blocked BOOLEAN NOT NULL DEFAULT false, last_reserved_at TIMESTAMPTZ,
        PRIMARY KEY(account,api))""",
    """CREATE TABLE IF NOT EXISTS api_budget_usage (
        account TEXT NOT NULL, api TEXT NOT NULL, kind TEXT NOT NULL,
        period_start TIMESTAMPTZ NOT NULL, reserved BIGINT NOT NULL DEFAULT 0,
        PRIMARY KEY(account,api,kind,period_start))""",
    """CREATE OR REPLACE FUNCTION reserve_api_call(acct TEXT, endpoint TEXT, shared_required BOOLEAN DEFAULT false)
    RETURNS TEXT LANGUAGE plpgsql AS $$
    DECLARE p api_budget_policy; t TIMESTAMPTZ; k TEXT; n BIGINT; b TIMESTAMPTZ;
    BEGIN
      SELECT * INTO p FROM api_budget_policy WHERE account=acct AND api=endpoint FOR UPDATE;
      IF NOT FOUND THEN RETURN 'unconfigured'; END IF;
      IF shared_required AND NOT p.platform_integrated THEN RETURN 'platform_unintegrated'; END IF;
      IF p.blocked THEN RETURN 'blocked'; END IF;
      t := clock_timestamp();
      IF p.last_reserved_at IS NOT NULL AND t < p.last_reserved_at + p.min_interval * interval '1 second'
        THEN RETURN 'interval'; END IF;
      FOR k,n IN SELECT key,value::text::bigint FROM jsonb_each(p.limits) LOOP
        IF k NOT IN ('second','minute','day','month') OR n<1 THEN RAISE EXCEPTION 'Invalid budget'; END IF;
        b := date_trunc(k,t AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul';
        IF COALESCE((SELECT reserved FROM api_budget_usage WHERE account=acct AND api=endpoint
          AND kind=k AND period_start=b),0)>=n THEN RETURN 'exhausted:' || k; END IF;
      END LOOP;
      IF p.limits='{}'::jsonb THEN RETURN 'unconfigured'; END IF;
      FOR k IN SELECT jsonb_object_keys(p.limits) LOOP
        b := date_trunc(k,t AT TIME ZONE 'Asia/Seoul') AT TIME ZONE 'Asia/Seoul';
        INSERT INTO api_budget_usage VALUES(acct,endpoint,k,b,1)
        ON CONFLICT(account,api,kind,period_start) DO UPDATE SET reserved=api_budget_usage.reserved+1;
      END LOOP;
      UPDATE api_budget_policy SET last_reserved_at=t WHERE account=acct AND api=endpoint;
      RETURN 'reserved';
    END $$""",
    """CREATE OR REPLACE FUNCTION ops_input_write() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    DECLARE s TEXT; pid BIGINT; sources TEXT[];
    BEGIN
      IF TG_OP='DELETE' THEN RAISE EXCEPTION 'Input retention requires reviewed maintenance'; END IF;
      IF TG_TABLE_NAME IN ('products','product_events','product_discovery_targets') THEN
        IF TG_OP='DELETE' THEN s:=OLD.platform; ELSE s:=NEW.platform; END IF;
        IF TG_OP='UPDATE' AND OLD.platform<>NEW.platform THEN
          RAISE EXCEPTION 'Changing product source is forbidden'; END IF;
      ELSIF TG_TABLE_NAME IN ('product_history','product_options','scoring_coverage') THEN
        IF TG_OP='UPDATE' AND OLD.product_id<>NEW.product_id THEN
          RAISE EXCEPTION 'Changing input parent is forbidden'; END IF;
        IF TG_OP='DELETE' THEN pid:=OLD.product_id; ELSE pid:=NEW.product_id; END IF;
        SELECT platform INTO s FROM products WHERE id=pid;
      END IF;
      sources:=CASE WHEN s IS NULL THEN ARRAY['domeggook','ownerclan'] ELSE ARRAY[s] END;
      FOREACH s IN ARRAY sources LOOP
        IF s IN ('domeggook','ownerclan') THEN
          IF NOT pg_try_advisory_xact_lock_shared(CASE s WHEN 'domeggook' THEN 724639118801 ELSE 724639118802 END)
            THEN RAISE EXCEPTION 'Scoring input fenced: %',s USING ERRCODE='55P03'; END IF;
          PERFORM nextval('ops_' || s || '_revision');
        END IF;
      END LOOP;
      IF TG_OP='DELETE' THEN RETURN OLD; END IF;
      RETURN NEW;
    END $$""",
)
DDL += ("""CREATE OR REPLACE FUNCTION ops_no_truncate() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN RAISE EXCEPTION 'Protected input cannot be truncated'; END $$""",)
for table in ('products','product_events','product_history','product_options',
              'product_discovery_targets','scoring_coverage','scoring_supplier_identity'):
    DDL += (f"CREATE OR REPLACE TRIGGER ops_input_fence BEFORE INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION ops_input_write()",)
    DDL += (f"CREATE OR REPLACE TRIGGER ops_truncate_fence BEFORE TRUNCATE ON {table} FOR EACH STATEMENT EXECUTE FUNCTION ops_no_truncate()",)
