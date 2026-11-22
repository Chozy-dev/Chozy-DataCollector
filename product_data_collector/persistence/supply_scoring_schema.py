"""Explicit additive migration only. No API/checkpoint certifies coverage implicitly."""

DDL = (
    """CREATE TABLE IF NOT EXISTS scoring_supplier_samples (
        vendor_key TEXT PRIMARY KEY, as_of TIMESTAMPTZ NOT NULL,
        valid BOOLEAN NOT NULL DEFAULT true, sample JSONB NOT NULL)""",
    """CREATE TABLE IF NOT EXISTS scoring_queue (
        product_id BIGINT PRIMARY KEY REFERENCES products(id),
        platform TEXT NOT NULL,
        generation BIGINT NOT NULL DEFAULT 1, reason TEXT NOT NULL,
        requested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())""",
    "CREATE INDEX IF NOT EXISTS idx_scoring_queue_source ON scoring_queue(platform,product_id)",
    """CREATE TABLE IF NOT EXISTS scoring_supplier_jobs (
        vendor_key TEXT PRIMARY KEY, generation BIGINT NOT NULL DEFAULT 1,
        after_id BIGINT NOT NULL DEFAULT 0)""",
    """CREATE TABLE IF NOT EXISTS scoring_coverage (
        product_id BIGINT NOT NULL REFERENCES products(id),
        metric TEXT NOT NULL CHECK(metric IN ('stockout','price')),
        valid_from TIMESTAMPTZ NOT NULL, valid_until TIMESTAMPTZ NOT NULL,
        evidence JSONB NOT NULL CHECK(evidence <> '{}'::jsonb),
        verified BOOLEAN NOT NULL DEFAULT false,
        PRIMARY KEY(product_id,metric), CHECK(valid_from<valid_until))""",
    """CREATE TABLE IF NOT EXISTS scoring_supplier_identity (
        vendor_key TEXT PRIMARY KEY, valid_from TIMESTAMPTZ NOT NULL,
        valid_until TIMESTAMPTZ NOT NULL, verified BOOLEAN NOT NULL DEFAULT false,
        evidence JSONB NOT NULL CHECK(evidence <> '{}'::jsonb),
        CHECK(valid_from<valid_until))""",
    """CREATE TABLE IF NOT EXISTS product_supply_scores (
        product_id BIGINT PRIMARY KEY REFERENCES products(id),
        platform TEXT NOT NULL, vendor_key TEXT, as_of TIMESTAMPTZ NOT NULL,
        formula_version TEXT NOT NULL, observed_at TIMESTAMPTZ,
        stability NUMERIC CHECK(stability BETWEEN 0 AND 100),
        reliability NUMERIC CHECK(reliability BETWEEN 0 AND 100),
        home NUMERIC CHECK(home BETWEEN 0 AND 100),
        next_due TIMESTAMPTZ, result JSONB NOT NULL, input_fingerprint TEXT NOT NULL,
        payload JSONB NOT NULL)""",
    "CREATE INDEX IF NOT EXISTS idx_scoring_due ON product_supply_scores(next_due,product_id) WHERE next_due IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_scoring_rank ON product_supply_scores(platform,home DESC,product_id)",
    "CREATE INDEX IF NOT EXISTS idx_scoring_vendor ON products((source_specific->>'vendorKey'),id) WHERE platform='ownerclan'",
    """CREATE TABLE IF NOT EXISTS scoring_runs (
        run_id TEXT PRIMARY KEY, mode TEXT NOT NULL CHECK(mode IN ('initial','incremental')),
        platform TEXT NOT NULL CHECK(platform IN ('domeggook','ownerclan')),
        as_of TIMESTAMPTZ NOT NULL, high_id BIGINT NOT NULL, after_id BIGINT NOT NULL DEFAULT 0,
        processed BIGINT NOT NULL DEFAULT 0, scanned BOOLEAN NOT NULL DEFAULT false, status TEXT NOT NULL DEFAULT 'running',
        last_error TEXT, last_error_code TEXT, failed_product_id BIGINT,
        formula_version TEXT NOT NULL, completed_at TIMESTAMPTZ)""",
    """CREATE TABLE IF NOT EXISTS supply_home_batches (
        batch_id TEXT PRIMARY KEY REFERENCES scoring_runs(run_id), platform TEXT NOT NULL,
        as_of TIMESTAMPTZ NOT NULL, formula_version TEXT NOT NULL,
        completed_at TIMESTAMPTZ, item_count BIGINT)""",
    """CREATE TABLE IF NOT EXISTS supply_home_items (
        batch_id TEXT NOT NULL REFERENCES supply_home_batches(batch_id),
        rank BIGINT NOT NULL, product_id BIGINT NOT NULL, payload JSONB NOT NULL,
        PRIMARY KEY(batch_id,rank), UNIQUE(batch_id,product_id))""",
    """CREATE OR REPLACE FUNCTION scoring_enqueue(pid BIGINT, why TEXT) RETURNS VOID
    LANGUAGE SQL AS $$
      INSERT INTO scoring_queue(product_id,platform,reason) SELECT id,platform,why FROM products WHERE id=pid
      ON CONFLICT(product_id) DO UPDATE SET generation=scoring_queue.generation+1,
        reason=EXCLUDED.reason, requested_at=clock_timestamp()
    $$""",
    """CREATE OR REPLACE FUNCTION scoring_vendor(v TEXT) RETURNS VOID LANGUAGE SQL AS $$
      UPDATE scoring_supplier_samples SET valid=false WHERE vendor_key=v;
      INSERT INTO scoring_supplier_jobs(vendor_key) SELECT v WHERE v IS NOT NULL AND v<>''
      ON CONFLICT(vendor_key) DO UPDATE SET generation=scoring_supplier_jobs.generation+1,after_id=0
    $$""",
    """CREATE OR REPLACE FUNCTION scoring_product_input(p products) RETURNS JSONB
    LANGUAGE SQL IMMUTABLE AS $$ SELECT jsonb_build_array(
      p.stock_quantity,p.stock_is_infinite,p.average_shipping_days,p.fast_shipping,
      p.current_supply_price,p.supply_price_basis,p.supply_price_status,p.sellable,
      p.status,p.seller_average_satisfaction,p.seller_review_count,p.seller_type,
      p.seller_excellent_seller,p.source_specific,p.product_name,p.product_url,p.image_url)
    $$""",
    """CREATE OR REPLACE FUNCTION scoring_product_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='UPDATE' AND scoring_product_input(OLD) IS NOT DISTINCT FROM scoring_product_input(NEW) THEN RETURN NEW; END IF;
      PERFORM scoring_enqueue(NEW.id,'product_input');
      IF NEW.platform='ownerclan' THEN
        PERFORM scoring_vendor(NEW.source_specific->>'vendorKey');
        IF TG_OP='UPDATE' THEN PERFORM scoring_vendor(OLD.source_specific->>'vendorKey'); END IF;
      END IF;
      RETURN NEW;
    END $$""",
    """DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='products'::regclass AND tgname='stage7_product_changed') THEN
    CREATE TRIGGER stage7_product_changed AFTER INSERT OR UPDATE ON products FOR EACH ROW EXECUTE FUNCTION scoring_product_changed(); END IF; END $$""",
    """CREATE OR REPLACE FUNCTION scoring_evidence_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    DECLARE pid BIGINT; v TEXT;
    BEGIN
      IF TG_TABLE_NAME='scoring_supplier_identity' THEN
        PERFORM scoring_vendor(NEW.vendor_key); RETURN NEW;
      END IF;
      IF TG_TABLE_NAME='product_events' THEN
        SELECT id INTO pid FROM products WHERE platform=NEW.platform AND external_product_id=NEW.external_product_id;
      ELSE pid := NEW.product_id; END IF;
      PERFORM scoring_enqueue(pid,'evidence');
      SELECT source_specific->>'vendorKey' INTO v FROM products WHERE id=pid AND platform='ownerclan';
      PERFORM scoring_vendor(v);
      RETURN NEW;
    END $$""",
) + tuple(f"""DO $$ BEGIN IF NOT EXISTS(SELECT 1 FROM pg_trigger WHERE tgrelid='{table}'::regclass AND tgname='stage7_evidence_changed') THEN
    CREATE TRIGGER stage7_evidence_changed AFTER INSERT OR UPDATE ON {table} FOR EACH ROW EXECUTE FUNCTION scoring_evidence_changed(); END IF; END $$"""
    for table in ('product_events', 'product_history', 'scoring_coverage', 'scoring_supplier_identity')) + (
    """CREATE OR REPLACE VIEW supply_home_published AS
    SELECT DISTINCT ON (platform) * FROM supply_home_batches WHERE completed_at IS NOT NULL
    ORDER BY platform,as_of DESC,batch_id DESC""",
    """CREATE OR REPLACE FUNCTION read_supply_home(source TEXT, requested_size INTEGER DEFAULT 10)
    RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
    DECLARE platform_key TEXT; answer JSONB;
    BEGIN
      platform_key := CASE source WHEN 'DOMEME' THEN 'domeggook' WHEN 'OWNERCLAN' THEN 'ownerclan' END;
      IF platform_key IS NULL OR requested_size IS NULL OR requested_size<1 OR requested_size>1000 THEN
        RAISE EXCEPTION 'Invalid source or size'; END IF;
      SELECT jsonb_build_object('updatedAt',b.completed_at,'asOf',b.as_of,'batchId',b.batch_id,
        'formulaVersion',b.formula_version,'items',COALESCE((SELECT jsonb_agg(
          i.payload || jsonb_build_object('rank',i.rank) ORDER BY i.rank)
          FROM supply_home_items i WHERE i.batch_id=b.batch_id AND i.rank<=requested_size),'[]'::jsonb))
        INTO answer FROM supply_home_published b WHERE b.platform=platform_key;
      RETURN answer;
    END $$""",
)
