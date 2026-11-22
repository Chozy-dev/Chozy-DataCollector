"""Scoring invalidation trigger refinement; no table/index/data removal."""
DDL = (
    """CREATE OR REPLACE FUNCTION scoring_product_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
      IF TG_OP='UPDATE' AND scoring_product_input(OLD) IS NOT DISTINCT FROM scoring_product_input(NEW) THEN RETURN NEW; END IF;
      PERFORM scoring_enqueue(NEW.id,'product_input');
      IF NEW.platform='ownerclan' THEN
        IF TG_OP='INSERT' THEN
          PERFORM scoring_vendor(NEW.source_specific->>'vendorKey');
        ELSIF OLD.source_specific->>'vendorKey' IS DISTINCT FROM NEW.source_specific->>'vendorKey' THEN
          PERFORM scoring_vendor(OLD.source_specific->>'vendorKey');
          PERFORM scoring_vendor(NEW.source_specific->>'vendorKey');
        END IF;
      END IF;
      RETURN NEW;
    END $$""",
    """CREATE OR REPLACE FUNCTION scoring_evidence_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    DECLARE pid BIGINT; v TEXT; propagate BOOLEAN := false;
    BEGIN
      IF TG_TABLE_NAME='scoring_supplier_identity' THEN
        IF TG_OP<>'INSERT' THEN PERFORM scoring_vendor(OLD.vendor_key); END IF;
        IF TG_OP<>'DELETE' THEN PERFORM scoring_vendor(NEW.vendor_key); END IF;
      ELSE
        IF TG_TABLE_NAME='product_events' THEN
          IF TG_OP='DELETE' THEN
            SELECT id INTO pid FROM products WHERE platform=OLD.platform AND external_product_id=OLD.external_product_id;
          ELSE
            SELECT id INTO pid FROM products WHERE platform=NEW.platform AND external_product_id=NEW.external_product_id;
          END IF;
          IF TG_OP<>'INSERT' THEN propagate := OLD.event_origin='source' AND OLD.event_scope='product' AND OLD.semantic IN ('soldout','restocked'); END IF;
          IF TG_OP<>'DELETE' THEN propagate := propagate OR (NEW.event_origin='source' AND NEW.event_scope='product' AND NEW.semantic IN ('soldout','restocked')); END IF;
        ELSE
          IF TG_OP='DELETE' THEN pid:=OLD.product_id; ELSE pid:=NEW.product_id; END IF;
          IF TG_TABLE_NAME='scoring_coverage' THEN
            IF TG_OP<>'INSERT' THEN propagate:=OLD.metric='stockout'; END IF;
            IF TG_OP<>'DELETE' THEN propagate:=propagate OR NEW.metric='stockout'; END IF;
          ELSE
            -- Any soldout observation can affect the 90-day experience EXISTS.
            IF TG_OP<>'INSERT' THEN propagate:=OLD.status='soldout'; END IF;
            IF TG_OP<>'DELETE' THEN propagate:=propagate OR NEW.status='soldout'; END IF;
          END IF;
        END IF;
        PERFORM scoring_enqueue(pid,'evidence');
        IF propagate THEN
          SELECT source_specific->>'vendorKey' INTO v FROM products WHERE id=pid AND platform='ownerclan';
          PERFORM scoring_vendor(v);
        END IF;
      END IF;
      IF TG_OP='DELETE' THEN RETURN OLD; ELSE RETURN NEW; END IF;
    END $$""",
) + tuple(f"CREATE OR REPLACE TRIGGER stage7_evidence_changed AFTER INSERT OR UPDATE OR DELETE ON {table} FOR EACH ROW EXECUTE FUNCTION scoring_evidence_changed()" for table in ('product_events','product_history','scoring_coverage','scoring_supplier_identity')) + (
    """CREATE OR REPLACE FUNCTION scoring_membership_deleted() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN IF OLD.platform='ownerclan' THEN PERFORM scoring_vendor(OLD.source_specific->>'vendorKey'); END IF; RETURN OLD; END $$""",
    "CREATE TRIGGER stage10_membership_deleted AFTER DELETE ON products FOR EACH ROW EXECUTE FUNCTION scoring_membership_deleted()",
)
