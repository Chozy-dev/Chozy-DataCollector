"""Additive history policy repair after scoring and optional operations migrations."""

DDL = (
    """CREATE OR REPLACE FUNCTION scoring_observation_ready() RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
      IF NEW.current_observed_at IS DISTINCT FROM OLD.current_observed_at
        AND NEW.current_observed_at > OLD.current_observed_at
        AND NEW.current_supply_price IS NOT NULL
        AND NEW.supply_price_basis IN ('minimum_quantity_supply','uniform_option_final')
        AND EXISTS (SELECT 1 FROM product_supply_scores s JOIN scoring_coverage c
          ON c.product_id=s.product_id AND c.metric='price' AND c.verified
          WHERE s.product_id=NEW.id
          AND s.result->'history'->>'price_stream'='observed_supply_v1'
          AND s.result->'history'->>'price_count' IS NULL
          AND COALESCE((s.result->'history'->>'waiting_for_price_observation')::boolean,true)
          AND s.result->'history'->'reasons'->>'price'='fewer_than_two_verified_prices'
          AND c.evidence->>'stream'='observed_supply_v1'
          AND c.valid_from<=NEW.current_observed_at-interval '90 days'
          AND c.valid_until>NEW.current_observed_at
          AND EXISTS(SELECT 1 FROM product_history h WHERE h.product_id=NEW.id
            AND h.observed_at<=s.as_of-interval '90 days'
            AND h.observed_at<NEW.current_observed_at)) THEN
        PERFORM scoring_enqueue(NEW.id,'observed_price_readiness');
      END IF;
      RETURN NEW;
    END $$""",
    """CREATE TRIGGER scoring_observation_ready AFTER UPDATE ON products
      FOR EACH ROW EXECUTE FUNCTION scoring_observation_ready()""",
    # Repair only previously calculated history inputs, through the existing queue.
    # Invalidate supplier caches because eligible peer stockout counts may change.
    """SELECT scoring_enqueue(s.product_id,'history_policy_v2') FROM product_supply_scores s
      WHERE s.formula_version='supply_v1' AND (
        EXISTS(SELECT 1 FROM scoring_coverage c WHERE c.product_id=s.product_id
          AND c.verified AND c.metric='stockout')
        OR s.result->'history'->>'price_stream'='observed_supply_v1')""",
    """SELECT scoring_vendor(v) FROM (SELECT DISTINCT p.source_specific->>'vendorKey' AS v
      FROM products p JOIN scoring_queue q ON q.product_id=p.id
      WHERE p.platform='ownerclan' AND q.reason='history_policy_v2') vendors""",
)
