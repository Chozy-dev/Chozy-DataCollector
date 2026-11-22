"""Bounded status reads; large aggregate scans are opt-in SQL in the runbook."""
def report(conn,config):
    queries={
        'jobs':"SELECT *,extract(epoch FROM (COALESCE(completed_at,clock_timestamp())-started_at)) AS duration_seconds FROM ops_jobs ORDER BY scheduled_at DESC LIMIT 100",
        'alerts':"SELECT * FROM ops_alerts WHERE acknowledged_at IS NULL ORDER BY id DESC LIMIT 100",
        'publication':"SELECT * FROM supply_home_published",
        'scoring':"SELECT * FROM scoring_runs ORDER BY as_of DESC LIMIT 20",
        'budgets':"SELECT * FROM api_budget_policy",
        'usage':"SELECT * FROM api_budget_usage ORDER BY period_start DESC LIMIT 100",
        'db':"SELECT numbackends,xact_commit,xact_rollback,tup_inserted,tup_updated,deadlocks,temp_bytes FROM pg_stat_database WHERE datname=current_database()",
        'size':"SELECT pg_database_size(current_database()) AS database_bytes,pg_current_wal_lsn() AS wal_lsn",
    }
    result={k:conn.execute(q).fetchall() for k,q in queries.items()}
    if config.get('status_full',False):
        # Explicit opt-in: these aggregates scan large operational tables.
        result['details']=conn.execute("""SELECT platform,count(*) AS targets,
          count(*) FILTER(WHERE requested_version>completed_version) AS pending,
          count(*) FILTER(WHERE failure_count>0) AS failed,
          sum(failure_count) AS failure_attempts FROM product_discovery_targets GROUP BY platform""").fetchall()
        result['coverage']=conn.execute("""SELECT metric,count(*) AS certificates,
          count(*) FILTER(WHERE verified AND valid_from<=clock_timestamp()-interval '90 days'
            AND valid_until>clock_timestamp()) AS valid_90d FROM scoring_coverage GROUP BY metric""").fetchall()
        result['scores']=conn.execute("""SELECT platform,count(*) AS scored,
          count(*) FILTER(WHERE home IS NULL) AS missing,
          count(*) FILTER(WHERE next_due<=clock_timestamp()) AS expired FROM product_supply_scores GROUP BY platform""").fetchall()
        result['queue']=conn.execute('SELECT platform,count(*) AS remaining FROM scoring_queue GROUP BY platform').fetchall()
        result['observed_changes']=conn.execute("""SELECT platform,count(*) AS products,
          count(*) FILTER(WHERE first_seen_at>clock_timestamp()-interval '1 day') AS new_24h,
          count(*) FILTER(WHERE last_collected_at>clock_timestamp()-interval '1 day') AS observed_24h
          FROM products GROUP BY platform""").fetchall()
        result['history']=conn.execute("""SELECT count(*) AS changed_snapshots_24h,count(DISTINCT product_id) AS changed_products_24h
          FROM product_history WHERE observed_at>clock_timestamp()-interval '1 day'""").fetchall()
        result['events']=conn.execute("SELECT platform,count(*) AS durable_events FROM product_events GROUP BY platform").fetchall()
    lag=config.get('alert_lag_hours',36)
    conn.execute("""INSERT INTO ops_alerts(job_id,kind)
      SELECT task,'collection_delay' FROM ops_jobs GROUP BY task
      HAVING max(completed_at) IS NULL OR max(completed_at)<clock_timestamp()-%s*interval '1 hour'
      ON CONFLICT DO NOTHING""",(lag,))
    return result
