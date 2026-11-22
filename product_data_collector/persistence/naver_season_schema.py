"""Naver seasonal keyword and comparison schema; explicit migration only."""
DDL = (
    """CREATE TABLE IF NOT EXISTS naver_season_batches (
        snapshot_date DATE PRIMARY KEY, scope JSONB NOT NULL,
        period_start DATE NOT NULL, period_end DATE NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('running','paused','failed','completed')),
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(), published_at TIMESTAMPTZ, error TEXT,
        CHECK(period_end=period_start+55),
        CHECK((status='completed')=(published_at IS NOT NULL)))""",
    """CREATE TABLE IF NOT EXISTS naver_season_themes (
        snapshot_date DATE NOT NULL REFERENCES naver_season_batches(snapshot_date),
        theme_code INTEGER NOT NULL, theme_name TEXT NOT NULL,
        target_month INTEGER NOT NULL CHECK(target_month BETWEEN 1 AND 12),
        PRIMARY KEY(snapshot_date,theme_code))""",
    """CREATE TABLE IF NOT EXISTS naver_season_requests (
        snapshot_date DATE NOT NULL, theme_code INTEGER NOT NULL,
        request_part INTEGER NOT NULL CHECK(request_part BETWEEN 0 AND 2),
        request JSONB NOT NULL, response JSONB NOT NULL, observed_at TIMESTAMPTZ NOT NULL,
        PRIMARY KEY(snapshot_date,theme_code,request_part),
        FOREIGN KEY(snapshot_date,theme_code) REFERENCES naver_season_themes(snapshot_date,theme_code))""",
    """CREATE TABLE IF NOT EXISTS naver_season_items (
        snapshot_date DATE NOT NULL, theme_code INTEGER NOT NULL,
        keyword TEXT NOT NULL CHECK(length(keyword)>0), rank INTEGER NOT NULL CHECK(rank BETWEEN 1 AND 10),
        pc_count BIGINT NOT NULL CHECK(pc_count>=0), mobile_count BIGINT NOT NULL CHECK(mobile_count>=0),
        monthly_search_count BIGINT NOT NULL CHECK(monthly_search_count=pc_count+mobile_count),
        growth_rate DOUBLE PRECISION, growth_status TEXT,
        PRIMARY KEY(snapshot_date,theme_code,keyword), UNIQUE(snapshot_date,theme_code,rank),
        FOREIGN KEY(snapshot_date,theme_code) REFERENCES naver_season_themes(snapshot_date,theme_code),
        CHECK(growth_status IN ('available','missing_group','invalid_dates_or_values','zero_baseline')),
        CHECK((growth_rate IS NOT NULL)=(growth_status IS NOT DISTINCT FROM 'available')))""",
    """CREATE TABLE IF NOT EXISTS naver_call_reservations (
        api TEXT NOT NULL CHECK(api IN ('ad','trend')), period_kind TEXT NOT NULL CHECK(period_kind IN ('day','month')),
        period_start DATE NOT NULL, reserved INTEGER NOT NULL CHECK(reserved>=0),
        PRIMARY KEY(api,period_kind,period_start))""",
    """CREATE OR REPLACE VIEW naver_season_published AS
        SELECT i.*,t.theme_name,t.target_month,b.period_start,b.period_end,b.published_at
        FROM naver_season_items i JOIN naver_season_themes t USING(snapshot_date,theme_code)
        JOIN naver_season_batches b USING(snapshot_date) WHERE b.status='completed'""",
)
