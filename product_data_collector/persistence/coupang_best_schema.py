"""Additive migration only. Collector startup never executes this DDL."""
DDL = (
    """CREATE TABLE IF NOT EXISTS coupang_best_batches (
        snapshot_date DATE PRIMARY KEY,
        scope JSONB NOT NULL,
        status TEXT NOT NULL CHECK (status IN ('running','paused','failed','completed')),
        calls_reserved INTEGER NOT NULL DEFAULT 0 CHECK (calls_reserved >= 0),
        started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        published_at TIMESTAMPTZ,
        error TEXT,
        CHECK ((status = 'completed') = (published_at IS NOT NULL))
    )""",
    """CREATE TABLE IF NOT EXISTS coupang_best_categories (
        snapshot_date DATE NOT NULL REFERENCES coupang_best_batches(snapshot_date),
        category_id BIGINT NOT NULL,
        category_name TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','completed')),
        product_count INTEGER,
        observed_at TIMESTAMPTZ,
        comparison_status TEXT CHECK (comparison_status IN ('available','previous_unavailable')),
        response_metadata JSONB,
        PRIMARY KEY (snapshot_date, category_id),
        CHECK ((status = 'completed') = (product_count IS NOT NULL AND observed_at IS NOT NULL)),
        CHECK (product_count > 0)
    )""",
    """CREATE TABLE IF NOT EXISTS coupang_best_items (
        snapshot_date DATE NOT NULL,
        category_id BIGINT NOT NULL,
        product_id BIGINT NOT NULL CHECK (product_id > 0),
        rank INTEGER NOT NULL CHECK (rank > 0),
        product_name TEXT NOT NULL,
        price NUMERIC(18,2) NOT NULL CHECK (price >= 0),
        is_rocket BOOLEAN NOT NULL,
        product_url TEXT NOT NULL,
        image_url TEXT NOT NULL,
        raw JSONB NOT NULL,
        previous_rank INTEGER,
        rank_change INTEGER,
        rank_status TEXT CHECK (rank_status IN ('UP','DOWN','SAME','NEW')),
        PRIMARY KEY (snapshot_date, category_id, product_id),
        UNIQUE (snapshot_date, category_id, rank),
        FOREIGN KEY (snapshot_date, category_id)
            REFERENCES coupang_best_categories(snapshot_date, category_id)
    )""",
    """CREATE OR REPLACE VIEW coupang_best_published AS
        SELECT i.*, c.category_name, c.observed_at, c.comparison_status, b.published_at
        FROM coupang_best_items i
        JOIN coupang_best_categories c USING (snapshot_date, category_id)
        JOIN coupang_best_batches b USING (snapshot_date)
        WHERE b.status = 'completed' AND c.status = 'completed'""",
)
