"""Immutable daily observations with a DB checkpoint in the same transaction."""
from datetime import timedelta

from psycopg.types.json import Jsonb

from coupang_API.services.best import rank_movement


class BestStore:
    def __init__(self, connection):
        self.conn = connection

    def begin(self, day, config):
        with self.conn.transaction():
            self.conn.execute("""INSERT INTO coupang_best_batches(snapshot_date,scope,status)
                VALUES (%s,%s,'running') ON CONFLICT DO NOTHING""", (day, Jsonb(config.scope())))
            batch = self.conn.execute("SELECT * FROM coupang_best_batches WHERE snapshot_date=%s FOR UPDATE", (day,)).fetchone()
            if batch["scope"] != config.scope():
                raise ValueError("Daily category/limit scope is frozen; resume with the original scope")
            if batch["status"] == "completed":
                return False
            self.conn.execute("UPDATE coupang_best_batches SET status='running',error=NULL WHERE snapshot_date=%s", (day,))
            for category_id, name in config.categories:
                self.conn.execute("""INSERT INTO coupang_best_categories(snapshot_date,category_id,category_name)
                    VALUES (%s,%s,%s) ON CONFLICT DO NOTHING""", (day, category_id, name))
        return True

    def pending(self, day):
        with self.conn.transaction():
            return self.conn.execute("""SELECT category_id FROM coupang_best_categories
                WHERE snapshot_date=%s AND status='pending' ORDER BY category_id""", (day,)).fetchall()

    def reserve(self, day, max_calls):
        # Commit BEFORE HTTP; a crash conservatively consumes this reservation.
        with self.conn.transaction():
            return self.conn.execute("""UPDATE coupang_best_batches SET calls_reserved=calls_reserved+1
                WHERE snapshot_date=%s AND status='running' AND calls_reserved < %s RETURNING calls_reserved""",
                (day, max_calls)).fetchone() is not None

    def save_category(self, day, category_id, records, payload, observed_at):
        with self.conn.transaction():
            category = self.conn.execute("""SELECT * FROM coupang_best_categories
                WHERE snapshot_date=%s AND category_id=%s FOR UPDATE""", (day, category_id)).fetchone()
            if category is None:
                raise ValueError("Category outside frozen scope")
            if category["status"] == "completed":
                return
            if not records:
                raise ValueError("Empty category cannot complete")
            for record in records:
                self.conn.execute("""INSERT INTO coupang_best_items
                    (snapshot_date,category_id,product_id,rank,product_name,price,is_rocket,product_url,image_url,raw)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (day, category_id, record["product_id"], record["rank"], record["product_name"],
                     record["price"], record["is_rocket"], record["product_url"], record["image_url"], Jsonb(record["raw"])))
            self.conn.execute("""UPDATE coupang_best_categories SET status='completed',product_count=%s,
                observed_at=%s,response_metadata=%s WHERE snapshot_date=%s AND category_id=%s""",
                (len(records), observed_at, Jsonb({k:v for k,v in payload.items() if k != "data"}), day, category_id))

    def stop(self, day, status, error=None):
        with self.conn.transaction():
            self.conn.execute("""UPDATE coupang_best_batches SET status=%s,error=%s
                WHERE snapshot_date=%s AND status <> 'completed'""", (status, error, day))

    def publish(self, day):
        with self.conn.transaction():
            # Serialize adjacent dates' baseline selection and publication.
            self.conn.execute("SELECT pg_advisory_xact_lock(724639118505)")
            batch = self.conn.execute("SELECT * FROM coupang_best_batches WHERE snapshot_date=%s FOR UPDATE", (day,)).fetchone()
            if batch["status"] == "completed":
                return
            categories = self.conn.execute("SELECT * FROM coupang_best_categories WHERE snapshot_date=%s", (day,)).fetchall()
            if {c["category_id"] for c in categories} != {c[0] for c in batch["scope"]["categories"]}:
                raise ValueError("Category scope mismatch")
            for category in categories:
                cid = category["category_id"]
                items = self.conn.execute("SELECT * FROM coupang_best_items WHERE snapshot_date=%s AND category_id=%s", (day,cid)).fetchall()
                if category["status"] != "completed" or not items or len(items) != category["product_count"]:
                    raise ValueError("Incomplete category; publication refused")
                baseline = self.conn.execute("""SELECT c.category_id FROM coupang_best_categories c
                    JOIN coupang_best_batches b USING(snapshot_date)
                    WHERE c.snapshot_date=%s AND c.category_id=%s AND c.status='completed' AND b.status='completed'""",
                    (day-timedelta(days=1),cid)).fetchone()
                previous = {}
                if baseline:
                    previous = {r["product_id"]:r["rank"] for r in self.conn.execute(
                        "SELECT product_id,rank FROM coupang_best_items WHERE snapshot_date=%s AND category_id=%s",
                        (day-timedelta(days=1),cid)).fetchall()}
                for item in items:
                    prev = previous.get(item["product_id"])
                    change, status = rank_movement(item["rank"],prev,bool(baseline))
                    self.conn.execute("""UPDATE coupang_best_items SET previous_rank=%s,rank_change=%s,rank_status=%s
                        WHERE snapshot_date=%s AND category_id=%s AND product_id=%s""", (prev,change,status,day,cid,item["product_id"]))
                self.conn.execute("""UPDATE coupang_best_categories SET comparison_status=%s
                    WHERE snapshot_date=%s AND category_id=%s""", ('available' if baseline else 'previous_unavailable',day,cid))
            self.conn.execute("UPDATE coupang_best_batches SET status='completed',published_at=clock_timestamp(),error=NULL WHERE snapshot_date=%s", (day,))


def read_published(connection, category_id):
    """One SQL statement selects one entire latest batch, including category list.

    Missing requested category returns an empty items array, never an older category
    mixed into the latest batch. Platform owns category validation/default selection.
    """
    with connection.transaction():
        row = connection.execute("""WITH latest AS (
            SELECT snapshot_date,published_at FROM coupang_best_batches
            WHERE status='completed' ORDER BY snapshot_date DESC LIMIT 1)
            SELECT l.*, (SELECT jsonb_agg(jsonb_build_object('categoryId',category_id,'categoryName',category_name)
                ORDER BY category_id) FROM coupang_best_categories WHERE snapshot_date=l.snapshot_date) AS categories,
            COALESCE((SELECT jsonb_agg(jsonb_build_object('rank',rank,'productId',product_id,
                'productName',product_name,'price',price,'isRocketDelivery',is_rocket,
                'previousRank',previous_rank,'rankChange',rank_change,'rankStatus',rank_status,
                'productUrl',product_url,'productImage',image_url,'comparisonStatus',comparison_status)
                ORDER BY rank) FROM coupang_best_published WHERE snapshot_date=l.snapshot_date AND category_id=%s),
                '[]'::jsonb) AS items FROM latest l""", (category_id,)).fetchone()
    if row is None:
        return None
    return {"updatedAt":row["published_at"], "snapshotDate":row["snapshot_date"],
            "categories":row["categories"], "selectedCategoryId":category_id, "items":row["items"]}
