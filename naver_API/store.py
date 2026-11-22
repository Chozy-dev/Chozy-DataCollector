"""Immutable daily scope, request checkpoints and atomic publication."""
from datetime import timedelta
from psycopg.types.json import Jsonb
from .services import select_keywords, growth_results


class SeasonStore:
    def __init__(self, connection):
        self.conn = connection

    def begin(self, day, config):
        with self.conn.transaction():
            self.conn.execute("""INSERT INTO naver_season_batches(snapshot_date,scope,period_start,period_end,status)
                VALUES (%s,%s,%s,%s,'running') ON CONFLICT DO NOTHING""",
                (day, Jsonb(config.scope()), day-timedelta(days=56), day-timedelta(days=1)))
            batch = self.conn.execute('SELECT * FROM naver_season_batches WHERE snapshot_date=%s FOR UPDATE', (day,)).fetchone()
            if batch['scope'] != config.scope():
                raise ValueError('Daily scope frozen; resume with original themes')
            if batch['status'] == 'completed':
                return False
            self.conn.execute("UPDATE naver_season_batches SET status='running',error=NULL WHERE snapshot_date=%s", (day,))
            for code, name, month in config.themes:
                self.conn.execute('''INSERT INTO naver_season_themes VALUES (%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING''', (day, code, name, month))
        return True

    def reserve(self, api, config):
        # DB clock (KST), independent of the requested snapshot date and process clock.
        with self.conn.transaction():
            self.conn.execute('SELECT pg_advisory_xact_lock(724639118506)')
            today = self.conn.execute("SELECT (clock_timestamp() AT TIME ZONE 'Asia/Seoul')::date AS day").fetchone()['day']
            limits = [('day', today, config.ad_daily_calls if api == 'ad' else config.trend_daily_calls)]
            if api == 'trend':
                limits.append(('month', today.replace(day=1), config.trend_monthly_calls))
            for kind, start, limit in limits:
                self.conn.execute('''INSERT INTO naver_call_reservations VALUES (%s,%s,%s,0)
                    ON CONFLICT DO NOTHING''', (api, kind, start))
                used = self.conn.execute('''SELECT reserved FROM naver_call_reservations
                    WHERE api=%s AND period_kind=%s AND period_start=%s''', (api, kind, start)).fetchone()['reserved']
                if used >= limit:
                    return False
            for kind, start, _ in limits:
                self.conn.execute('''UPDATE naver_call_reservations SET reserved=reserved+1
                    WHERE api=%s AND period_kind=%s AND period_start=%s''', (api, kind, start))
        return True

    def done(self, day, code, part):
        with self.conn.transaction():
            return self.conn.execute('''SELECT 1 FROM naver_season_requests
                WHERE snapshot_date=%s AND theme_code=%s AND request_part=%s''', (day, code, part)).fetchone() is not None

    def words(self, day, code, part):
        with self.conn.transaction():
            rows = self.conn.execute('''SELECT keyword FROM naver_season_items WHERE snapshot_date=%s AND theme_code=%s
                AND rank BETWEEN %s AND %s ORDER BY rank''', (day, code, (part-1)*5+1, part*5)).fetchall()
        return [r['keyword'] for r in rows]

    def save(self, day, code, part, request, response, observed_at):
        with self.conn.transaction():
            batch = self.conn.execute('SELECT status FROM naver_season_batches WHERE snapshot_date=%s FOR UPDATE', (day,)).fetchone()
            if batch['status'] == 'completed' or self.done(day, code, part):
                return
            if part == 0:
                for r in select_keywords(response):
                    self.conn.execute('''INSERT INTO naver_season_items
                        (snapshot_date,theme_code,keyword,rank,pc_count,mobile_count,monthly_search_count)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)''', (day,code,r['keyword'],r['rank'],r['pc'],r['mobile'],r['total']))
            else:
                words = self.words(day, code, part)
                if len(words) != 5 or request['keywordGroups'] != [{'groupName':w,'keywords':[w]} for w in words]:
                    raise ValueError('Frozen trend group mismatch')
                for word, (rate, status) in growth_results(response, words, day-timedelta(days=56), day-timedelta(days=1)).items():
                    self.conn.execute('''UPDATE naver_season_items SET growth_rate=%s,growth_status=%s
                        WHERE snapshot_date=%s AND theme_code=%s AND keyword=%s''', (rate,status,day,code,word))
            self.conn.execute('INSERT INTO naver_season_requests VALUES (%s,%s,%s,%s,%s,%s)',
                (day,code,part,Jsonb(request),Jsonb(response),observed_at))

    def stop(self, day, status, error=None):
        with self.conn.transaction():
            self.conn.execute("UPDATE naver_season_batches SET status=%s,error=%s WHERE snapshot_date=%s AND status<>'completed'", (status,error,day))

    def publish(self, day):
        with self.conn.transaction():
            batch = self.conn.execute('SELECT * FROM naver_season_batches WHERE snapshot_date=%s FOR UPDATE', (day,)).fetchone()
            if batch['status'] == 'completed':
                return
            themes = self.conn.execute('SELECT theme_code,theme_name,target_month FROM naver_season_themes WHERE snapshot_date=%s ORDER BY theme_code', (day,)).fetchall()
            if sorted([list(r.values()) for r in themes]) != sorted(batch['scope']['themes']):
                raise ValueError('Theme scope mismatch')
            for code, _, _ in batch['scope']['themes']:
                parts = self.conn.execute('SELECT request_part FROM naver_season_requests WHERE snapshot_date=%s AND theme_code=%s', (day,code)).fetchall()
                items = self.conn.execute('SELECT rank,growth_status FROM naver_season_items WHERE snapshot_date=%s AND theme_code=%s', (day,code)).fetchall()
                if {r['request_part'] for r in parts} != {0,1,2} or {r['rank'] for r in items} != set(range(1,11)) or any(r['growth_status'] is None for r in items):
                    raise ValueError('Incomplete theme; publication refused')
            self.conn.execute("UPDATE naver_season_batches SET status='completed',published_at=clock_timestamp(),error=NULL WHERE snapshot_date=%s", (day,))
