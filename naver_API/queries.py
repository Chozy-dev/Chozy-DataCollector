"""PostgreSQL read contract for Platform; one statement per response.

Default theme selection, HTTP envelopes, serialization and user logic belong to
Platform. Missing publication returns None; unknown theme returns empty items.
"""


def read_published(connection, theme_code, *, compare=False):
    comparison = """, cb AS (
        SELECT b.snapshot_date,b.published_at FROM coupang_best_batches b JOIN latest l USING(snapshot_date)
        WHERE b.status='completed' AND EXISTS (SELECT 1 FROM coupang_best_published p WHERE p.snapshot_date=b.snapshot_date))""" if compare else ''
    count = """,'matchedProductCount',CASE WHEN EXISTS(SELECT 1 FROM cb) THEN
        (SELECT count(DISTINCT p.product_id) FROM coupang_best_published p
         WHERE p.snapshot_date=i.snapshot_date AND position(
            regexp_replace(i.keyword,'[[:space:]]','','g') IN
            regexp_replace(p.product_name,'[[:space:]]','','g'))>0) ELSE NULL END""" if compare else ''
    meta = """,(SELECT snapshot_date FROM cb) AS coupang_date,
        (SELECT published_at FROM cb) AS coupang_published_at,
        COALESCE((SELECT jsonb_agg(jsonb_build_object('categoryId',category_id,'categoryName',category_name) ORDER BY category_id)
         FROM coupang_best_categories c WHERE c.snapshot_date=l.snapshot_date AND c.status='completed'
         AND EXISTS(SELECT 1 FROM cb)),'[]'::jsonb) AS categories""" if compare else ''
    with connection.transaction():
        row = connection.execute("""WITH latest AS (
            SELECT snapshot_date,published_at,period_start,period_end FROM naver_season_batches
            WHERE status='completed' ORDER BY snapshot_date DESC LIMIT 1)""" + comparison + """
            SELECT l.*,
              (SELECT jsonb_agg(jsonb_build_object('themeCode',theme_code,'themeName',theme_name,'targetMonth',target_month)
                  ORDER BY theme_code) FROM naver_season_themes WHERE snapshot_date=l.snapshot_date) AS themes,
              COALESCE((SELECT jsonb_agg(jsonb_build_object('rank',rank,'keyword',keyword,
                'monthlySearchCount',monthly_search_count,'growthRate',growth_rate,'growthStatus',growth_status""" + count + """) ORDER BY rank)
                FROM naver_season_published i WHERE i.snapshot_date=l.snapshot_date AND theme_code=%s),'[]'::jsonb) AS items
              """ + meta + ' FROM latest l', (theme_code,)).fetchone()
    if row is None:
        return None
    result = {'updatedAt':row['published_at'],'snapshotDate':row['snapshot_date'],
              'periodStart':row['period_start'],'periodEnd':row['period_end'],
              'themes':row['themes'],'selectedThemeCode':theme_code,'items':row['items']}
    if compare:
        available = row['coupang_date'] is not None
        theme = next((t for t in row['themes'] if t['themeCode']==theme_code), None)
        result.update(themeCode=theme_code,themeName=theme['themeName'] if theme else None,
            comparisonStatus='available' if available else 'coupang_unavailable',
            coupangSnapshotDate=row['coupang_date'],coupangPublishedAt=row['coupang_published_at'],
            comparedCategories=row['categories'],
            matchedKeywords=[i for i in row['items'] if available and i['matchedProductCount']>0],
            unmatchedKeywords=[i for i in row['items'] if available and i['matchedProductCount']==0],
            unavailableKeywords=[] if available else row['items'],
            notice='쿠팡은 동일 기준일에 발행 완료된 수집 베스트 카테고리만 비교한 결과입니다.')
        if available:
            result['updatedAt'] = max(row['published_at'], row['coupang_published_at'])
    return result


def read_compare(connection, theme_code):
    return read_published(connection,theme_code,compare=True)
