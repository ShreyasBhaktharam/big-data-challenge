import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

result = con.execute("""
    WITH secret_listings AS (
        SELECT DISTINCT lc.id, lc.state
        FROM listing_cameras lc
        JOIN review_cameras rc ON rc.listing_id = lc.id
        WHERE rc.has_camera = true 
        AND lc.has_camera = false
    )
    SELECT 
        sl.state,
        COUNT(DISTINCT sl.id) as secret_count
    FROM secret_listings sl
    GROUP BY sl.state
    ORDER BY 
        CAST(COUNT(DISTINCT sl.id) AS DOUBLE) / 
        (SELECT COUNT(DISTINCT id) FROM listing_cameras WHERE state = sl.state) DESC,
        secret_count DESC
    LIMIT 1
""").fetchone()

print(result[0])
print(result[1])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")