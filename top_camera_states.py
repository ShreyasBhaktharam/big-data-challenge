import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

result = con.execute("""
    WITH unique_reviews AS (
        SELECT DISTINCT state, id, has_camera
        FROM review_cameras
    )
    SELECT 
        state, 
        SUM(CASE WHEN has_camera THEN 1 ELSE 0 END) as camera_count
    FROM unique_reviews
    GROUP BY state
    ORDER BY 
        CAST(SUM(CASE WHEN has_camera THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) DESC,
        camera_count DESC
    LIMIT 1
""").fetchone()

print(result[0])
print(result[1])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")