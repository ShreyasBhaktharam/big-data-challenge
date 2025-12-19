import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

result = con.execute("""
    SELECT state, camera_reviews
    FROM state_camera_agg
    ORDER BY 
        CAST(camera_reviews AS DOUBLE) / total_reviews DESC,
        camera_reviews DESC
    LIMIT 1
""").fetchone()

print(result[0])
print(result[1])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")