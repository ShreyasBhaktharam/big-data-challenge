import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

print(con.execute("""
    SELECT COUNT(DISTINCT id)
    FROM listing_cameras
    WHERE has_camera = true
""").fetchone()[0])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")
