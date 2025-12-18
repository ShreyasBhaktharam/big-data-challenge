import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

print(con.execute("""
    SELECT COUNT(DISTINCT r.id)
    FROM reviews r
    JOIN listings l ON r.listing_id = l.id
    GROUP BY l.host_id
    ORDER BY 1 DESC
    LIMIT 1
""").fetchone()[0])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")