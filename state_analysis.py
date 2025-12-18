import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

results = con.execute("""
    WITH counts AS (
        SELECT state, COUNT(DISTINCT id) as cnt
        FROM listings
        GROUP BY state
    )
    SELECT 
        (SELECT state FROM counts ORDER BY cnt DESC LIMIT 1),
        (SELECT state FROM counts ORDER BY cnt ASC LIMIT 1)
""").fetchone()

print(results[0])
print(results[1])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")