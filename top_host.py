import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

print(con.execute("""
    SELECT review_count
    FROM host_review_counts
    ORDER BY review_count DESC
    LIMIT 1
""").fetchone()[0])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")