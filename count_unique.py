import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

results = con.execute("""
    SELECT 
        (SELECT COUNT(DISTINCT id) FROM listings),
        (SELECT COUNT(DISTINCT id) FROM reviews),
        (SELECT COUNT(DISTINCT reviewer_id) FROM reviews)
""").fetchone()

for r in results:
    print(r)

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")