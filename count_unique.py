import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

result = con.execute("""
    SELECT unique_listings, unique_reviews, unique_reviewers 
    FROM precomputed_counts
""").fetchone()

for r in result:
    print(r)

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")