import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Find host with most reviews
top_host = con.execute("""
    SELECT host_id, COUNT(DISTINCT r.id) as review_count
    FROM listings l
    JOIN reviews r ON l.id = r.listing_id
    GROUP BY host_id
    ORDER BY review_count DESC
    LIMIT 1
""").fetchone()

print(top_host[1])

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
