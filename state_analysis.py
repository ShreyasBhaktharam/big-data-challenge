import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Count unique listings by state
state_counts = con.execute("""
    SELECT state, COUNT(DISTINCT id) as listing_count
    FROM listings
    GROUP BY state
    ORDER BY listing_count DESC
""").fetchall()

# State with most listings
print(state_counts[0][0])

# State with least listings
print(state_counts[-1][0])

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
