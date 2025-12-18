import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Count the number of rows across all the listings
listings_count = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
print(listings_count)

# Count the number of rows across all the reviews
reviews_count = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]
print(reviews_count)

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
