import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Count the number of unique listings by the "id" field across all the listings
unique_listings = con.execute("SELECT COUNT(DISTINCT id) FROM listings").fetchone()[0]
print(unique_listings)

# Count the number of unique reviews by the "id" field across all the reviews
unique_reviews = con.execute("SELECT COUNT(DISTINCT id) FROM reviews").fetchone()[0]
print(unique_reviews)

# Count the number of unique reviewers by the "reviewer_id" field across all the reviews
unique_reviewers = con.execute("SELECT COUNT(DISTINCT reviewer_id) FROM reviews").fetchone()[0]
print(unique_reviewers)

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
