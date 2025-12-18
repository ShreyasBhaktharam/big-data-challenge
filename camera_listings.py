import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Count unique listings mentioning camera
camera_listings = con.execute("""
    SELECT COUNT(DISTINCT id)
    FROM listings
    WHERE LOWER(description) LIKE '%camera%'
       OR LOWER(host_about) LIKE '%camera%'
       OR LOWER(amenities) LIKE '%camera%'
""").fetchone()[0]

print(camera_listings)

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
