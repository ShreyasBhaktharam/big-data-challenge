import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Find state with highest percentage of secret camera listings
secret_cameras = con.execute("""
    WITH camera_in_reviews AS (
        SELECT DISTINCT listing_id, state
        FROM reviews
        WHERE LOWER(comments) LIKE '%camera%'
    ),
    camera_in_listings AS (
        SELECT DISTINCT id, state
        FROM listings
        WHERE LOWER(description) LIKE '%camera%'
           OR LOWER(host_about) LIKE '%camera%'
           OR LOWER(amenities) LIKE '%camera%'
    ),
    secret_camera_listings AS (
        SELECT c.listing_id, c.state
        FROM camera_in_reviews c
        LEFT JOIN camera_in_listings l ON c.listing_id = l.id
        WHERE l.id IS NULL
    ),
    state_counts AS (
        SELECT state, COUNT(*) as count
        FROM secret_camera_listings
        GROUP BY state
    ),
    total_listings_by_state AS (
        SELECT state, COUNT(DISTINCT id) as total_listings
        FROM listings
        GROUP BY state
    )
    SELECT s.state,
           s.count as secret_count,
           t.total_listings,
           (s.count * 100.0 / t.total_listings) as percentage
    FROM state_counts s
    JOIN total_listings_by_state t ON s.state = t.state
    ORDER BY percentage DESC
    LIMIT 1
""").fetchone()

print(secret_cameras[0])
print(secret_cameras[1])

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
