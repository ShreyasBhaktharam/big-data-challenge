import duckdb
import time

# Connect to the database
con = duckdb.connect('airbnb.db')

def time_query(query_name, query):
    """Time a query and return the result"""
    start_time = time.time()
    result = con.execute(query).fetchone()
    end_time = time.time()
    print(f"{query_name} took {end_time - start_time:.3f} seconds")
    return result

print("=== Count rows ===")
listings_count = time_query("Listings count", "SELECT COUNT(*) FROM listings")
reviews_count = time_query("Reviews count", "SELECT COUNT(*) FROM reviews")

print(f"{listings_count[0]}")
print(f"{reviews_count[0]}")

print("\n=== Count unique listings and reviews ===")
unique_listings = time_query("Unique listings", "SELECT COUNT(DISTINCT id) FROM listings")
unique_reviews = time_query("Unique reviews", "SELECT COUNT(DISTINCT id) FROM reviews")
unique_reviewers = time_query("Unique reviewers", "SELECT COUNT(DISTINCT reviewer_id) FROM reviews")

print(f"{unique_listings[0]}")
print(f"{unique_reviews[0]}")
print(f"{unique_reviewers[0]}")

print("\n=== Identify the state with the most/least number of listings ===")
# Count unique listings by state
state_counts_query = """
    SELECT state, COUNT(DISTINCT id) as listing_count
    FROM listings
    GROUP BY state
    ORDER BY listing_count DESC
"""

state_counts = con.execute(state_counts_query).fetchall()
most_listings_state = state_counts[0]
least_listings_state = state_counts[-1]

print(f"{most_listings_state[0]}")
print(f"{least_listings_state[0]}")

print("\n=== Host with the most number of reviews ===")
# Find host with most reviews
top_host_query = """
    SELECT host_id, COUNT(DISTINCT r.id) as review_count
    FROM listings l
    JOIN reviews r ON l.id = r.listing_id
    GROUP BY host_id
    ORDER BY review_count DESC
    LIMIT 1
"""

top_host = con.execute(top_host_query).fetchone()
print(f"{top_host[1]}")

print("\n=== Listings that mention 'cameras' ===")
# Find listings mentioning camera in description, host_about, or amenities
camera_listings_query = """
    SELECT COUNT(DISTINCT id)
    FROM listings
    WHERE LOWER(description) LIKE '%camera%'
       OR LOWER(host_about) LIKE '%camera%'
       OR LOWER(amenities) LIKE '%camera%'
"""

camera_listings = con.execute(camera_listings_query).fetchone()
print(f"{camera_listings[0]}")

print("\n=== Top states with camera listings ===")
# Find state with highest percentage of camera reviews
camera_reviews_by_state_query = """
    WITH camera_reviews AS (
        SELECT DISTINCT r.id, r.state
        FROM reviews r
        WHERE LOWER(r.comments) LIKE '%camera%'
    ),
    total_reviews_by_state AS (
        SELECT state, COUNT(DISTINCT id) as total_reviews
        FROM reviews
        GROUP BY state
    ),
    camera_reviews_count AS (
        SELECT state, COUNT(*) as camera_reviews
        FROM camera_reviews
        GROUP BY state
    )
    SELECT c.state,
           c.camera_reviews,
           t.total_reviews,
           (c.camera_reviews * 100.0 / t.total_reviews) as percentage
    FROM camera_reviews_count c
    JOIN total_reviews_by_state t ON c.state = t.state
    ORDER BY percentage DESC
    LIMIT 1
"""

top_camera_state = con.execute(camera_reviews_by_state_query).fetchone()
print(f"{top_camera_state[0]}")
print(f"{top_camera_state[1]}")

print("\n=== Secret cameras ===")
# Find listings that mention cameras in reviews but NOT in listing details
secret_cameras_query = """
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
"""

secret_cameras = con.execute(secret_cameras_query).fetchone()
print(f"{secret_cameras[0]}")
print(f"{secret_cameras[1]}")

con.close()
