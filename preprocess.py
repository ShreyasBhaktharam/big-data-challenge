import duckdb
import glob
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# DuckDB optimizations
con.execute("SET preserve_insertion_order=false")
con.execute("SET threads=4")
con.execute("SET memory_limit='4GB'")

def extract_state(filename):
    parts = filename.replace('_listings.csv', '').replace('_reviews.csv', '').split('_')
    return parts[-1].upper()

# Load listings
listings_start = time.time()
listings_files = glob.glob('*_listings.csv')
listings_unions = [f"SELECT *, '{extract_state(f)}' as state FROM read_csv_auto('{f}', ignore_errors=true, parallel=true)" 
                   for f in listings_files]
con.execute(f"CREATE TABLE listings AS {' UNION ALL '.join(listings_unions)}")
print(f"Listings loaded in {time.time() - listings_start:.2f} seconds")

# Load reviews
reviews_start = time.time()
reviews_files = glob.glob('*_reviews.csv')
reviews_unions = [f"SELECT *, '{extract_state(f)}' as state FROM read_csv_auto('{f}', ignore_errors=true, parallel=true)" 
                  for f in reviews_files]
con.execute(f"CREATE TABLE reviews AS {' UNION ALL '.join(reviews_unions)}")
print(f"Reviews loaded in {time.time() - reviews_start:.2f} seconds")

# Materialize review_cameras (essential for Q8, Q9)
camera_start = time.time()
con.execute("""
    CREATE TABLE review_cameras AS 
    SELECT 
        id,
        listing_id,
        state,
        LOWER(COALESCE(comments, '')) LIKE '%camera%' as has_camera
    FROM reviews
""")
print(f"Review camera flags materialized in {time.time() - camera_start:.2f} seconds")

# NEW: Materialize listing_cameras (was VIEW - optimize Q7 and Q9)
listing_camera_start = time.time()
con.execute("""
    CREATE TABLE listing_cameras AS 
    SELECT 
        id,
        state,
        host_id,
        (LOWER(COALESCE(description, '')) LIKE '%camera%' OR
         LOWER(COALESCE(host_about, '')) LIKE '%camera%' OR
         LOWER(COALESCE(amenities, '')) LIKE '%camera%') as has_camera
    FROM listings
""")
print(f"Listing camera flags materialized in {time.time() - listing_camera_start:.2f} seconds")

# NEW: Precompute unique counts (optimize Q4)
counts_start = time.time()
con.execute("""
    CREATE TABLE precomputed_counts AS 
    SELECT 
        (SELECT COUNT(DISTINCT id) FROM listings) as unique_listings,
        (SELECT COUNT(DISTINCT id) FROM reviews) as unique_reviews,
        (SELECT COUNT(DISTINCT reviewer_id) FROM reviews) as unique_reviewers
""")
print(f"Unique counts precomputed in {time.time() - counts_start:.2f} seconds")

# NEW: Precompute state camera review aggregations (optimize Q8)
state_agg_start = time.time()
con.execute("""
    CREATE TABLE state_camera_agg AS 
    SELECT 
        state,
        COUNT(DISTINCT id) as total_reviews,
        COUNT(DISTINCT CASE WHEN has_camera THEN id END) as camera_reviews
    FROM review_cameras
    GROUP BY state
""")
print(f"State camera aggregations precomputed in {time.time() - state_agg_start:.2f} seconds")

# NEW: Precompute host review counts (optimize Q6)
host_agg_start = time.time()
con.execute("""
    CREATE TABLE host_review_counts AS 
    SELECT 
        l.host_id,
        COUNT(DISTINCT r.id) as review_count
    FROM reviews r
    JOIN listings l ON r.listing_id = l.id
    GROUP BY l.host_id
""")
print(f"Host review counts precomputed in {time.time() - host_agg_start:.2f} seconds")

# Minimal indexing (only for queries that still need it)
index_start = time.time()
con.execute("CREATE INDEX idx_review_cameras_listing ON review_cameras(listing_id)")
con.execute("CREATE INDEX idx_listing_cameras_id ON listing_cameras(id)")
print(f"Indexes created in {time.time() - index_start:.2f} seconds")

total_preprocess_time = time.time() - start_time
print(f"\n=== TOTAL PREPROCESSING TIME: {total_preprocess_time:.2f} seconds ===\n")
print("Preprocessing complete")