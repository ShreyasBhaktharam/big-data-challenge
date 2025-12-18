import duckdb
import glob
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

def extract_state(filename):
    parts = filename.replace('_listings.csv', '').replace('_reviews.csv', '').split('_')
    return parts[-1].upper()

# Load listings
listings_start = time.time()
listings_files = glob.glob('*_listings.csv')
listings_unions = [f"SELECT *, '{extract_state(f)}' as state FROM read_csv_auto('{f}', ignore_errors=true, sample_size=-1)" 
                   for f in listings_files]
con.execute(f"CREATE TABLE listings AS {' UNION ALL '.join(listings_unions)}")
print(f"Listings loaded in {time.time() - listings_start:.2f} seconds")

# Load reviews
reviews_start = time.time()
reviews_files = glob.glob('*_reviews.csv')
reviews_unions = [f"SELECT *, '{extract_state(f)}' as state FROM read_csv_auto('{f}', ignore_errors=true, sample_size=-1)" 
                  for f in reviews_files]
con.execute(f"CREATE TABLE reviews AS {' UNION ALL '.join(reviews_unions)}")
print(f"Reviews loaded in {time.time() - reviews_start:.2f} seconds")

# Pre-compute camera flags for listings
listing_cameras_start = time.time()
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
print(f"Listing camera flags computed in {time.time() - listing_cameras_start:.2f} seconds")

# Pre-compute camera flags for reviews
review_cameras_start = time.time()
con.execute("""
    CREATE TABLE review_cameras AS 
    SELECT 
        id,
        listing_id,
        state,
        LOWER(COALESCE(comments, '')) LIKE '%camera%' as has_camera
    FROM reviews
""")
print(f"Review camera flags computed in {time.time() - review_cameras_start:.2f} seconds")

# Create indexes
index_start = time.time()
con.execute("CREATE INDEX idx_listing_cameras_id ON listing_cameras(id)")
con.execute("CREATE INDEX idx_review_cameras_listing ON review_cameras(listing_id)")
con.execute("CREATE INDEX idx_listings_host ON listings(host_id)")
con.execute("CREATE INDEX idx_reviews_listing ON reviews(listing_id)")
print(f"Indexes created in {time.time() - index_start:.2f} seconds")

total_preprocess_time = time.time() - start_time
print(f"\n=== TOTAL PREPROCESSING TIME: {total_preprocess_time:.2f} seconds ===\n")
print("Preprocessing complete")