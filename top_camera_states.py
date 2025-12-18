import duckdb
import time

start_time = time.time()

con = duckdb.connect('airbnb.db')

# Find state with highest percentage of camera reviews
top_camera_state = con.execute("""
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
""").fetchone()

print(top_camera_state[0])
print(top_camera_state[1])

con.close()

end_time = time.time()
print(f"Execution time: {end_time - start_time:.3f} seconds")
