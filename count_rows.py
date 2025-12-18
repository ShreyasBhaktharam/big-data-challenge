import duckdb
import time

con = duckdb.connect('airbnb.db')
start_time = time.time()

print(con.execute("SELECT COUNT(*) FROM listings").fetchone()[0])
print(con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0])

print(f"[Execution time: {time.time() - start_time:.4f} seconds]")