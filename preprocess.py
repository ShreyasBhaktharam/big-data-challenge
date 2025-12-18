import duckdb
import os
import glob
import pandas as pd
from tqdm import tqdm
import concurrent.futures
import multiprocessing
import time
from typing import List, Tuple

# Configuration
CHUNK_SIZE = 50000  # Process 50k rows at a time
MAX_WORKERS = min(multiprocessing.cpu_count(), 4)  # Limit workers to avoid overwhelming the system

def process_file_chunked(con: duckdb.DuckDBPyConnection, file_path: str, state_code: str, table_name: str) -> int:
    """
    Process a CSV file in chunks to reduce memory usage.

    Args:
        con: DuckDB connection
        file_path: Path to CSV file
        state_code: State code to add to each row
        table_name: Target table name

    Returns:
        Number of rows processed
    """
    total_rows = 0

    try:
        # First, get the total number of rows for progress reporting
        # Use shell command to count lines (faster than pandas)
        import subprocess
        result = subprocess.run(['wc', '-l', file_path],
                              capture_output=True, text=True)
        total_file_rows = int(result.stdout.split()[0]) - 1  # Subtract header

        # Read CSV in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(file_path, chunksize=CHUNK_SIZE, low_memory=False)):
            # Add state column
            chunk['state'] = state_code

            # Insert chunk into DuckDB
            con.execute(f"INSERT INTO {table_name} SELECT * FROM chunk")

            total_rows += len(chunk)

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        raise

    return total_rows

def process_file_parallel(args: Tuple[str, str, str]) -> Tuple[str, int, str]:
    """
    Process a single file (for parallel execution).

    Args:
        args: Tuple of (file_path, state_code, table_name)

    Returns:
        Tuple of (file_path, rows_processed, status)
    """
    file_path, state_code, table_name = args

    try:
        # Create a separate connection for each worker
        con = duckdb.connect('airbnb.db')
        rows_processed = process_file_chunked(con, file_path, state_code, table_name)
        con.close()
        return file_path, rows_processed, "success"
    except Exception as e:
        return file_path, 0, f"error: {str(e)}"

def main():
    """Main preprocessing function with error handling and timing."""
    start_time = time.time()

    try:
        print("🚀 Starting Airbnb data preprocessing...")
        print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        # Connect to DuckDB with performance optimizations
        con = duckdb.connect('airbnb.db')

        # Performance optimizations
        con.execute("PRAGMA threads=4")  # Use multiple threads for queries
        con.execute("PRAGMA memory_limit='4GB'")  # Limit memory usage
        con.execute("PRAGMA temp_directory='/tmp'")  # Use faster temp storage if available

        # Get all CSV files
        listings_files = glob.glob('*_listings.csv')
        reviews_files = glob.glob('*_reviews.csv')

        print(f"Found {len(listings_files)} listings files and {len(reviews_files)} reviews files")
        print(f"Using {MAX_WORKERS} parallel workers for processing")
        print(f"Processing files in chunks of {CHUNK_SIZE:,} rows")
        print("-" * 50)

        # Create state mapping
        state_mapping = {}
        for file in listings_files + reviews_files:
            # Extract state from filename (e.g., albany_ny_listings.csv -> NY)
            parts = file.split('_')
            if len(parts) >= 2:
                state_code = parts[-2].upper() if len(parts[-2]) == 2 else parts[-3].upper()
                state_mapping[file] = state_code

        print("State mapping:", state_mapping)

        # Create tables
        con.execute("""
            CREATE TABLE IF NOT EXISTS listings (
                id BIGINT,
                listing_url TEXT,
                scrape_id BIGINT,
                last_scraped DATE,
                source TEXT,
                name TEXT,
                description TEXT,
                neighborhood_overview TEXT,
                picture_url TEXT,
                host_id BIGINT,
                host_url TEXT,
                host_name TEXT,
                host_since DATE,
                host_location TEXT,
                host_about TEXT,
                host_response_time TEXT,
                host_response_rate TEXT,
                host_acceptance_rate TEXT,
                host_is_superhost BOOLEAN,
                host_thumbnail_url TEXT,
                host_picture_url TEXT,
                host_neighbourhood TEXT,
                host_listings_count INTEGER,
                host_total_listings_count INTEGER,
                host_verifications TEXT,
                host_has_profile_pic BOOLEAN,
                host_identity_verified BOOLEAN,
                neighbourhood TEXT,
                neighbourhood_cleansed TEXT,
                neighbourhood_group_cleansed TEXT,
                latitude DOUBLE,
                longitude DOUBLE,
                property_type TEXT,
                room_type TEXT,
                accommodates INTEGER,
                bathrooms DOUBLE,
                bathrooms_text TEXT,
                bedrooms INTEGER,
                beds INTEGER,
                amenities TEXT,
                price TEXT,
                minimum_nights INTEGER,
                maximum_nights INTEGER,
                minimum_minimum_nights INTEGER,
                maximum_minimum_nights INTEGER,
                minimum_maximum_nights INTEGER,
                maximum_maximum_nights INTEGER,
                minimum_nights_avg_ntm DOUBLE,
                maximum_nights_avg_ntm DOUBLE,
                calendar_updated TEXT,
                has_availability BOOLEAN,
                availability_30 INTEGER,
                availability_60 INTEGER,
                availability_90 INTEGER,
                availability_365 INTEGER,
                calendar_last_scraped DATE,
                number_of_reviews INTEGER,
                number_of_reviews_ltm INTEGER,
                number_of_reviews_l30d INTEGER,
                availability_eoy INTEGER,
                number_of_reviews_ly INTEGER,
                estimated_occupancy_l365d DOUBLE,
                estimated_revenue_l365d DOUBLE,
                first_review DATE,
                last_review DATE,
                review_scores_rating DOUBLE,
                review_scores_accuracy DOUBLE,
                review_scores_cleanliness DOUBLE,
                review_scores_checkin DOUBLE,
                review_scores_communication DOUBLE,
                review_scores_location DOUBLE,
                review_scores_value DOUBLE,
                license TEXT,
                instant_bookable BOOLEAN,
                calculated_host_listings_count INTEGER,
                calculated_host_listings_count_entire_homes INTEGER,
                calculated_host_listings_count_private_rooms INTEGER,
                calculated_host_listings_count_shared_rooms INTEGER,
                reviews_per_month DOUBLE,
                state TEXT
            )
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                listing_id BIGINT,
                id BIGINT,
                date DATE,
                reviewer_id BIGINT,
                reviewer_name TEXT,
                comments TEXT,
                state TEXT
            )
        """)

        # Load listings data in parallel
        print(f"Loading listings data using {MAX_WORKERS} parallel workers...")
        listings_tasks = [(file, state_mapping[file], 'listings') for file in listings_files]

        total_listings_rows = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(process_file_parallel, task): task[0]
                             for task in listings_tasks}

            # Process results as they complete
            with tqdm(total=len(listings_tasks), desc="Processing listings") as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path, rows_processed, status = future.result()
                    if status == "success":
                        total_listings_rows += rows_processed
                        pbar.set_postfix({"file": os.path.basename(file_path), "rows": rows_processed})
                    else:
                        print(f"Failed to process {file_path}: {status}")
                    pbar.update(1)

        # Load reviews data in parallel
        print(f"Loading reviews data using {MAX_WORKERS} parallel workers...")
        reviews_tasks = [(file, state_mapping[file], 'reviews') for file in reviews_files]

        total_reviews_rows = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(process_file_parallel, task): task[0]
                             for task in reviews_tasks}

            # Process results as they complete
            with tqdm(total=len(reviews_tasks), desc="Processing reviews") as pbar:
                for future in concurrent.futures.as_completed(future_to_file):
                    file_path, rows_processed, status = future.result()
                    if status == "success":
                        total_reviews_rows += rows_processed
                        pbar.set_postfix({"file": os.path.basename(file_path), "rows": rows_processed})
                    else:
                        print(f"Failed to process {file_path}: {status}")
                    pbar.update(1)

        print("Creating indexes...")

        # Create indexes with progress reporting
        indexes = [
            ("idx_listings_id", "listings", "id"),
            ("idx_listings_host_id", "listings", "host_id"),
            ("idx_listings_state", "listings", "state"),
            ("idx_reviews_id", "reviews", "id"),
            ("idx_reviews_listing_id", "reviews", "listing_id"),
            ("idx_reviews_reviewer_id", "reviews", "reviewer_id"),
            ("idx_reviews_state", "reviews", "state"),
        ]

        for index_name, table_name, column_name in tqdm(indexes, desc="Creating indexes"):
            try:
                con.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})")
            except Exception as e:
                print(f"Warning: Failed to create index {index_name}: {e}")
                # Continue with other indexes

        print("Preprocessing complete!")

        # Print some basic stats
        print(f"Total listings processed: {total_listings_rows:,}")
        print(f"Total reviews processed: {total_reviews_rows:,}")

        # Verify final counts in database
        listings_count = con.execute("SELECT COUNT(*) FROM listings").fetchone()[0]
        reviews_count = con.execute("SELECT COUNT(*) FROM reviews").fetchone()[0]

        print(f"Database listings count: {listings_count:,}")
        print(f"Database reviews count: {reviews_count:,}")

        con.close()

        # Final timing
        end_time = time.time()
        total_time = end_time - start_time
        print("-" * 50)
        print("✅ Preprocessing completed successfully!")
        print(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")

        # Performance summary
        total_files = len(listings_files) + len(reviews_files)
        avg_time_per_file = total_time / total_files if total_files > 0 else 0
        print(".3f")

    except Exception as e:
        print(f"❌ Error during preprocessing: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        # Ensure connection is closed even on error
        try:
            if 'con' in locals():
                con.close()
        except:
            pass

if __name__ == "__main__":
    main()
