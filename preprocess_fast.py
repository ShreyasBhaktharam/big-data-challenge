#!/usr/bin/env python3
"""
Fast preprocessing script using DuckDB's native CSV import.
Alternative to preprocess.py - may be faster on some systems.
"""

import duckdb
import os
import glob
import time
from tqdm import tqdm

# Configuration
MAX_WORKERS = 2  # DuckDB handles parallelism internally

def create_tables(con):
    """Create the database tables."""
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

def import_csv_files(con, file_pattern, state_mapping, table_name):
    """Import CSV files using DuckDB's native CSV reader."""
    files = glob.glob(file_pattern)
    total_rows = 0

    for file_path in tqdm(files, desc=f"Importing {table_name}"):
        try:
            state_code = state_mapping[file_path]

            # Use DuckDB's native CSV import with state column added
            query = f"""
                INSERT INTO {table_name}
                SELECT *, '{state_code}' as state
                FROM read_csv_auto('{file_path}')
            """
            con.execute(query)

            # Count rows in this file
            file_rows = con.execute(f"SELECT COUNT(*) FROM {table_name} WHERE state = '{state_code}'").fetchone()[0]
            total_rows += file_rows

        except Exception as e:
            print(f"Error importing {file_path}: {e}")
            continue

    return total_rows

def create_indexes(con):
    """Create database indexes."""
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

def main():
    """Main preprocessing function using DuckDB native CSV import."""
    start_time = time.time()

    try:
        print("🚀 Starting fast Airbnb data preprocessing (DuckDB native)...")
        print(f"Started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 50)

        # Connect to DuckDB with optimizations
        con = duckdb.connect('airbnb.db')
        con.execute("PRAGMA threads=4")
        con.execute("PRAGMA memory_limit='4GB'")
        con.execute("SET enable_progress_bar=true")

        # Get all CSV files
        listings_files = glob.glob('*_listings.csv')
        reviews_files = glob.glob('*_reviews.csv')

        print(f"Found {len(listings_files)} listings files and {len(reviews_files)} reviews files")

        # Create state mapping
        state_mapping = {}
        for file in listings_files + reviews_files:
            parts = file.split('_')
            if len(parts) >= 2:
                state_code = parts[-2].upper() if len(parts[-2]) == 2 else parts[-3].upper()
                state_mapping[file] = state_code

        # Create tables
        print("Creating tables...")
        create_tables(con)

        # Import data using DuckDB native CSV reader
        print("Importing listings data...")
        total_listings = import_csv_files(con, '*_listings.csv', state_mapping, 'listings')

        print("Importing reviews data...")
        total_reviews = import_csv_files(con, '*_reviews.csv', state_mapping, 'reviews')

        # Create indexes
        create_indexes(con)

        # Final statistics
        print("Preprocessing complete!")
        print(f"Total listings: {total_listings:,}")
        print(f"Total reviews: {total_reviews:,}")

        con.close()

        # Timing
        end_time = time.time()
        total_time = end_time - start_time
        print("-" * 50)
        print("✅ Fast preprocessing completed successfully!")
        print(f"Total execution time: {total_time:.2f} seconds ({total_time/60:.1f} minutes)")

    except Exception as e:
        print(f"❌ Error during preprocessing: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
