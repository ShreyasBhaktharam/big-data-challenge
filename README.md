# Airbnb Big Data Challenge Analysis

This project analyzes Airbnb listings and reviews data across multiple US cities to answer various analytical questions about the platform's data.

## Dataset

**Dataset Source:** [Airbnb Dataset](https://big-data-challenge.hdanny.org/dataset/)

The dataset contains:
- **34 cities/regions** across the United States
- **68 CSV files** total (34 listings + 34 reviews files)
- **22GB+ of raw data** including 1.4M+ listings and 68M+ reviews
- Data covers major metropolitan areas like New York City, Los Angeles, San Francisco, Chicago, Boston, etc.

### Download Instructions

1. Visit the [dataset page](https://big-data-challenge.hdanny.org/dataset/)
2. Download all 68 CSV files to this project directory
3. Ensure all files are in the same directory as the Python scripts

**Note:** The dataset is quite large (~22GB). Make sure you have sufficient disk space and a stable internet connection for the download.

## Project Structure

```
big_data_challenge/
├── requirements.txt             # Python dependencies
├── install_packages.py          # Package installation notes
├── preprocess.py                # Data preprocessing using DuckDB native CSV import
├── count_rows.py               # Count total rows in listings and reviews
├── count_unique.py             # Count unique listings, reviews, and reviewers
├── state_analysis.py           # Find states with most/least listings
├── top_host.py                 # Find host with most reviews
├── camera_listings.py          # Count listings mentioning cameras
├── top_camera_states.py        # Find state with highest camera review percentage
├── secret_cameras.py           # Find state with highest secret camera listings
├── .gitignore                  # Git ignore rules
└── env/                        # Virtual environment (created after setup)
```

## Setup Instructions

### 1. Install Dependencies

```bash
# Create virtual environment
python3 -m venv env

# Activate virtual environment
source env/bin/activate  # On macOS/Linux
# or
env\Scripts\activate     # On Windows

# Install packages
pip install -r requirements.txt
```

### 2. Download Dataset

Download all CSV files from [https://big-data-challenge.hdanny.org/dataset/](https://big-data-challenge.hdanny.org/dataset/) into this directory.

### 3. Preprocess Data

Run the preprocessing script to create the DuckDB database:

```bash
python3 preprocess.py
```

This will:
- Process 22GB+ of CSV data using DuckDB's native CSV import
- Create a DuckDB database (`airbnb.db`) with listings and reviews tables
- Pre-compute camera flags in helper tables (`listing_cameras` and `review_cameras`) for fast analysis queries
- Create indexes for optimized query performance
- Display timing information for each processing step

## Execution Order

Run the scripts in this exact order:

### 1. Data Preprocessing (Required)
```bash
python3 preprocess.py
```
**Time:** Varies by system (typically faster than pandas-based approaches)
**Output:** Creates `airbnb.db` database file with:
  - `listings` table: All listing data with state column
  - `reviews` table: All review data with state column
  - `listing_cameras` table: Pre-computed camera flags for listings
  - `review_cameras` table: Pre-computed camera flags for reviews
  - Indexes for fast query performance

### 2. Analysis Scripts (Run after preprocessing)

#### Count Rows
```bash
python3 count_rows.py
```
**Output:** Total listings and reviews counts

#### Count Unique Values
```bash
python3 count_unique.py
```
**Output:** Unique listings, reviews, and reviewers counts

#### State Analysis
```bash
python3 state_analysis.py
```
**Output:** States with most and least listings

#### Top Host
```bash
python3 top_host.py
```
**Output:** Review count for the host with the most reviews

#### Camera Listings
```bash
python3 camera_listings.py
```
**Output:** Count of listings mentioning cameras

#### Top Camera States
```bash
python3 top_camera_states.py
```
**Output:** State with highest camera review percentage and count

#### Secret Cameras
```bash
python3 secret_cameras.py
```
**Output:** State with highest percentage of secret camera listings and count

## Database Schema

After preprocessing, the `airbnb.db` database contains:

- **`listings`**: All listing data with columns including `id`, `host_id`, `state`, `description`, `host_about`, `amenities`, etc.
- **`reviews`**: All review data with columns including `id`, `listing_id`, `reviewer_id`, `state`, `comments`, etc.
- **`listing_cameras`**: Pre-computed table with `id`, `state`, `host_id`, and `has_camera` flag (true if camera mentioned in description, host_about, or amenities)
- **`review_cameras`**: Pre-computed table with `id`, `listing_id`, `state`, and `has_camera` flag (true if camera mentioned in comments)

The camera tables are created during preprocessing to speed up analysis queries that search for camera mentions.

## Performance Optimizations

The preprocessing script uses several optimizations:

- **Native DuckDB CSV Import:** Uses DuckDB's built-in `read_csv_auto()` function for fast, efficient CSV parsing
- **UNION ALL Operations:** Combines all CSV files in a single SQL operation for maximum efficiency
- **Pre-computed Camera Flags:** Creates helper tables (`listing_cameras` and `review_cameras`) during preprocessing to speed up analysis queries
- **Optimized Indexes:** Creates indexes on frequently queried columns (id, listing_id, host_id)
- **Error Handling:** Uses `ignore_errors=true` flag to handle malformed CSV rows gracefully
- **Execution Time Tracking:** Displays timing information for each processing step

## Output Format

Each analysis script outputs results in a specific format, followed by execution time:

- `count_rows.py`: 
  - Two numbers (listings_count, reviews_count)
  - Execution time in seconds
  
- `count_unique.py`: 
  - Three numbers (unique_listings, unique_reviews, unique_reviewers)
  - Execution time in seconds
  
- `state_analysis.py`: 
  - Two state codes (most_listings_state, least_listings_state)
  - Execution time in seconds
  
- `top_host.py`: 
  - One number (review_count for host with most reviews)
  - Execution time in seconds
  
- `camera_listings.py`: 
  - One number (count of listings mentioning cameras)
  - Execution time in seconds
  
- `top_camera_states.py`: 
  - State code (state with highest camera review percentage)
  - Camera review count
  - Execution time in seconds
  
- `secret_cameras.py`: 
  - State code (state with highest percentage of secret camera listings)
  - Secret camera listing count
  - Execution time in seconds

## Troubleshooting

### Common Issues

1. **"Conflicting lock" Error**
   - Delete `airbnb.db` and rerun preprocessing
   - Ensure no other scripts are running simultaneously

2. **Memory Errors**
   - DuckDB's native CSV import is memory-efficient
   - Ensure sufficient disk space for the database file (~24GB)

3. **Slow Performance**
   - Remote environments may be slower than local machines
   - DuckDB's native CSV import is typically faster than pandas-based approaches
   - Pre-computed camera tables speed up analysis queries significantly

4. **Missing CSV Files**
   - Ensure all 68 CSV files are downloaded to the project directory
   - Check file names match exactly

### Performance Notes

- **DuckDB Native Import:** Uses DuckDB's optimized CSV reader, which is typically faster than pandas
- **Single SQL Operations:** Combines all CSV files using UNION ALL for efficient processing
- **Pre-computed Tables:** Camera flags are computed once during preprocessing, making analysis queries much faster
- **Execution Time Tracking:** Each script displays its execution time for performance monitoring

## Dependencies

- **duckdb==1.4.3**: High-performance analytical database with native CSV import capabilities

**Note:** The preprocessing script uses DuckDB's native `read_csv_auto()` function, eliminating the need for pandas or other CSV processing libraries. This makes the code simpler and faster.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is for educational purposes as part of the Big Data Challenge.
