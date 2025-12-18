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
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── install_packages.py          # Package installation script
├── preprocess.py                # Optimized data preprocessing (recommended)
├── preprocess_fast.py           # Alternative fast preprocessing
├── analysis.py                  # Original analysis script
├── count_rows.py               # Count total rows
├── count_unique.py             # Count unique listings/reviews/reviewers
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

Run the optimized preprocessing script to create the DuckDB database:

```bash
python3 preprocess.py
```

This will:
- Process 22GB+ of CSV data
- Create a 24GB+ DuckDB database (`airbnb.db`)
- Add proper indexes for fast queries
- Take approximately 4-5 minutes on most systems

**Alternative:** Use the fast preprocessing script:
```bash
python3 preprocess_fast.py
```

## Execution Order

Run the scripts in this exact order:

### 1. Data Preprocessing (Required)
```bash
python3 preprocess.py
```
**Time:** ~4-5 minutes
**Output:** Creates `airbnb.db` database file

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
**Output:** Host ID with most reviews

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

## Performance Optimizations

The preprocessing script includes several optimizations for remote environments:

- **Parallel Processing:** Uses 4 concurrent workers to process files simultaneously
- **Chunked Reading:** Processes CSV files in 50,000-row chunks to reduce memory usage
- **Optimized Indexes:** Creates efficient database indexes for fast queries
- **Progress Tracking:** Real-time progress bars for long-running operations
- **Error Handling:** Continues processing even if individual files fail

## Output Format

Each analysis script outputs results in a specific format:

- `count_rows.py`: Two numbers (listings_count, reviews_count)
- `count_unique.py`: Three numbers (unique_listings, unique_reviews, unique_reviewers)
- `state_analysis.py`: Two state codes (most_listings_state, least_listings_state)
- `top_host.py`: One number (host_id_with_most_reviews)
- `camera_listings.py`: One number (camera_listings_count)
- `top_camera_states.py`: State code and count (state_code, camera_reviews_count)
- `secret_cameras.py`: State code and count (state_code, secret_camera_count)

## Troubleshooting

### Common Issues

1. **"Conflicting lock" Error**
   - Delete `airbnb.db` and rerun preprocessing
   - Ensure no other scripts are running simultaneously

2. **Memory Errors**
   - The optimized script uses chunked processing to minimize memory usage
   - Ensure at least 8GB RAM available

3. **Slow Performance**
   - Remote environments may be slower than local machines
   - The optimizations reduce processing time from ~10 minutes to ~4-5 minutes

4. **Missing CSV Files**
   - Ensure all 68 CSV files are downloaded to the project directory
   - Check file names match exactly

### Performance Comparison

| Environment | Original Script | Optimized Script | Improvement |
|-------------|----------------|------------------|-------------|
| Local Machine | ~4 minutes | ~4 minutes | Same (already optimized) |
| Remote Environment | ~10+ minutes | ~4-5 minutes | **2x faster** |

## Dependencies

- **duckdb==1.4.3**: High-performance analytical database
- **pandas==2.3.3**: Data manipulation and CSV processing
- **tqdm==4.67.1**: Progress bars for long operations

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is for educational purposes as part of the Big Data Challenge.
