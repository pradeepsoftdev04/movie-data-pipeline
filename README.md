# Movie ETL Pipeline with IMDb ID Fallback

A comprehensive ETL (Extract, Transform, Load) pipeline that extracts movie data from CSV files, enriches it with OMDb API data using intelligent fallback strategies, and loads it into a relational database with **99.2% API match success rate**.

## 📑 Table of Contents

- [Features](#-features)
- [Project Structure](#-project-structure)
- [Requirements](#-requirements)
- [Database Schema](#-database-schema)
- [Installation](#-installation)
- [Configuration](#-configuration)
- [Usage](#-usage)
- [How It Works](#-how-it-works)
- [API Rate Limits](#-api-rate-limits)
- [Troubleshooting](#-troubleshooting)
- [Sample Queries](#-sample-queries)
- [Performance](#-performance)
- [License](#-license)

## ✨ Features

- **CSV Data Extraction**: Reads movie, ratings, and IMDb ID mapping data
- **Intelligent Title Normalization**: Handles foreign language titles and special formats
- **Triple-Strategy API Fetching**: 
  - Strategy 1: Search by normalized title with year
  - Strategy 2: Search by normalized title without year
  - Strategy 3: Fallback to IMDb ID from links.csv ⭐
- **99.2% Success Rate**: Near-perfect API matching with IMDb ID fallback
- **Batch Database Loading**: Efficiently loads 100K+ records
- **Comprehensive Logging**: Detailed operation tracking and error reporting
- **Rate Limiting**: Configurable delays to respect API limits
- **Database Agnostic**: Supports MySQL, PostgreSQL, and SQLite

## 📁 Project Structure

```
movie-data-pipeline/
│
├── data/
│   ├── movies.csv          # Movie metadata (movieId, title, genres)
│   ├── ratings.csv         # User ratings (userId, movieId, rating, timestamp)
│   └── links.csv           # IMDb and TMDb ID mappings ⭐ REQUIRED
│
├── etl1.py                 # Main ETL pipeline script
├── .env                    # Environment variables (API keys, DB config)
├── requirements.txt        # Python dependencies
├── schema.sql              # Database schema script
└── README.md               # This file
```

## 📋 Requirements

### Python Version
- Python 3.7 or higher

### Python Dependencies

```bash
pip install pandas requests sqlalchemy pymysql python-dotenv
```

**Package details:**
- `pandas>=1.3.0` - Data manipulation and analysis
- `requests>=2.26.0` - HTTP library for API calls
- `sqlalchemy>=1.4.0` - SQL toolkit and ORM
- `pymysql>=1.0.0` - MySQL database connector
- `python-dotenv>=0.19.0` - Load environment variables

### Database
One of the following:
- MySQL 5.7+ (Recommended)
- PostgreSQL 10+
- SQLite 3+ (Easiest for testing)

### OMDb API Key
Free API key from [OMDb API](http://www.omdbapi.com/apikey.aspx)
- **Free Tier**: 1,000 requests/day
- **Paid Tiers**: Up to 500,000 requests/day

## 🗄️ Database Schema

### Complete Schema Script

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS movie_db;
USE movie_db;

-- Movies table
CREATE TABLE movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    release_year INT,
    imdb_id VARCHAR(20),
    director VARCHAR(200),
    plot TEXT,
    box_office VARCHAR(50),
    imdb_rating DECIMAL(3,1),
    runtime VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_title (title),
    INDEX idx_year (release_year),
    INDEX idx_imdb_id (imdb_id)
);

-- Genres table
CREATE TABLE genres (
    genre_id INT AUTO_INCREMENT PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_genre_name (genre_name)
);

-- Movie-Genre relationship table
CREATE TABLE movie_genres (
    movie_id INT NOT NULL,
    genre_id INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE,
    INDEX idx_movie (movie_id),
    INDEX idx_genre (genre_id)
);

-- Ratings table
CREATE TABLE ratings (
    rating_id INT AUTO_INCREMENT PRIMARY KEY,
    movie_id INT NOT NULL,
    user_id INT NOT NULL,
    rating DECIMAL(2,1) NOT NULL,
    timestamp INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    INDEX idx_movie (movie_id),
    INDEX idx_user (user_id),
    INDEX idx_rating (rating)
);
```

## 🚀 Installation

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd movie-data-pipeline
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install pandas requests sqlalchemy pymysql python-dotenv
```

### Step 3: Get OMDb API Key

1. Visit [OMDb API Registration](http://www.omdbapi.com/apikey.aspx)
2. Select free tier (1,000 requests/day)
3. Check email for activation link
4. Copy your API key

### Step 4: Prepare CSV Files

Create `data/` directory:
```bash
mkdir data
```

Place these CSV files in `data/` directory:

**1. movies.csv** (Required)
```csv
movieId,title,genres
1,Toy Story (1995),Adventure|Animation|Children|Comedy|Fantasy
2,Jumanji (1995),Adventure|Children|Fantasy
```

**2. ratings.csv** (Required)
```csv
userId,movieId,rating,timestamp
1,1,4.0,964982703
1,3,4.0,964981247
```

**3. links.csv** ⭐ (Required for 99% success rate)
```csv
movieId,imdbId,tmdbId
1,114709,862
2,113497,8844
```

**Important**: IMDb IDs should be numeric only (without "tt" prefix)

### Step 5: Setup Database

**For MySQL:**
```bash
mysql -u root -p < schema.sql
```

**For SQLite:**
```python
# Update etl1.py connection string to:
connection_string = "sqlite:///movie_database.db"
```

**For PostgreSQL:**
```bash
psql -U postgres -d movie_db -f schema.sql
```

### Step 6: Configure Environment

Create `.env` file in project root:

```env
# Database Configuration
DB_HOST=localhost
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_NAME=movie_db

# OMDb API Configuration
OMDB_API_KEY=your_omdb_api_key_here
```

**Example:**
```env
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=mypassword123
DB_NAME=movie_db
OMDB_API_KEY=82d5b52e
```

## ⚙️ Configuration

### API Request Limit

Control how many movies to enrich:

```python
# In etl1.py
API_REQUEST_LIMIT = 500  # Default: 500 movies

# Options:
# 100  - Quick test
# 500  - Standard batch (recommended for free tier)
# 1000 - Full day's free tier limit
# 9742 - All movies (requires paid plan)
```

### API Call Delay

Adjust delay between requests:

```python
self.api_call_delay = 0.2  # 0.2 seconds = 5 requests/second

# Options:
# 0.1 - Faster (6-10 req/sec) - Use with paid plan
# 0.2 - Balanced (5 req/sec) - Recommended
# 0.5 - Conservative (2 req/sec)
```

### Database Connection

Update in `etl1.py` main() function:

```python
# MySQL
connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"

# PostgreSQL
connection_string = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"

# SQLite
connection_string = "sqlite:///movie_database.db"
```

### Logging Level

```python
# In etl1.py
logging.basicConfig(level=logging.INFO)  # Default

# Options:
# logging.DEBUG   - Detailed troubleshooting
# logging.INFO    - Standard operational messages
# logging.WARNING - Only warnings and errors
# logging.ERROR   - Only errors
```

## ▶️ Usage

### Running the Pipeline

```bash
python etl1.py
```

### Expected Output

```
================================================================================
Starting ETL pipeline...
================================================================================
2025-10-25 10:39:32 - INFO - Extracting data from CSV files...
2025-10-25 10:39:32 - INFO - Loaded 9742 IMDb ID mappings from links.csv
2025-10-25 10:39:32 - INFO - Loaded 9742 movies and 100836 ratings
2025-10-25 10:39:32 - INFO - Transforming movie data...
2025-10-25 10:39:34 - INFO - Fetching data from OMDb API for first 500 movies...
2025-10-25 10:40:10 - INFO - Processed 20/500 movies... (20 successful API matches)
2025-10-25 10:53:37 - INFO - Processed 500/500 movies... (496 successful API matches)
2025-10-25 10:53:37 - INFO - Successfully enriched 496 out of 500 movies (99.2% success rate)
2025-10-25 10:53:37 - INFO - Loading movies into database...
2025-10-25 10:53:45 - INFO - Loaded 9742 movies successfully
2025-10-25 10:53:46 - INFO - Loaded 20 genres and 23423 movie-genre relationships
2025-10-25 10:54:12 - INFO - Loaded 100836 ratings successfully
================================================================================
ETL pipeline completed successfully in 840.23 seconds
================================================================================
```

## 🎯 How It Works

### Triple-Strategy API Matching

```
Movie: "City of Lost Children, The (Cité des enfants perdus, La) (1995)"
                    ↓
┌──────────────────────────────────────────┐
│ STRATEGY 1: Title + Year                 │
│ Search: "The City of Lost Children" + 1995│
└──────────────┬───────────────────────────┘
               ├─[Found?]─YES→ ✅ Return data
               NO ↓
┌──────────────────────────────────────────┐
│ STRATEGY 2: Title Only                   │
│ Search: "The City of Lost Children"      │
└──────────────┬───────────────────────────┘
               ├─[Found?]─YES→ ✅ Return data
               NO ↓
┌──────────────────────────────────────────┐
│ STRATEGY 3: IMDb ID Fallback ⭐          │
│ 1. Look up movieId in links.csv         │
│ 2. Get imdbId: "112682"                 │
│ 3. Format: "tt0112682"                  │
│ 4. Search by IMDb ID                    │
└──────────────┬───────────────────────────┘
               ├─[Found?]─YES→ ✅ Return data
               NO ↓
            ❌ Not found
```

### Title Normalization

The pipeline handles various title formats:

```python
# Article movement
"Movie, The" → "The Movie"
"Adventures of Priscilla, Queen of the Desert, The" → "The Adventures of Priscilla, Queen of the Desert"

# Foreign language titles
"City of Lost Children, The (Cité des enfants perdus, La)" → "The City of Lost Children"

# Year extraction
"Toy Story (1995)" → Title: "Toy Story", Year: 1995
```

### Success Rate Impact

- **Without IMDb fallback**: ~85-90% success rate
- **With IMDb fallback**: **99.2% success rate** ⭐
- **Improvement**: +9-14% more movies enriched

## 🚨 API Rate Limits

### OMDb API Limits

| Plan | Daily Limit | Cost |
|------|-------------|------|
| Free | 1,000 | $0 |
| Patreon $1 | 100,000 | $1/mo |
| Patreon $5 | 500,000 | $5/mo |

### Handling Rate Limits

**Symptom**: 401 Unauthorized errors

**Solutions:**

1. **Wait and Resume** (Free)
   - Wait 24 hours for daily limit reset
   - Modify code to continue from where you left off

2. **Process in Batches** (Free)
   ```python
   # Day 1: Process 900 movies
   API_REQUEST_LIMIT = 900
   
   # Day 2: Skip first 900, process next 900
   for idx, row in movies_df.iloc[900:1800].iterrows():
       # Process
   ```

3. **Upgrade API Plan** ($1-5/month)
   - Visit [OMDb Patreon](https://www.patreon.com/omdb)
   - $1/month: Process all 9,742 movies in one run

4. **Implement Caching** (Advanced)
   ```python
   import json
   
   def save_to_cache(movie_id, data):
       cache = load_cache()
       cache[str(movie_id)] = data
       with open('api_cache.json', 'w') as f:
           json.dump(cache, f)
   ```

## 🐛 Troubleshooting

### Common Issues

**Issue**: `FileNotFoundError: data/movies.csv`

**Solution**:
```bash
mkdir data
# Place CSV files in data/ directory
```

---

**Issue**: `401 Unauthorized - API request failed`

**Cause**: Invalid API key OR hit rate limit

**Solution**:
1. Verify API key in `.env` file
2. Check email for API activation
3. If key is valid, you've hit daily limit (wait 24 hours)
4. Upgrade to paid plan

---

**Issue**: `sqlalchemy.exc.OperationalError`

**Cause**: Database connection failed

**Solution**:
```bash
# Check if database is running
sudo systemctl status mysql

# Test connection
mysql -u your_user -p -h localhost movie_db

# Verify credentials in .env file
```

---

**Issue**: Low API match rate (< 90%)

**Cause**: Missing `links.csv` or incorrect format

**Solution**:
1. ✅ Ensure `links.csv` exists in `data/` directory
2. ✅ Verify IMDb IDs are numeric (no "tt" prefix)
3. ✅ Check `movieId` values match across all CSV files

```python
# Verify data consistency
import pandas as pd
movies = pd.read_csv('data/movies.csv')
links = pd.read_csv('data/links.csv')
missing = set(movies['movieId']) - set(links['movieId'])
print(f"Missing IMDb IDs: {len(missing)}")
```

---

**Issue**: `Memory Error` or `Out of Memory`

**Solution**:
```python
# Reduce batch size
batch_size = 500  # Instead of 1000

# Process fewer movies at once
API_REQUEST_LIMIT = 100
```

---

**Issue**: Pipeline runs slowly

**Solution**:
1. Check internet connection
2. Reduce API call delay (if on paid plan):
   ```python
   self.api_call_delay = 0.1
   ```
3. Use MySQL instead of SQLite for large datasets

---

**Issue**: Duplicate key errors

**Solution**:
```sql
-- Clear tables before re-running
DELETE FROM ratings;
DELETE FROM movie_genres;
DELETE FROM genres;
DELETE FROM movies;
```

## 📊 Sample Queries

### Top 10 Highest Rated Movies

```sql
SELECT 
    title, 
    imdb_rating, 
    release_year, 
    director
FROM movies
WHERE imdb_rating IS NOT NULL
ORDER BY imdb_rating DESC
LIMIT 10;
```

### Movies by Genre

```sql
SELECT 
    m.title, 
    m.release_year, 
    GROUP_CONCAT(g.genre_name) as genres
FROM movies m
JOIN movie_genres mg ON m.movie_id = mg.movie_id
JOIN genres g ON mg.genre_id = g.genre_id
WHERE g.genre_name = 'Action'
GROUP BY m.movie_id
LIMIT 20;
```

### Most Rated Movies

```sql
SELECT 
    m.title,
    m.release_year,
    COUNT(r.rating_id) as rating_count,
    AVG(r.rating) as avg_user_rating,
    m.imdb_rating
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id
ORDER BY rating_count DESC
LIMIT 10;
```

### Movies by Director

```sql
SELECT 
    director,
    COUNT(*) as movie_count,
    AVG(imdb_rating) as avg_rating
FROM movies
WHERE director IS NOT NULL AND director != 'N/A'
GROUP BY director
HAVING movie_count >= 3
ORDER BY avg_rating DESC
LIMIT 20;
```

### Average Rating by User

```sql
SELECT 
    user_id, 
    COUNT(*) as rating_count,
    AVG(rating) as avg_rating
FROM ratings
GROUP BY user_id
HAVING rating_count > 100
ORDER BY avg_rating DESC
LIMIT 10;
```

## 📈 Performance

### Time Benchmarks

| Phase | Time | Percentage |
|-------|------|------------|
| Extract | ~2 seconds | <1% |
| Transform (500 movies) | ~13 minutes | 92% |
| Load (all data) | ~30 seconds | 8% |
| **Total** | **~14 minutes** | **100%** |

### API Performance

- **Average per movie**: ~2 seconds
- **Strategy 1 success**: ~70% (title + year)
- **Strategy 2 success**: ~20% (title only)
- **Strategy 3 success**: ~9% (IMDb ID fallback)
- **Combined success**: **99.2%** ⭐

### Database Insert Performance

| Operation | Records | Time | Rate |
|-----------|---------|------|------|
| Movies | 9,742 | ~5 sec | ~2,000/sec |
| Genres | 20 | <1 sec | instant |
| Movie-Genres | 23,423 | ~10 sec | ~2,300/sec |
| Ratings (batched) | 100,836 | ~15 sec | ~6,700/sec |

### Scaling Estimates

| Dataset Size | API Calls | Time (Free Tier) | Time (Paid Tier) |
|--------------|-----------|------------------|------------------|
| 500 movies | 500 | 14 minutes | 14 minutes |
| 1,000 movies | 1,000 | 28 minutes | 28 minutes |
| 5,000 movies | 5,000 | 11 days* | 2.3 hours |
| 10,000 movies | 10,000 | 22 days* | 4.6 hours |

*Processing 900 movies/day to stay under free tier limit

## 🔐 Security Best Practices

1. **Never commit `.env` file**:
   ```bash
   echo ".env" >> .gitignore
   ```

2. **Use environment variables in production**:
   ```bash
   export OMDB_API_KEY=your_key
   export DB_PASSWORD=your_password
   ```

3. **Create limited database user**:
   ```sql
   CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'password';
   GRANT SELECT, INSERT, UPDATE, DELETE ON movie_db.* TO 'etl_user'@'localhost';
   ```

4. **Rotate credentials regularly**

## 🧪 Testing

### Test with Small Dataset

```python
# In etl1.py
API_REQUEST_LIMIT = 10  # Test with 10 movies first
```

### Verify Data Quality

```sql
-- Check for NULL values
SELECT 
    COUNT(*) as total_movies,
    SUM(CASE WHEN imdb_id IS NULL THEN 1 ELSE 0 END) as missing_imdb_id,
    SUM(CASE WHEN director IS NULL THEN 1 ELSE 0 END) as missing_director
FROM movies;

-- Check rating distribution
SELECT rating, COUNT(*) as count
FROM ratings
GROUP BY rating
ORDER BY rating;
```

## 📚 Resources

- [OMDb API Documentation](http://www.omdbapi.com/)
- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [MovieLens Dataset](https://grouplens.org/datasets/movielens/)

## 📄 License

This project is provided for educational and personal use.

**Dataset License**: MovieLens data by GroupLens Research (non-commercial use)

**API Terms**: OMDb API usage subject to their terms of service

## 👥 Support

### Getting Help

1. Check **Troubleshooting** section above
2. Review logs for detailed error messages
3. Verify all CSV files are properly formatted
4. Test with small dataset first (10 movies)

### Reporting Issues

Include in your report:
- Python version (`python --version`)
- Database type and version
- Complete error message with traceback
- Relevant log output
- Steps to reproduce

---

## 📊 Project Stats

- **Lines of Code**: ~600
- **CSV Files**: 3 (movies, ratings, links)
- **Database Tables**: 4 (movies, genres, movie_genres, ratings)
- **API Strategies**: 3 (title+year, title, IMDb ID)
- **Success Rate**: 99.2% ⭐
- **Supported Databases**: MySQL, PostgreSQL, SQLite

---

**Version**: 2.0 (with IMDb ID Fallback)  
**Last Updated**: October 2025  
**Author**: Pradeep kumar

⭐ **Star this project if you found it helpful!**