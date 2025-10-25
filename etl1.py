import pandas as pd
import requests
import time
import re
import os
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables (with fallbacks)
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'user': os.getenv('DB_USER', 'your_user'),
    'password': os.getenv('DB_PASSWORD', 'your_password'),
    'database': os.getenv('DB_NAME', 'movie_db')
}
OMDB_API_KEY = os.getenv('OMDB_API_KEY', 'your_api_key_here')
OMDB_BASE_URL = 'http://www.omdbapi.com/'

# File paths
MOVIES_CSV = 'data/movies.csv'
RATINGS_CSV = 'data/ratings.csv'
LINKS_CSV = 'data/links.csv'
MISSING_MOVIES_LOG = 'logs/missing_movies.csv'

# API request limit
API_REQUEST_LIMIT = 400

class MovieETL:
    def __init__(self, db_connection_string, api_key):
        """Initialize ETL pipeline with database connection and API key"""
        self.engine = create_engine(db_connection_string)
        self.api_key = api_key
        self.api_call_delay = 0.2  # Delay between API calls to avoid rate limiting
        self.links_df = None  # Will store IMDb IDs mapping
        self.missing_movies = []  # Track missing movies for logging
        
        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)
        
    def extract_csv_data(self):
        """Extract data from CSV files"""
        logger.info("Extracting data from CSV files...")
        
        try:
            # Read movies and ratings
            movies_df = pd.read_csv(MOVIES_CSV)
            ratings_df = pd.read_csv(RATINGS_CSV)
            
            # Read links.csv for IMDb IDs
            try:
                self.links_df = pd.read_csv(LINKS_CSV)
                logger.info(f"Loaded {len(self.links_df)} IMDb ID mappings from links.csv")
            except FileNotFoundError:
                logger.warning("links.csv not found. IMDb ID fallback will not be available.")
                self.links_df = pd.DataFrame(columns=['movieId', 'imdbId', 'tmdbId'])
            
            logger.info(f"Loaded {len(movies_df)} movies and {len(ratings_df)} ratings")
            return movies_df, ratings_df
        except FileNotFoundError as e:
            logger.error(f"CSV file not found: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error reading CSV files: {str(e)}")
            raise
    
    def get_imdb_id_from_links(self, movie_id):
        """Get IMDb ID from links.csv for a given movieId"""
        if self.links_df is None or self.links_df.empty:
            return None
        
        try:
            link_row = self.links_df[self.links_df['movieId'] == movie_id]
            if not link_row.empty:
                imdb_id = link_row.iloc[0]['imdbId']
                if pd.notna(imdb_id):
                    # Format IMDb ID with leading zeros (7 digits total)
                    return f"tt{int(imdb_id):07d}"
        except Exception as e:
            logger.debug(f"Error getting IMDb ID for movie {movie_id}: {str(e)}")
        
        return None
    
    def extract_year_from_title(self, title):
        """Extract year from movie title (format: 'Movie Name (YEAR)')"""
        # Match year at the end of the title, but not within foreign language titles
        match = re.search(r'\((\d{4})\)(?:\s*$)', title)
        if match:
            year = int(match.group(1))
            # Remove the year from the title
            clean_title = re.sub(r'\s*\(\d{4}\)\s*$', '', title).strip()
            return year, clean_title
        return None, title
    
    def normalize_title(self, title):
        """
        Normalize title for better API matching.
        Handles formats like:
        - "Movie, The" -> "The Movie"
        - "Adventures of Priscilla, Queen of the Desert, The" -> "The Adventures of Priscilla, Queen of the Desert"
        - "City of Lost Children, The (Cité des enfants perdus, La)" -> "The City of Lost Children"
        - "Shanghai Triad (Yao a yao yao dao waipo qiao)" -> "Shanghai Triad"
        - "Junior, The" -> "The Junior"
        """
        title = title.strip()
        
        # Remove ALL text in parentheses (foreign titles, alternate names, etc.)
        # This removes everything like (Cité des enfants perdus, La), (1995), (Yao a yao yao dao waipo qiao)
        title = re.sub(r'\s*\([^)]*\)', '', title).strip()
        
        # Handle article movement at the end for different patterns:
        # 1. ", The" at the very end
        # Move trailing articles to the front (English + French)
        if title.endswith(', The'):
            title = 'The ' + title[:-5].strip()
        elif title.endswith(', A'):
            title = 'A ' + title[:-3].strip()
        elif title.endswith(', An'):
            title = 'An ' + title[:-4].strip()
        elif title.endswith(', Le'):
            title = 'Le ' + title[:-4].strip()
        elif title.endswith(', La'):
            title = 'La ' + title[:-4].strip()
        elif title.endswith(', Les'):
            title = 'Les ' + title[:-4].strip()

        
        # Additional cleanup: remove extra spaces and commas
        title = ' '.join(title.split())
        title = title.strip(', ')
        
        return title
    
    def fetch_omdb_data_by_imdb_id(self, imdb_id):
        """Fetch movie data from OMDb API using IMDb ID"""
        if not imdb_id:
            return None
        
        params = {
            'apikey': self.api_key,
            'i': imdb_id,
            'type': 'movie'
        }
        
        try:
            response = requests.get(OMDB_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if data.get('Response') == 'True':
                logger.debug(f"✓ Found by IMDb ID: {imdb_id}")
                return {
                    'imdb_id': data.get('imdbID'),
                    'director': data.get('Director'),
                    'plot': data.get('Plot'),
                    'box_office': data.get('BoxOffice'),
                    'imdb_rating': data.get('imdbRating'),
                    'runtime': data.get('Runtime')
                }
            
            logger.debug(f"✗ Not found by IMDb ID: {imdb_id}")
            return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for IMDb ID {imdb_id}: {str(e)}")
            return None
    
    def fetch_omdb_data(self, title, year=None, movie_id=None):
        """Fetch movie data from OMDb API with multiple strategies"""
        # Strategy 1: Try normalized title with year
        normalized_title = self.normalize_title(title)
        params = {
            'apikey': self.api_key,
            't': normalized_title,
            'type': 'movie'
        }
        
        strategy_attempted = []
        
        if year:
            params['y'] = str(year)
        
        try:
            # Strategy 1: Title + Year
            response = requests.get(OMDB_BASE_URL, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            strategy_attempted.append('Title+Year')
            
            if data.get('Response') == 'True':
                logger.debug(f"✓ Found: {normalized_title} ({year})")
                return {
                    'imdb_id': data.get('imdbID'),
                    'director': data.get('Director'),
                    'plot': data.get('Plot'),
                    'box_office': data.get('BoxOffice'),
                    'imdb_rating': data.get('imdbRating'),
                    'runtime': data.get('Runtime')
                }, 'Title+Year'
            
            # Strategy 2: Try without year if first attempt failed
            if year and 'y' in params:
                del params['y']
                time.sleep(0.1)  # Small delay between retries
                response = requests.get(OMDB_BASE_URL, params=params, timeout=10)
                data = response.json()
                strategy_attempted.append('Title Only')
                
                if data.get('Response') == 'True':
                    logger.debug(f"✓ Found (without year): {normalized_title}")
                    return {
                        'imdb_id': data.get('imdbID'),
                        'director': data.get('Director'),
                        'plot': data.get('Plot'),
                        'box_office': data.get('BoxOffice'),
                        'imdb_rating': data.get('imdbRating'),
                        'runtime': data.get('Runtime')
                    }, 'Title Only'
            
            # Strategy 3: Try with IMDb ID from links.csv if title search failed
            if movie_id:
                imdb_id = self.get_imdb_id_from_links(movie_id)
                if imdb_id:
                    logger.debug(f"Trying IMDb ID fallback: {imdb_id} for movie {movie_id}")
                    time.sleep(0.1)  # Small delay between retries
                    strategy_attempted.append('IMDb ID')
                    imdb_data = self.fetch_omdb_data_by_imdb_id(imdb_id)
                    if imdb_data:
                        return imdb_data, 'IMDb ID'
            
            logger.debug(f"✗ Not found: {normalized_title} ({year if year else 'no year'})")
            return None, ','.join(strategy_attempted)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for {normalized_title}: {str(e)}")
            return None, 'API Error'
    
    def log_missing_movie(self, movie_id, original_title, normalized_title, year, genres, strategies_attempted, error_reason='Not found in API'):
        """Log missing movie details"""
        imdb_id = self.get_imdb_id_from_links(movie_id)
        
        self.missing_movies.append({
            'movie_id': movie_id,
            'original_title': original_title,
            'normalized_title': normalized_title,
            'release_year': year if year else 'N/A',
            'genres': genres if genres else 'N/A',
            'imdb_id_available': 'Yes' if imdb_id else 'No',
            'imdb_id': imdb_id if imdb_id else 'N/A',
            'strategies_attempted': strategies_attempted,
            'error_reason': error_reason,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    
    def save_missing_movies_log(self):
        """Save missing movies to CSV file"""
        if not self.missing_movies:
            logger.info("No missing movies to log")
            return
        
        try:
            missing_df = pd.DataFrame(self.missing_movies)
            missing_df.to_csv(MISSING_MOVIES_LOG, index=False)
            logger.info(f"Saved {len(self.missing_movies)} missing movies to {MISSING_MOVIES_LOG}")
            
            # Print summary statistics
            logger.info("Missing Movies Summary:")
            logger.info(f"  - Total missing: {len(self.missing_movies)}")
            logger.info(f"  - With IMDb ID available: {missing_df['imdb_id_available'].value_counts().get('Yes', 0)}")
            logger.info(f"  - Without IMDb ID: {missing_df['imdb_id_available'].value_counts().get('No', 0)}")
            
        except Exception as e:
            logger.error(f"Error saving missing movies log: {str(e)}")
    
    def transform_movies(self, movies_df):
        """Transform and enrich movie data"""
        logger.info("Transforming movie data...")
        
        # Extract year from title
        extracted_data = movies_df['title'].apply(
            lambda x: pd.Series(self.extract_year_from_title(x))
        )
        movies_df['release_year'] = extracted_data[0]
        movies_df['clean_title'] = extracted_data[1]
        
        # Initialize columns for API data
        api_columns = ['imdb_id', 'director', 'plot', 'box_office', 'imdb_rating', 'runtime']
        for col in api_columns:
            movies_df[col] = None
        
        # Fetch data from OMDb API (limited to API_REQUEST_LIMIT)
        logger.info(f"Fetching data from OMDb API for first {API_REQUEST_LIMIT} movies...")
        success_count = 0
        imdb_id_fallback_count = 0
        
        for idx, row in movies_df.head(API_REQUEST_LIMIT).iterrows():
            # Check if we have IMDb ID before making the call
            has_imdb_id = self.get_imdb_id_from_links(row['movieId']) is not None
            
            api_data, strategy = self.fetch_omdb_data(
                row['clean_title'], 
                row['release_year'],
                row['movieId']
            )
            
            if api_data:
                for col, value in api_data.items():
                    movies_df.at[idx, col] = value
                success_count += 1
                
                # Track if this was found via IMDb ID fallback
                if strategy == 'IMDb ID':
                    imdb_id_fallback_count += 1
            else:
                # Log missing movie
                self.log_missing_movie(
                    movie_id=row['movieId'],
                    original_title=row['title'],
                    normalized_title=row['clean_title'],
                    year=row['release_year'],
                    genres=row.get('genres', ''),
                    strategies_attempted=strategy,
                    error_reason='Not found in OMDb API'
                )
            
            time.sleep(self.api_call_delay)  # Rate limiting
            
            if (idx + 1) % 10 == 0:
                logger.info(f"Processed {idx + 1}/{API_REQUEST_LIMIT} movies... ({success_count} successful, {len(self.missing_movies)} missing)")
        
        # Clean data types
        movies_df['imdb_rating'] = pd.to_numeric(movies_df['imdb_rating'], errors='coerce')
        movies_df['release_year'] = pd.to_numeric(movies_df['release_year'], errors='coerce')
        
        success_rate = (success_count / API_REQUEST_LIMIT) * 100 if API_REQUEST_LIMIT > 0 else 0
        logger.info(f"Transformation complete. Successfully enriched {success_count} out of {API_REQUEST_LIMIT} movies ({success_rate:.1f}% success rate)")
        if imdb_id_fallback_count > 0:
            logger.info(f"IMDb ID fallback helped find {imdb_id_fallback_count} additional movies")
        
        # Save missing movies log
        self.save_missing_movies_log()
        
        return movies_df
    
    def transform_ratings(self, ratings_df):
        """Transform ratings data"""
        logger.info("Transforming ratings data...")
        
        # Ensure proper data types
        ratings_df['rating'] = pd.to_numeric(ratings_df['rating'], errors='coerce')
        ratings_df['userId'] = pd.to_numeric(ratings_df['userId'], errors='coerce')
        ratings_df['movieId'] = pd.to_numeric(ratings_df['movieId'], errors='coerce')
        
        # Remove any rows with missing critical data
        original_count = len(ratings_df)
        ratings_df = ratings_df.dropna(subset=['movieId', 'userId', 'rating'])
        cleaned_count = len(ratings_df)
        
        if original_count > cleaned_count:
            logger.info(f"Removed {original_count - cleaned_count} invalid ratings")
        
        return ratings_df
    
    def load_movies(self, movies_df):
        """Load movies data into database"""
        logger.info("Loading movies into database...")
        
        try:
            with self.engine.connect() as conn:
                # Clear existing data for idempotency
                logger.info("Clearing existing movie data...")
                conn.execute(text("DELETE FROM movie_genres"))
                conn.execute(text("DELETE FROM genres"))
                conn.execute(text("DELETE FROM movies"))
                conn.commit()
                
                # Insert movies
                inserted_count = 0
                error_count = 0
                
                for _, row in movies_df.iterrows():
                    try:
                        conn.execute(text("""
                            INSERT INTO movies (movie_id, title, release_year, imdb_id, 
                                              director, plot, box_office, imdb_rating, runtime)
                            VALUES (:movie_id, :title, :year, :imdb_id, :director, 
                                   :plot, :box_office, :imdb_rating, :runtime)
                        """), {
                            'movie_id': int(row['movieId']),
                            'title': row['clean_title'],
                            'year': int(row['release_year']) if pd.notna(row['release_year']) else None,
                            'imdb_id': row['imdb_id'] if pd.notna(row['imdb_id']) else None,
                            'director': row['director'] if pd.notna(row['director']) else None,
                            'plot': row['plot'] if pd.notna(row['plot']) else None,
                            'box_office': row['box_office'] if pd.notna(row['box_office']) else None,
                            'imdb_rating': float(row['imdb_rating']) if pd.notna(row['imdb_rating']) else None,
                            'runtime': row['runtime'] if pd.notna(row['runtime']) else None
                        })
                        inserted_count += 1
                        
                        if inserted_count % 100 == 0:
                            logger.info(f"Inserted {inserted_count} movies...")
                            
                    except Exception as e:
                        logger.error(f"Error inserting movie {row['movieId']} ({row['clean_title']}): {str(e)}")
                        error_count += 1
                        continue
                
                conn.commit()
                logger.info(f"Loaded {inserted_count} movies successfully ({error_count} errors)")
        except Exception as e:
            logger.error(f"Error in load_movies: {str(e)}")
            raise
    
    def load_genres(self, movies_df):
        """Load genres into database"""
        logger.info("Loading genres into database...")
        
        try:
            with self.engine.connect() as conn:
                # Extract all unique genres
                all_genres = set()
                for genres_str in movies_df['genres'].dropna():
                    genres = [g.strip() for g in str(genres_str).split('|') if g.strip()]
                    all_genres.update(genres)
                
                # Insert genres
                genre_id_map = {}
                for genre in sorted(all_genres):
                    try:
                        result = conn.execute(text("""
                            INSERT INTO genres (genre_name) VALUES (:genre)
                        """), {'genre': genre})
                        genre_id_map[genre] = result.lastrowid
                    except Exception as e:
                        logger.error(f"Error inserting genre {genre}: {str(e)}")
                        continue
                
                conn.commit()
                
                # Insert movie-genre relationships
                relationship_count = 0
                for _, row in movies_df.iterrows():
                    if pd.notna(row['genres']):
                        genres = [g.strip() for g in str(row['genres']).split('|') if g.strip()]
                        for genre in genres:
                            if genre in genre_id_map:
                                try:
                                    conn.execute(text("""
                                        INSERT INTO movie_genres (movie_id, genre_id)
                                        VALUES (:movie_id, :genre_id)
                                    """), {
                                        'movie_id': int(row['movieId']),
                                        'genre_id': genre_id_map[genre]
                                    })
                                    relationship_count += 1
                                except Exception as e:
                                    logger.error(f"Error inserting movie-genre relationship: {str(e)}")
                                    continue
                
                conn.commit()
                logger.info(f"Loaded {len(all_genres)} genres and {relationship_count} movie-genre relationships")
        except Exception as e:
            logger.error(f"Error in load_genres: {str(e)}")
            raise
    
    def load_ratings(self, ratings_df):
        """Load ratings into database"""
        logger.info("Loading ratings into database...")
        
        try:
            with self.engine.connect() as conn:
                # Clear existing ratings for idempotency
                logger.info("Clearing existing ratings...")
                conn.execute(text("DELETE FROM ratings"))
                conn.commit()
                
                # Batch insert ratings
                ratings_data = []
                for _, row in ratings_df.iterrows():
                    ratings_data.append({
                        'movie_id': int(row['movieId']),
                        'user_id': int(row['userId']),
                        'rating': float(row['rating']),
                        'timestamp': int(row['timestamp']) if pd.notna(row['timestamp']) else None
                    })
                
                # Insert in batches of 1000
                batch_size = 1000
                total_inserted = 0
                
                for i in range(0, len(ratings_data), batch_size):
                    batch = ratings_data[i:i + batch_size]
                    try:
                        conn.execute(text("""
                            INSERT INTO ratings (movie_id, user_id, rating, timestamp)
                            VALUES (:movie_id, :user_id, :rating, :timestamp)
                        """), batch)
                        conn.commit()
                        total_inserted += len(batch)
                        
                        if total_inserted % 10000 == 0:
                            logger.info(f"Loaded {total_inserted} ratings...")
                    except Exception as e:
                        logger.error(f"Error inserting batch at position {i}: {str(e)}")
                        continue
                
                logger.info(f"Loaded {total_inserted} ratings successfully")
        except Exception as e:
            logger.error(f"Error in load_ratings: {str(e)}")
            raise
    
    def run(self):
        """Execute the complete ETL pipeline"""
        logger.info("=" * 80)
        logger.info("Starting ETL pipeline...")
        logger.info("=" * 80)
        start_time = time.time()
        
        try:
            # Extract
            movies_df, ratings_df = self.extract_csv_data()
            
            # Transform
            movies_df = self.transform_movies(movies_df)
            ratings_df = self.transform_ratings(ratings_df)
            
            # Load
            self.load_movies(movies_df)
            self.load_genres(movies_df)
            self.load_ratings(ratings_df)
            
            elapsed_time = time.time() - start_time
            logger.info("=" * 80)
            logger.info(f"ETL pipeline completed successfully in {elapsed_time:.2f} seconds")
            logger.info("=" * 80)
            
        except Exception as e:
            logger.error("=" * 80)
            logger.error(f"ETL pipeline failed: {str(e)}")
            logger.error("=" * 80)
            raise

def main():
    """Main function to run the ETL pipeline"""
    try:
        # Validate environment variables
        if OMDB_API_KEY == 'your_api_key_here':
            logger.warning("WARNING: Using default API key. Please set OMDB_API_KEY in .env file")
        
        # Database connection string (modify based on your database)
        # For MySQL:
        connection_string = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        
        # For SQLite (simpler option):
        # connection_string = "sqlite:///movie_database.db"
        
        # For PostgreSQL:
        # connection_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
        
        # Initialize and run ETL
        etl = MovieETL(connection_string, OMDB_API_KEY)
        etl.run()
        
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()