-- Drop tables if they exist (for idempotency)
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movie_genres;
DROP TABLE IF EXISTS genres;
DROP TABLE IF EXISTS movies;

-- Movies table: stores core movie information
CREATE TABLE movies (
    movie_id INTEGER PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    release_year INTEGER,
    imdb_id VARCHAR(20),
    director VARCHAR(255),
    plot TEXT,
    box_office VARCHAR(50),
    imdb_rating DECIMAL(3,1),
    runtime VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Genres table: stores unique genres
CREATE TABLE genres (
    genre_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    genre_name VARCHAR(100) UNIQUE NOT NULL
);

-- Movie_Genres junction table: many-to-many relationship
CREATE TABLE movie_genres (
    movie_id INTEGER,
    genre_id INTEGER,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id) ON DELETE CASCADE
);

-- Ratings table: stores user ratings
CREATE TABLE ratings (
    rating_id INTEGER PRIMARY KEY AUTO_INCREMENT,
    movie_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    rating DECIMAL(2,1) NOT NULL,
    timestamp INTEGER,
    FOREIGN KEY (movie_id) REFERENCES movies(movie_id) ON DELETE CASCADE,
    INDEX idx_movie_id (movie_id),
    INDEX idx_user_id (user_id)
);

-- Note: For SQLite, use this version instead:
-- Replace AUTO_INCREMENT with AUTOINCREMENT
-- Remove VARCHAR lengths (use TEXT)
-- Remove INDEX statements and create them separately