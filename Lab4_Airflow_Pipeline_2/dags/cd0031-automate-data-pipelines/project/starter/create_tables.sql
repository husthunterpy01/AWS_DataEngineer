-- DROP TABLE 
DROP TABLE IF EXISTS staging_events;
DROP TABLE IF EXISTS staging_songs;
DROP TABLE IF EXISTS songplay_table;
DROP TABLE IF EXISTS user_table;
DROP TABLE IF EXISTS song_table;
DROP TABLE IF EXISTS artist_table;
DROP TABLE IF EXISTS time_table;

-- CREATE TABLE
CREATE TABLE staging_events (
    artist TEXT,
    auth TEXT,
    firstName TEXT,
    gender TEXT,
    itemInSession INT,
    lastName TEXT,
    length FLOAT,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration FLOAT,
    sessionId INT,
    song TEXT,
    status INT,
    ts BIGINT,
    userAgent TEXT,
    userId INT
);


CREATE TABLE staging_songs (
    num_songs INT,
    artist_id TEXT,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location TEXT,
    artist_name TEXT,
    song_id TEXT,
    title TEXT,
    duration FLOAT,
    year INT
);


CREATE TABLE songplay_table (
    song_play_id TEXT PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT,
    level TEXT,
    song_id TEXT,
    artist_id TEXT,
    session_id INT,
    location TEXT,
    user_agent TEXT
);


CREATE TABLE user_table (
    user_id INT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    gender TEXT,
    level TEXT
);


CREATE TABLE song_table (
    song_id TEXT PRIMARY KEY,
    title TEXT,
    artist_id TEXT,
    year INT,
    duration FLOAT
);


CREATE TABLE artist_table (
    artist_id TEXT PRIMARY KEY,
    name TEXT,
    location TEXT,
    latitude FLOAT,
    longtitude FLOAT
);


CREATE TABLE time_table (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT, 
    month INT,
    year INT,
    weekday INT
);
