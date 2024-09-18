import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
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
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
num_songs INT,
artist_id TEXT,
artist_latitude FLOAT,
artist_longtitude FLOAT,
artist_location TEXT,
artist_name TEXT,
song_id TEXT,
title TEXT,
duration FLOAT,
year INT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table(
song_play_id BIGINT IDENTITY(0,1) PRIMARY KEY,
start_time TIMESTAMP NOT NULL,
user_id INT,
level TEXT,
song_id TEXT,
artist_id TEXT,
session_id INT,
location TEXT,
user_agent TEXT
);
""")

user_table_create = (""" 
CREATE TABLE IF NOT EXISTS user_table(
user_id INT PRIMARY KEY,
first_name TEXT,
last_name TEXT,
gender TEXT,
level TEXT
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
song_id TEXT PRIMARY KEY,
title TEXT,
artist_id TEXT,
year INT,
duration FLOAT
);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
artist_id TEXT PRIMARY KEY,
name TEXT,
location TEXT,
latitude FLOAT,
longtitude FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
start_time TIMESTAMP PRIMARY KEY,
hour INT,
day INT,
week INT, 
month INT,
year INT,
weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
IAM_ROLE {}
COMPUPDATE OFF 
STATUPDATE OFF
FORMAT AS JSON {}
REGION '{}'
TIMEFORMAT AS 'epochmillisecs';
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'), config.get('CLUSTER', 'REGION'))

staging_songs_copy = ("""
COPY staging_songs FROM {}
IAM_ROLE {}
COMPUPDATE OFF 
STATUPDATE OFF
FORMAT AS JSON 'auto'
REGION '{}';
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('CLUSTER', 'REGION'))



# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    TIMESTAMP 'epoch' + se.ts/1000 * interval '0.01 second' AS start_time,
    se.userId,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId,
    se.location,
    se.userAgent
FROM staging_songs ss
JOIN staging_events se
ON se.artist = ss.artist_name
AND se.length = ss.duration
WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    se.userId,
    se.firstName,
    se.lastName,
    se.gender,
    se.level
FROM staging_events se
WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration)
SELECT DISTINCT
ss.song_id,
ss.title,
ss.artist_id,
ss.year,
ss.duration
FROM staging_songs ss
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, latitude, longtitude)
SELECT DISTINCT
ss.artist_id,
ss.artist_name,
ss.artist_location,
ss.artist_latitude,
ss.artist_longtitude
FROM staging_songs ss
""")

time_table_insert = ("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
     TIMESTAMP 'epoch' + ts/1000 * interval '0.01 second' AS start_time,
     EXTRACT (HOUR FROM start_time) AS hour,
     EXTRACT (DAY FROM start_time) AS day,
     EXTRACT (WEEK FROM start_time) AS week,
     EXTRACT (MONTH FROM start_time) AS month,
     EXTRACT (YEAR FROM start_time) AS year,
     EXTRACT (WEEKDAY FROM start_time) AS weekday
     FROM staging_events
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
