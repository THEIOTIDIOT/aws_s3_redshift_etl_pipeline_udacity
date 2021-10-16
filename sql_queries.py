import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events(
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender CHAR(1),
    item_in_session INT,
    last_name VARCHAR,
    length DECIMAL,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration BIGINT,
    session_id INT,
    song VARCHAR,
    status INT,
    ts DOUBLE PRECISION,
    user_agent VARCHAR,
    user_id INT);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs(
    num_songs INT,
    artist_id VARCHAR,
    artist_latitude DECIMAL,
    artist_longitude DECIMAL,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    duration DECIMAL,
    year INT);
""")

#1199
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays(
    songplay_id BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP sortkey,
    user_id VARCHAR,
    level VARCHAR,
    song_id VARCHAR distkey,
    artist_id VARCHAR,
    session_id INT,
    location VARCHAR,
    user_agent VARCHAR);""")

#104 rows of users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
    user_id int PRIMARY KEY,
    first_name varchar,
    last_name varchar,
    gender char(1),
    level varchar)
diststyle ALL;
""")

#672
song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs(
    song_id varchar PRIMARY KEY distkey,
    title varchar,
    artist_id varchar,
    year integer,
    duration decimal);
""")

#10025
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists(
    artist_id varchar PRIMARY KEY sortkey,
    name varchar,
    location varchar,
    latitude decimal,
    longitude decimal)
DISTSTYLE AUTO;
""")

#6820
time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
    time_id BIGINT IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time TIMESTAMP sortkey,
    hour int,
    day int,
    week int,
    month int,
    year int,
    weekday INT)
DISTSTYLE AUTO;
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM {}
IAM_ROLE {}
FORMAT AS JSON {}
COMPUPDATE ON
REGION 'us-west-2';
""").format(config.get('S3','LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE {}
FORMAT AS JSON 'auto'
COMPUPDATE ON 
REGION 'us-west-2';
""").format(config.get('S3','SONG_DATA'),
            config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES
songplay_table_insert = ("""
INSERT INTO songplays(
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent)
SELECT
    timestamp 'epoch' + e.ts / 1000 * interval '1 second' AS start_time,
    e.user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.session_id,
    s.artist_location as location,
    e.user_agent
FROM staging_events e
JOIN staging_songs s ON (e.song = s.title)
WHERE user_id IS NOT NULL AND e.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users(
    user_id,
    first_name,
    last_name,
    gender,
    level)
SELECT 
    DISTINCT e.user_id,
    e.first_name,
    e.last_name,
    e.gender,
    e.level
FROM staging_events e
WHERE e.user_id IS NOT NULL AND e.page = 'NextSong';
""")

##Distribution Style ALL??
song_table_insert = ("""
INSERT INTO songs( 
    song_id,
    title, 
    artist_id, 
    year, 
    duration)
SELECT
    DISTINCT s.song_id,
    e.song AS title,
    s.artist_id,
    s.year,
    s.duration
FROM staging_songs s
JOIN staging_events e ON (s.title = e.song)
WHERE song_id IS NOT NULL AND e.page = 'NextSong';
""")

artist_table_insert = ("""
INSERT INTO artists(
    artist_id, 
    name, 
    location, 
    latitude, 
    longitude)
SELECT 
    DISTINCT artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS latitude,
    artist_longitude AS longitude
FROM staging_songs;
""")

##Distribution Style EVEN
time_table_insert = ("""
INSERT INTO time(
    start_time,
    hour,
    day,
    week,
    month,
    year,
    weekday)
SELECT 
    timestamp 'epoch' + e.ts / 1000 * interval '1 second' AS start_time,
    EXTRACT(hour FROM start_time) AS hour,
    EXTRACT(day FROM start_time) AS day,
    EXTRACT(week FROM start_time) AS day,
    EXTRACT(month FROM start_time) AS day,
    EXTRACT(year FROM start_time) AS day,
    EXTRACT(weekday FROM start_time) AS day
FROM staging_events e
WHERE user_id IS NOT NULL AND e.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
