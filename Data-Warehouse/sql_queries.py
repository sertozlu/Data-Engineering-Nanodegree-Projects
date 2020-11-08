import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist        VARCHAR,
        auth          VARCHAR,
        firstName     VARCHAR(50),
        gender        CHAR,
        itemInSession INTEGER,
        lastName      VARCHAR(50),
        length        FLOAT,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  FLOAT,
        sessionId     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            TIMESTAMP,
        userAgent     VARCHAR,
        userId        INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id            VARCHAR,
        num_songs          INTEGER,
        title              VARCHAR,
        artist_name        VARCHAR,
        artist_latitude    FLOAT,
        year               INTEGER,
        duration           FLOAT,
        artist_id          VARCHAR,
        artist_longitude   FLOAT,
        artist_location    VARCHAR
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
		songplay_id INT IDENTITY (1,1) PRIMARY KEY,
		start_time TIMESTAMP sortkey distkey,
		user_id INT NOT NULL,
		level VARCHAR,
		song_id VARCHAR,
		artist_id VARCHAR,
		session_id INT,
		location VARCHAR,
		user_agent TEXT
	);
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY sortkey, 
        first_name VARCHAR, 
        last_name VARCHAR, 
        gender CHAR(1), 
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
		song_id VARCHAR PRIMARY KEY sortkey,
		title  VARCHAR NOT NULL,
		artist_id  VARCHAR,
		year INT CHECK (year >= 0),
		duration FLOAT NOT NULL
	);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
		artist_id VARCHAR PRIMARY KEY sortkey,
		name VARCHAR NOT NULL,
		location VARCHAR,
		latitude FLOAT,
		longitude FLOAT
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
		start_time  TIMESTAMP PRIMARY KEY sortkey distkey,
		hour INT NOT NULL CHECK (hour >= 0),
		day INT NOT NULL CHECK (day >= 0),
		week INT NOT NULL CHECK (week >= 0),
		month INT NOT NULL CHECK (month >= 0),
		year INT NOT NULL CHECK (year >= 0),
		weekday VARCHAR NOT NULL
	);
""")

# STAGING TABLES
log_data = config.get("S3","LOG_DATA")
iam_role = config.get("IAM_ROLE","ARN")
log_jsonpath = config.get("S3", "LOG_JSONPATH")

staging_events_copy = ("""
    COPY staging_events 
    FROM {} 
    CREDENTIALS 'aws_iam_role={}' 
    FORMAT AS JSON {};
""").format(log_data, iam_role, log_jsonpath)

song_data = config.get("S3", "SONG_DATA")

staging_songs_copy = ("""
    COPY staging_songs 
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    FORMAT as JSON {};
""").format(song_data, iam_role, log_jsonpath)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, 
                            user_agent)
    SELECT DISTINCT (se.ts) AS start_time, 
            se.userId AS user_id, 
            se.level AS level, 
            se.song_id AS song_id, 
            se.artist_id AS artist_id, 
            se.sessionId AS session_id, 
            se.location AS location, 
            se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs  ss   ON (se.song = ss.title AND se.artist = ss.artist_name) 
                                AND se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT (userId) AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL AND page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT (song_id) AS song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT (artist_id) AS artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT (ts) AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(dayofweek FROM start_time) AS weekday
    FROM staging_events
    WHERE ts IS NOT NULL;;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
