import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP table IF EXISTS staging_events"
staging_songs_table_drop = "DROP table IF EXISTS staging_songs"
songplay_table_drop = "DROP table IF EXISTS songplays"
user_table_drop = "DROP table IF EXISTS users"
song_table_drop = "DROP table IF EXISTS songs"
artist_table_drop = "DROP table IF EXISTS artists"
time_table_drop = "DROP table IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist VARCHAR(200),
        auth VARCHAR(200),
        first_name VARCHAR(200),
        gender VARCHAR(10),
        item_in_session INT,
        last_name VARCHAR(200),
        length NUMERIC,
        level VARCHAR(200),                        
        location VARCHAR(300),
        method VARCHAR(10),
        page VARCHAR(300),
        registration DOUBLE PRECISION,
        session_id INT,
        song VARCHAR(300),
        status INT,
        ts TIMESTAMP,
        user_agent VARCHAR(300),
        user_id INT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        song_id VARCHAR(200),
        num_songs INT,
        title VARCHAR(300),
        artist_name VARCHAR(300),
        artist_latitude NUMERIC,
        year INT,
        duration NUMERIC,
        artist_id VARCHAR(200),
        artist_longitude NUMERIC,
        artist_location VARCHAR(300)
    );
""")


songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT IDENTITY(0,1) PRIMARY KEY,
        start_time TIMESTAMP NOT NULL,
        user_id INT NOT NULL REFERENCES users(user_id),
        level VARCHAR(200),
        song_id VARCHAR(200) NOT NULL REFERENCES songs(song_id) distkey,
        artist_id VARCHAR(200) NOT NULL REFERENCES artists(artist_id) sortkey,
        session_id INT,
        location VARCHAR(300),
        user_agent VARCHAR(300)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY NOT NULL sortkey,
        first_name VARCHAR,
        last_name VARCHAR,
        gender VARCHAR,
        level VARCHAR
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id VARCHAR PRIMARY KEY NOT NULL distkey sortkey,
        title VARCHAR,
        artist_id VARCHAR,
        year NUMERIC,
        duration NUMERIC
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id VARCHAR PRIMARY KEY NOT NULL sortkey,
        artist_name VARCHAR,
        artist_location VARCHAR,
        artist_latitude NUMERIC,
        artist_longitude NUMERIC
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY NOT NULL,
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
    copy staging_events from {}
        credentials 'aws_iam_role={}'
        compupdate off region 'us-west-2' FORMAT AS JSON {}
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """).format(config['S3'].get('LOG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"),
            config['S3'].get('LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs
        from {}
        credentials 'aws_iam_role={}'
        compupdate off region 'us-west-2'
        FORMAT AS JSON 'auto'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
    """).format(config['S3'].get('SONG_DATA'),
            config['IAM_ROLE'].get('ARN').strip("'"))     


# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (
        start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT 
        TO_TIMESTAMP(TO_CHAR(e.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
        e.user_id, 
        e.level, 
        s.song_id,
        s.artist_id, 
        e.session_id, 
        s.artist_location,
        e.user_agent
    FROM staging_songs s
    JOIN staging_events e 
    ON s.title = e.song AND s.artist_name = e.artist AND s.duration = e.length;
""")


user_table_insert = ("""
    INSERT INTO users
        (user_id, first_name, last_name, gender, level)
    SELECT
        DISTINCT user_id, first_name, last_name, gender, level
    FROM staging_events
    WHERE user_id IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs 
        (song_id, title, artist_id, year, duration)
    SELECT
        DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists 
        (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    SELECT
        DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time 
        (start_time, hour, day, week, month, year, weekday)
    SELECT 
        DISTINCT ts, 
            EXTRACT(hour from ts), 
            EXTRACT(day from ts),
            EXTRACT(week from ts), 
            EXTRACT(month from ts),
            EXTRACT(year from ts), 
            EXTRACT(weekday from ts)
    FROM staging_events
    WHERE ts IS NOT NULL;
""")


# QUERY LISTS

create_table_queries = [staging_events_table_create,
                        staging_songs_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create,
                        songplay_table_create]
drop_table_queries = [staging_events_table_drop,
                      staging_songs_table_drop,
                      songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert,
                        user_table_insert,
                        song_table_insert,
                        artist_table_insert,
                        time_table_insert]



# # Validate row counts
# staging_events_count = "SELECT COUNT(*) FROM staging_events"
# staging_songs_count = "SELECT COUNT(*) FROM staging_songs"

# songplay_table_count = 'SELECT COUNT(*) FROM songplays'
# user_table_count = 'SELECT COUNT(*) FROM users'
# song_table_count = 'SELECT COUNT(*) FROM songs'
# artist_table_count = 'SELECT COUNT(*) FROM artists'
# time_table_count = 'SELECT COUNT(*) FROM time'


# # Row Counts
# staging_tables_counts = [staging_events_count, staging_songs_count]
# analytics_tables_counts = [songplay_table_count, user_table_count, song_table_count, artist_table_count, time_table_count]
