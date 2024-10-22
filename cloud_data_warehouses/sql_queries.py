import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

drop_table_query = """
    DROP TABLE IF EXISTS {table};
"""

# CREATE TABLES

staging_events_table_create = (
    """
        CREATE TABLE staging_events (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR(1),
            iteminSession INTEGER,
            lastName VARCHAR,
            length FLOAT,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration FLOAT,
            sessionId INTEGER,
            song VARCHAR,
            status INTEGER,
            ts BIGINT,
            userAgent VARCHAR,
            userId INTEGER
        );
    """
)

staging_songs_table_create = (
    """
        CREATE TABLE staging_songs (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_latitude FLOAT,
            artist_longitude FLOAT,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration FLOAT,
            year INTEGER
        );
    """
)

songplay_table_create = (
    """
        CREATE TABLE songplays (
            songplay_id INTEGER IDENTITY(0,1) PRIMARY KEY,
            start_time TIMESTAMP,
            user_id INTEGER NOT NULL,
            level VARCHAR,
            song_id VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            session_id INTEGER NOT NULL,
            location VARCHAR,
            user_agent VARCHAR
        );
    """
)

user_table_create = (
    """
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            gender VARCHAR,
            level VARCHAR NOT NULL
        );
    """
)

song_table_create = (
    """
        CREATE TABLE songs (
            song_id VARCHAR PRIMARY KEY,
            title VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            year INTEGER,
            duration FLOAT
        );
    """
)

artist_table_create = (
    """
        CREATE TABLE artists (
            artist_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            location VARCHAR,
            latitude FLOAT,
            longitude FLOAT
        );
    """
)

time_table_create = (
    """
        CREATE TABLE timestamps (
            start_time TIMESTAMP PRIMARY KEY,
            hour INTEGER NOT NULL,
            day INTEGER NOT NULL,
            week INTEGER NOT NULL,
            month INTEGER NOT NULL,
            year INTEGER NOT NULL,
            weekday INTEGER NOT NULL
        );
    """
)

# STAGING TABLES

staging_events_copy = (
    """
        COPY staging_events
        FROM {log_data}
        IAM_ROLE '{iam}'
        JSON {json_path}
    """
).format(
    log_data=config.get("S3", "LOG_DATA"),
    json_path=config.get("S3", "LOG_JSONPATH"),
    iam=config.get("DWH", "DWH_ROLE_ARN")
)

staging_songs_copy = (
    """
        COPY staging_songs
        FROM {song_data}
        IAM_ROLE '{iam}'
        JSON 'auto';
    """
).format(
    song_data=config.get("S3", "SONG_DATA"),
    iam=config.get("DWH", "DWH_ROLE_ARN")
)

# FINAL TABLES

songplay_table_insert = (
    """
        INSERT INTO songplays (
            start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        )(
            SELECT DISTINCT
                TIMESTAMP 'epoch' + e.ts::INT8/1000 * INTERVAL '1 second' AS start_time,
                e.userID AS user_id,
                e.level,
                s.song_id AS song_id,
                s.artist_id AS artist_id,
                e.sessionId AS session_id,
                e.location,
                e.userAgent AS user_agent
            FROM staging_events e
            LEFT JOIN staging_songs s
                ON e.song = s.title
                AND e.artist = s.artist_name
            WHERE e.ts IS NOT NULL
                AND e.userID IS NOT NULL
                AND e.song IS NOT NULL
                AND e.sessionID IS NOT NULL
                AND s.song_id IS NOT NULL
                AND s.artist_id IS NOT NULL
                AND e.page='NextSong'
        );
    """)

user_table_insert = (
    """
        INSERT INTO users (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )(
            SELECT DISTINCT
                user_id,
                first_name,
                last_name,
                gender,
                level
            FROM (
                SELECT
                    userId AS user_id,
                    firstName AS first_name,
                    lastName AS last_name,
                    gender,
                    level,
                    row_number() OVER (PARTITION BY user_id ORDER BY ts) AS user_id_ranked
                FROM staging_events
                WHERE userId IS NOT NULL
                    AND firstName IS NOT NULL
                    AND lastName IS NOT NULL
                    AND level IS NOT NULL
                    AND page='NextSong'
                ) AS ranked
            WHERE ranked.user_id_ranked = 1

        );
    """
)

song_table_insert = (
    """
        INSERT INTO songs (
            song_id,
            title,
            artist_id,
            year,
            duration
        )(
            SELECT DISTINCT
                song_id,
                title,
                artist_id,
                year,
                duration
            FROM (
                SELECT 
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration,
                    row_number() OVER (PARTITION BY song_id) AS song_id_ranked
                FROM staging_songs
                WHERE song_id IS NOT NULL
                    AND title IS NOT NULL
                    AND artist_id IS NOT NULL
                ) AS ranked
            WHERE ranked.song_id_ranked = 1

        );
    """
)

artist_table_insert = (
    """
        INSERT INTO artists (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )(
            SELECT DISTINCT
                artist_id,
                name,
                location,
                latitude,
                longitude            
            FROM (
                SELECT
                    artist_id,
                    artist_name AS name,
                    artist_location AS location,
                    artist_latitude AS latitude,
                    artist_longitude AS longitude,
                    row_number() OVER (PARTITION BY artist_id) AS artist_id_ranked
                FROM staging_songs
                WHERE artist_id IS NOT NULL
                    AND artist_name IS NOT NULL
                ) AS ranked
            WHERE ranked.artist_id_ranked = 1

        );
    """)

time_table_insert = (
    """
        INSERT INTO timestamps (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )(
            SELECT DISTINCT
                TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' AS start_time,
                EXTRACT(h FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS hour,
                EXTRACT(d FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS day,
                EXTRACT(w FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS week,
                EXTRACT(mon FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS month,
                EXTRACT(y FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS year,
                EXTRACT(dow FROM TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second') AS weekday
            FROM staging_events
            WHERE ts IS NOT NULL
        );        
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
