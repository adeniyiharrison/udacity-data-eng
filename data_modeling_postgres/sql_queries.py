# DROP TABLES

songplay_table_drop = (
    """
        DROP TABLE IF EXISTS songplays
    """
    )

user_table_drop = (
    """
        DROP TABLE IF EXISTS users
    """
    )

song_table_drop = (
    """
        DROP TABLE IF EXISTS songs
    """
    )
artist_table_drop = (
    """
        DROP TABLE IF EXISTS artists
    """
    )
time_table_drop = (
    """
        DROP TABLE IF EXISTS timestamps
    """
    )

# CREATE TABLES

songplay_table_create = (
    """
        CREATE SEQUENCE IF NOT EXISTS songplay_seq;

        CREATE TABLE songplays (
            songplay_id integer NOT NULL DEFAULT nextval('songplay_seq') ,
            start_time timestamp,
            user_id int,
            level varchar,
            song_id varchar,
            artist_id varchar,
            session_id integer,
            location varchar,
            user_agent varchar
        )
    """
)

user_table_create = (
    """
        CREATE TABLE users (
            user_id int PRIMARY KEY,
            first_name varchar NOT NULL,
            last_name varchar NOT NULL,
            gender varchar,
            level varchar NOT NULL
        )
    """
)

song_table_create = (
    """
        CREATE TABLE songs (
            song_id varchar PRIMARY KEY,
            title varchar NOT NULL,
            artist_id varchar NOT NULL,
            year integer,
            duration float
        )
    """
)

artist_table_create = (
    """
        CREATE TABLE artists (
            artist_id varchar PRIMARY KEY,
            name varchar NOT NULL,
            location varchar,
            latitude float,
            longitude float
        )
    """
)

time_table_create = (
    """
        CREATE TABLE timestamps (
            start_time timestamp PRIMARY KEY,
            hour integer NOT NULL,
            day integer NOT NULL,
            week integer NOT NULL,
            month integer NOT NULL,
            year integer NOT NULL,
            weekday integer NOT NULL
        )
    """
)

# INSERT RECORDS

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
        )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
)

user_table_insert = (
    """
        INSERT INTO users VALUES
        (%s, %s, %s, %s, %s)
        ON CONFLICT (user_id)
        DO UPDATE SET level=EXCLUDED.level;
    """
)

song_table_insert = (
    """
        INSERT INTO songs VALUES
        (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
)

artist_table_insert = (
    """
        INSERT INTO artists VALUES
        (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """
)


time_table_insert = (
    """
        INSERT INTO timestamps VALUES
        (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
    """)

# FIND SONGS

song_select = (
    """
        SELECT
            s.song_id,
            a.artist_id
        FROM songs s
        LEFT JOIN artists a
            ON s.artist_id = a.artist_id
        WHERE s.title = %s
            AND a.name = %s
            AND s.duration = %s
    """
)

# QUERY LISTS

create_table_queries = [
    songplay_table_create, user_table_create, song_table_create,
    artist_table_create, time_table_create
]
drop_table_queries = [
    songplay_table_drop, user_table_drop, song_table_drop,
    artist_table_drop, time_table_drop
]