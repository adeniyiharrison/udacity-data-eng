CREATE TABLE IF NOT EXISTS tracks_staging (
    song_uri VARCHAR NOT NULL PRIMARY KEY,
    song_name VARCHAR,
    artist_name VARCHAR,
    album_type VARCHAR,
    album_release_date VARCHAR,
    album_name VARCHAR,
    popularity VARCHAR
) sortkey("song_uri");