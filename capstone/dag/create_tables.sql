CREATE TABLE IF NOT EXISTS streams_staging (
    position INTEGER NOT NULL,
    track_name VARCHAR,
    artist VARCHAR,
    streams INTEGER,
    url VARCHAR,
    date DATE,
    region VARCHAR
);

CREATE TABLE IF NOT EXISTS tracks_metadata (
	album_name VARCHAR,
	song_uri VARCHAR,
	album_type VARCHAR,
	album_release_date VARCHAR,
	popularity INTEGER,
	duration INTEGER,
	explicit BOOLEAN
);