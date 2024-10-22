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
    track_name VARCHAR,
    artist_name VARCHAR,
    track_url VARCHAR NOT NULL,
	album_name VARCHAR,
	album_type VARCHAR,
	album_release_date VARCHAR,
	popularity INTEGER,
	duration INTEGER,
	explicit BOOLEAN
)sortkey(track_url);

CREATE TABLE IF NOT EXISTS streams (
	event_stamp_date DATE,
	position INTEGER,
	track_id INTEGER,
	artist_id INTEGER,
    album_id INTEGER,
	region_id INTEGER,
	stream_count INTEGER
	);

CREATE TABLE IF NOT EXISTS tracks (
	track_id INTEGER IDENTITY(1, 1),
	track_url VARCHAR,
	track_name VARCHAR,
	duration INTEGER,
	popularity INTEGER,
	explicit BOOLEAN
	)sortkey(track_id);

CREATE TABLE IF NOT EXISTS artists (
	artist_id INTEGER IDENTITY(1, 1),
	artist_name VARCHAR
	)sortkey(artist_id);    

CREATE TABLE IF NOT EXISTS albums (
	album_id INTEGER IDENTITY(1, 1),
	album_name VARCHAR,
	album_type VARCHAR,
	release_date VARCHAR
	)sortkey(album_id);

CREATE TABLE IF NOT EXISTS regions (
	region_id INTEGER IDENTITY(1, 1),
	region_name VARCHAR
	)sortkey(region_id);