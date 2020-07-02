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

# Fact
CREATE TABLE IF NOT EXISTS streams (
	event_stamp_date DATE,
	position INTEGER,
	track_id INTEGER,
	artist_name VARCHAR,
	region_name VARCHAR,
	stream_count INTEGER
	);

# Dimension
CREATE TABLE IF NOT EXISTS tracks (
	track_id INTEGER IDENTITY(1, 1),
	track_url VARCHAR,
	track_name VARCHAR,
	duration INTEGER,
	popularity INTEGER,
	explicit BOOLEAN
	)sortkey(track_id);