SELECT
    artist_id,
    artist_name AS name,
    artist_location AS location,
    CASE WHEN artist_latitude = 'null' THEN NULL ELSE artist_latitude END latitude,
    CASE WHEN artist_longitude = 'null' THEN NULL ELSE artist_longitude END longitude
FROM {table_name}
WHERE artist_id IS NOT NULL
    AND artist_name IS NOT NULL