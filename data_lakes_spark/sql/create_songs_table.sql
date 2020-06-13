SELECT DISTINCT
    song_id,
    title,
    artist_id,
    artist_name,
    CASE WHEN year = 0 THEN NULL ELSE year END AS year,
    duration
FROM {table_name}
