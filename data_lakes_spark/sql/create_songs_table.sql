SELECT
    song_id,
    title,
    artist_id,
    CASE WHEN year = 0 THEN NULL ELSE year END AS year,
    duration
FROM {table_name}
WHERE song_id IS NOT NULL
    AND title IS NOT NULL
    AND artist_id IS NOT NULL