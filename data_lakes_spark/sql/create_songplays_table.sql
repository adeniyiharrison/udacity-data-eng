SELECT
    ns.startTime AS start_time,
    year(ns.ts) AS year,
    month(ns.ts) AS month,
    ns.userId AS user_id,
    ns.level,
    s.song_id,
    s.artist_id,
    ns.sessionId AS session_id,
    ns.location,
    userAgent AS user_agent
FROM next_songs ns
LEFT JOIN songs_data s
    ON ns.song = s.title
WHERE s.artist_id IS NOT NULL
    AND s.title IS NOT NULL