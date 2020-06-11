SELECT DISTINCT
    hour(ts) AS hour, 
    day(ts)  AS day, 
    weekofyear(ts) AS week,
    month(ts) AS month,
    year(ts) AS year,
    dayofweek(ts) AS weekday
FROM {table_name}
WHERE ts IS NOT NULL