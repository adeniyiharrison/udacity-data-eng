SELECT DISTINCT
    userID AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM {table_name}
WHERE userID IS NOT NULL
    AND userID != ''