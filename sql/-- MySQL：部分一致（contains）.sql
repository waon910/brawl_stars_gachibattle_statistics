-- MySQL：部分一致（contains）
SELECT * FROM players WHERE name LIKE CONCAT('%', "see", '%');

-- SELECT * FROM players WHERE tag LIKE CONCAT('%', "#8JYYJ80YJ", '%');
