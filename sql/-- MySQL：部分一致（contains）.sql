-- MySQL：部分一致（contains）
SELECT * FROM players WHERE name LIKE CONCAT('%', "I see", '%');

-- SELECT * FROM players WHERE tag LIKE CONCAT('%', "#8Y2Y0GYYG", '%');
