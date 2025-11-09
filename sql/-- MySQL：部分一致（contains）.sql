-- MySQL：部分一致（contains）
-- SELECT * FROM players WHERE name LIKE CONCAT('%', "ZETA|L", '%');

SELECT * FROM players WHERE tag LIKE CONCAT('%', "#8LJJ0", '%');
