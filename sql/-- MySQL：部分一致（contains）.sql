-- MySQL：部分一致（contains）
-- SELECT * FROM players WHERE name LIKE CONCAT('%', "CR|Milk", '%');
SELECT * FROM players WHERE tag LIKE CONCAT('%', "#UR2UL8YR", '%');
