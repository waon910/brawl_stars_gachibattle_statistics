-- MySQL：部分一致（contains）
-- SELECT * FROM players WHERE name LIKE CONCAT('%', "CR|Milk", '%');
SELECT * FROM players WHERE tag LIKE CONCAT('%', "#2C8L29PRL", '%');
