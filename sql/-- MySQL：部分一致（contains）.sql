-- MySQL：部分一致（contains）
SELECT * FROM players WHERE name LIKE CONCAT('%', "CR|Ten", '%');