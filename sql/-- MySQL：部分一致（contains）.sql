-- MySQL：部分一致（contains）
-- SELECT * FROM players WHERE name LIKE CONCAT('%', "Naipi", '%');

SELECT * FROM players WHERE tag LIKE CONCAT('%', "#2POVOC", '%');
