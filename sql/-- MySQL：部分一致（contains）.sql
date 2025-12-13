-- Active: 1765505710054@@127.0.0.1@3306@brawl_stats
-- MySQL：部分一致（contains）
-- SELECT * FROM players WHERE name LIKE CONCAT('%', "まっくろくろすけ", '%');
SELECT * FROM players WHERE tag LIKE CONCAT('%', "#8PRCYCL", '%');
