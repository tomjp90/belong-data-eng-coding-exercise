--check only distinct values within range
SELECT DISTINCT `sensor_id`,
FROM sensor_id;

SELECT DISTINCT `year`,
FROM ped_counts;

SELECT DISTINCT `month`,
FROM ped_counts;

SELECT DISTINCT `mdate`,
FROM ped_counts;

SELECT DISTINCT `time`,
FROM ped_counts;

SELECT DISTINCT `day`,
FROM ped_counts;

-- check IDs are only unique
SELECT CASE WHEN count(DISTINCT `id`) = count(`id`)
THEN 'Column values are unique' 
ELSE 'Column values are NOT unique' 
END
FROM ped_counts;

