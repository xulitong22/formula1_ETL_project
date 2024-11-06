-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

SELECT * , concat(driver_ref, '-', code ) AS new_driver_ref
FROM drivers

-- COMMAND ----------

SELECT * , SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surname
FROM drivers;

-- COMMAND ----------

SELECT * , current_timestamp()
FROM drivers;

-- COMMAND ----------

SELECT *, date_format(dob, 'dd-MM-yyyy')
FROM drivers;

-- COMMAND ----------

SELECT count(*)
FROM drivers;

-- COMMAND ----------

SELECT max(dob)
FROM drivers;

-- COMMAND ----------

SELECT *
FROM drivers
WHERE dob = '2000-05-11';

-- COMMAND ----------

SELECT count(*)
FROM drivers
WHERE nationality = 'British';

-- COMMAND ----------

SELECT nationality, count(*)
FROM drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, count(*)
FROM drivers
GROUP BY nationality
HAVING count(*) > 10
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_rank
FROM drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------

