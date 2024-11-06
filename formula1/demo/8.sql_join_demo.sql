-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standings;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM driver_standings_2018;

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM driver_standings_2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Inner join

-- COMMAND ----------

SELECT *
FROM driver_standings_2018 d_2018
JOIN driver_standings_2020 d_2020
ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Left join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  LEFT JOIN driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Right join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  RIGHT JOIN driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Full join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  FULL JOIN driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Semi join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  SEMI JOIN driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Anti join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  ANTI JOIN driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cross join

-- COMMAND ----------

SELECT *
  FROM driver_standings_2018 d_2018
  CROSS JOIN driver_standings_2020 d_2020;

-- COMMAND ----------

