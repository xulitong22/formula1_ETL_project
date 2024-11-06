-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Managed Table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create managed table using SQL

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop managed table

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### External table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create external table using python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet") \
-- MAGIC     .option("path", f"{presentation_folder_path}/race_results_ext_py") \
-- MAGIC     .saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Create external table using SQL

-- COMMAND ----------

/* CREATE TABLE demo.race_results_ext_sql(
  Schema
)
USING parquet
LOCATION '/mnt/formula1proj123/presentation/race_results_ext_sql'; */

-- COMMAND ----------

/* INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020; */

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Drop the external table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### View on tables

-- COMMAND ----------

