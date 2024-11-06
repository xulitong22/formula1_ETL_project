# Databricks notebook source
# MAGIC %md
# MAGIC ### Accessing dataframes using SQL
# MAGIC #### Objectives
# MAGIC 1. Create temporary view on dataframes
# MAGIC 2. Accesse the view from SQL cell
# MAGIC 3. Accesse the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019 = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Global temporary view
# MAGIC 1. Create global temporary view on dataframes
# MAGIC 2. Accesse the view from SQL cell
# MAGIC 3. Accesse the view from python cell

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * \
FROM global_temp.gv_race_results").show()

# COMMAND ----------

