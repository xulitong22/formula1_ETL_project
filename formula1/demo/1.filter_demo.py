# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter in sql way

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter in pythonic style

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------
