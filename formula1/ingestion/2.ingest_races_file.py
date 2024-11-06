# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step1 - Read the csv file using the spark dataframe read

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Rename the columns as required

# COMMAND ----------

races_renamed_df = races_df \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id") 

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col, when

# COMMAND ----------

races_with_timestamp_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit (" "), when(col("time").like("%N%"), lit("00:00:00")
                         ).otherwise(col("time"))
    ), "yyyy-MM-dd HH:mm:ss")).withColumn("data_source", lit(v_data_source)) \
        .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Select required columns

# COMMAND ----------

races_selected_df = races_with_ingestion_date_df.select(col("race_id"), col("race_year"),
col("round"), col("circuit_id"), col("name"), col("ingestion_date"), col("race_timestamp"), col("data_source"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to the datalake as parquet

# COMMAND ----------

races_selected_df.write.mode("overwrite").partitionBy("race_year").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")