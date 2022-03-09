# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Schema Enforcement & Evolution
# MAGIC ## Evolution of Data Being Ingested
# MAGIC It is not uncommon that the data being ingested into the EDSS will evolve over time. In this case, the simulated health tracker device has a new version available and the data being transmitted now indicates which type of device is being used.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration
# MAGIC 
# MAGIC Before you run this cell, make sure to add a unique user name to the file
# MAGIC `includes/configuration`, e.g.
# MAGIC 
# MAGIC ```
# MAGIC username = "yourfirstname_yourlastname"
# MAGIC ```

# COMMAND ----------

# MAGIC %run ./includes/configuration

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Health tracker data sample
# MAGIC 
# MAGIC ```
# MAGIC {"device_id":0,"heartrate":57.6447293596,"name":"Deborah Powell","time":1.5830208E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.6175546013,"name":"Deborah Powell","time":1.5830244E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.8486376876,"name":"Deborah Powell","time":1.583028E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":57.8821378637,"name":"Deborah Powell","time":1.5830316E9,"device_type":"version 2"}
# MAGIC {"device_id":0,"heartrate":59.0531490807,"name":"Deborah Powell","time":1.5830352E9,"device_type":"version 2"}
# MAGIC ```
# MAGIC This shows a sample of the health tracker data we will be using. Note that each line is a valid JSON object.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Health tracker data schema
# MAGIC The data has the following schema:
# MAGIC 
# MAGIC ```
# MAGIC name: string
# MAGIC heartrate: double
# MAGIC device_id: long
# MAGIC time: long
# MAGIC device_type: string
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Appending Files to an Existing Delta Table
# MAGIC Our goal is to append the next month of data.
# MAGIC ### Step 1: Load the Next Month of Data
# MAGIC We begin by loading the data from the file health_tracker_data_2020_3.json, using the .format("json") option as before.

# COMMAND ----------

file_path = health_tracker + "raw/health_tracker_data_2020_3.json"


health_tracker_data_2020_3_df = (
  spark.read
  .format("json")
  .load(file_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Data
# MAGIC We perform the same data engineering on the data:
# MAGIC - Use the from_unixtime Spark SQL function to transform the unixtime into a time string
# MAGIC - Cast the time column to type timestamp to replace the column time
# MAGIC - Cast the time column to type date to create the column dte

# COMMAND ----------

# MAGIC %sql
# MAGIC select from_unixtime(time)

# COMMAND ----------

from pyspark.sql.functions import col, from_unixtime
def process_health_tracker_data(dataframe):
    return (
     dataframe
     .select(
         from_unixtime("time").cast("date").alias("dte"),
         from_unixtime("time").cast("timestamp").alias("time"),
         "heartrate",
         "name",
         col("device_id").cast("integer").alias("p_device_id"),
         "device_type"
       )
     )
processedDF = process_health_tracker_data(health_tracker_data_2020_3_df)

# COMMAND ----------

display(processedDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Append the Data to the health_tracker_processed Delta table
# MAGIC We do this using `.mode("append")`.

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import lit

try:
  (
    processedDF.write
    .mode("append")
    .format("delta")
    .save(health_tracker + "processed")
  )
except AnalysisException as error:
  print("Analysis Exception:")
  print(error)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE add column

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema Mismatch
# MAGIC When we try to run this command, we receive the error shown below because there is a mismatch between the table and data schemas.
# MAGIC `AnalysisException: A schema mismatch detected when writing to the Delta table (Table ID: ...).`
# MAGIC 
# MAGIC To enable schema migration using DataFrameWriter or DataStreamWriter, please set: '.option("mergeSchema", "true")'.
# MAGIC 
# MAGIC For other operations, set the session configuration spark.databricks.delta.schema.autoMerge.enabled to "true". See the documentation specific to the operation for details.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is Schema Enforcement?
# MAGIC Schema enforcement, also known as schema validation, is a safeguard in Delta Lake that ensures data quality by rejecting writes to a table that do not match the table’s schema. Like the front desk manager at a busy restaurant that only accepts reservations, it checks to see whether each column in data inserted into the table is on its list of expected columns (in other words, whether each one has a “reservation”), and rejects any writes with columns that aren’t on the list.
# MAGIC Appending Files to an Existing Delta Table with Schema Evolution
# MAGIC In this case, we would like our table to accept the new schema and add the data to the table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## What Is Schema Evolution?
# MAGIC Schema evolution is a feature that allows users to easily change a table’s current schema to accommodate data that is changing over time. Most commonly, it’s used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns.
# MAGIC ### Step 1: Append the Data with Schema Evolution to the health_tracker_processed Delta table
# MAGIC We do this using .mode("append").

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE ADD COLUMN ...

# COMMAND ----------

# ANSWER
(processedDF.write
 .mode("append")
 .option("mergeSchema", True)
 .format("delta")
 .save(health_tracker + "processed"))


# COMMAND ----------

(spark.read
 .format("delta")
#  .option("versionAsOf", 1)
 .load(health_tracker + "processed")).printSchema()

# COMMAND ----------

(spark.read
 .format("delta")
 .option("versionAsOf", 1)
 .load(health_tracker + "processed")).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Commit
# MAGIC ### Step 1: Count the Most Recent Version

# COMMAND ----------

spark.read.table("health_tracker_processed").count()


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
