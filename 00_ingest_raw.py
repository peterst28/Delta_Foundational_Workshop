# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Raw Data Retrieval

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Objective
# MAGIC 
# MAGIC In this notebook we:
# MAGIC 
# MAGIC 1. Ingest data from a remote source into our source directory, `rawPath`.

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
# MAGIC ## Make Course Idempotent

# COMMAND ----------

dbutils.fs.rm(health_tracker, recurse=True)

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_processed
""")

spark.sql(f"""
DROP TABLE IF EXISTS health_tracker_gold_aggregate_heartrate
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieve First Month of Data
# MAGIC 
# MAGIC Next, we use the utility function, `retrieve_data` to
# MAGIC retrieve the files we will ingest. The function takes
# MAGIC three arguments:
# MAGIC 
# MAGIC - `year: int`
# MAGIC - `month: int`
# MAGIC - `rawPath: str`
# MAGIC - `is_late: bool` (optional)

# COMMAND ----------

retrieve_data(2020, 1, health_tracker + "raw/")
retrieve_data(2020, 2, health_tracker + "raw/")
retrieve_data(2020, 2, health_tracker + "raw/", is_late=True)
retrieve_data(2020, 3, health_tracker + "raw/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Expected File
# MAGIC 
# MAGIC The expected file has the following name:

# COMMAND ----------

file_2020_1 = "health_tracker_data_2020_1.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(health_tracker + "raw/"))

# COMMAND ----------

# MAGIC %md
# MAGIC **Exercise:** Write an Assertion Statement to Verify File Ingestion
# MAGIC 
# MAGIC Note: the `print` statement would typically not be included in production code, nor in code used to test this notebook.

# COMMAND ----------

# ANSWER
assert file_2020_1 in [
    item.name for item in dbutils.fs.ls(health_tracker + "raw/")
], "File not present in Raw Path"
print("Assertion passed.")


# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
