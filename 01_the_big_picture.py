# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # The Big Picture: the Lakehouse
# MAGIC 
# MAGIC ![Lakehouse](https://s3-us-west-2.amazonaws.com/files.training.databricks.com/images/delta-lake-hands-on/Intro+to+Delta+Lake.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building a Lakehouse with Delta Lake
# MAGIC In the Databricks Academy course *Fundamentals of Delta Lake*,
# MAGIC we discussed Enterprise Decision Support Systems (EDSS) and their use
# MAGIC in Online Analytics Processing (OLAP).
# MAGIC 
# MAGIC In particular, we reviewed how using Delta Lake technology can help
# MAGIC build a robust Lakehouse, a single sources of truth to help organizations
# MAGIC make intelligent, data-driven business decisions.
# MAGIC 
# MAGIC In this course, we go from theory to practice.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## An Internet of Things Data Ingestion Pipeline
# MAGIC In this course, we will use Apache Spark and Delta Lake to:
# MAGIC - Ingest data
# MAGIC - Create a table that will serve as a single source of truth
# MAGIC - Build a downstream aggregate table on this single source of truth
# MAGIC - Use Delta Lake to perform operations  on the table to make our data more robust

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Internet of Things (IoT) Data
# MAGIC 
# MAGIC The domain of this course is the Internet of Things (IoT).
# MAGIC In particular, we will be using simulated health tracker data passing
# MAGIC easurements of a user’s heart rate once an hour. Such a pipeline might appear as follows:

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC <img
# MAGIC      alt="Big Picture OLTP OLAP"
# MAGIC      src=https://files.training.databricks.com/images/delta-lake-hands-on/01_big_pic_01-oltp_olap.jpg
# MAGIC      width=600px
# MAGIC >

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Components of The System
# MAGIC 
# MAGIC - High Flux Event Data - Heart rates are recorded by user devices.
# MAGIC - Operational Data Store (ODS) - The transactional database records all measurements.
# MAGIC - ETL - On some timeframe, this data is made available to an ETL process for loading into an EDSS.
# MAGIC - Enterprise Decision Support System (EDSS) - Data is loaded into a Delta table and is the Single Source of Truth.
# MAGIC - Online Analytics Processing (OLAP) - Delta Lake is used to manage and analyze the Single Source of Truth and downstream aggregate tables.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Next, we'll review how to configure your Databricks Workspace to perform these workflows.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
