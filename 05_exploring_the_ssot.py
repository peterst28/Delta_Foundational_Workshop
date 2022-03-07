# Databricks notebook source
# MAGIC 
# MAGIC %md-sandbox
# MAGIC 
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploring the Single Source of Truth
# MAGIC Late-Arriving Data
# MAGIC In the previous lesson, we used Apache Spark to perform a batch update of the health_tracker_processed table. When the process was complete, we counted the number of records in the table. We discovered that some records were missing.
# MAGIC The absence of records from the last few days of the month shows a phenomenon that may often occur in a production data pipeline: late-arriving data. Delta Lake allows us to process data as it arrives and is prepared to handle the occurrence of late-arriving data.

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
# MAGIC #### Step 1: Count the Number of Records Per Device
# MAGIC Let’s run a query to count the number of records per device.
# MAGIC Recall that we will need to tell Spark that our format is a Delta table,
# MAGIC which we can do with our `.format()` method. Additionally, instead of passing in the path
# MAGIC as we did in previous notebooks, we need to pass in the health tracker variable.
# MAGIC Finally, we'll do a groupby and aggregation on our `p_device_id` column.
# MAGIC It looks like device 4 is missing 72 records.

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import count

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .groupby("p_device_id")
  .agg(count("*"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Plot the Missing Records
# MAGIC Let’s run a query to discover the timing of the missing records. We use a Databricks visualization to display the number of records per day. It appears that we have no records for device 4 for the last few days of the month.

# COMMAND ----------

from pyspark.sql.functions import col

display(
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .where(col("p_device_id").isin([3,4]))
)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Configuring the Visualization
# MAGIC Create a Databricks visualization to view the sensor counts by day.
# MAGIC We have used the following options to configure the visualization:
# MAGIC ```
# MAGIC Keys: dte
# MAGIC Series groupings: p_device_id
# MAGIC Values: heartrate
# MAGIC Aggregation: COUNT
# MAGIC Display Type: Bar Chart
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Broken Readings in the Table
# MAGIC Upon our initial load of data into the health_tracker_processed table, we noted that there are broken records in the data. In particular, we made a note of the fact that several negative readings were present even though it is impossible to record a negative heart rate.
# MAGIC Let’s assess the extent of these broken readings in our table.

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 1: Create Temporary View for Broken Readings
# MAGIC First, we create a temporary view for the Broken Readings in the health_tracker_processed table.
# MAGIC Here, we want to find the columns where `heartrate` is less than 0.

# COMMAND ----------

# ANSWER
broken_readings = (
  spark.read
  .format("delta")
  .load(health_tracker + "processed")
  .select(col("heartrate"), col("dte"))
  .where(col("heartrate") < 0)
  .groupby("dte")
  .agg(count("heartrate"))
  .orderBy("dte")
)
broken_readings.createOrReplaceTempView("broken_readings")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 2: Display broken_readings
# MAGIC Display the records in the broken_readings view, again using a Databricks visualization.
# MAGIC Note that most days have at least one broken reading and that some have more than one.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM broken_readings

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Step 3: Sum the Broken Readings
# MAGIC Next, we sum the records in the view.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(`count(heartrate)`) FROM broken_readings

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2020 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>
