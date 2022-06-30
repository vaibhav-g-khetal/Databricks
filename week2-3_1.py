# Databricks notebook source
# MAGIC %md
# MAGIC ### Assignement 1

# COMMAND ----------

# 1. Create a Spark data frame using the above file

file_path = "/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv"

fire_df = spark.read.format("csv")\
                .option("inferschema", "true")\
                .option("header", "true") \
                .load(file_path)

display(fire_df)

# COMMAND ----------

# 2. Create a Global Temporary View using the above data frame. Assume the view name is fire_service_calls_view.

fire_df.createGlobalTempView("fire_service_calls_view")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.fire_service_calls_view
