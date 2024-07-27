# Databricks notebook source
sales_df = spark.read.csv('/FileStore/tables/sales_data_sample.csv', header=True, inferSchema=True)

# COMMAND ----------

sales_df.createOrReplaceTempView('sales_data')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Save the DataFrame as a table using SQL
# MAGIC CREATE TABLE sales_data AS
# MAGIC SELECT * FROM sales_data
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from sales_data;
