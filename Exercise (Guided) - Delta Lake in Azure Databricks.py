# Databricks notebook source
# MAGIC %md
# MAGIC # Follow steps from the MS Learning Exercise
# MAGIC https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/03-Delta-lake-in-Azure-Databricks.html
# MAGIC
# MAGIC Read more about dataframes: https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes#language-Python
# MAGIC
# MAGIC It requires Github for Cluster and worksapce setup:
# MAGIC git clone https://github.com/MicrosoftLearning/mslearn-databricks
# MAGIC
# MAGIC And access to Free data from :
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/devices1.json
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/devices2.json

# COMMAND ----------

# DBTITLE 1,Shell commands to download data files to DBFS path
# MAGIC %sh
# MAGIC rm -r /dbfs/delta_lab
# MAGIC mkdir /dbfs/delta_lab
# MAGIC wget -O /dbfs/delta_lab/products.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv

# COMMAND ----------

# DBTITLE 1,load the data from the files into DataFrame without schema
df = spark.read.load('/delta_lab/products.csv', format='csv', header=True)
display(df.limit(10))

# COMMAND ----------

# DBTITLE 1,Load the file data into a delta table
delta_table_path = "/delta/products-delta"
df.write.format("delta").save(delta_table_path)

# COMMAND ----------

# DBTITLE 1,shell commands to view the contents of the delta folder
# MAGIC  %sh
# MAGIC  ls /dbfs/delta/products-delta

# COMMAND ----------

# DBTITLE 1,view and update as DeltaTable object
from delta.tables import *
from pyspark.sql.functions import *
   
# Create a deltaTable object
deltaTable = DeltaTable.forPath(spark, delta_table_path)
# Update the table (reduce price of product 771 by 10%)
deltaTable.update(
    condition = "ProductID == 771",
    set = { "ListPrice": "ListPrice * 0.9" })
# View the updated data as a dataframe
deltaTable.toDF().show(10)

# COMMAND ----------

# DBTITLE 1,DataFrame from Delta
new_df = spark.read.format("delta").load(delta_table_path)
new_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explore logging and time-travel

# COMMAND ----------

# DBTITLE 1,original version of the product data
new_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
new_df.show(10)

# COMMAND ----------

# DBTITLE 1,the last 10 changes from log
deltaTable.history(10).show(10, False, True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create catalog tables

# COMMAND ----------

# DBTITLE 1,create a new database and then creates an external table in that database based on the path to the Delta files
spark.sql("CREATE DATABASE AdventureWorks")
spark.sql("CREATE TABLE AdventureWorks.ProductsExternal USING DELTA LOCATION '{0}'".format(delta_table_path))
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsExternal").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,query the table
# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SELECT * FROM ProductsExternal;

# COMMAND ----------

# DBTITLE 1,Create a managed table
df.write.format("delta").saveAsTable("AdventureWorks.ProductsManaged")
spark.sql("DESCRIBE EXTENDED AdventureWorks.ProductsManaged").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Compare external and managed tables
# MAGIC  %sh
# MAGIC  echo "External table:"
# MAGIC  ls /dbfs/delta/products-delta
# MAGIC  echo
# MAGIC  echo "Managed table:"
# MAGIC  ls /dbfs/user/hive/warehouse/adventureworks.db/productsmanaged

# COMMAND ----------

# DBTITLE 1,delete both tables from the database
# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC DROP TABLE IF EXISTS ProductsExternal;
# MAGIC DROP TABLE IF EXISTS ProductsManaged;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %md
# MAGIC Now rerun the cell (15) containing the following code to view the contents of the delta folders. The files for the managed table are deleted automatically when the table is dropped. However, the files for the external table remain. Dropping an external table only removes the table metadata from the database; it does not delete the data files.

# COMMAND ----------

# DBTITLE 1,create a new table based on the delta files in the products-delta folder
# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC CREATE TABLE Products
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/products-delta';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE AdventureWorks;
# MAGIC SELECT * FROM Products;

# COMMAND ----------

# MAGIC %md
# MAGIC Because the table is based on the existing delta files, which include the logged history of changes, it reflects the modifications you previously made to the products data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use delta tables for streaming data

# COMMAND ----------

# DBTITLE 1,Download  device data is in JSON format
# MAGIC  %sh
# MAGIC  rm -r /dbfs/device_stream
# MAGIC  mkdir /dbfs/device_stream
# MAGIC  wget -O /dbfs/device_stream/devices1.json https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/devices1.json

# COMMAND ----------

# DBTITLE 1,create a stream based on JSON device data
from pyspark.sql.types import *
from pyspark.sql.functions import *
   
# Create a stream that reads data from the folder, using a JSON schema
inputPath = '/device_stream/'
jsonSchema = StructType([
StructField("device", StringType(), False),
StructField("status", StringType(), False)
])
iotstream = spark.readStream.schema(jsonSchema).option("maxFilesPerTrigger", 1).json(inputPath)
print("Source stream created...")

# COMMAND ----------

# DBTITLE 1,write the stream of data to a delta folder
# Write the stream to a delta table
delta_stream_table_path = '/delta/iotdevicedata'
checkpointpath = '/delta/checkpoint'
deltastream = iotstream.writeStream.format("delta").option("checkpointLocation", checkpointpath).start(delta_stream_table_path)
print("Streaming to delta sink...")

# COMMAND ----------

# DBTITLE 1,Read the data in delta format into a dataframe
df = spark.read.format("delta").load(delta_stream_table_path)
display(df)

# COMMAND ----------

# DBTITLE 1,create a table based on the delta folder to which the streaming data is being written
# create a catalog table based on the streaming sink
spark.sql("CREATE TABLE IotDeviceData USING DELTA LOCATION '{0}'".format(delta_stream_table_path))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM IotDeviceData;

# COMMAND ----------

# DBTITLE 1,Add more streaming data
# MAGIC  %sh
# MAGIC  wget -O /dbfs/device_stream/devices2.json https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/devices2.json

# COMMAND ----------

# DBTITLE 1,stop the stream
deltastream.stop()
