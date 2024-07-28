# Databricks notebook source
# MAGIC %md
# MAGIC 1. Load data into DBFS first from:
# MAGIC https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv
# MAGIC
# MAGIC 2. Also create a Hive Metastore default schema table call Products witht eh same data

# COMMAND ----------

# MAGIC %sql
# MAGIC --Using SQL to directly query the hive table and then creating a visualization:
# MAGIC SELECT * FROM `hive_metastore`.`default`.`products`;

# COMMAND ----------

#Using spark.sql to do the same as above and saving to data frame:
dfsql = spark.sql("SELECT * FROM products")
display(dfsql)

# COMMAND ----------

#reading directly from a file in DBFS
dffs = spark.read.load('/FileStore/tables/products.csv',
    format='csv',
    header=True
)
display(dffs)

# COMMAND ----------

# MAGIC %md
# MAGIC Slicing and dicing of data

# COMMAND ----------

#display the first 10 rows
display(dffs.limit(10))
display(dfsql.limit(10))
#note the difference in the datatype of the 2 outputs. File based DataFrame did not get the ProductID and ListPrice datatypes correctly. 

# COMMAND ----------

#display data for particular category
dfsql2 = dfsql.filter("Category == 'Road Bikes'")
display(dfsql2)
dffs2 = dffs.filter("Category == 'Road Bikes'")
display(dffs2.limit(10))

# COMMAND ----------

#Filtering and grouping dataframes
pricelist_df = dffs.select("ProductID", "ListPrice") # or dffs["ProductID", "ListPrice"]
display(pricelist_df.limit(15))
display(pricelist_df.count())

# COMMAND ----------

#chain methods together to perform a series of manipulations that results in a transformed dataframe.
bikes_df = dffs["ProductName", "ListPrice"].where((dffs["Category"]=="Mountain Bikes") | (dffs["Category"]=="Road Bikes"))
display(bikes_df)

# COMMAND ----------

#To group and aggregate data, you can use the groupBy method and aggregate functions.
counts_df = dffs.select("ProductID", "Category").groupBy("Category").count()
display(counts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Specifying a dataframe schema while working with DBFS files

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

productSchema = StructType([
    StructField("ProductID", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("Category", StringType()),
    StructField("ListPrice", FloatType())
    ])

dfs = spark.read.load('/FileStore/tables/products.csv',
    format='csv',
    schema=productSchema,
    header=False)
display(dfs)

# COMMAND ----------

dffs2.createOrReplaceTempView("products1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from products1
