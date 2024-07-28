# Databricks notebook source
# MAGIC %md
# MAGIC # Follow steps from the MS Learning Exercise
# MAGIC https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/01-Explore-Azure-Databricks.html
# MAGIC
# MAGIC It requires Github for Cluster and worksapce setup:
# MAGIC git clone https://github.com/MicrosoftLearning/mslearn-databricks
# MAGIC
# MAGIC And access to Free data from :
# MAGIC https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/products.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`products`;

# COMMAND ----------

 df = spark.sql("SELECT * FROM products")
 df = df.filter("Category == 'Road Bikes'")
 display(df)
