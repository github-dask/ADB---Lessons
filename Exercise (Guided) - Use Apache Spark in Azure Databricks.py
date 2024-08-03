# Databricks notebook source
# MAGIC %md
# MAGIC # Follow steps from the MS Learning Exercise
# MAGIC https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/02-Use-Spark-in-Azure-Databricks.html
# MAGIC https://microsoftlearning.github.io/mslearn-databricks/Instructions/Exercises/LA-02-Explore-data.html
# MAGIC
# MAGIC Read more about dataframes: https://learn.microsoft.com/en-us/azure/databricks/getting-started/dataframes#language-Python
# MAGIC
# MAGIC It requires Github for Cluster and worksapce setup:
# MAGIC git clone https://github.com/MicrosoftLearning/mslearn-databricks
# MAGIC
# MAGIC And access to Free data from :
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2019.csv
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2020.csv
# MAGIC  https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2021.csv

# COMMAND ----------

# DBTITLE 1,Ingest data into DBFS
# MAGIC  %sh
# MAGIC  rm -r /dbfs/spark_lab
# MAGIC  mkdir /dbfs/spark_lab
# MAGIC  wget -O /dbfs/spark_lab/2019.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2019.csv
# MAGIC  wget -O /dbfs/spark_lab/2020.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2020.csv
# MAGIC  wget -O /dbfs/spark_lab/2021.csv https://raw.githubusercontent.com/MicrosoftLearning/mslearn-databricks/main/data/2021.csv

# COMMAND ----------

# DBTITLE 1,Query data across files loaded in Dataframe
df = spark.read.load('spark_lab/*.csv', format='csv') # we didnot give schema details
display(df.limit(100))
display(df['_c2','_c0'].groupby('_c2').count())

# COMMAND ----------

# DBTITLE 1,Define Schema and Create DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *

orderSchema = StructType([
     StructField("SalesOrderNumber", StringType()),
     StructField("SalesOrderLineNumber", IntegerType()),
     StructField("OrderDate", DateType()),
     StructField("CustomerName", StringType()),
     StructField("Email", StringType()),
     StructField("Item", StringType()),
     StructField("Quantity", IntegerType()),
     StructField("UnitPrice", FloatType()),
     StructField("Tax", FloatType())
])
df = spark.read.load('/spark_lab/*.csv', format='csv', schema=orderSchema)
display(df.limit(100))

# COMMAND ----------

# DBTITLE 1,Display Dataframe Schema
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter a dataframe
# MAGIC Add a new code cell and use it to run the following code, which will:
# MAGIC - Filter the columns of the sales orders dataframe to include only the customer name and email address.
# MAGIC - Count the total number of order records
# MAGIC - Count the number of distinct customers
# MAGIC - Display the distinct customers

# COMMAND ----------

customers = df['CustomerName', 'Email']
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

# MAGIC %md
# MAGIC Observe the following details:
# MAGIC
# MAGIC - When you perform an operation on a dataframe, the result is a new dataframe (in this case, a new customers dataframe is created by selecting a specific subset of columns from the df dataframe)
# MAGIC - Dataframes provide functions such as count and distinct that can be used to summarize and filter the data they contain.
# MAGIC - The dataframe['Field1', 'Field2', ...] syntax is a shorthand way of defining a subset of column. You can also use select method, so the first line of the code above could be written as customers = df.select("CustomerName", "Email")
# MAGIC
# MAGIC Now let’s apply a filter to include only the customers who have placed an order for a specific product by running the following code in a new code cell:

# COMMAND ----------

customers = df.select("CustomerName", "Email").where(df['Item']=='Road-250 Red, 52')
print(customers.count())
print(customers.distinct().count())
display(customers.distinct())

# COMMAND ----------

# DBTITLE 1,Aggregate and group data in a dataframe
productSales = df.select("Item", "Quantity").groupBy("Item").sum()
display(productSales)

yearlySales = df.select(year("OrderDate").alias("Year")).groupBy("Year").count().orderBy("Year")
display(yearlySales)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query data using Spark SQL
# MAGIC The native methods of the dataframe object you used previously enable you to query and analyze data quite effectively. However, many data analysts are more comfortable working with SQL syntax. Spark SQL is a SQL language API in Spark that you can use to run SQL statements, or even persist data in relational tables. The code you will ran below creates a relational view of the data in a dataframe, and then uses the **spark.sql** library to embed Spark SQL syntax within your Python code and query the view and return the results as a dataframe.

# COMMAND ----------

df.createOrReplaceTempView("salesorders")
spark_df = spark.sql("SELECT * FROM salesorders")
display(spark_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run SQL code in a cell
# MAGIC While it’s useful to be able to embed SQL statements into a cell containing PySpark code, data analysts often just want to work directly in SQL. 
# MAGIC - Add a new code cell and use it to run the following code.

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC SELECT YEAR(OrderDate) AS OrderYear,
# MAGIC        SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue
# MAGIC FROM salesorders
# MAGIC GROUP BY YEAR(OrderDate)
# MAGIC ORDER BY OrderYear;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observe that:
# MAGIC
# MAGIC - The ``%sql` line at the beginning of the cell (called a magic) indicates that the Spark SQL language runtime should be used to run the code in this cell instead of PySpark.
# MAGIC - The SQL code references the **salesorder** view that you created previously. YThe code highlightes that the view is not stored in hive_metastore.
# MAGIC - The output from the SQL query is automatically displayed as the result under the cell.

# COMMAND ----------

# MAGIC %md
# MAGIC # Visualize data with Spark

# COMMAND ----------

# DBTITLE 1,Built-in notebook Charts
# MAGIC %sql
# MAGIC     
# MAGIC SELECT * FROM salesorders

# COMMAND ----------

# DBTITLE 1,matplotlib
sqlQuery = "SELECT CAST(YEAR(OrderDate) AS CHAR(4)) AS OrderYear, \
                SUM((UnitPrice * Quantity) + Tax) AS GrossRevenue \
         FROM salesorders \
         GROUP BY CAST(YEAR(OrderDate) AS CHAR(4)) \
         ORDER BY OrderYear"
df_spark = spark.sql(sqlQuery)
df_spark.show()
print(df_spark)
display(df_spark)

from matplotlib import pyplot as plt
    
# matplotlib requires a Pandas dataframe, not a Spark one
df_sales = df_spark.toPandas()

# Clear the plot area
plt.clf()

# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='blue')
# Display the plot
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Review the results, which consist of a column chart with the total gross revenue for each year. Note the following features of the code used to produce this chart:
# MAGIC - The **matplotlib** library requires a Pandas dataframe, so you need to convert the Spark dataframe returned by the Spark SQL query to this format.
# MAGIC - At the core of the matplotlib library is the **pyplot** object. This is the foundation for most plotting functionality.
# MAGIC - A plot is technically contained with a Figure. In the previous examples, the figure was created implicitly for you, but you can create it explicitly.
# MAGIC - The default settings result in a usable chart, but there’s considerable scope to customize it.
# MAGIC

# COMMAND ----------

# DBTITLE 1,matplotlib customised with Figure
# Clear the plot area
plt.clf()
# Create a Figure
fig = plt.figure(figsize=(8,3))
# Create a bar plot of revenue by year
plt.bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
# Customize the chart
plt.title('Revenue by Year')
plt.xlabel('Year')
plt.ylabel('Revenue')
plt.grid(color='#95a5a6', linestyle='--', linewidth=2, axis='y', alpha=0.7)
plt.xticks(rotation=45)
# Show the figure
plt.show()

# COMMAND ----------

# DBTITLE 1,matplotlib with Figure subplots
# Clear the plot area
plt.clf()
# Create a figure for 2 subplots (1 row, 2 columns)
fig, ax = plt.subplots(1, 2, figsize = (10,4))
# Create a bar plot of revenue by year on the first axis
ax[0].bar(x=df_sales['OrderYear'], height=df_sales['GrossRevenue'], color='orange')
ax[0].set_title('Revenue by Year')
# Create a pie chart of yearly order counts on the second axis
yearly_counts = df_sales['OrderYear'].value_counts()
ax[1].pie(yearly_counts)
ax[1].set_title('Orders per Year')
ax[1].legend(yearly_counts.keys().tolist())
# Add a title to the Figure
fig.suptitle('Sales Data')
# Show the figure
plt.show()

# COMMAND ----------

# DBTITLE 1,seaborn
import seaborn as sns
   
# Clear the plot area
plt.clf()
# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

# COMMAND ----------

# Clear the plot area
plt.clf()
   
# Set the visual theme for seaborn
sns.set_theme(style="whitegrid")
   
# Create a bar chart
ax = sns.barplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()

# COMMAND ----------

# Clear the plot area
plt.clf()
   
# Create a bar chart
ax = sns.lineplot(x="OrderYear", y="GrossRevenue", data=df_sales)
plt.show()
