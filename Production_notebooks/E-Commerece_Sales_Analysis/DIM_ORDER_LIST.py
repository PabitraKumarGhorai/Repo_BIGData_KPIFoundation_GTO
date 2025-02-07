# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df_1 = spark.read.format('csv').option('header','true') \
                                .option('inferSchema','true') \
                                .load('dbfs:/FileStore/List_of_Orders.csv')
display(df_1)

# COMMAND ----------

df_1 = df_1.withColumnRenamed("Order ID", "Order_ID") \
           .withColumnRenamed("Order Date", "Order_Date") \
           .withColumnRenamed("CustomerName", "Customer_Name")

# If specified any, the method drops a row if it contains any nulls. 
# If specified all, the method drops a row only if all its values are null.

df = df_1.dropna(how='all', thresh=None, subset=None)
display(df)
df.createOrReplaceTempView("TEMP_D_ORDER_LIST")
           

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_D_ORDER_LIST

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC Order_Id, 
# MAGIC Order_Date,
# MAGIC month(Order_Date) as Month,
# MAGIC year(Order_Date) as Year,
# MAGIC Customer_Name,
# MAGIC State,
# MAGIC City
# MAGIC from TEMP_D_ORDER_LIST

# COMMAND ----------

sql_str = """
select 
Order_Id, 
Order_Date,
month(Order_Date) as Month,
year(Order_Date) as Year,
Customer_Name,
State,
City
from TEMP_D_ORDER_LIST
"""
df_order = spark.sql(sql_str)
df_order.write.format('delta').mode('overwrite').option("overwriteSchema", "true").saveAsTable('DIM_ORDER_LIST')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.DIM_ORDER_LIST

# COMMAND ----------

