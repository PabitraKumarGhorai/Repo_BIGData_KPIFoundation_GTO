# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Dataframe").getOrCreate()
df_1 = spark.read.format('csv').option('header','true') \
                               .option('inferSchema','true') \
                               .load('dbfs:/FileStore/Order_Details.csv')
                               
display(df_1)

# COMMAND ----------

df_1 = df_1.withColumnRenamed("Order ID","Order_ID") \
           .withColumnRenamed("Sub-Category","Sub_Category")

display(df_1)
df_1.createOrReplaceTempView("TEMP_ORDER_DETAILS")
df_1.write.format('delta').mode('overwrite').saveAsTable('FACT_ORDER_DETAILS')

# COMMAND ----------

