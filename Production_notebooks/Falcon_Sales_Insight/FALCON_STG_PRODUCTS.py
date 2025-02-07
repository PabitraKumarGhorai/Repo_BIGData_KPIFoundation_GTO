# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/products.csv")
display(df1)


# COMMAND ----------

#This Line of code is use to remove all null values which is coming in each new row

df2 = df1.filter(df1.product_type.isNotNull())
display(df2)
df2.createOrReplaceTempView("TEMP_PRODUCTS")

# COMMAND ----------

sql_str = '''
select
"INDIA" as COUNTRY_NAME,
"ASIA" as REGION,
product_code,
product_type
from TEMP_PRODUCTS
'''
df = spark.sql(sql_str)
df.createOrReplaceTempView("TEMP_STG_PRODUCTS")
df.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_PRODUCTS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_PRODUCTS

# COMMAND ----------

