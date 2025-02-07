# Databricks notebook source
clear cache

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/date.csv")
display(df)

# COMMAND ----------

df1 = df.filter(df.cy_date.isNotNull())
display(df1)
df1.createOrReplaceTempView("TEMP_DATE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_DATE

# COMMAND ----------

sql_str = '''
select
date,
cy_date,
year,
month_name,
date_yy_mmm
from
TEMP_DATE
'''
df2 = spark.sql(sql_str)
df2.createOrReplaceTempView("TEMP_STG_DATE")
df2.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_DATE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_DATE

# COMMAND ----------

