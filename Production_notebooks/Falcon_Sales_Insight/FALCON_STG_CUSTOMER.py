# Databricks notebook source
clear cache

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/customers.csv")
display(df1)
df1.createOrReplaceTempView("TEMP_CUSTOMER")


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 'INDIA' as COUNTRY_NAME,
# MAGIC 'ASIA' as REGION,
# MAGIC customer_code as CUSTOMER_CODE,
# MAGIC custmer_name as CUSTOMER_NAME,
# MAGIC customer_type as CUSTOMER_TYPE
# MAGIC from 
# MAGIC TEMP_CUSTOMER

# COMMAND ----------

str_sql = '''
select
'INDIA' as COUNTRY_NAME,
'ASIA' as REGION,
customer_code as CUSTOMER_CODE,
custmer_name as CUSTOMER_NAME,
customer_type as CUSTOMER_TYPE
from 
TEMP_CUSTOMER
'''
df1 = spark.sql(str_sql)
df1.createOrReplaceTempView("TEMP_STG_CUTOMER")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_STG_CUTOMER

# COMMAND ----------

df1.write.format("delta").mode("overwrite").saveAsTable('FALCON_STG_CUSTOMER')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_CUSTOMER

# COMMAND ----------

