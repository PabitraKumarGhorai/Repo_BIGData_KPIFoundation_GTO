# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()

df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/markets.csv")
display(df1)
df1.createOrReplaceTempView("TEMP_MARKETS")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC "INDIA" as COUNTRY_NAME,
# MAGIC "ASIA" as REGION,
# MAGIC markets_code,
# MAGIC markets_name,
# MAGIC zone
# MAGIC from TEMP_MARKETS
# MAGIC where zone != 'NULL'

# COMMAND ----------

sql_str = '''
SELECT
"INDIA" as COUNTRY_NAME,
"ASIA" as REGION,
markets_code,
markets_name,
zone
from TEMP_MARKETS
where zone != "NULL"
'''
df1 = spark.sql(sql_str)
df1.createOrReplaceTempView("TEMP_STG_MARKETS")
df1.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_MARKETS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_STG_MARKETS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_MARKETS

# COMMAND ----------

