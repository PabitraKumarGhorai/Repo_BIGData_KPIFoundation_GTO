# Databricks notebook source
# Read Delta Tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df_read = spark.read.format('delta').load('dbfs:/user/hive/warehouse/spotify_raw_table')
print(df_read.count())
df_read.createOrReplaceTempView("TEMP_RAW_TABLE")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_RAW_TABLE

# COMMAND ----------

# DBTITLE 1,SPOTIFY_FOUNDATION_TABLE
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


sql_str = '''
select
Index_no,
artist_name as Artist_Name,
track_name as Track_Name,
track_id as Track_Id,
popularity as Popularity,
Year, genre as Genre,
danceability as Danceability,
energy as Energy,
loudness as Loudness,
mode as Mode,
tempo as BeatsPerMinute,
duration_ms as Duration_MS,
time_signature as Estimated_Time
from TEMP_RAW_TABLE
where popularity > 0
'''
df_foundation = spark.sql(sql_str)
df_foundation = df_foundation.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_foundation.createOrReplaceTempView("TEMP_SPOTIFY_FOUNDATION_TABLE")



# COMMAND ----------

df_foundation.write.format('delta').mode("overwrite").saveAsTable('SPOTIFY_FOUNDATION_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.SPOTIFY_FOUNDATION_TABLE

# COMMAND ----------

