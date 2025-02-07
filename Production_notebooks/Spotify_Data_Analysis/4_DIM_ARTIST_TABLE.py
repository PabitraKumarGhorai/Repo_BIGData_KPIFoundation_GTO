# Databricks notebook source
# Read SPOTIFY_FOUNDATION_TABLE Delta Tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df_read = spark.read.format('delta').load('dbfs:/user/hive/warehouse/fact_spotify_foundation_table')
print(df_read.count())
df_read.createOrReplaceTempView("TEMP_FACT_FOUNDATION_TABLE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_FACT_FOUNDATION_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC Artist_Name,
# MAGIC Artist_ID,
# MAGIC count(Track_Id) as Total_Track,
# MAGIC Year,
# MAGIC case
# MAGIC     WHEN count(Track_Id) <=20 THEN 'Low Track Level'
# MAGIC     WHEN count(Track_Id) >=21 and count(Track_Id) <=90 THEN 'Mid Track Level'
# MAGIC     ELSE 'High Track Level'
# MAGIC End as Track_Level
# MAGIC from 
# MAGIC TEMP_FACT_FOUNDATION_TABLE
# MAGIC group by
# MAGIC Artist_Name,
# MAGIC Artist_ID,
# MAGIC Year
# MAGIC order by
# MAGIC Artist_Name, 
# MAGIC Year asc
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
sql_str = '''

select
Artist_Name,
Artist_ID,
count(Track_Id) as Total_Track,
Year,
case
    WHEN count(Track_Id) <=20 THEN 'Low Track Level'
    WHEN count(Track_Id) >=21 and count(Track_Id) <=90 THEN 'Mid Track Level'
    ELSE 'High Track Level'
End as Track_Level
from 
TEMP_FACT_FOUNDATION_TABLE
group by
Artist_Name,
Artist_ID,
Year
order by
Artist_Name, 
Year asc
'''
dim_art = spark.sql(sql_str)
dim_art = dim_art.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
dim_art.createOrReplaceTempView("TEMP_DIM_SPOTIFY_ARTIST_TABLE")
display(dim_art.count())


# COMMAND ----------

dim_art.write.format('delta').mode('overwrite').saveAsTable('DIM_SPOTIFY_ARTIST_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.DIM_SPOTIFY_ARTIST_TABLE

# COMMAND ----------

