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
# MAGIC select * from TEMP_FACT_FOUNDATION_TABLE where Artist_Name = "Arijit Singh"

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC Track_Id,
# MAGIC Track_Name,
# MAGIC Artist_ID,
# MAGIC Popularity,
# MAGIC case
# MAGIC   WHEN Popularity <= 40 THEN 'Low Popular Track'
# MAGIC   WHEN Popularity <= 70 and Popularity >= 41 THEN 'Mid Popular Track'
# MAGIC   WHEN Popularity >= 71 THEN 'High Popular Track'
# MAGIC   ELSE 'Undefined Popularity'
# MAGIC END AS Track_Popularity_Level,
# MAGIC Danceability,
# MAGIC case
# MAGIC   WHEN Danceability <=0.49 THEN 'Low Danceable Track'
# MAGIC   WHEN Danceability >=0.5000 and Danceability <=0.79 THEN 'Mid Danceable Track'
# MAGIC   WHEN Danceability >=0.8000 THEN 'High Danceable Track'
# MAGIC   ELSE 'Undefined Danceability'
# MAGIC END AS Danceability_Level,
# MAGIC Energy,
# MAGIC Loudness,
# MAGIC Mode,
# MAGIC BeatsPerMinute,
# MAGIC Duration_MS,
# MAGIC (Duration_MS/60000) as Duration_Minutes,
# MAGIC Estimated_Time
# MAGIC from TEMP_FACT_FOUNDATION_TABLE

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


sql_str = '''
select
Track_Id,
Track_Name,
Artist_ID,
Popularity,
case
  WHEN Popularity <= 40 THEN 'Low Popular Track'
  WHEN Popularity <= 70 and Popularity >= 41 THEN 'Mid Popular Track'
  WHEN Popularity >= 71 THEN 'High Popular Track'
  ELSE 'Undefined Popularity'
END AS Track_Popularity_Level,
Danceability,
case
  WHEN Danceability <=0.49 THEN 'Low Danceable Track'
  WHEN Danceability >=0.5000 and Danceability <=0.79 THEN 'Mid Danceable Track'
  WHEN Danceability >=0.8000 THEN 'High Danceable Track'
  ELSE 'Undefined Danceability'
END AS Danceability_Level,
Energy,
Loudness,
Mode,
BeatsPerMinute,
Duration_MS,
(Duration_MS/60000) as Duration_Minutes,
Estimated_Time
from TEMP_FACT_FOUNDATION_TABLE
'''
df_track = spark.sql(sql_str)
df_track = df_track.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_track.createOrReplaceTempView("TEMP_DIM_SPOTIFY_TRACK_TABLE")
display(df_track.count())


# COMMAND ----------

df_track.write.format('delta').mode('overwrite').saveAsTable('DIM_SPOTIFY_TRACK_TABLE')

# COMMAND ----------

