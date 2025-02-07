# Databricks notebook source
# Read SPOTIFY_FOUNDATION_TABLE Delta Tables
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('DataFrame').getOrCreate()
df_read = spark.read.format('delta').load('dbfs:/user/hive/warehouse/spotify_foundation_table')
print(df_read.count())
df_read.createOrReplaceTempView("TEMP_FOUNDATION_TABLE")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_FOUNDATION_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC with common_table as (
# MAGIC with cte1 as 
# MAGIC (
# MAGIC select 
# MAGIC Artist_Name,
# MAGIC count(Track_Id) as Total_Track
# MAGIC from
# MAGIC TEMP_FOUNDATION_TABLE
# MAGIC group by
# MAGIC Artist_Name
# MAGIC )
# MAGIC ,cte2 as 
# MAGIC (
# MAGIC   select
# MAGIC   Index_no,
# MAGIC   Artist_Name,
# MAGIC   Track_Name,
# MAGIC   Track_Id,
# MAGIC   Popularity,
# MAGIC   Year,
# MAGIC   Genre, Danceability, 
# MAGIC   Energy, Loudness, Mode, 
# MAGIC   BeatsPerMinute, 
# MAGIC   Duration_MS, 
# MAGIC   Estimated_Time
# MAGIC   from TEMP_FOUNDATION_TABLE
# MAGIC
# MAGIC )
# MAGIC select
# MAGIC c2.Index_no,
# MAGIC c1.Artist_Name, c1.Total_Track,
# MAGIC c2.Track_Name, c2.Track_Id, c2.Popularity,
# MAGIC c2.Year, c2.Genre, c2.Danceability, c2.Energy,
# MAGIC c2.Loudness, c2.Mode, c2.BeatsPerMinute,
# MAGIC c2.Duration_MS, c2.Estimated_Time
# MAGIC
# MAGIC from cte1 c1
# MAGIC join cte2 c2 on c1.Artist_Name = c2.Artist_Name)
# MAGIC select Index_no,
# MAGIC Artist_Name,
# MAGIC concat(Artist_Name,Total_Track) as Artist_ID,
# MAGIC Total_Track,
# MAGIC Track_Name,Track_Id,Popularity,
# MAGIC Year,Genre,Danceability,Energy,
# MAGIC Loudness, Mode, BeatsPerMinute,
# MAGIC Duration_MS, Estimated_Time
# MAGIC from 
# MAGIC common_table
# MAGIC where 
# MAGIC Artist_Name not like ('!%')
# MAGIC and Artist_Name not like ('"%')
# MAGIC and Artist_Name not like ('$%')
# MAGIC and Artist_Name not like ('4%')
# MAGIC and Artist_Name not like ('(%')
# MAGIC and Artist_Name not like ('#%')

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *


sql_str = """
with common_table as (
with cte1 as 
(
select 
Artist_Name,
count(Track_Id) as Total_Track
from
TEMP_FOUNDATION_TABLE
group by
Artist_Name
)
,cte2 as 
(
  select
  Index_no,
  Artist_Name,
  Track_Name,
  Track_Id,
  Popularity,
  Year,
  Genre, Danceability, 
  Energy, Loudness, Mode, 
  BeatsPerMinute, 
  Duration_MS, 
  Estimated_Time
  from TEMP_FOUNDATION_TABLE

)
select
c2.Index_no,
c1.Artist_Name, c1.Total_Track,
c2.Track_Name, c2.Track_Id, c2.Popularity,
c2.Year, c2.Genre, c2.Danceability, c2.Energy,
c2.Loudness, c2.Mode, c2.BeatsPerMinute,
c2.Duration_MS, c2.Estimated_Time

from cte1 c1
join cte2 c2 on c1.Artist_Name = c2.Artist_Name)
select Index_no,
Artist_Name,
concat(Artist_Name,Total_Track) as Artist_ID,
Total_Track,
Track_Name,Track_Id,Popularity,
Year,Genre,Danceability,Energy,
Loudness, Mode, BeatsPerMinute,
Duration_MS, Estimated_Time
from 
common_table
where 
Artist_Name not like ('!%')
and Artist_Name not like ('"%')
and Artist_Name not like ('$%')
and Artist_Name not like ('4%')
and Artist_Name not like ('(%')
and Artist_Name not like ('#%')
"""

df_fact = spark.sql(sql_str)
df_fact = df_fact.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_fact.createOrReplaceTempView("TEMP_FACT_SPOTIFY_TABLE")



# COMMAND ----------

df_fact.write.format('delta').mode("overwrite").saveAsTable('FACT_SPOTIFY_FOUNDATION_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.FACT_SPOTIFY_FOUNDATION_TABLE

# COMMAND ----------

