# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Dataframe").getOrCreate()
read_df = spark.read.format('csv').option("header","true") \
                                  .option("inferSchema","true") \
                                  .load('dbfs:/FileStore/spotify_data.csv')
read_df.show(2)

df = read_df.withColumnRenamed("_c0","Index_No")
#display(df)

# Exclude Few Columns
df1 = df.drop('valance','liveness','valence','instrumentalness','acousticness','speechiness')

#display(df1)



df_raw = df1.withColumn("Year",df1["Year"].cast('int')) \
                  .withColumn("popularity",df1["popularity"].cast('int')) \
                  .withColumn("key",df1["key"].cast('int')) \
                  .withColumn("mode",df1["mode"].cast('int')) \
                  .withColumn("energy",df1["energy"].cast('float')) \
                  .withColumn("danceability",df1["danceability"].cast('float')) \
                  .withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
#df_raw.printSchema()

df_raw.createOrReplaceTempView("TEMP_SPOTIFY_RAW_TABLE")

# COMMAND ----------

# DBTITLE 1,Creating TEMP_SPOTIFY_FOUNDATION_TABLE 
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
from TEMP_SPOTIFY_RAW_TABLE
where popularity > 0
'''
df_foundation = spark.sql(sql_str)
df_foundation = df_foundation.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_foundation.createOrReplaceTempView("TEMP_SPOTIFY_FOUNDATION_TABLE")

# Write this we can create delta table.
#df_foundation.write.format('delta').mode("overwrite").saveAsTable('SPOTIFY_FOUNDATION_TABLE')


# COMMAND ----------

# DBTITLE 1,FACT_SPOTIFY_FOUNDATION_TABLE Creation
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
TEMP_SPOTIFY_FOUNDATION_TABLE
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
  from TEMP_SPOTIFY_FOUNDATION_TABLE

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
df_fact.createOrReplaceTempView("FACT_SPOTIFY_FOUNDATION_TABLE")

display(df_fact)

# COMMAND ----------

# DBTITLE 1,DIM_SPOTIFY_ARTIST_TABLE Creation
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
FACT_SPOTIFY_FOUNDATION_TABLE
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
dim_art.createOrReplaceTempView("DIM_SPOTIFY_ARTIST_TABLE")
display(dim_art)


# COMMAND ----------

# DBTITLE 1,DIM_SPOTIFY_TRACK_TABLE Creation
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
from FACT_SPOTIFY_FOUNDATION_TABLE
'''
df_track = spark.sql(sql_str)
df_track = df_track.withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_track.createOrReplaceTempView("DIM_SPOTIFY_TRACK_TABLE")
display(df_track.count())
display(df_track)

# COMMAND ----------

# DBTITLE 1,Visualization
# MAGIC %sql
# MAGIC select * from FACT_SPOTIFY_FOUNDATION_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_SPOTIFY_ARTIST_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from DIM_SPOTIFY_TRACK_TABLE

# COMMAND ----------

# DBTITLE 1,Viz 1: Top 20 Artist who has sung maximum songs
sql_str1 = '''
select Artist_Name, sum(Total_Track) from DIM_SPOTIFY_ARTIST_TABLE
group by Artist_Name
order by sum(Total_Track) desc
limit 20
'''
df_1 = spark.sql(sql_str1)
display(df_1)

# COMMAND ----------

# DBTITLE 1,Viz 2: Top 20 High Popular Tracks
sql_str2='''
select  distinct B.Artist_Name, A.*
from DIM_SPOTIFY_TRACK_TABLE A left join FACT_SPOTIFY_FOUNDATION_TABLE B
on A.Artist_ID = B.Artist_ID 
where A.Track_Popularity_Level = "High Popular Track"
order by A.Popularity desc
limit 20
'''
df_2 = spark.sql(sql_str2)
display(df_2)

# COMMAND ----------

# DBTITLE 1,Viz 3: Year wise Total no of track and total number of Artist
sql_str3 = '''
select  sum(Total_Track), count(Artist_Name), Year from FACT_SPOTIFY_FOUNDATION_TABLE
group by Year
order by Year asc
'''
df_3 = spark.sql(sql_str3)
display(df_3)


# COMMAND ----------

# DBTITLE 1,Viz 4: Top 20 Genre by Popularity
sql_str4 = '''
select Genre, sum(Popularity) from FACT_SPOTIFY_FOUNDATION_TABLE
group by Genre 
order by sum(Popularity) desc
limit 20
'''
df_4 = spark.sql(sql_str4)
display(df_4)

# COMMAND ----------


