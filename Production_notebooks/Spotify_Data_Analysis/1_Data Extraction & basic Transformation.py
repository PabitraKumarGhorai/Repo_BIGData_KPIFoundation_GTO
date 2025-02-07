# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("Dataframe").getOrCreate()
read_df = spark.read.format('csv').option("header","true") \
                                  .option("inferSchema","true") \
                                  .load('dbfs:/FileStore/spotify_data.csv')
read_df.show(2)

df = read_df.withColumnRenamed("_c0","Index_No")
display(df)


# COMMAND ----------

#exclude : valance, liveness, instrumental,instrumentalness

#int : popularity,year,key,mode,
#float: danceability,energy,


df1 = df.drop('valance','liveness','valence','instrumentalness','acousticness','speechiness')

display(df1)

# COMMAND ----------

# DBTITLE 1,Trimming Dataframe
#exclude : valance, liveness, instrumental,instrumentalness

#int : popularity,year,key,mode,
#float: danceability,energy,


df_raw = df1.withColumn("Year",df1["Year"].cast('int')) \
                  .withColumn("popularity",df1["popularity"].cast('int')) \
                  .withColumn("key",df1["key"].cast('int')) \
                  .withColumn("mode",df1["mode"].cast('int')) \
                  .withColumn("energy",df1["energy"].cast('float')) \
                  .withColumn("danceability",df1["danceability"].cast('float')) \
                  .withColumn("LOAD_DATE_TIMESTAMP",current_timestamp()) 
df_raw.printSchema()

df_raw.createOrReplaceTempView("TEMP_SPOTIFY_RAW_TABLE")



# COMMAND ----------

df_raw.write.format('delta').mode("overwrite").saveAsTable('SPOTIFY_RAW_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.SPOTIFY_RAW_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from default.SPOTIFY_RAW_TABLE

# COMMAND ----------

