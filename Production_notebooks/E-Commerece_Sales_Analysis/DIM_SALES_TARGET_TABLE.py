# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Dataframe').getOrCreate()
df_1 = spark.read.format('csv').option('header','true') \
                               .option('inferSchema','true') \
                               .load('dbfs:/FileStore/Sales_target.csv')

display(df_1)

# COMMAND ----------

df_1 = df_1.withColumnRenamed("Month of Order Date","Month_of_Order_Date")

df2 = df_1.withColumn("Month", split(df_1['Month_of_Order_Date'], '-').getItem(0)) \
          .withColumn("Year", split(df_1['Month_of_Order_Date'], '-').getItem(1)) 
          

display(df2)


# COMMAND ----------

#changing data-type of year filter
df2 = df2.withColumn('Year',df2.Year.cast("integer"))

# COMMAND ----------

dict_month = {
    'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6,
    'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12
}
a='Jan'
if a in dict_month:
    # Print the value associated with the key 'Jan'
    jan_value = dict_month[a]
    print("The value for 'Jan' is:", jan_value)
else:
    print("'Jan' not found in the dictionary.")
 

# COMMAND ----------

def month_conversion(a):
    dict_month = {
    'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6,
    'Jul':7, 'Aug':8, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12
    }
    
    if a in dict_month:
        # Print the value associated with the key 'Jan'
        month_value = dict_month[a]
        


    return month_value

# Testing above function
month_conversion('Apr')

# COMMAND ----------

from pyspark.sql.functions import col,udf
month_conversion_udf = udf(month_conversion)

# COMMAND ----------

df2 = df2.withColumn("Month_Conv", month_conversion_udf(col("Month")))
df2_final = df2.withColumn("Month_Conv", df2.Month_Conv.cast("Integer"))
display(df2_final)
df2_final.createOrReplaceTempView('TEMP_TARGET')

# COMMAND ----------

# MAGIC %sql
# MAGIC with cte1
# MAGIC as (
# MAGIC select 2000 as new_col,
# MAGIC Month_of_Order_Date, 
# MAGIC Category, Target, Month, Year, Month_Conv
# MAGIC from TEMP_TARGET
# MAGIC )
# MAGIC select Month_of_Order_Date, 
# MAGIC Category, 
# MAGIC Target, 
# MAGIC Month, 
# MAGIC (Year+new_col) as Year_Conv, 
# MAGIC Month_Conv
# MAGIC from cte1

# COMMAND ----------

sql_str = """
with cte1
as (
select 2000 as new_col,
Month_of_Order_Date, 
Category, Target, Month, Year, Month_Conv
from TEMP_TARGET
)
select Month_of_Order_Date, 
Category, 
Target, 
Month, 
(Year+new_col) as Year_Conv, 
Month_Conv
from cte1 """

df_target = spark.sql(sql_str)
df_target.write.format('delta').mode('overwrite').option('overwriteSchema','true').saveAsTable('DIM_SALES_TARGET_TABLE')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.DIM_SALES_TARGET_TABLE

# COMMAND ----------

