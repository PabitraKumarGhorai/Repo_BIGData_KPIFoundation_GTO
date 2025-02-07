# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df_check = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/transaction.csv")
df_check.createOrReplaceTempView('transaction_tbl')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from transaction_tbl

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from transaction_tbl where sales_amount >= 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct currency from transaction_tbl 

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df = spark.read.format('csv').option("header","true").option('inferSchema','true').load("dbfs:/FileStore/transaction.csv")
display(df)


# COMMAND ----------

#This is use to remove -1 and 0 from 'sales_amount' column.
df1 = df.where(df.sales_amount>=1)
df1.show(5)
#Remove Null from 'currency' column.
df2 = df1.filter(df1.currency.isNotNull())
df2.show(5)
df2.createOrReplaceTempView("TEMP_TRANSACTION")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 'INDIA' as COUNTRY_NAME,
# MAGIC 'ASIA' as REGION,
# MAGIC product_code,
# MAGIC customer_code,
# MAGIC market_code,
# MAGIC order_date,
# MAGIC sales_qty,
# MAGIC sales_amount,
# MAGIC currency,
# MAGIC case
# MAGIC   when currency = 'USD' then sales_amount*85
# MAGIC   when currency = 'USD#(cr)' then sales_amount*85
# MAGIC   else sales_amount
# MAGIC end as currency_conv
# MAGIC from
# MAGIC TEMP_TRANSACTION

# COMMAND ----------

sql_str = '''
select
'INDIA' as COUNTRY_NAME,
'ASIA' as REGION,
product_code,
customer_code,
market_code,
order_date,
sales_qty,
sales_amount,
currency,
case
  when currency = 'USD' then sales_amount*85
  when currency = 'USD#(cr)' then sales_amount*85
  else sales_amount
end as currency_conv
from
TEMP_TRANSACTION
'''
df_final = spark.sql(sql_str)
df_final.createOrReplaceTempView("TEMP_STG_TRANSACTION")
df_final.write.format('delta').mode('overwrite').saveAsTable('FALCON_STG_TRANSACTION')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_TRANSACTION

# COMMAND ----------

