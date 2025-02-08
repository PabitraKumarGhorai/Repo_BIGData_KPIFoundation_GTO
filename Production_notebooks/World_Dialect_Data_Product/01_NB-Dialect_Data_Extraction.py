# Databricks notebook source
from bs4 import BeautifulSoup
import requests
import pandas as pd

url = "https://www.loc.gov/standards/iso639-2/php/code_list.php"
page = requests.get(url)
soup = BeautifulSoup(page.text, 'html')
print(soup)

# COMMAND ----------

from datetime import datetime
src_cnts = soup.find_all('p')[3]

print(src_cnts)
data_mdf = [ele.text.strip() for ele in src_cnts]

date_string = data_mdf[1]
last_modified = datetime.strptime(date_string, "%Y-%m-%d")

print(last_modified, type(last_modified))

# COMMAND ----------

#Extracting table details
table_contains = soup.find_all('table')[1]
table_head = table_contains.find_all('th')
print(table_head)

# COMMAND ----------

#Extracting columns
table_columns = [head.text.strip() for head in table_head]
print(table_columns)

# COMMAND ----------

#Extracting data
table_body = table_contains.find_all('tr')

row_list = []
for row in table_body[1:]:
    row_data = row.find_all('td')
    individual_row = [data.text.strip() for data in row_data]
    row_list.append(individual_row)
print(row_list)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import * 

df_table = spark.createDataFrame(data = row_list, schema = table_columns)
df_table = df_table.withColumnRenamed('English name of Language','English_Name_of_Language') \
                   .withColumnRenamed('French name of Language','French_Name_of_Language') \
                   .withColumnRenamed('German name of Language','German_Name_of_Language') \
                   .withColumnRenamed('ISO 639-2 Code','ISO_639-2_Code') \
                   .withColumnRenamed('ISO 639-1 Code','ISO_639-1_Code') 
display(df_table)

df_table.write.format('delta').mode('overwrite').save('dbfs:/FileStore/ENRICHED/Dialect_Data_Product')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists Dialect_Data_Enriched using delta location "dbfs:/FileStore/ENRICHED/Dialect_Data_Product"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Dialect_Data_Enriched

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Dialect_Data_Enriched
