# Databricks notebook source
# MAGIC %sql
# MAGIC select * from default.DIM_ORDER_LIST

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.fact_order_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.DIM_SALES_TARGET_TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with cte1
# MAGIC as 
# MAGIC (
# MAGIC select 
# MAGIC A.Order_ID,A.Amount,
# MAGIC A.Profit,A.Quantity,
# MAGIC A.Category,A.Sub_Category, 
# MAGIC B.Order_Date,
# MAGIC B.Month, 
# MAGIC B.Year,B.Order_Date,
# MAGIC B.Customer_Name, 
# MAGIC B.State, B.City
# MAGIC from default.fact_order_details A 
# MAGIC left join default.dim_order_list B
# MAGIC on A.Order_ID = B.Order_Id
# MAGIC )
# MAGIC select C.Order_ID, 
# MAGIC sum(C.Amount) as Volume, 
# MAGIC sum(C.Profit) as Profit, 
# MAGIC C.Quantity, 
# MAGIC C.Category, 
# MAGIC C.Sub_Category, 
# MAGIC C.Month,
# MAGIC C.Year,
# MAGIC C.Order_Date,
# MAGIC C.State, City,
# MAGIC D.Month_of_Order_Date,
# MAGIC D.Target
# MAGIC from cte1 C 
# MAGIC left join 
# MAGIC default.dim_sales_target_table D
# MAGIC on C.Month = D.Month_Conv and 
# MAGIC C.Year = D.Year_Conv and
# MAGIC C.Category = D.Category
# MAGIC group by 
# MAGIC C.Order_ID,C.Quantity, 
# MAGIC C.Category, C.Sub_Category, C.Month,Year, 
# MAGIC D.Month_of_Order_Date,
# MAGIC D.Target,
# MAGIC C.Order_Date,
# MAGIC C.State, 
# MAGIC C.City
# MAGIC

# COMMAND ----------

sql_str = """

with cte1
as 
(
select 
A.Order_ID,A.Amount,
A.Profit,A.Quantity,
A.Category,A.Sub_Category, 
B.Order_Date,
B.Month, 
B.Year,B.Order_Date,
B.Customer_Name, 
B.State, B.City
from default.fact_order_details A 
left join default.dim_order_list B
on A.Order_ID = B.Order_Id
)
select C.Order_ID, 
sum(C.Amount) as Volume, 
sum(C.Profit) as Profit, 
C.Quantity, 
C.Category, 
C.Sub_Category, 
C.Month,
C.Year,
C.Order_Date,
C.State, City,
D.Month_of_Order_Date,
D.Target
from cte1 C 
left join 
default.dim_sales_target_table D
on C.Month = D.Month_Conv and 
C.Year = D.Year_Conv and
C.Category = D.Category
group by 
C.Order_ID,C.Quantity, 
C.Category, C.Sub_Category, C.Month,Year, 
D.Month_of_Order_Date,
D.Target,
C.Order_Date,
C.State, 
C.City
"""

df = spark.sql(sql_str)
df.createOrReplaceTempView("TEMP_KPI_INTRA_SALES")


# COMMAND ----------

# Writing Data as delta

df.write.format('delta').mode("overwrite").option("overwriteSchema","true").saveAsTable("KPI_AGG_INTRA_SALES")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from default.kpi_agg_intra_sales

# COMMAND ----------

