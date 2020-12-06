# Databricks notebook source
# MAGIC %md
# MAGIC ### Date Cleanup
# MAGIC ## Clean up date columns for Spark 3
# MAGIC Spark 2 Delta Lake parquet files allow invalid dates. Spark 3 has much stricter validation rules. Even if run in legacy mode. 
# MAGIC Here is a routine to clean dates in your dataframe before writting them to Delta
# MAGIC 
# MAGIC This example was written with the Databricks Community Edition. If you want to follow along. https://community.cloud.databricks.com/

# COMMAND ----------

# Default imports

from datetime import timedelta, datetime
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window as W
from pyspark.storagelevel import StorageLevel
from delta.tables import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sample Data
# MAGIC We will use sample data provided by Databricks

# COMMAND ----------

# get sample data

df = spark.read.format("csv").option("header", "true").load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")

df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Add Date column with valid date

# COMMAND ----------

# add date column
df = df.withColumn("TestDate", to_date(lit("2020-12-01"), "yyyy-MM-dd"))
df.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC Set the first 10 rows to an invalid date

# COMMAND ----------

# Create invalid date

df = df.withColumn("TestDate",
    when(df["_c0"] < 11, to_date(lit('0220-06-06'), "yyyy-MM-dd")).
    otherwise(df["TestDate"]))

df.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC Create view

# COMMAND ----------

# Delta table

df.createOrReplaceTempView("SourceView")  



# COMMAND ----------

# MAGIC %md
# MAGIC Remove target directory

# COMMAND ----------

dbutils.fs.rm('/delta/diamonds_target/', True) 

# COMMAND ----------

# MAGIC %md
# MAGIC Create table will fail because of bad dates. 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS diamonds_target;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS diamonds_target
# MAGIC         USING DELTA
# MAGIC         PARTITIONED BY(clarity)
# MAGIC         LOCATION '/delta/diamonds_target/'
# MAGIC         as
# MAGIC         select * from SourceView  

# COMMAND ----------

# MAGIC %md
# MAGIC If you scroll through the error message to find the Caused by section, you will see the following:  
# MAGIC 
# MAGIC 
# MAGIC >You may get a different result due to the upgrading of Spark 3.0: writing dates before 1582-10-15 or timestamps before 1900-01-01T00:00:00Z into Parquet files can be dangerous

# COMMAND ----------

# MAGIC %md
# MAGIC Here is a dynamic routine to clean up dates. 

# COMMAND ----------

col_list = df.dtypes
 
for x,y in col_list:
    if y == 'date':
        df = df.withColumn(x, 
            when(df[x] < '1900-01-01', to_date(lit(None), "yyyy-MM-dd")).
            otherwise(df[x]))
    if y == 'timestamp':
        df = sdf.withColumn(x, 
            when(df[x] < '1900-01-01', to_timestamp(lit(None), "yyyy-MM-dd HH:mm:ss.S")).
            otherwise(df[x]))     

        
# Update view with new copy of df
df.createOrReplaceTempView("SourceView")  

# COMMAND ----------

# MAGIC %md
# MAGIC Now when we run the Create Table. It creates the Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS diamonds_target;
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS diamonds_target
# MAGIC         USING DELTA
# MAGIC         PARTITIONED BY(clarity)
# MAGIC         LOCATION '/delta/diamonds_target/'
# MAGIC         as
# MAGIC         select * from SourceView  

# COMMAND ----------

# MAGIC %md
# MAGIC To verify, we will select rows where the date was updated to  NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from diamonds_target
# MAGIC where TestDate is null
# MAGIC order by _c0
