# Databricks notebook source
# MAGIC %md
# MAGIC ### Load_DimError

# COMMAND ----------

from datetime import timedelta, datetime
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window as W
from pyspark.storagelevel import StorageLevel
from delta.tables import *
 
import os #for adding filename to dataframe
import re #regular expressions

import pandas as pd #Used to greatly simplify converting JSON payload into a dataframe
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Parms

# COMMAND ----------

# dbutils.widgets.removeAll()

dbutils.widgets.text("TargetTable","")
dbutils.widgets.text("LastRefreshDate","")
dbutils.widgets.text("BatchID","")

pTargetTable = dbutils.widgets.get("TargetTable") 
pLastRefreshDate = dbutils.widgets.get("LastRefreshDate") 
pBatchID = dbutils.widgets.get("BatchID") 

delta_location = "Delta"
batch_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
error_flag = 0

print('Batch ID:\t', pBatchID)
print('Target Table:\t', pTargetTable)
print('Last refresh date query:\t', pLastRefreshDate)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Set up dataframe for return parms
# MAGIC Used for formatting json return argument

# COMMAND ----------

schema = StructType([
    StructField("TargetTable", StringType(), True), StructField("LastRefreshDate", TimestampType(), True), StructField("ErrorFlag", IntegerType(), True), StructField("ReturnMessage", StringType(), True)])
result_df = sqlContext.createDataFrame([],schema)
result_columns = ['TargetTable', 'LastRefreshDate', 'ErrorFlag', 'ReturnMessage']

print(result_df.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Logic
# MAGIC ### Return Error

# COMMAND ----------

# Your Load Logic here

dbutils.notebook.exit(-1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Return results as JSON

# COMMAND ----------

newRow = spark.createDataFrame([(pTargetTable,batch_time, 0, 'Completed')], result_columns)
result_df = result_df.union(newRow)

return_args = result_df.toJSON().map(lambda j: json.loads(j)).collect()
print(return_args)

dbutils.notebook.exit(return_args)
