# Databricks notebook source
# MAGIC %md
# MAGIC ### Load Lakehouse

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

# dbutils.widgets.removeAll()


dbutils.widgets.text("TableGroup","")
dbutils.widgets.text("ConfigTable","")
dbutils.widgets.text("BatchID","")

pConfigTable = dbutils.widgets.get("ConfigTable")
pTableGroup = dbutils.widgets.get("TableGroup").upper()
pBatchID = dbutils.widgets.get("BatchID")

BasePath = f"./{pTableGroup}"
BatchTime = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

print('Table Group:\t\t', pTableGroup)
print('Base path:\t\t', BasePath) 
print('Config table:\t\t', pConfigTable)
print('Batch ID:\t\t', pBatchID)

# Used to build return JSON
schema = StructType([
    StructField("TargetTable", StringType(), True), 
    StructField("LastRefreshDate", TimestampType(), True), 
    StructField("ErrorFlag", IntegerType(), True),
    StructField("ReturnMessage", StringType(), True), ])

ResultDF = sqlContext.createDataFrame([],schema)
ResultColumns = ['TargetTable', 'LastRefreshDate', 'ErrorFlag', 'ReturnMessage']


print(ResultDF.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Convert Config Table to dataframe for processing

# COMMAND ----------

if pConfigTable:
    configargs = json.loads(pConfigTable)
else:
    configargs = ''

if configargs:
    # Use Pandas for easy conversion of JSON to dataframe
    pdf = pd.json_normalize(configargs,'TableList') 

display(pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loop through Table List and call notebooks

# COMMAND ----------

for index, row in pdf.iterrows():
    rTargetTable = row['TargetTable']
    rLastRefreshDate = row['LastRefreshDate']
    LoadNotebook = f"""{BasePath}/Load_{rTargetTable}"""

    args = {
              "TargetTable": rTargetTable,  
              "BatchID": pBatchID,
              "LastRefreshDate": rLastRefreshDate
            }
 
    try:
        # Call notebook
        ResultStatus = dbutils.notebook.run(LoadNotebook, 3600, args)   
        try:
            # if notebook was called, get return parms
            ResultRow = spark.read.json(sc.parallelize([ResultStatus])).select("TargetTable", "LastRefreshDate", "ErrorFlag", "ReturnMessage")
        except:
            # If notebook didn't not return parms an error occured and set the error flag
            ResultRow = spark.createDataFrame([(rTargetTable,rLastRefreshDate, 1, "Error")], ResultColumns)

    except:
        # Notebook did not exist
        ResultRow = spark.createDataFrame([(rTargetTable,rLastRefreshDate, 1, "Missing notebook")], ResultColumns)
        
    ResultDF = ResultDF.union(ResultRow)
        
#display returned parms        
display(ResultDF)     

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Format results for Data Factory

# COMMAND ----------

results =  ResultDF.toJSON().map(lambda j: json.loads(j)).collect()
results = json.dumps({"TableList": f"{results}"})
results = results.replace("'", '"').replace('"[', '[').replace(']"', ']')

print(results)

dbutils.notebook.exit(results)
