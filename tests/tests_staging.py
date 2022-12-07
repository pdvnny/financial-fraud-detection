# Databricks notebook source
# MAGIC %run "./Lib"

# COMMAND ----------

import pytest

# COMMAND ----------

tableName   = "test_data"
dbName      = "default"
columnName  = "isFraud"
columnValue = True

# COMMAND ----------

if tableExists(tableName, dbName):

  df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")

  # And the specified column exists in that table...
  if columnExists(df, columnName):
    # Then report the number of rows for the specified value in that column.
    numRows = numRowsInColumnForValue(df, columnName, columnValue)

    print(f"There are {numRows} rows in '{tableName}' where '{columnName}' equals '{columnValue}'.")
  else:
    print(f"Column '{columnName}' does not exist in table '{tableName}' in schema (database) '{dbName}'.")
else:
  print(f"Table '{tableName}' does not exist in schema (database) '{dbName}'.") 

# COMMAND ----------

!pip3 install pytest
import pytest
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType

tableName   = "test_data"
dbName      = "default"
columnName  = "isFraud"
columnValue = True

# Because this file is not a Databricks notebook, you
# must create a Spark session. Databricks notebooks
# create a Spark session for you by default.
spark = SparkSession.builder \
                    .appName('integrity-tests') \
                    .getOrCreate()

df = spark.sql(f"SELECT * FROM {dbName}.{tableName}")




# COMMAND ----------

# Does the table exist?


# COMMAND ----------

class test_ut:
    def test_tableExists(self):
      #assert tableExists(tableName, dbName) is True
      return (tableExists(tableName, dbName)==True)
# Does the column exist?
    def test_columnExists(self):
      #assert columnExists(df, columnName) is True
      return (columnExists(df, columnName)==True)

# Is there at least one row for the value in the specified column?
    def test_numRowsInColumnForValue(self):
      #assert numRowsInColumnForValue(df, columnName, columnValue) > 0
      return (numRowsInColumnForValue(df, columnName, columnValue) > 0)

# COMMAND ----------

unitTests = test_ut()
print(unitTests.test_tableExists())
print(unitTests.test_columnExists())
print(unitTests.test_numRowsInColumnForValue())

# COMMAND ----------

testResult = unitTests.test_tableExists() and unitTests.test_columnExists() and unitTests.test_numRowsInColumnForValue()
dbutils.notebook.exit(f"{testResult}")
