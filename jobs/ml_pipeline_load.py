# Databricks notebook source
# Load

"""
Part 1 from "Repos/financial_fraud_dev/jobs/ml_pipeline"

The code in this file loads data needed for ML tasks
"""

# COMMAND ----------

df = spark.table("hive_metastore.default.synthetic_financial_fraud_data_stage_2")


# COMMAND ----------

# display(df)
