# Databricks notebook source
# Load

"""
Part 2 from "Repos/financial_fraud_dev/jobs/ml_pipeline"

The code in this file transforms the loaded data to prepare it for model training.
Steps:
(1) Add 'label' column - represents domain knowledge financial fraud approach
(2) Split data into training and test
(3) Balance training data - want equal numbers of pos/neg samples
"""

# COMMAND ----------

# Load the data first - THIS IS REPEATED CODE FROM "ml_pipeline_transform"
df = spark.table("hive_metastore.default.synthetic_financial_fraud_data_stage_2")

# COMMAND ----------

# Add 'label'
import pyspark

df = df.withColumn("label",         
                  pyspark.sql.functions.when(
                          ((df.oldbalanceOrg <= 56900) & (df.type == "TRANSFER") & (df.newbalanceDest <= 105)) # rule 1
                          | ((df.oldbalanceOrg > 56900) & (df.newbalanceOrig <= 12))                           # rule 2
                          | ((df.oldbalanceOrg > 56900) & (df.newbalanceOrig > 12) & (df.amount > 1160000))    # rule 3
                      , 1
                      ).otherwise(0)
                  )

# COMMAND ----------

# Split

# split
[train_df, test_df] = df.randomSplit([0.7, 0.3], seed=12345)
# Using cross validation so there is no need for a validation_df

# prepare train_df
dfn = train_df.filter(train_df.label == 0)
dfy = train_df.filter(train_df.label == 1)

N = train_df.count()
y = dfy.count()

p = y/N

train = dfn.sample(False, p, seed = 92285).union(dfy)

display(train.groupBy("label").count())

# COMMAND ----------

# Store tables for reference (maybe usage at some point)

train.write.saveAsTable("balanced_training_data")
test_df.write.saveAsTable("test_data")
train.groupBy("label").count().write.saveAsTable("train_data_distribution")
