# Databricks notebook source
# LOAD
df = spark.table("hive_metastore.default.synthetic_financial_fraud_data_stage_2")

# COMMAND ----------

# TRANSFORM
import pyspark.sql.functions

# make "label" column
df = df.withColumn("label",         
                  pyspark.sql.functions.when(
                          ((df.oldbalanceOrg <= 56900) & (df.type == "TRANSFER") & (df.newbalanceDest <= 105)) # rule 1
                          | ((df.oldbalanceOrg > 56900) & (df.newbalanceOrig <= 12))                           # rule 2
                          | ((df.oldbalanceOrg > 56900) & (df.newbalanceOrig > 12) & (df.amount > 1160000))    # rule 3
                      , 1
                      ).otherwise(0)
                  )

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

# CREATE MODEL

from pyspark.ml import Pipeline
# from pyspark.ml.feature import StringIndexer  # -- I ended up doing in this in another step for now
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier

from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder

va = VectorAssembler(inputCols=['typeIndexed', 'amount',
                               'oldbalanceOrg', 'newbalanceOrig',
                               'oldbalanceDest', 'newbalanceDest'],
                    outputCol="features"
                    )

# Use BinaryClassificationEvaluator to evaluate our model
evaluatorPR = BinaryClassificationEvaluator(labelCol = "label", rawPredictionCol = "prediction", metricName = "areaUnderPR")
evaluatorAUC = BinaryClassificationEvaluator(labelCol = "label", rawPredictionCol = "prediction", metricName = "areaUnderROC")

dt = DecisionTreeClassifier(labelCol='label', featuresCol='features', seed=54321)

paramGrid = (
                ParamGridBuilder()
                    .addGrid(dt.maxDepth, [5, 10, 15])
                    .addGrid(dt.maxBins, [10, 20, 30])
                    .build()
            )


cv = CrossValidator(
                    estimator = dt,
                    estimatorParamMaps = paramGrid,
                    evaluator = evaluatorPR,
                    numFolds = 3
                    )

pipeline_cv = Pipeline(stages=[va, cv])

# COMMAND ----------

# TRAIN
cv_model = pipeline_cv.fit(train_df)

# COMMAND ----------

# EVALUATE
test_predictions = cv_model.transform(test_df)

pr_test = evaluatorPR.evaluate(test_predictions)
auc_test = evaluatorAUC.evaluate(test_predictions)

print(f"Precision-Recall (PR) on TEST data: {pr_test:.3f}")
print(f"Area under the Curve (AUC) on TEST data: {auc_test:.3f}")

# COMMAND ----------

# LOG RESULTS WITH MLFLOW

import mlflow
import mlflow.spark

import os

mlflow_experiment_id = 3794168472159363

with mlflow.start_run(experiment_id = mlflow_experiment_id) as run:
    # Log metrics
    mlflow.log_metric("PR", pr_test)
    mlflow.log_metric("AUC", auc_test)
    
    # Log model
    # add feature in code above that allows me to track the base model too (i.e., a fit "dt" model)
    mlflow.spark.log_model(cv_model, "trained_model")
