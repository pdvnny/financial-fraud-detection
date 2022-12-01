# Databricks notebook source
# CREATE MODEL & TRAIN

"""
Part 3 from "Repos/financial_fraud_dev/jobs/ml_pipeline"

The code in this file creates a PySpark DecisionTreeClassifier and trains the model on the training data
"""

# COMMAND ----------

# Load training data -> saved in Part 2 as a table "balanced_training_data"
train_df = spark.table("hive_metastore.default.balanced_training_data")

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
test_df = spark.table("test_data")

test_predictions = cv_model.transform(test_df)

pr_test = evaluatorPR.evaluate(test_predictions)
auc_test = evaluatorAUC.evaluate(test_predictions)

print(f"Precision-Recall (PR) on TEST data: {pr_test:.3f}")
print(f"Area under the Curve (AUC) on TEST data: {auc_test:.3f}")

# COMMAND ----------

import mlflow
import mlflow.spark

mlflow_experiment_id = 3794168472159363

with mlflow.start_run(experiment_id = mlflow_experiment_id) as run:
    # print(run.info.run_id) THIS WORKS! # https://www.mlflow.org/docs/latest/python_api/mlflow.html
    # Log metrics
    mlflow.log_metric("PR", pr_test)
    mlflow.log_metric("AUC", auc_test)
    
    # Log model
    # add feature in code above that allows me to track the base model too (i.e., a fit "dt" model)
    mlflow.spark.log_model(cv_model, "trained_model")
