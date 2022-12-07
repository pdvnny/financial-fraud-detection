# Databricks notebook source
# NOT IMPLEMENTED RIGHT NOW

"""
This notebook is meant to remove "model serving"
from an ML registry model after it is no longer needed.

The notebook receives a model name, then goes into the
model registry, removes serving, and moves the model status
to "archived"
"""

# COMMAND ----------

model_to_archive = dbutils.jobs.taskValues.get(taskKey    = 'deploy_new_model', \
                                               key        = 'new_model_name', \
                                                debugValue = 'No model')

# This cell should determine which model needs to be archived and retrieve it, but not right now

# COMMAND ----------

print("Model name: ", model_name) # received as input

# COMMAND ----------

print("Model serving has been removed and model is archived.")
