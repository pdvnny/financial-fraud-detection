# Databricks notebook source
# SITUATION

"""
Upon staging code, there is a model recently added to the model
registry and put in staging phase.

The name of this model and the version are stored in 
dbfs:/FileStore/deployment/new_model.json.

This notebook handles determining if the "new_model" should
continue to being deployed.

If it is deployed, then the currently deployed model is archived.
"""

# Author: Parker Dunn (pgdunn@bu.edu)
# Created Dec 6, 2022

# COMMAND ----------

# DBFS FILE PATHS

# new_model_path = "dbfs:/FileStore/deployment/new_model.json"
# current_deployment = "dbfs:/FileStore/deployment/deployed_model.json"

# GENERAL FILE PATHS USABLE WITH PYTHON PACKAGES
new_model_path = "/dbfs/FileStore/deployment/new_model.json"
current_deployment = "/dbfs/FileStore/deployment/deployed_model.json"

deploy_new = False

# COMMAND ----------

import os
import json

if "deployed_model.json" not in os.listdir("/dbfs/FileStore/deployment"):
    deploy_new = True
    current_deployment = None
else:
    with open(new_model_path, 'r') as new_f:
        new_model_info = json.load(new_f)
    with open(current_deployment, 'r') as deployed_f:
        deployed_model_info = json.load(deployed_f)
    print(new_model_info)
    print(deployed_model_info)
        
    # Alright, I wanted to setup a check that confirms which of the two models
    # had better performance. Due to the rush that we have been, I can't go back
    # and refactor my setup in time for the presentation.
    # For now, the new model will always be deployed.
    
    deploy_new = True

# COMMAND ----------

# MODIFYING the MLflow model registry and deploying a new model if needed

import requests

HOST = "https://dbc-1f875879-7090.cloud.databricks.com"
TOKEN = "dapi45c8fa00829d08991bbb9ae455da52a5"

HEADER = {'Authorization': f'Bearer {TOKEN}'}

ENDPOINT = HOST + "/preview/mlflow/endpoints-v2/enable"
print(ENDPOINT)

dbutils.jobs.taskValues.set(key    = 'new_model_name', \
                            value  = new_model_info['name']
                           )

dbutils.jobs.taskValues.set(key    = 'deployed_model_name', \
                            value  = deployed_model_info['name']
                           )

if deploy_new:
    # deploy the new model and return the model name so that we can check it is operational
    
    # new_model = mlflow.spark.load_model(f"models:/{new_model_info['name']}/Staging")
    # data = {
    #     "registered_model_name": new_model_info['name']
    # }
    # response = requests.post(ENDPOINT, json=data, headers=HEADER)
    # print(response)
    # response_dict = response.json()
    # print(response_dict)
    
    dbutils.notebook.exit(new_model_info['name'])
    
else:
    dbutils.notebook.exit(deployed_model_info['name'])
    
# THIS ISN'T WORKING FOR NOW
# See: https://docs.databricks.com/mlflow/create-manage-serverless-model-endpoints.html
# See: https://docs.databricks.com/mlflow/create-manage-serverless-model-endpoints.html#core-api-objects

# COMMAND ----------

# export DATABRICKS_HOST="https://dbc-1f875879-7090.cloud.databricks.com"
# export DATABRICKS_TOKEN="dapi45c8fa00829d08991bbb9ae455da52a5"

# COMMAND ----------

# Moving the take down process to another notebook

# if current_deployment is not None:
#     current_model = mlflow.spark.load_model(f"models::/{deployed_model_info['name']}/Production")

# COMMAND ----------

# with open(current_deployment, 'w') as f:
#     write_this = {'name': 'model_5_2022-12-02', 'version': '1'}
#     json.dump(write_this, f)
