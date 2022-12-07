"""
Parker Dunn (pgdunn@bu.edu)

Created Dec 1, 2022

GOALS:
(1) Check that "run_info.json" indicates that the model passed
(2) Change model status in Databricks to staged
"""

import os
import requests
import json

from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.dbfs.api import DbfsApi
# https://github.com/databricks/databricks-cli/blob/main/databricks_cli/dbfs/api.py
from databricks_cli.dbfs.dbfs_path import DbfsPath
# https://github.com/databricks/databricks-cli/blob/main/databricks_cli/dbfs/dbfs_path.py

"""
FIRST

Retrieve data about the most recent experiment run using DBFS API

**Not able to sort out accesing MLflow experiments on Databricks at the moment**

ALTERNATIVE

I created a dbfs location to track model training results!!!
Access the run corresponding with run_id provided.

"""

# with open('jobs_info.json', 'r') as local_f:
#     data = json.load(local_f)
# RUN_ID = data["run_id"]
# RUN_ID = os.getenv("RUN_ID")

api_client = ApiClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

PATH = DbfsPath("dbfs:/FileStore/run_tracking/model_dev.json")

dbfs_api = DbfsApi(api_client)
dbfs_api.get_file(PATH, "db_run.json", overwrite=True)

os.listdir(".")

with open("db_run.json", 'r') as db_f:
    run_data = json.load(db_f)
print(run_data)

"""
SECOND

The MLflow API does not have a python version  unfortunately!

I'm going to try to configure a direct GET/POST requests.

GOAL: Get run information from file
"""
HOST = os.getenv('DATABRICKS_HOST')
TOKEN = os.getenv('DATABRICKS_TOKEN')

HEADER = {'Authorization': f'Bearer {TOKEN}'}
data = {
    "archive_existing_versions": True,
    "comment": "Model passed CI tests",
    "name": run_data["name"],
    "stage": "Staging",
    "version": 1
}

ENDPOINT = HOST + "/api/2.0/mlflow/databricks/model-versions/transition-stage"
print(ENDPOINT)
response = requests.post(ENDPOINT, json=data, headers=HEADER)
print(response)
response_dict = response.json()
print(response_dict)

"""
THIRD

Save information about the newly staged model in DBFS

** IMPORTANT STRUCTURE **
@ "dbfs:/FileStore/run_tracking/deployment/"
there will be two models stored.
(1) A model currently deployed
(2) A model that might be deployed (this will be added in the second part here)
"""
"""
Sample of 'response_dict'

{'model_version_databricks': 
    {
        'name': 'model_606436540a50453b92fcafa33f417d5c_2022-12-07',
        'version': '1', 
        'creation_timestamp': 1670383029822, 
        'last_updated_timestamp': 1670383070525, 
        'user_id': '3277622445539731', 
        'current_stage': 'Staging', 
        'source': 'dbfs:/databricks/mlflow-tracking/3794168472159363/606436540a50453b92fcafa33f417d5c/artifacts/trained_model', 
        'run_id': '606436540a50453b92fcafa33f417d5c', 
        'status': 'READY'
    }
}
"""

# Create dictionary of the important information
newly_registered_model = {
    'name': response_dict['model_version_databricks']['name'],
    'version': response_dict['model_version_databricks']['version']
}

# Save important information as a .json
with open("model_data.json", 'w') as mdl_f:
    json.dump(newly_registered_model, mdl_f)

NEW_MODEL_PATH = DbfsPath("dbfs:/FileStore/deployment/new_model.json")
put_respone = dbfs_api.put_file("model_data.json", NEW_MODEL_PATH, overwrite=True)
print(put_response)
