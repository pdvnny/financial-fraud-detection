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


# host = os.getenv("DATABRICKS_HOST")
# token = os.getenv("DATABRICKS_TOKEN")


