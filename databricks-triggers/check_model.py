"""
Parker Dunn (pgdunn@bu.edu)

Created Dec 1, 2022

GOAL: Fetch the most recent MLflow run saved.
      I am working with a hardcoded experiment ID for now!
"""

import os
import requests
import json
# from databricks_cli.sdk.api_client import ApiClient
# from databricks_cli.dbfs.api import DbfsApi
# https://github.com/databricks/databricks-cli/blob/main/databricks_cli/dbfs/api.py
# from databricks_cli.dbfs.dbfs_path import DbfsPath
# https://github.com/databricks/databricks-cli/blob/main/databricks_cli/dbfs/dbfs_path.py

"""
FIRST

Retrieve data about the most recent experiment run using DBFS API

**Not able to sort out accesing MLflow experiments on Databricks at the moment**
"""

with open("run_info.json", 'r') as f:
    run_info = json.load(f)

RUN_ID = run_info["run_id"]
METRIC = run_info["metric"]
#
# api_client = ApiClient(
#     host=os.getenv("DATABRICKS_HOST"),
#     token=os.getenv("DATABRICKS_TOKEN")
# )

# PATH = DbfsPath("dbfs:/Users/pgdunn@bu.edu/financial-fraud-detection")
# PATH = DbfsPath("dbfs:/databricks/mlflow-tracking/3794168472159363")
#
# dbfs_api = DbfsApi(api_client)
# files_list = dbfs_api.list_files(PATH)
# print(files_list)

"""
The MLflow API does not have a python version  unfortunately!

I'm going to try to configure a direct GET/POST requests.
"""


# host = os.getenv("DATABRICKS_HOST")
# token = os.getenv("DATABRICKS_TOKEN")


