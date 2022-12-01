"""
Parker Dunn (pgdunn@bu.edu)

Created Dec 1, 2022

------------- DEPRECATED ON DEC 1 2022 ------------
Reason:
I redefined the work flow a little so that I don't
really need to retrieve model information from Databricks.
The CI pipeline will basically automatically detect
if there is a problem with the model training procedure.

In a separate workflow, I will move the trained model to staging
using the MLflow API
__________________________________________________

GOAL: Fetch the most recent MLflow run saved.
      I am working with a hardcoded experiment ID for now!
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
Access the run corresponding with 

"""

api_client = ApiClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

PATH = DbfsPath("dbfs:/FileStore/run_tracking/")

dbfs_api = DbfsApi(api_client)
files_list = dbfs_api.list_files(PATH)
print(files_list)

"""
SECOND

The MLflow API does not have a python version  unfortunately!

I'm going to try to configure a direct GET/POST requests.

GOAL: Get run information from file
"""


# host = os.getenv("DATABRICKS_HOST")
# token = os.getenv("DATABRICKS_TOKEN")


