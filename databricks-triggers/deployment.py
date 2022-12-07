"""
Parker Dunn (pgdunn@bu.edu)

Created Dec 6, 2022

GOALS:
(1) Trigger deployment workflow on databricks
   (I'm going to make a jobs that takes care of
   deploying and making updates)
"""

import os
import requests
import json
import time

from databricks_cli.sdk.api_client import ApiClient
# for working with jobs
from databricks_cli.jobs.api import JobsApi
# for working with job runs
from databricks_cli.runs.api import RunsApi

api_client = ApiClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

jobs_api = JobsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/jobs/api.py
runs_api = RunsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/runs/api.py