"""
Parker Dunn (pgdunn@bu.edu)

Created Nov 29, 2022

GOAL: Create and run a job in databricks using the Jobs 2.1 API
"""

import json
import time

import os
from databricks_cli.sdk.api_client import ApiClient
# for working with jobs
from databricks_cli.jobs.api import JobsApi
# for working with job runs
from databricks_cli.runs.api import RunsApi

# ----------------------- SETUP ---------------------------------
BASE_ENDPOINT = "dbc-1f875879-7090.cloud.databricks.com"

# --------- Setup authentication with Databricks API ------------
# https://docs.databricks.com/dev-tools/python-api.html

# Wasn't working...
# api_client = ApiClient(
#               host="https://dbc-1f875879-7090.cloud.databricks.com",
#               password="dapi45c8fa00829d08991bbb9ae455da52a5"
#               )

api_client = ApiClient(
    host=os.getenv("DATABRICKS_HOST"),
    token=os.getenv("DATABRICKS_TOKEN")
)

jobs_api = JobsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/jobs/api.py
runs_api = RunsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/runs/api.py

# ---------------- GET JOB INFORMATION REQUEST -----------------
# Ref:  curl --netrc -X GET https://dbc-1f875879-7090.cloud.databricks.com/api/2.1/jobs/list
# ENDPOINT = BASE_ENDPOINT + "/api/2.1/jobs/list" - automatically configured actually
# all_jobs = jobs_api.list_jobs()
# print(type(all_jobs))
# print(all_jobs)

# --------------- TRIGGER JOB RUN ------------------------------
# Ref (wrong job_id though): curl --netrc -X POST
#               https://dbc-1f875879-7090.cloud.databricks.com/api/2.1/jobs/run-now
#               -H "Content-Type: application/json"
#               -d '{"job_id": 244203842649506}'
run_info = jobs_api.run_now(969761619068257,
                          jar_params=None,
                          notebook_params=None,
                          python_params=None,
                          spark_submit_params=None)
print(run_info)

# ---------------- WAIT FOR JOB COMPLETION -----------------------
# GET /2.1/jobs/runs/get
# Must provide "run_id"
status = runs_api.get_run(run_id=run_info["run_id"])
print(status)
while status["state"]["life_cycle_state"] != "TERMINATED":
    # print("waiting")
    time.sleep(10)
    status = runs_api.get_run(run_id=run_info["run_id"])

print("Run is done.")

