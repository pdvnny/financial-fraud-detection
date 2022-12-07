"""
Parker Dunn (pgdunn@bu.edu)

Created Dec 6, 2022

GOALS:
(1) Use databricks-cli to run "unit_tests" job (the job should return True/False)
    depending on whether the tests pass

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

# loc_host = os.getenv("DATABRICKS_HOST")
# loc_token = os.getenv("DATABRICKS_TOKEN")
# print(loc_host, loc_token)

jobs_api = JobsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/jobs/api.py
runs_api = RunsApi(api_client)  # https://github.com/databricks/databricks-cli/blob/main/databricks_cli/runs/api.py

# --------------- TRIGGER UNIT TESTS -----------------
# Run a single job: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
unit_tests_run = jobs_api.run_now(800632553416329,
                                  jar_params=None,
                                  notebook_params=None,
                                  python_params=None,
                                  spark_submit_params=None)

# Returns:
# 'run_id' and 'number_in_job'
# print(unit_tests_run)

# -------------- GET RESULTS OF JOB -------------------
# Get job results: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsGet

run_results = runs_api.get_run_output(unit_tests_run["run_id"])
status = run_results['metadata']['state']['life_cycle_state']
while status == 'PENDING':
    time.sleep(2)
    run_results = runs_api.get_run_output(unit_tests_run["run_id"])
    status = run_results['metadata']['state']['life_cycle_state']

print(status)

while status == 'RUNNING':
    time.sleep(2)
    run_results = runs_api.get_run_output(unit_tests_run["run_id"])
    status = run_results['metadata']['state']['life_cycle_state']

print(status)
notebook_result = run_results['notebook_output']['result']
if not bool(notebook_result):
    raise Error("Unit tests failed!")


