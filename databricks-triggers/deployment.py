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

# --------------- TRIGGER UNIT TESTS -----------------
# Run a single job: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow
deployment_run = jobs_api.run_now(824332610962496,
                                  jar_params=None,
                                  notebook_params=None,
                                  python_params=None,
                                  spark_submit_params=None)

# Returns:
# 'run_id' and 'number_in_job'
print(deployment_run)

# -------------- GET RESULTS OF JOB -------------------
# Get job results: https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsGet

# THIS STUFF DOESN'T WORK FOR A JOB WITH MULTIPLE NOTEBOOKS/TASKS

# run_results = runs_api.get_run_output(deployment_run["run_id"])
# status = run_results['metadata']['state']['life_cycle_state']
# while status == 'PENDING':
#     time.sleep(2)
#     run_results = runs_api.get_run_output(unit_tests_run["run_id"])
#     status = run_results['metadata']['state']['life_cycle_state']
#
# print(status)
#
# while status == 'RUNNING':
#     time.sleep(2)
#     run_results = runs_api.get_run_output(unit_tests_run["run_id"])
#     status = run_results['metadata']['state']['life_cycle_state']
#
# print(status)
# notebook_result = run_results['notebook_output']['result']

# ERROR RECEIVED WHEN RUNNING MULTIPLE TASKS

# """
# Traceback (most recent call last):
#   File "/opt/hostedtoolcache/Python/3.9.15/x64/lib/python3.9/site-packages/databricks_cli/sdk/api_client.py", line 166, in perform_query
#     resp.raise_for_status()
#   File "/opt/hostedtoolcache/Python/3.9.15/x64/lib/python3.9/site-packages/requests/models.py", line 1021, in raise_for_status
#     raise HTTPError(http_error_msg, response=self)
# requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: ***/api/2.0/jobs/runs/get-output?run_id=62232
#
# During handling of the above exception, another exception occurred:
#
# Traceback (most recent call last):
#   File "/home/runner/work/financial-fraud-detection/financial-fraud-detection/databricks-triggers/deployment.py", line 46, in <module>
#     run_results = runs_api.get_run_output(deployment_run["run_id"])
#   File "/opt/hostedtoolcache/Python/3.9.15/x64/lib/python3.9/site-packages/databricks_cli/runs/api.py", line 46, in get_run_output
#     return self.client.get_run_output(run_id, version=version)
#   File "/opt/hostedtoolcache/Python/3.9.15/x64/lib/python3.9/site-packages/databricks_cli/sdk/service.py", line 465, in get_run_output
#     return self.client.perform_query(
#   File "/opt/hostedtoolcache/Python/3.9.15/x64/lib/python3.9/site-packages/databricks_cli/sdk/api_client.py", line 174, in perform_query
#     raise requests.exceptions.HTTPError(message, response=e.response)
# requests.exceptions.HTTPError: 400 Client Error: Bad Request for url: ***/api/2.0/jobs/runs/get-output?run_id=62232
#  Response from server:
#  { 'error_code': 'INVALID_PARAMETER_VALUE',
#   'message': 'Retrieving the output of runs with multiple tasks is not '
#              'supported. Please retrieve the output of each individual task '
#              'run instead.'}
# """
