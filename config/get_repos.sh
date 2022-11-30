#!/usr/bin/bash
curl --netrc -X GET https://dbc-1f875879-7090.cloud.databricks.com/api/2.0/repos >> response.json
gedit response.json
