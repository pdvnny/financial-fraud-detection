# Config Directory

This directory contains configruation files and reference config info for interacting with Databricks.  
.  
Anyone who adds to this directory should list below what the purpose is of the files that you add. You can use the first entry below as an example.

# Configuration Files Added

1. Configuration for access DB with `curl`
  * A `~/.netrc` file can be used along with `curl --netrc` to store and provide authentication when making an API request.
  * The `.netrc` file added in this directory will primarily be used to interact with the Databricks Repos API for now.
  * In theory, the file should be usable for making requests to any Databricks API.
  

