"""
This module defines the following routines used by the 'ingest' step:

- ``load_file_as_dataframe``: Defines customizable logic for parsing dataset formats that are not
  natively parsed by MLflow Recipes (i.e. formats other than Parquet, Delta, and Spark SQL).
  
"""

from pandas import DataFrame

def load_file_as_dataframe(location: str) -> DataFrame:
    """
    Load content from the specified dataset file as a Pandas DataFrame.
    
    This method is used to load dataset types that are not natively  managed by MLflow Recipes
    (datasets that are not in Parquet, Delta Table, or Spark SQL Table format). This method is
    called once for each file in the dataset, and MLflow Recipes automatically combines the
    resulting DataFrames together.
    
    :param location: The path to the dataset file.
    :return: A Pandas DataFrame representing the content of the specified file.
    """
    
    # I don't need this method since the data we are using for now is available in Databricks as a table
    
    raise NotImplementedError