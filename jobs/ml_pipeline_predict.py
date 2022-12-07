# Databricks notebook source
# LOADING DATA **ONLY** AS AN EXAMPLE (in general we would expect to get new data that is not in databricks)

sample = spark.table("test_data").limit(10)
display(sample)

# COMMAND ----------

# NOTE

# this approach below is not realistic
# The data above was processed to have extra columns (typeIndexed & label)

# Normally, we would want to put this type of notebook in a Workflow with a "transform" notebook **OR** do some transformation on the incoming data

# HOWEVER, I'm skipping that stuff for now

# COMMAND ----------

# ANOTHER NOTE

# I'm actually pulling this model from Experiments -> /Users/pgdunn@bu.edu/financial-fraud-detection
# It's also in the model registry, but I'm not sure if I can pull models from there.


# You can definitely deploy the registry models on databricks (open one up and try clicking the "use for inference option")

# COMMAND ----------

# SPARK DATAFRAME VERSION

import mlflow
logged_model = 'runs:/806fa47fbcf744aaa091d1f68c45610c/trained_model'

# Load model
loaded_model = mlflow.spark.load_model(logged_model)

# Perform inference via model.transform()
predictions = loaded_model.transform(sample)
display(predictions.select("prediction"))

# COMMAND ----------

# PANDAS DATAFRAME VERSION

import mlflow
logged_model = 'runs:/806fa47fbcf744aaa091d1f68c45610c/trained_model'

# Load model as a PyFuncModel.
loaded_model = mlflow.pyfunc.load_model(logged_model)

# Predict on a Pandas DataFrame.
import pandas as pd
loaded_model.predict(pd.DataFrame(sample))
