# Databricks notebook source
# MAGIC %md
# MAGIC Following reference material defined by MLflow documentation
# MAGIC 
# MAGIC * MLflow recipes [template repo provided on GitHub](https://github.com/mlflow/recipes-classification-template)
# MAGIC * Template Jupyter Notebook (from link above): https://github.com/mlflow/recipes-classification-template/blob/main/notebooks/jupyter.ipynb

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC # Notebook that uses MLflow Our Custom Classification Recipe
# MAGIC 
# MAGIC This notebook runs the MLflow Classification Recipe on Databricks and inspects its results. For more information about the MLflow Classification Recipe, including usage examples, see the [Classification Recipe overview documentation](https://mlflow.org/docs/latest/recipes.html#classification-recipe) the [Classification Recipe API documentation](https://mlflow.org/docs/latest/python_api/mlflow.recipes.html#module-mlflow.recipes.classification.v1.recipe).

# COMMAND ----------

from mlflow.recipes import Recipe

r = Recipe(profile="databricks")

# COMMAND ----------

r.inspect()

# COMMAND ----------

r.run("ingest")

# COMMAND ----------

r.run("split")

# COMMAND ----------

training_data = r.get_artifact("training_data")
training_data.describe()

# COMMAND ----------

r.run("transform")

# COMMAND ----------

validation_data = r.get_artifact("validation_data")
validation_data.describe()

# COMMAND ----------

r.run("train")

# COMMAND ----------

r.run("")
