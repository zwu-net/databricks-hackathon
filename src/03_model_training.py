# Databricks notebook source
# MAGIC %md
# MAGIC # Part 5: Machine Learning with MLflow
# MAGIC 
# MAGIC This notebook implements the MLOps part of the project:
# MAGIC 1.  Loads a pretrained HuggingFace Transformer model for sentiment analysis.
# MAGIC 2.  Logs the model to MLflow.
# MAGIC 3.  Registers the model in the Unity Catalog Model Registry.
# MAGIC 
# MAGIC The Databricks Asset Bundle (`databricks.yml`) will then pick up this registered model and deploy it to a Model Serving endpoint.

# COMMAND ----------

# MAGIC %pip install mlflow "transformers[torch]" "accelerate>=0.21.0"

# COMMAND ----------

import mlflow
from transformers import pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Model Name and Registry

# COMMAND ----------

# Using hardcoded values for simplicity in the bundle
catalog = "main"
schema = "hackathon"
model_name = "sentiment_analysis_model"

# This is the three-level name for the model in Unity Catalog
uc_model_name = f"{catalog}.{schema}.{model_name}"

# Set the registry URI to use Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 5.0: Load Pretrained Model
# MAGIC 
# MAGIC We'll use a standard, lightweight sentiment analysis model from HuggingFace.

# COMMAND ----------

hf_model_name = "distilbert-base-uncased-finetuned-sst-2-english"
sentiment_pipeline = pipeline("sentiment-analysis", model=hf_model_name)

print(f"Model {hf_model_name} loaded.")

# Test the pipeline
print(sentiment_pipeline("Databricks is awesome!"))
print(sentiment_pipeline("This project is challenging but fun."))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 5.1 & 5.2: Log and Register Model
# MAGIC 
# MAGIC We will log the model to MLflow and register it as a new version in Unity Catalog.

# COMMAND ----------

print(f"Logging and registering model to {uc_model_name}...")

with mlflow.start_run() as run:
    # Log the model using the transformers flavor
    model_info = mlflow.transformers.log_model(
        transformers_model=sentiment_pipeline,
        artifact_path="model",
        input_example=["This is a sample sentence."],
        registered_model_name=uc_model_name  # This automatically registers the model
    )

print(f"Model logged successfully.")
print(f"Model URI: {model_info.model_uri}")
print(f"Registered model version: {model_info.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Model Registration Complete
# MAGIC 
# MAGIC The model is now registered in Unity Catalog. The `databricks.yml` file is configured to find this model (`main.hackathon.sentiment_analysis_model`) and deploy it to a Model Serving endpoint named `sentiment-analysis`.
# MAGIC 
# MAGIC After deploying the bundle (`databricks bundle deploy`), you can go to the **Serving** tab in your workspace to see the endpoint being created.

# COMMAND ----------

dbutils.notebook.exit(f"Model registered to {uc_model_name} as version {model_info.version}")
