# Databricks Hackathon Submission
## Project: End-to-End Data & MLOps Pipeline

### 1. Introduction
This repository contains my submission for the Databricks Hackathon. The project implements a complete, end-to-end data and machine learning pipeline built entirely on the Databricks Data Intelligence Platform.

The pipeline automates the following processes:
* **Data Ingestion:** Sourcing data from a public FTP site and a REST API, landing it in Unity Catalog Volumes.
* **Data Analytics:** Running analytical queries using PySpark to derive insights from the ingested data.
* **ML Pipeline:** Training, registering, and serving a HuggingFace Transformer model using MLflow and Databricks Model Serving.
* **Orchestration:** Automating the data ingestion and analytics tasks using Databricks Workflows.
* **Deployment:** Packaging the entire project (notebooks, jobs, and endpoints) as a Databricks Asset Bundle (DAB) for reproducible, CI/CD-friendly deployment.

---

### 2. Project Components

This project is broken down into the following parts, which are orchestrated by a Databricks Asset Bundle.

#### Part 1 & 2: Data Ingestion
Two data sources are ingested and stored in Unity Catalog Volumes:
1.  **BLS Time Series Data:** A script fetches and syncs files from the [BLS open dataset](https://download.bls.gov/pub/time.series/pr/). It handles new/removed files and avoids duplicate uploads.
2.  **US Population Data:** A script calls the [DataUSA API](https://datausa.io/api/data?drilldowns=Nation&measures=Population) and saves the resulting JSON to a UC Volume.

#### Part 3: Data Analytics
An analysis notebook (`02_data_analytics.ipynb`) is run on the ingested data to generate three reports:
1.  **Population Statistics:** The mean and standard deviation of the US population between 2013 and 2018.
2.  **Best Year Report:** For each `series_id` in the BLS data, finds the year with the maximum sum of values across all quarters.
3.  **Joined Report:** A combined report showing the `value` for `series_id = PRS30006032` (Q01) joined with the `population` for that same year.

#### Part 4: Data Pipeline Orchestration
A **Databricks Workflow** automates the data pipeline. It consists of two main tasks:
1.  **Task 1 (Ingestion):** Runs the notebook/script to execute Parts 1 and 2. This is scheduled to run daily.
2.  **Task 2 (Analysis):** Runs the analytics notebook (Part 3) after the ingestion task successfully completes.

#### Part 5: MLOps with MLflow
A full MLOps lifecycle is implemented for a `Sentiment Analysis` model:
1.  **Train:** A notebook (`03_model_training.ipynb`) logs a pretrained HuggingFace Transformer model to MLflow.
2.  **Register:** The model is registered in the Unity Catalog Model Registry.
3.  **Serve:** A **Databricks Model Serving Endpoint** is created to serve the registered model, providing a REST API for real-time inference.

#### Part 6: Deployment with Databricks Asset Bundles (DABs)
The entire project is defined within a `databricks.yml` file. This allows for one-command deployment of all project resources, including:
* The Databricks Workflow (Job)
* The Model Serving Endpoint
* All associated notebooks and library dependencies

---

### 3. 🚀 How to Deploy and Run

This project is packaged as a Databricks Asset Bundle.

**Prerequisites:**
* [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) installed and configured with your workspace.
* Permissions to create jobs, volumes, and model serving endpoints in your Databricks workspace.

**Deployment Steps:**
1.  Clone this repository:
    ```bash
    git clone [YOUR_REPO_URL]
    cd [YOUR_REPO_NAME]
    ```
2.  Deploy the bundle. This will create the job and model serving endpoint defined in `databricks.yml`.
    ```bash
    databricks bundle deploy
    ```
3.  Run the pipeline. Replace `databricks-hackathon-job` with the job name you defined in your `databricks.yml` file.
    ```bash
    databricks bundle run databricks-hackathon-job
    ```
4.  After the job completes, you can view the notebook outputs in your workspace and query the model serving endpoint.

### 4. 📂 Project Structure

Here is the project's directory structure:

* `/` (Project Root)
    * **`databricks.yml`**: The Databricks Asset Bundle configuration file.
    * **`README.md`**: This file.
    * **`src/`**: A directory containing all source notebooks.
        * `01_data_ingestion.ipynb`
        * `02_data_analytics.ipynb`
        * `03_model_training.ipynb`

---
