# Databricks Hackathon Submission
## Project: End-to-End Data & MLOps Pipeline
[Demo Video in LinkedIn](https://www.linkedin.com/posts/activity-7395691734997008384-9ZvB?utm_source=share&utm_medium=member_desktop&rcm=ACoAAALCRCMBwvDODdnUtEYs0mBkVQVeBA2iKbA) [Demo Video in Reddit](https://www.reddit.com/r/dataengineering/comments/1oymuvd/databricks_free_edition_hackathon/)
### 1. Introduction
This repository contains my submission for the Databricks Hackathon. The project implements a complete, end-to-end data and machine learning pipeline built on the Databricks Data Intelligence Platform, designed for **Databricks Free Edition** (available since June 2024).

The pipeline automates the following processes:
* **Data Ingestion:** Sourcing data from a public FTP site and a REST API, landing it in Unity Catalog Volumes.
* **Data Analytics:** Running analytical queries using PySpark to derive insights from the ingested data.
* **ML Pipeline:** Training, registering, and serving a HuggingFace Transformer model using MLflow and Databricks Model Serving.
* **Orchestration:** Automating the entire pipeline using Databricks Workflows with serverless compute.
* **Deployment:** Packaging the entire project (notebooks, jobs, and endpoints) as a Databricks Asset Bundle (DAB) for reproducible, CI/CD-friendly deployment.

---

### 2. Project Components

This project is broken down into the following parts, which are orchestrated by a Databricks Asset Bundle.

#### Part 0: Schema Setup
A SQL script (`set_up_schema.sql`) creates the necessary Unity Catalog structures:
* Catalog: `main`
* Schema: `main.hackathon`
* Volumes: `main.hackathon.bls_data` and `main.hackathon.population_data`

This runs as the first task in the workflow to ensure the infrastructure is ready.

#### Part 1 & 2: Data Ingestion
Two data sources are ingested and stored in Unity Catalog Volumes:
1.  **BLS Time Series Data:** A script fetches and syncs files from the [BLS open dataset](https://download.bls.gov/pub/time.series/pr/). It handles new/removed files and avoids duplicate uploads.
2.  **US Population Data:** A script calls the [DataUSA API](https://datausa.io/api/data?drilldowns=Nation&measures=Population) and saves the resulting JSON to a UC Volume.

**Notebook:** `01_data_ingestion.py`

#### Part 3: Data Analytics
An analysis notebook generates three reports:
1.  **Population Statistics:** The mean and standard deviation of the US population between 2013 and 2018.
2.  **Best Year Report:** For each `series_id` in the BLS data, finds the year with the maximum sum of values across all quarters.
3.  **Joined Report:** A combined report showing the `value` for `series_id = PRS30006032` (Q01) joined with the `population` for that same year.

**Notebook:** `02_data_analytics.py`

#### Part 4: Data Pipeline Orchestration
A **Databricks Workflow** with **serverless compute** automates the data pipeline with the following tasks:
1.  **Task 0 (Setup):** Runs the SQL script to create schema and volumes.
2.  **Task 1 (Ingestion):** Runs the notebook to ingest data from BLS and DataUSA API.
3.  **Task 2 (Analysis):** Runs the analytics notebook after ingestion completes.
4.  **Task 3 (ML Training):** Trains and registers the sentiment analysis model.

All tasks run on serverless compute, eliminating the need to manage clusters.

#### Part 5: MLOps with MLflow
A full MLOps lifecycle is implemented for a `Sentiment Analysis` model:
1.  **Train:** A notebook (`03_model_training.py`) loads a pretrained HuggingFace Transformer model.
2.  **Log:** The model is logged to MLflow with tracking.
3.  **Register:** The model is registered in the Unity Catalog Model Registry as `main.hackathon.sentiment_analysis_model`.
4.  **Serve:** A **Databricks Model Serving Endpoint** is created to serve the registered model, providing a REST API for real-time inference.

**Notebook:** `03_model_training.py`

#### Part 6: Deployment with Databricks Asset Bundles (DABs)
The entire project is defined within a `databricks.yml` file. This allows for one-command deployment of all project resources, including:
* The Databricks Workflow (Job) with serverless compute
* The Model Serving Endpoint
* All associated notebooks and SQL scripts
* Library dependencies

---

### 3. 🚀 How to Deploy and Run

This project is packaged as a Databricks Asset Bundle and is optimized for **Databricks Free Edition** with serverless compute.

#### Prerequisites:
* A [Databricks Free Edition](https://www.databricks.com/try-databricks) account (free tier available since June 2024)
* [Databricks CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) installed (v0.205.0 or higher recommended)
* Python 3.8+ installed locally

#### Initial Setup:

1. **Install Databricks CLI:**
   ```bash
   pip install databricks-cli
   ```

2. **Configure Databricks CLI:**
   ```bash
   databricks configure --token
   ```
   You'll be prompted to enter:
   - **Host:** Your workspace URL (e.g., `https://dbc-abc123-def.cloud.databricks.com`)
   - **Token:** Generate a personal access token from User Settings > Developer > Access Tokens

3. **Clone this repository:**
   ```bash
   git clone [YOUR_REPO_URL]
   cd databricks-hackathon
   ```

4. **Create the src directory structure:**
   ```bash
   mkdir -p src
   ```

5. **Move the notebooks and SQL file to the src folder:**
   ```bash
   mv 01_data_ingestion.py src/
   mv 02_data_analytics.py src/
   mv 03_model_training.py src/
   mv set_up_schema.sql src/
   ```

#### Deployment Steps:

1. **Validate the bundle configuration:**
   ```bash
   databricks bundle validate
   ```

2. **Deploy the bundle:**
   ```bash
   databricks bundle deploy -t dev
   ```
   This will:
   - Upload all notebooks to your workspace
   - Create the workflow job with serverless compute
   - Set up task dependencies
   - Create the model serving endpoint configuration

3. **Run the complete pipeline:**
   ```bash
   databricks bundle run databricks-hackathon-job -t dev
   ```

4. **Monitor the job:**
   - Go to your Databricks workspace
   - Navigate to **Workflows** in the left sidebar
   - Find `[Hackathon] End-to-End Pipeline`
   - Click on it to view task progress and outputs
   - All tasks run on serverless compute - no cluster management needed!

5. **Check the Model Serving Endpoint:**
   - Navigate to **Serving** in the left sidebar
   - Find the `sentiment-analysis` endpoint
   - Wait for it to be in "Ready" state (may take a few minutes)
   - Click on it to view the endpoint details and get the API URL

### 4. 📊 Viewing Results

After the pipeline completes:

#### Data Analytics Results:
- Open the `02_data_analytics.py` notebook output in the workflow run
- View the three analytical reports:
  * Population statistics (2013-2018)
  * Best year by series ID
  * Joined BLS and population data

#### ML Model:
- Navigate to **Machine Learning** > **Models** in the left sidebar
- Find `main.hackathon.sentiment_analysis_model`
- View model versions, metadata, and lineage

#### Model Serving Endpoint:

**Test via UI:**
1. Go to **Serving** > `sentiment-analysis`
2. Click on the **Query endpoint** tab
3. Test with sample input:
   ```json
   {
     "inputs": ["Databricks is awesome!", "This project is challenging."]
   }
   ```

**Test via API:**
```bash
# Get your endpoint URL from the Serving UI
curl -X POST \
  https://[YOUR-WORKSPACE].cloud.databricks.com/serving-endpoints/sentiment-analysis/invocations \
  -H "Authorization: Bearer [YOUR-TOKEN]" \
  -H "Content-Type: application/json" \
  -d '{
    "inputs": ["Databricks makes data engineering easy!"]
  }'
```

**Test via Python:**
```python
import requests
import os

# Set these from your workspace
DATABRICKS_HOST = "https://[YOUR-WORKSPACE].cloud.databricks.com"
DATABRICKS_TOKEN = "[YOUR-TOKEN]"

endpoint_url = f"{DATABRICKS_HOST}/serving-endpoints/sentiment-analysis/invocations"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

data = {
    "inputs": ["I love using Databricks!", "This is frustrating."]
}

response = requests.post(endpoint_url, headers=headers, json=data)
print(response.json())
```

### 5. 📂 Project Structure

```
databricks-hackathon/
├── databricks.yml              # DAB configuration file
├── README.md                   # This file
└── src/                        # Source code directory
    ├── set_up_schema.sql       # Schema and volume creation
    ├── 01_data_ingestion.py    # Data ingestion from BLS and API
    ├── 02_data_analytics.py    # Data analysis and reporting
    └── 03_model_training.py    # ML model training and registration
```

### 6. 🔧 Troubleshooting

**Issue:** Bundle deployment fails with authentication error  
**Solution:** Ensure your Databricks CLI is configured correctly with `databricks configure --token`. Verify your token is valid and hasn't expired.

**Issue:** SQL task fails to find `set_up_schema.sql`  
**Solution:** Ensure the file is in the `src/` folder and the `artifact_path` in `databricks.yml` is set to `src`.

**Issue:** Model serving endpoint stays in "Not Ready" state  
**Solution:** This can take 5-10 minutes on first deployment. Check the endpoint logs for any errors. Ensure the model version "1" exists in the registry.

**Issue:** "User-Agent required" error when downloading BLS data  
**Solution:** The notebooks already include User-Agent headers. If you still see this error, update the email in the User-Agent string to your own in `01_data_ingestion.py`.

**Issue:** Volume creation fails  
**Solution:** Ensure you have the necessary permissions in your workspace. The `main` catalog should be available by default in Free Edition.

**Issue:** Notebook runs out of memory  
**Solution:** Serverless compute automatically scales, but if you encounter memory issues, you can optimize the data processing by filtering or sampling in the notebooks.

### 7. 📝 Notes for Free Edition Users

* **Serverless Compute:** All jobs run on serverless compute - no need to create or manage clusters!
* **Model Serving:** Fully supported with scale-to-zero enabled to optimize costs.
* **Unity Catalog:** Full Unity Catalog support for data governance and model registry.
* **Usage Limits:** Free Edition has usage limits. Monitor your usage in the workspace settings.
* **Auto-scaling:** Serverless compute automatically scales based on workload.

### 8. 🎯 Key Features of This Implementation

* **Idempotent Data Ingestion:** The BLS sync only downloads new files and removes stale ones.
* **Serverless-First Design:** No cluster management required - everything runs on serverless compute.
* **End-to-End MLOps:** From data ingestion to model serving, all automated.
* **Asset Bundle:** One-command deployment of the entire project.
* **Production-Ready:** Uses Unity Catalog for governance and includes proper error handling.
* **Real-Time Inference:** Model serving endpoint provides REST API for predictions.

### 9. 🚀 Future Enhancements

* **Scheduled Runs:** Add a schedule to the workflow to run daily/weekly
* **Data Validation:** Add data quality checks using Great Expectations
* **Model Monitoring:** Track model performance and data drift
* **CI/CD Integration:** Add GitHub Actions workflow for automated deployment
* **Alerting:** Set up email/Slack notifications for job failures
* **Additional Models:** Train custom models on the BLS economic data
* **Dashboard:** Create a Databricks SQL dashboard to visualize insights

### 10. 📚 Resources

* [Databricks Free Edition](https://www.databricks.com/try-databricks)
* [Databricks Asset Bundles Documentation](https://docs.databricks.com/en/dev-tools/bundles/index.html)
* [Serverless Compute Documentation](https://docs.databricks.com/en/serverless-compute/index.html)
* [Model Serving Documentation](https://docs.databricks.com/en/machine-learning/model-serving/index.html)
* [Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html)

---

### 11. 📧 Contact

For questions or feedback about this project:
* Email: zhou.wu@lwtech.edu
* GitHub: https://github.com/zwu-net

---

**License:** MIT License 

**Acknowledgments:**
* Bureau of Labor Statistics for the open data
* DataUSA for the population API
* HuggingFace for the pretrained sentiment analysis model
* Databricks for the Free Edition platform and hackathon opportunity
