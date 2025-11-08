# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1 & 2: Data Ingestion
# MAGIC 
# MAGIC This notebook ingests two data sources:
# MAGIC 1.  **BLS Time Series Data:** Scrapes and syncs files from the BLS FTP-style website into the `bls_data` UC Volume.
# MAGIC 2.  **US Population Data:** Fetches data from the DataUSA API and saves it to the `population_data` UC Volume.
# MAGIC 
# MAGIC It's designed to be idempotent—it won't re-download files that already exist and will remove files that are no longer in the source.

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4

# COMMAND ----------

import requests
import os
import json
from bs4 import BeautifulSoup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Widgets and Paths
# MAGIC 
# MAGIC Define the UC Volume paths.

# COMMAND ----------

# dbutils.widgets.text("catalog_name", "main", "Catalog Name")
# dbutils.widgets.text("schema_name", "hackathon", "Schema Name")

# catalog = dbutils.widgets.get("catalog_name")
# schema = dbutils.widgets.get("schema_name")

# Using hardcoded values for simplicity in the bundle
catalog = "main"
schema = "hackathon"

bls_volume_path = f"/Volumes/{catalog}/{schema}/bls_data"
pop_volume_path = f"/Volumes/{catalog}/{schema}/population_data"

# Create the directories if they don't exist (idempotent)
os.makedirs(bls_volume_path, exist_ok=True)
os.makedirs(pop_volume_path, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 1: BLS Time Series Data
# MAGIC 
# MAGIC Scrape the BLS directory and sync files.

# COMMAND ----------

def sync_bls_data():
    """
    Syncs files from the BLS data website to the UC Volume.
    - Downloads new files.
    - Deletes files from the volume that are no longer on the website.
    """
    base_url = "https://download.bls.gov/pub/time.series/pr/"
    
    # Per the hackathon hint, a User-Agent is required to avoid 403 Forbidden errors.
    # You should replace this with your own email or contact info.
    headers = {
        "User-Agent": "DatabricksHackathonParticipant/1.0 (your-email@example.com)"
    }

    print(f"Syncing data from {base_url} to {bls_volume_path}...")

    try:
        # 1. Get list of files from BLS website
        response = requests.get(base_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all <a> tags, which link to files
        source_files = set()
        for link in soup.find_all('a'):
            href = link.get('href')
            # Filter for actual data files (e.g., end in .txt, .data, .series)
            if href and not href.startswith(('?', '/')):
                source_files.add(href)
        
        print(f"Found {len(source_files)} files at source.")

        # 2. Get list of files currently in the UC Volume
        try:
            volume_files = set(os.listdir(bls_volume_path))
            print(f"Found {len(volume_files)} files in volume.")
        except FileNotFoundError:
            volume_files = set()
            print("Volume directory not found or is empty.")

        # 3. Find files to add or update
        files_to_download = source_files - volume_files
        print(f"Downloading {len(files_to_download)} new files...")
        for i, filename in enumerate(files_to_download):
            file_url = f"{base_url}{filename}"
            local_path = os.path.join(bls_volume_path, filename)
            
            try:
                file_response = requests.get(file_url, headers=headers)
                file_response.raise_for_status()
                with open(local_path, 'wb') as f:
                    f.write(file_response.content)
                if (i + 1) % 10 == 0:
                    print(f"  Downloaded {i + 1}/{len(files_to_download)}...")
            except requests.RequestException as e:
                print(f"  Failed to download {filename}: {e}")

        # 4. Find files to delete
        files_to_delete = volume_files - source_files
        print(f"Deleting {len(files_to_delete)} stale files...")
        for filename in files_to_delete:
            local_path = os.path.join(bls_volume_path, filename)
            os.remove(local_path)
            print(f"  Deleted {filename}")

        print("BLS data sync complete.")

    except requests.RequestException as e:
        print(f"Failed to access BLS website: {e}")
        raise

sync_bls_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 2: US Population API Data

# COMMAND ----------

def fetch_population_data():
    """
    Fetches US population data from the DataUSA API and saves it as a single JSON file.
    """
    api_url = "https://datausa.io/api/data?drilldowns=Nation&measures=Population"
    output_path = os.path.join(pop_volume_path, "population_data.json")

    print(f"Fetching population data from {api_url}...")
    
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        data = response.json()
        
        with open(output_path, 'w') as f:
            json.dump(data, f)
            
        print(f"Successfully saved population data to {output_path}")
        
    except requests.RequestException as e:
        print(f"Failed to fetch population data: {e}")
        raise

fetch_population_data()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingestion Complete
# MAGIC 
# MAGIC Both datasets are now available in their respective UC Volumes.

# COMMAND ----------

dbutils.notebook.exit("Data ingestion complete.")
