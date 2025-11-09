# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3: Data Analytics
# MAGIC
# MAGIC This notebook loads the data ingested in the previous step from their UC Volume locations and performs the required analytics.
# MAGIC
# MAGIC 1.  Calculate mean and standard deviation of the US population (2013-2018).
# MAGIC 2.  Find the "best year" (max sum of `value`) for each `series_id` in the BLS data.
# MAGIC 3.  Join the BLS data with the population data for a specific series.

# COMMAND ----------

from pyspark.sql.functions import col, mean, stddev, sum, desc, row_number, trim, explode
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup Paths

# COMMAND ----------

# Using hardcoded values for simplicity in the bundle
catalog = "main"
schema = "hackathon"

bls_file_path = f"/Volumes/{catalog}/{schema}/bls_data/pr.data.0.Current"
pop_file_path = f"/Volumes/{catalog}/{schema}/population_data/population_data.json"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3.0: Load Datasets

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Population Data
# MAGIC
# MAGIC The data is nested inside a 'data' array in the JSON. We'll explode it and select the inner fields.

# COMMAND ----------

try:
    # Read the raw JSON
    raw_pop_df = spark.read.json(pop_file_path)
    
    # Explode the 'data' array and select the nested fields
    pop_df = raw_pop_df.select(explode("data").alias("data")).select("data.*")
    
    # Cast Population and Year to numeric types for analysis
    pop_df = pop_df.withColumn("Population", col("Population").cast("long")) \
                   .withColumn("Year", col("Year").cast("int"))

    print("Population data loaded and flattened:")
    pop_df.printSchema()
    pop_df.show(5)

except Exception as e:
    print(f"Error loading population data: {e}")
    dbutils.notebook.exit(f"Failed to load population data from {pop_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load BLS Time Series Data
# MAGIC
# MAGIC This is a tab-separated file (.tsv) and requires trimming whitespace, as per the hackathon hint.

# COMMAND ----------

# Using hardcoded values for simplicity in the bundle

try:
    # Read the tab-delimited file with more robust options
    raw_bls_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("delimiter", "\t") \
        .option("inferSchema", "false") \
        .option("mode", "PERMISSIVE") \
        .option("columnNameOfCorruptRecord", "_corrupt_record") \
        .load(bls_file_path)

    # Check if we got any data
    row_count = raw_bls_df.count()
    print(f"Rows loaded: {row_count}")
    
    if row_count == 0:
        print("WARNING: No rows loaded. The file might be empty or format is incorrect.")
    
    # First, clean up column names by removing leading/trailing whitespace
    cleaned_columns = {c: c.strip() for c in raw_bls_df.columns}
    bls_df = raw_bls_df
    for old_name, new_name in cleaned_columns.items():
        if old_name != new_name:
            bls_df = bls_df.withColumnRenamed(old_name, new_name)
    
    print(f"Column names after cleaning: {bls_df.columns}")
    
    # Trim whitespace from all column values
    bls_df = bls_df.select([trim(col(c)).alias(c) for c in bls_df.columns])
    
    # Cast value to Double for aggregation (with error handling)
    if "value" in bls_df.columns:
        bls_df = bls_df.withColumn("value", col("value").cast(DoubleType()))
    else:
        print(f"WARNING: 'value' column not found. Available columns: {bls_df.columns}")

    print("BLS data loaded and trimmed:")
    bls_df.printSchema()
    bls_df.show(5, truncate=False)
    
    # Check for corrupt records
    if "_corrupt_record" in bls_df.columns:
        corrupt_count = bls_df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            print(f"\nWARNING: Found {corrupt_count} corrupt records")
            bls_df.filter(col("_corrupt_record").isNotNull()).show(5, truncate=False)

except Exception as e:
    print(f"Error loading BLS data: {e}")
    import traceback
    traceback.print_exc()
    dbutils.notebook.exit(f"Failed to load BLS data from {bls_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3.1: Population Data Analysis
# MAGIC
# MAGIC Generate the mean and the standard deviation of the annual US population across the years [2013, 2018] inclusive.

# COMMAND ----------

print("Report 1: US Population Mean & StdDev (2013-2018)")

pop_stats_df = pop_df.filter("Year >= 2013 AND Year <= 2018") \
    .select(
        mean("Population").alias("mean_population"),
        stddev("Population").alias("stddev_population")
    )

pop_stats_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3.2: Time-Series "Best Year" Analysis
# MAGIC
# MAGIC For every series_id, find the *best year*: the year with the max/largest sum of "value" for all quarters in that year.

# COMMAND ----------

print("Report 2: Best Year by Series ID")

# 1. Sum values by series_id and year
yearly_sum_df = bls_df.groupBy("series_id", "year") \
    .agg(sum("value").alias("total_value"))

# 2. Define a window to rank years by total_value for each series_id
window_spec = Window.partitionBy("series_id").orderBy(col("total_value").desc())

# 3. Find the top-ranked year for each series_id
best_year_df = yearly_sum_df.withColumn("rank", row_number().over(window_spec)) \
    .filter("rank = 1") \
    .select("series_id", "year", "total_value") \
    .orderBy("series_id")

best_year_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part 3.3: Joined Report
# MAGIC
# MAGIC Generate a report that will provide the `value` for `series_id = PRS30006032` and `period = Q01` and the `population` for that given year.

# COMMAND ----------

print("Report 3: Joined BLS and Population Data")

# 1. Filter BLS data
bls_filtered_df = bls_df.filter(
    (col("series_id") == "PRS30006032") & (col("period") == "Q01")
)

# 2. Prepare population data for join (cast Year to String to match BLS 'year' column)
pop_join_df = pop_df.withColumn("year_str", col("Year").cast("string"))

# 3. Join the two dataframes
joined_df = bls_filtered_df.join(
    pop_join_df,
    bls_filtered_df.year == pop_join_df.year_str,
    "inner"  # Explicitly specify join type
)

# 4. Select and show the final report
# Use explicit dataframe references to avoid ambiguity
final_report_df = joined_df.select(
    bls_filtered_df.series_id,
    bls_filtered_df.year,
    bls_filtered_df.period,
    bls_filtered_df.value,
    pop_join_df.Population
).orderBy(bls_filtered_df.year.desc())

final_report_df.show()

# COMMAND ----------

dbutils.notebook.exit("Data analysis complete.")
