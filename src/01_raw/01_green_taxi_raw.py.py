# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Ingest Green Taxi Data
from pyspark.sql import functions as F
from datetime import datetime

# Read parquet files from volume and save with metadata to nyc_mobility.raw schema
volume_path = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/green_taxi/"

# Generate batch_id based on current timestamp
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

months = [
    ("green_tripdata_2026-03.parquet", "green_03_2026"),
    ("green_tripdata_2026-04.parquet", "green_04_2026"),
    ("green_tripdata_2026-05.parquet", "green_05_2026"),
]

for filename, table_name in months:
    df = spark.read.parquet(f"{volume_path}{filename}")
    
    # Add metadata columns: source_system, ingested_at, batch_id
    df_with_metadata = df.withColumns({
        "source_system": F.lit("NYC_TLC_Green_Taxi"),
        "ingested_at": F.current_timestamp(),
        "batch_id": F.lit(batch_id)
    })
    
    full_table = f"nyc_mobility.raw.{table_name}"
    df_with_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)
    print(f"✓ Saved {filename} -> {full_table} ({df_with_metadata.count()} rows) [batch_id: {batch_id}]")