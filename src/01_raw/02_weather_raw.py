# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Weather
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from datetime import datetime
import json

# Read the weather JSON file
weather_file = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/weather/nyc_weather_2026-03_to_05.json"

# Read JSON file as text first
with open(weather_file, 'r') as f:
    weather_json = json.load(f)

# Extract metadata
latitude = weather_json['latitude']
longitude = weather_json['longitude']
timezone = weather_json['timezone']
elevation = weather_json['elevation']

# Extract hourly data arrays
hourly = weather_json['hourly']
times = hourly['time']
temperatures = hourly['temperature_2m']
precipitation = hourly['precipitation']
rain = hourly['rain']
snowfall = hourly['snowfall']
weather_codes = hourly['weather_code']
wind_speeds = hourly['wind_speed_10m']

# Create list of records
records = []
for i in range(len(times)):
    records.append({
        'observation_time': times[i],
        'temperature_2m': temperatures[i],
        'precipitation': precipitation[i],
        'rain': rain[i],
        'snowfall': snowfall[i],
        'weather_code': weather_codes[i],
        'wind_speed_10m': wind_speeds[i],
        'latitude': latitude,
        'longitude': longitude,
        'timezone': timezone,
        'elevation': elevation
    })

# Create DataFrame from records
df = spark.createDataFrame(records)

# Convert observation_time to timestamp and add metadata
batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

df_with_metadata = df \
    .withColumn('observation_time', F.to_timestamp('observation_time')) \
    .withColumns({
        'source_system': F.lit('OpenMeteo_Weather_API'),
        'ingested_at': F.current_timestamp(),
        'batch_id': F.lit(batch_id)
    })

# Save to Delta table
table_name = "nyc_mobility.raw.weather"
df_with_metadata.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✓ Saved weather data -> {table_name}")
print(f"  Records: {df_with_metadata.count():,}")
print(f"  Date range: {times[0]} to {times[-1]}")
print(f"  Batch ID: {batch_id}")

# COMMAND ----------

# DBTITLE 1,Preview Weather Data
# Preview the ingested weather data
display(spark.table("nyc_mobility.raw.weather").orderBy("observation_time").limit(20))