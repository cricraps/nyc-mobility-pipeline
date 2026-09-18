# NYC Mobility Data Pipeline Architecture

## Overview

This document describes the **layered architecture** of the NYC Mobility data pipeline, following the **Bronze → Silver → Gold** (Medallion) design pattern, plus a dedicated **Validation** layer. Each layer represents a stage of data refinement, moving from raw ingested data (taxi trips, weather, zones, and traffic advisories) to clean, structured data, and finally to business-ready analytical models.

<img width="773" height="412" alt="pipeline" src="https://github.com/user-attachments/assets/ad28807b-a3c5-4654-9e9f-d1a599770953" />




## Data Sources

The pipeline ingests from multiple heterogeneous sources — trip records, weather, reference data, and traffic advisories — accessed in Databricks via a configured **Unity Catalog Volume**.

- **Volume path:** `/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f`
- **Ingestion method:** `read_files()` for file-based sources; REST/API calls and web scraping for external live sources, landed into the Volume before being loaded into raw tables

| Source | Pattern | Where From | Raw Ingestion File |
|---|---|---|---|
| Green taxi trips | Monthly Parquet | `d37ci6vzurychx.cloudfront.net/trip-data/` | `01_green_taxi_raw.py` |
| Open-Meteo ERA5 (weather) | REST, JSON | `archive-api.open-meteo.com/v1/archive` | `02_weather_raw.py` |
| Taxi zone lookup | One CSV | `d37ci6vzurychx.cloudfront.net/misc/` | `03_taxi_zones_raw.py` |
| NYC DOT weekly advisory | Web scrape, HTML | `nyc.gov/html/dot/html/motorist/weektraf.shtml` | `04_traffic_advisory_raw.ipynb` |
| Construction closures | Socrata SoQL, CSV | `data.cityofnewyork.us/resource/ezy6-djsf.csv` | `04_traffic_advisory_raw.ipynb` |

**Example ingestion pattern:**

```python
df = spark.read.parquet(
  "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/01_raw/green_taxi/"
)
```

---

## Bronze Layer — `01_raw/`

**Purpose:** Ingests raw data directly from each source system (trip records, weather, zones, advisories) with **no transformation applied**. This layer is an exact, untouched copy of the source data — no cleaning, renaming, filtering, or restructuring — and acts as the single source of truth for raw records.

**Characteristics:**

- **No changes made to the data** — values, formats, and structure are identical to the source
- Schema mirrors each source system exactly
- Mixed ingestion methods: scheduled Parquet/CSV pulls, REST API calls, and HTML scraping — depending on the source
- Used for traceability and reprocessing if downstream layers need to be rebuilt

**Files:**

| File                             | Description                                              |
| --------------------------------- | ----------------------------------------------------------- |
| `01_green_taxi_raw.py`            | Raw ingestion of monthly green taxi trip Parquet files      |
| `02_weather_raw.py`               | Raw ingestion of Open-Meteo ERA5 weather data via REST API   |
| `03_taxi_zones_raw.py`            | Raw ingestion of taxi zone lookup CSV                        |
| `04_traffic_advisory_raw.ipynb`   | Raw ingestion of NYC DOT weekly advisory (scraped) and construction closures (Socrata) |

---

## Silver Layer — `02_clean/`

**Purpose:** Cleans, standardizes, and validates the raw data from the Bronze layer. This includes handling nulls, correcting data types, removing duplicates/invalid records, and applying consistent naming conventions across all four sources.

**Characteristics:**

- Data quality rules applied (e.g. removing invalid trips, filling/flagging missing weather readings, standardizing advisory text/dates)
- Standardized column names and formats across trip, weather, zone, and advisory data
- Deduplicated and validated records
- Serves as the trusted, analysis-ready foundation for the Gold layer

**Files:**

| File                              | Description                                       |
| ----------------------------------- | ---------------------------------------------------- |
| `01_green_taxi_clean.ipynb`         | Cleaned and standardized green taxi trip data         |
| `02_weather_clean.ipynb`            | Cleaned and standardized weather data                 |
| `03_taxi_zones_clean.ipynb`         | Cleaned and standardized taxi zone reference data      |
| `04_traffic_advisory_clean.ipynb`   | Cleaned and standardized traffic advisory/closure data |

---

## Gold Layer — `03_mart/`

**Purpose:** Transforms cleaned data into a **dimensional model** (star schema) optimized for analytics and reporting. This layer contains dimension and fact tables joining trip, weather, zone, and advisory data.

**Characteristics:**

- Star schema design (dimensions + facts)
- Business logic and aggregations applied (trip duration, weather conditions at trip time, advisory/incident flags, etc.)
- Optimized for query performance and reporting tools

**Files:**

| File                            | Description                                                     |
| ---------------------------------- | ------------------------------------------------------------------- |
| `01_dim_advisory_table.ipynb`      | Traffic advisory / construction closure dimension table              |
| `02_dim_date_table.ipynb`          | Date dimension table                                                  |
| `03_dim_weather_table.ipynb`       | Weather dimension table                                               |
| `04_dim_zone_table.ipynb`          | Pickup/dropoff taxi zone dimension table                              |
| `05_fact_trip_table.ipynb`         | Fact table capturing trip-level records, joined to date, weather, zone, and advisory dimensions |

---

## Visualization Layer — `04_visualisation/`

**Purpose:** Contains query logic that powers dashboards and reports, built on top of the Gold layer's dimensional model. These queries answer specific mobility, weather-impact, and traffic-incident questions.

**Files:**

| File                                       | Description                                              |
| --------------------------------------------- | ------------------------------------------------------------- |
| `01_bi_demand_and_popularity...`               | Trip demand and popularity analysis (by zone, time, etc.)      |
| `02_bi_temporal_patterns_and_...`              | Temporal/seasonal trip pattern analysis                        |
| `03_bi_weather_impact_analysis`                | Impact of weather conditions on trip demand and behavior       |
| `04_bi_traffic_advisory_and_inci...`           | Impact of traffic advisories/incidents on trip patterns        |

---

## Validation Layer — `05_validation/`

**Purpose:** Profiles and validates data quality across sources — checking completeness, freshness, and correctness before/after cleaning. Acts as a quality gate distinct from the Bronze/Silver/Gold transformation flow.

**Files:**

| File                                    | Description                                       |
| ------------------------------------------ | ---------------------------------------------------- |
| `01_profile_source_data.sql.dbquery`        | Source data profiling across all raw tables            |
| `02_green_taxi_validation.ipynb`            | Validation checks for green taxi trip data              |
| `03_weather_validation.dbquery`             | Validation checks for weather data                      |
| `04_taxi_zones_validation.ipynb`            | Validation checks for taxi zone reference data           |
| `05_traffic_advisory_validation.ipynb`      | Validation checks for traffic advisory/closure data      |

---

## Summary of Data Flow

1. **Bronze (`01_raw`)** — Raw data lands as-is from four source types (green taxi Parquet, Open-Meteo weather API, taxi zone CSV, NYC DOT/Socrata traffic advisories) into the Unity Catalog Volume, with no modifications.
2. **Silver (`02_clean`)** — Raw data from each source is cleaned, validated, and standardized independently.
3. **Gold (`03_mart`)** — Clean data is modeled into dimension tables (date, weather, zone, advisory) and a central trip fact table for analytics.
4. **Visualization (`04_visualisation`)** — Gold layer tables are queried to generate business insights on demand patterns, weather impact, and traffic advisory effects.
5. **Validation (`05_validation`)** — Source and cleaned data are profiled and checked at each stage to ensure data quality across the pipeline.
