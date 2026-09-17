# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Traffic advisory validation
# TRAFFIC ADVISORY, VALIDATION
# Source  nyc_mobility.raw.traffic_advisory, nyc_mobility.clean.traffic_advisory
# Grain   one row per check
# Output  nyc_mobility.validation.traffic_advisory_validation
# Before  run 01_raw/04_traffic_advisory_raw then 02_clean/04_traffic_advisory_clean
# Note    columns follow 02_weather_validation so the six sources union cleanly.
#         Profiling is not repeated here: 01_profile_source_data reads
#         information_schema for the raw schema, so raw.traffic_advisory is picked up
#         automatically once it exists. The clean schema is not covered by it.
from pyspark.sql import functions as F

RAW_TABLE = "nyc_mobility.raw.traffic_advisory"
CLEAN_TABLE = "nyc_mobility.clean.traffic_advisory"
VALIDATION_TABLE = "nyc_mobility.validation.traffic_advisory_validation"

# Belongs in 00_setup next to raw and clean. Repeated so this notebook runs standalone.
spark.sql("CREATE SCHEMA IF NOT EXISTS nyc_mobility.validation")

# COMMAND ----------

# LOCATION_FLOOR is a regression alarm, not an outlier rule, and it has a basis.
# The parser trap documented in source-profile.md is that a bolded paragraph can be
# misread as a location heading. With that regression present the page yields 11
# locations. With the parser correct it yields 26. 20 sits between the broken state and
# the healthy one, so a value at or below it means the parser drifted, not that the week
# was quiet. Revisit if a genuinely quiet week ever lands under it.
LOCATION_FLOOR = 20

BOROUGHS = "('Manhattan', 'Brooklyn', 'Queens', 'Bronx', 'Staten Island', 'Crossings')"

# Business key as the clean notebook builds it. Repeated here so the accuracy check can
# compare the two layers without depending on clean having run correctly.
CLOSURE_BK = ("SHA2(CONCAT_WS('|', section_id, location_name, "
              "COALESCE(detail_label, '~'), advisory_text), 256)")

# COMMAND ----------

# One row per check in the shared shape.
#
# dq_status follows the same rule as the weather validation: PASS at zero failures,
# WARN under 5 percent, FAIL at or above. Five checks override that to FAIL on any
# failure at all, because a duplicate key, a null business key or a row that reached
# clean without matching raw is structural. One of those breaks the MERGE or the joins
# downstream no matter how small the percentage is. Those five are marked strict below.
#
# dimension is one column more than the weather table has. It is here because the DQ
# dashboard is meant to group issues by dimension and nothing else supplies it. Drop the
# line if Engineer 6 would rather the six tables match exactly.

validation_sql = f"""
CREATE OR REPLACE TABLE {VALIDATION_TABLE} AS

WITH latest AS (
    SELECT MAX(scrape_date) AS d FROM {RAW_TABLE}
),
raw_latest AS (
    SELECT * FROM {RAW_TABLE} WHERE scrape_date = (SELECT d FROM latest)
),
raw_keys AS (
    SELECT DISTINCT {CLOSURE_BK} AS bk FROM raw_latest
),
counts AS (
    SELECT (SELECT COUNT(*) FROM raw_latest)                            AS raw_latest_rows,
           (SELECT COUNT(*) FROM {RAW_TABLE})                           AS raw_rows,
           (SELECT COUNT(*) FROM {CLEAN_TABLE})                         AS clean_rows,
           (SELECT COUNT(DISTINCT scrape_date) FROM {RAW_TABLE})        AS scrape_count,
           (SELECT COUNT(*) FROM raw_keys)                              AS raw_key_count
),
results AS (

    -- COMPLETENESS. A silent zero is the failure that actually costs us on a page with
    -- no archive. Table level: total_count is the row count, failed_rows is 0 or 1.
    SELECT 'raw.traffic_advisory'           AS table_name,
           '*'                              AS column_name,
           'rows_present_latest_scrape'     AS data_quality_check,
           'completeness'                   AS dimension,
           true                             AS strict,
           (SELECT raw_latest_rows FROM counts) AS total_count,
           CASE WHEN (SELECT raw_latest_rows FROM counts) > 0 THEN 0 ELSE 1 END AS failed_rows

    UNION ALL
    -- VALIDITY. The parser regression canary described above. Table level: total_count
    -- is the distinct location count, failed_rows is 0 or 1.
    SELECT 'raw.traffic_advisory', 'location_name', 'location_count_above_floor',
           'validity', false,
           (SELECT COUNT(DISTINCT location_name) FROM raw_latest),
           CASE WHEN (SELECT COUNT(DISTINCT location_name) FROM raw_latest)
                     > {LOCATION_FLOOR} THEN 0 ELSE 1 END

    UNION ALL
    -- UNIQUENESS, strict. The surrogate key is what makes the raw MERGE idempotent.
    SELECT 'raw.traffic_advisory', 'advisory_sk', 'advisory_sk_unique', 'uniqueness', true,
           (SELECT raw_rows FROM counts),
           (SELECT COUNT(*) - COUNT(DISTINCT advisory_sk) FROM {RAW_TABLE})

    UNION ALL
    -- TIMELINESS. A scrape date ahead of today means the clock or the load is wrong.
    SELECT 'raw.traffic_advisory', 'scrape_date', 'scrape_date_not_in_future',
           'timeliness', true,
           (SELECT raw_rows FROM counts),
           (SELECT COUNT_IF(CAST(scrape_date AS DATE) > current_date()) FROM {RAW_TABLE})

    UNION ALL
    -- UNIQUENESS, strict. One row per advisory in the Type 2 table, never one per sighting.
    SELECT 'clean.traffic_advisory', 'closure_bk', 'closure_bk_unique', 'uniqueness', true,
           (SELECT clean_rows FROM counts),
           (SELECT COUNT(*) - COUNT(DISTINCT closure_bk) FROM {CLEAN_TABLE})

    UNION ALL
    -- COMPLETENESS, strict. The business key is the join key for everything downstream.
    SELECT 'clean.traffic_advisory', 'closure_bk', 'closure_bk_not_null',
           'completeness', true,
           (SELECT clean_rows FROM counts),
           (SELECT COUNT_IF(closure_bk IS NULL) FROM {CLEAN_TABLE})

    UNION ALL
    -- VALIDITY. Dates are read out of prose, so a reversed pair means the sentence was
    -- parsed the wrong way round. The row is still usable, so this one can warn.
    SELECT 'clean.traffic_advisory', 'effective_from', 'effective_from_before_to',
           'validity', false,
           (SELECT clean_rows FROM counts),
           (SELECT COUNT_IF(effective_from > effective_to) FROM {CLEAN_TABLE})

    UNION ALL
    -- VALIDITY. Accepted values. A new borough string means the DOT renamed a section.
    SELECT 'clean.traffic_advisory', 'borough', 'borough_in_accepted_values',
           'validity', false,
           (SELECT clean_rows FROM counts),
           (SELECT COUNT_IF(borough NOT IN {BOROUGHS}) FROM {CLEAN_TABLE})

    UNION ALL
    -- ACCURACY, strict, layer against layer. Every advisory in the newest raw scrape
    -- should exist in clean. A gap means the clean MERGE dropped it, not the source.
    SELECT 'clean.traffic_advisory', 'closure_bk', 'clean_matches_raw_latest_scrape',
           'accuracy', true,
           (SELECT raw_key_count FROM counts),
           (SELECT COUNT(*) FROM raw_keys r
            LEFT ANTI JOIN {CLEAN_TABLE} c ON c.closure_bk = r.bk)

    UNION ALL
    -- CONSISTENCY, strict. This is the idempotency guard for the clean layer.
    -- times_seen counts the scrapes an advisory appeared in, so it can never exceed the
    -- number of scrapes raw holds. A rerun that double counted shows up here.
    SELECT 'clean.traffic_advisory', 'times_seen', 'times_seen_within_scrape_count',
           'consistency', true,
           (SELECT clean_rows FROM counts),
           (SELECT COUNT_IF(times_seen > (SELECT scrape_count FROM counts))
            FROM {CLEAN_TABLE})
)

SELECT
    table_name,
    column_name,
    data_quality_check,
    dimension,
    total_count,
    failed_rows,
    ROUND(100.0 * failed_rows / NULLIF(total_count, 0), 2) AS failed_percentage,
    CASE
        WHEN failed_rows = 0 THEN 'PASS'
        WHEN strict THEN 'FAIL'
        WHEN (100.0 * failed_rows / NULLIF(total_count, 0)) < 5 THEN 'WARN'
        ELSE 'FAIL'
    END AS dq_status,
    CURRENT_TIMESTAMP() AS validation_timestamp
FROM results
"""

spark.sql(validation_sql)

n_fail = spark.sql(
    f"SELECT COUNT(*) AS n FROM {VALIDATION_TABLE} WHERE dq_status = 'FAIL'").first().n
n_warn = spark.sql(
    f"SELECT COUNT(*) AS n FROM {VALIDATION_TABLE} WHERE dq_status = 'WARN'").first().n

print(f"Wrote checks -> {VALIDATION_TABLE}")
print(f"  FAIL: {n_fail}")
print(f"  WARN: {n_warn}")

display(spark.sql(f"""
    SELECT * FROM {VALIDATION_TABLE}
    ORDER BY CASE dq_status WHEN 'FAIL' THEN 1 WHEN 'WARN' THEN 2 ELSE 3 END,
             table_name, data_quality_check
"""))

# COMMAND ----------

# DBTITLE 1,Idempotency proof
# Sir Mark's point on Sep 16: every layer has to be idempotent, not just raw.
#
# Raw: rows landed for a scrape date always equals distinct keys for it, because the
# MERGE only inserts keys it has not seen before.
# Clean: times_seen is bounded by the number of scrapes held, because the first MATCHED
# branch is guarded on the scrape being newer than last_seen_scrape.
#
# To prove it, run 04_traffic_advisory_raw and 04_traffic_advisory_clean a second time
# and re-run this cell. Both numbers should be identical.
display(spark.sql(f"""
    SELECT 'raw rows per scrape date'  AS measure,
           CAST(scrape_date AS STRING) AS grouping,
           COUNT(*)                    AS rows_held,
           COUNT(DISTINCT advisory_sk) AS distinct_keys
    FROM   {RAW_TABLE}
    GROUP  BY scrape_date

    UNION ALL

    SELECT 'clean rows by times_seen',
           CAST(times_seen AS STRING),
           COUNT(*),
           COUNT(DISTINCT closure_bk)
    FROM   {CLEAN_TABLE}
    GROUP  BY times_seen
    ORDER  BY measure, grouping
"""))
