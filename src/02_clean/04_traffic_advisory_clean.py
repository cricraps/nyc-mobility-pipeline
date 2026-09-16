# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Traffic advisory
# TRAFFIC ADVISORY, CLEAN
# Source  nyc_mobility.raw.traffic_advisory
# Grain   one advisory, Type 2 history across the weekly scrapes
# Output  nyc_mobility.clean.traffic_advisory
# Before  run src/01_raw/04_traffic_advisory_raw first
from pyspark.sql import functions as F

RAW_TABLE = "nyc_mobility.raw.traffic_advisory"
CLEAN_TABLE = "nyc_mobility.clean.traffic_advisory"

# COMMAND ----------

# Type 2 history on purpose. The DOT page overwrites itself every week and keeps no
# archive, so an advisory that runs for three weeks has to stay one row that records
# when we first saw it and when we last saw it, not three rows or one overwritten row.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CLEAN_TABLE} (
        closure_bk          STRING  COMMENT 'business key: section + location + label + text, no scrape date',
        section_id          STRING,
        section_name        STRING,
        borough             STRING  COMMENT 'primary borough parsed from the section name',
        location_name       STRING,
        entry_type          STRING  COMMENT 'construction or event',
        detail_label        STRING,
        street_name         STRING,
        from_street         STRING,
        to_street           STRING,
        advisory_text       STRING,
        effective_from      DATE    COMMENT 'first date named in the advisory text',
        effective_to        DATE    COMMENT 'last date named in the advisory text',
        advisory_week_start DATE,
        advisory_week_end   DATE,
        first_seen_scrape   DATE,
        last_seen_scrape    DATE,
        is_current          BOOLEAN COMMENT 'false once the advisory drops off the page',
        times_seen          INT,
        source_system       STRING,
        source_url          STRING,
        ingested_at         TIMESTAMP,
        batch_id            STRING
    )
    COMMENT 'Type 2 history of NYC DOT weekly traffic advisories, accumulated one scrape at a time.'
""")

# COMMAND ----------

# Only the newest scrape is compared against the table. Older scrapes are already recorded.
latest_scrape = spark.sql(f"SELECT MAX(scrape_date) AS d FROM {RAW_TABLE}").first().d

if latest_scrape is None:
    raise ValueError(f"{RAW_TABLE} is empty. Run 01_raw/04_traffic_advisory_raw first.")

print(f"Cleaning scrape date {latest_scrape}")

spark.sql(f"""
    CREATE OR REPLACE TEMP VIEW stg_traffic_advisory AS
    SELECT
        SHA2(CONCAT_WS('|', section_id, location_name,
             COALESCE(detail_label, '~'), advisory_text), 256) AS closure_bk,
        section_id,
        section_name,
        -- 'Bronx/Manhattan' and 'Brooklyn/Queens' name two boroughs. Take the first as
        -- primary and keep the full section_name so nothing is lost.
        SPLIT(section_name, '/')[0]            AS borough,
        location_name,
        entry_type,
        detail_label,
        street_name,
        from_street,
        to_street,
        advisory_text,
        CAST(date_first_mentioned AS DATE)     AS effective_from,
        CAST(date_last_mentioned  AS DATE)     AS effective_to,
        CAST(advisory_week_start  AS DATE)     AS advisory_week_start,
        CAST(advisory_week_end    AS DATE)     AS advisory_week_end,
        CAST(scrape_date          AS DATE)     AS scrape_dt,
        source_system,
        source_url,
        batch_id
    FROM {RAW_TABLE}
    WHERE scrape_date = '{latest_scrape}'
""")

# COMMAND ----------

# Two MATCHED branches and the order matters.
#
# The first fires only when this scrape is genuinely newer than what the row already
# records, and only then does times_seen increment. Without that guard, re-running this
# notebook against the same scrape would tick times_seen up on every run and the table
# would quietly stop being idempotent.
#
# The second branch handles the same scrape seen again: mark it current, touch nothing else.
spark.sql(f"""
    MERGE INTO {CLEAN_TABLE} AS tgt
    USING stg_traffic_advisory AS src
      ON tgt.closure_bk = src.closure_bk
    WHEN MATCHED AND src.scrape_dt > tgt.last_seen_scrape THEN UPDATE SET
        tgt.last_seen_scrape = src.scrape_dt,
        tgt.is_current       = true,
        tgt.times_seen       = tgt.times_seen + 1,
        tgt.effective_to     = COALESCE(src.effective_to, tgt.effective_to),
        tgt.ingested_at      = current_timestamp(),
        tgt.batch_id         = src.batch_id
    WHEN MATCHED THEN UPDATE SET
        tgt.is_current  = true,
        tgt.ingested_at = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (
        closure_bk, section_id, section_name, borough, location_name, entry_type,
        detail_label, street_name, from_street, to_street, advisory_text,
        effective_from, effective_to, advisory_week_start, advisory_week_end,
        first_seen_scrape, last_seen_scrape, is_current, times_seen,
        source_system, source_url, ingested_at, batch_id
    ) VALUES (
        src.closure_bk, src.section_id, src.section_name, src.borough, src.location_name,
        src.entry_type, src.detail_label, src.street_name, src.from_street, src.to_street,
        src.advisory_text, src.effective_from, src.effective_to,
        src.advisory_week_start, src.advisory_week_end,
        src.scrape_dt, src.scrape_dt, true, 1,
        src.source_system, src.source_url, current_timestamp(), src.batch_id
    )
""")

# An advisory that is missing from the newest scrape has come off the page. Close it,
# never delete it, so the history stays readable.
spark.sql(f"""
    UPDATE {CLEAN_TABLE}
    SET    is_current = false, ingested_at = current_timestamp()
    WHERE  last_seen_scrape < DATE('{latest_scrape}')
""")

summary = spark.sql(f"""
    SELECT COUNT(*) AS rows_total,
           COUNT_IF(is_current) AS rows_current,
           COUNT(DISTINCT first_seen_scrape) AS scrapes_held
    FROM {CLEAN_TABLE}
""").first()

print(f"Saved traffic advisory data -> {CLEAN_TABLE}")
print(f"  Rows: {summary.rows_total:,}")
print(f"  Current: {summary.rows_current:,}")
print(f"  Scrape dates held: {summary.scrapes_held}")

# COMMAND ----------

# Checks on this source only. They are written as one result set with a PASS, WARN or
# FAIL per check so they can be read straight into the shared DQ results table once
# Engineer 6 settles its name.
display(spark.sql(f"""
    WITH latest AS (SELECT MAX(scrape_date) AS d FROM {RAW_TABLE})
    SELECT 'row_count' AS check_name,
           'raw' AS layer,
           COUNT(*) AS metric_value,
           CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM   {RAW_TABLE}
    WHERE  scrape_date = (SELECT d FROM latest)

    UNION ALL
    -- The canary for trap 1. A parser that stops reading bare <strong> headings collapses
    -- this number to roughly 4, and the load looks successful while losing most of the page.
    SELECT 'location_count', 'raw', COUNT(DISTINCT location_name),
           CASE WHEN COUNT(DISTINCT location_name) >= 20 THEN 'PASS' ELSE 'WARN' END
    FROM   {RAW_TABLE}
    WHERE  scrape_date = (SELECT d FROM latest)

    UNION ALL
    SELECT 'advisory_sk_unique', 'raw', COUNT(*) - COUNT(DISTINCT advisory_sk),
           CASE WHEN COUNT(*) = COUNT(DISTINCT advisory_sk) THEN 'PASS' ELSE 'FAIL' END
    FROM   {RAW_TABLE}

    UNION ALL
    SELECT 'closure_bk_unique', 'clean', COUNT(*) - COUNT(DISTINCT closure_bk),
           CASE WHEN COUNT(*) = COUNT(DISTINCT closure_bk) THEN 'PASS' ELSE 'FAIL' END
    FROM   {CLEAN_TABLE}

    UNION ALL
    SELECT 'closure_bk_not_null', 'clean', COUNT_IF(closure_bk IS NULL),
           CASE WHEN COUNT_IF(closure_bk IS NULL) = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM   {CLEAN_TABLE}

    UNION ALL
    SELECT 'date_order', 'clean', COUNT_IF(effective_from > effective_to),
           CASE WHEN COUNT_IF(effective_from > effective_to) = 0 THEN 'PASS' ELSE 'WARN' END
    FROM   {CLEAN_TABLE}

    UNION ALL
    SELECT 'borough_in_domain', 'clean',
           COUNT_IF(borough NOT IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx',
                                    'Staten Island', 'Crossings')),
           CASE WHEN COUNT_IF(borough NOT IN ('Manhattan', 'Brooklyn', 'Queens', 'Bronx',
                                              'Staten Island', 'Crossings')) = 0
                THEN 'PASS' ELSE 'WARN' END
    FROM   {CLEAN_TABLE}
"""))
