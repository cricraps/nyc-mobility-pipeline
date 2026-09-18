# Data Quality & Validation

Data quality is enforced per dataset in the Silver layer before anything downstream
consumes it. Each dataset gets its own validation table under `nyc_mobility.validation`,
built from column-level rules (nullability, range, enum membership, freshness) plus a
grain/uniqueness check. Every table follows the same output shape — `column_name`,
`total_count`, `failed_rows`, `failed_percentage`, `dq_status`, `validation_timestamp` —
so results can be compared across datasets. Checks live under `src/05_validation/` in
the pipeline repo, one notebook/query per dataset, numbered to match the profiling and
build order (`01_profile_source_data` → `02_green_taxi_validation` →
`03_weather_validation` → `04_taxi_zones_validation` → `05_traffic_advisory_validation`
→ `06_clean_dq_checks_dashboard`).

**Status rule (shared across all datasets):** `PASS` at zero failed rows, `WARN` under 5%
failed, `FAIL` at or above 5%.

---

## Green Taxi Dataset

**Validation query:** [`src/05_validation/02_green_taxi_validation.ipynb`](https://github.com/cricraps/nyc-mobility-pipeline/blob/main/src/05_validation/02_green_taxi_validation.ipynb)




### Data Quality Findings

| Check | Description | Rows Affected | % Affected | Decision |
| --- | --- | --- | --- | --- |
| `ehail_fee_always_null` | `ehail_fee` is null in every row across all months | 133,367 | 100.0% | PASS |
| `vendorID_structural_nulls` | VendorID has a 100% null rate on RatecodeID, store_and_fwd_flag, passenger_count, and payment_type for a subset of records | 14,180 | 10.63% | WARN |
| `pre2026_timestamp_corruption` | Pickup/dropoff year shows 2008/2009 instead of 2026, clustering at your baseline/rest of records | 1 | 0.001% | FAIL |
| `dropoff_before_pickup` | `lpep_dropoff_datetime` earlier than `lpep_pickup_datetime` | 1 | 0.001% | FAIL |
| `month_boundary_trips` | Pickup on Feb 28 crossing into March | 8 | 0.006% | PASS |
| `zero_duration_nonzero_distance` | Pickup and dropoff timestamps identical but `trip_distance` > 0 | 12 | 0.009% | WARN |

**Why `ehail_fee_always_null` still passes.** A 100%-null column reads like a red flag,
but green taxi trips never populate `ehail_fee` (it's an e-hail-specific field), so the
check treats total nullness on this column as expected rather than a failure.

**Why the two structural checks land on FAIL.** A corrupted timestamp year and a
dropoff before its pickup are both single-row anomalies, but they're the kind of
record-level nonsense that breaks downstream duration/date-key logic, so they fail
outright rather than being scored by percentage.

---

## Weather Dataset

### Data Hygiene 

Rules enforced on `nyc_mobility.clean.weather_silver`:

- `elevation` must be present and fall within a physically plausible range (-500 m to 9000 m).
- `latitude` / `longitude` must be present and within valid coordinate bounds (±90 / ±180).
- `observation_time` must be present and cannot be in the future.
- `precipitation`, `rain`, and `snowfall` must be present and non-negative.
- `temperature_2m` must be present and within a plausible range (-90°C to 60°C).
- `timezone` must be present and non-blank.
- `weather_code` must be present and match a known WMO code.
- `wind_speed_10m` must be present and within a plausible range (0–500).
- No duplicate rows for the same `latitude`, `longitude`, `observation_time` grain.

### Validation Query

```sql
CREATE OR REPLACE TABLE nyc_mobility.validation.weather_validation AS
WITH total_rows AS (
    SELECT COUNT(*) AS total_count
    FROM nyc_mobility.clean.weather_silver
),
validation_results AS (
    -- Elevation
    SELECT
        'elevation' AS column_name,
        COUNT(*) AS failed_rows
    FROM nyc_mobility.clean.weather_silver
    WHERE elevation IS NULL
       OR elevation < -500
       OR elevation > 9000

    UNION ALL

    -- Latitude
    SELECT
        'latitude',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE latitude IS NULL
       OR latitude NOT BETWEEN -90 AND 90

    UNION ALL

    -- Longitude
    SELECT
        'longitude',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE longitude IS NULL
       OR longitude NOT BETWEEN -180 AND 180

    UNION ALL

    -- Observation Time
    SELECT
        'observation_time',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE observation_time IS NULL
       OR observation_time > CURRENT_TIMESTAMP()

    UNION ALL

    -- Precipitation
    SELECT
        'precipitation',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE precipitation IS NULL
       OR precipitation < 0

    UNION ALL

    -- Rain
    SELECT
        'rain',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE rain IS NULL
       OR rain < 0

    UNION ALL

    -- Snowfall
    SELECT
        'snowfall',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE snowfall IS NULL
       OR snowfall < 0

    UNION ALL

    -- Temperature
    SELECT
        'temperature_2m',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE temperature_2m IS NULL
       OR temperature_2m < -90
       OR temperature_2m > 60

    UNION ALL

    -- Timezone
    SELECT
        'timezone',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE timezone IS NULL
       OR TRIM(timezone) = ''

    UNION ALL

    -- Weather Code
    SELECT
        'weather_code',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE weather_code IS NULL
       OR weather_code NOT IN (
           0, 1, 2, 3,
           45, 48,
           51, 53, 55,
           56, 57,
           61, 63, 65,
           66, 67,
           71, 73, 75,
           77,
           80, 81, 82,
           85, 86,
           95, 96, 99
       )

    UNION ALL

    -- Wind Speed
    SELECT
        'wind_speed_10m',
        COUNT(*)
    FROM nyc_mobility.clean.weather_silver
    WHERE wind_speed_10m IS NULL
       OR wind_speed_10m < 0
       OR wind_speed_10m > 500

    UNION ALL

    -- Duplicate Grain Check
    SELECT
        'duplicate_grain',
        COUNT(*)
    FROM (
        SELECT
            latitude,
            longitude,
            observation_time,
            COUNT(*) AS cnt
        FROM nyc_mobility.clean.weather_silver
        GROUP BY
            latitude,
            longitude,
            observation_time
        HAVING COUNT(*) > 1
    )
)
SELECT
    v.column_name,
    t.total_count,
    v.failed_rows,
    ROUND(
        100.0 * v.failed_rows / t.total_count,
        2
    ) AS failed_percentage,
    CASE
        WHEN v.failed_rows = 0 THEN 'PASS'
        WHEN (100.0 * v.failed_rows / t.total_count) < 5 THEN 'WARN'
        ELSE 'FAIL'
    END AS dq_status,
    CURRENT_TIMESTAMP() AS validation_timestamp
FROM validation_results v
CROSS JOIN total_rows t
ORDER BY
    CASE dq_status
        WHEN 'FAIL' THEN 1
        WHEN 'WARN' THEN 2
        ELSE 3
    END,
    failed_percentage DESC;
```

### Weather data quality findings

| Column | Dimension | Rule | If it trips |
| --- | --- | --- | --- |
| `elevation` | Validity | within -500m to 9000m, non-null | WARN/FAIL by % |
| `latitude` | Validity | within ±90, non-null | WARN/FAIL by % |
| `longitude` | Validity | within ±180, non-null | WARN/FAIL by % |
| `observation_time` | Timeliness | non-null, not in the future | WARN/FAIL by % |
| `precipitation` | Validity | non-null, ≥ 0 | WARN/FAIL by % |
| `rain` | Validity | non-null, ≥ 0 | WARN/FAIL by % |
| `snowfall` | Validity | non-null, ≥ 0 | WARN/FAIL by % |
| `temperature_2m` | Validity | non-null, within -90°C to 60°C | WARN/FAIL by % |
| `timezone` | Completeness | non-null, non-blank | WARN/FAIL by % |
| `weather_code` | Validity | non-null, valid WMO code | WARN/FAIL by % |
| `wind_speed_10m` | Validity | non-null, within 0–500 | WARN/FAIL by % |
| `duplicate_grain` | Uniqueness | one row per `latitude`, `longitude`, `observation_time` | WARN/FAIL by % |

Status is computed per column against the shared thresholds (0% = PASS, <5% = WARN, ≥5% =
FAIL), then the result set is sorted worst-first so any FAIL rows surface at the top of
`nyc_mobility.validation.weather_validation`.

---

## Taxi Zones Dataset

**Validation query:** [`src/05_validation/04_taxi_zones_validation.ipynb`](https://github.com/cricraps/nyc-mobility-pipeline/blob/main/src/05_validation/04_taxi_zones_validation.ipynb?short_path=36c320d)


### Data Quality Findings

| Column | Data Quality Check | Metric | Status |
| --- | --- | --- | --- |
| `location_id` | not_null | 0 | PASS |
| `location_id` | correct_type | 0 | PASS |
| `zone` | name_duplicates | 3 | PASS |
| `borough` | category_check | 1 | WARN |
| `service_zone` | category_check | 2 | WARN |

`zone` name duplicates pass despite being nonzero because the profiling step already
established they're legitimate (large neighborhoods sharing a name), not bad data — the
"Retain" decision above is baked into treating 3 duplicates as a PASS rather than a WARN.
The two `category_check` WARNs are the `'Unknown'`/`'N/A'` values in `borough` and
`service_zone` flagged during profiling; they're retained by design but still surfaced
as warnings so they stay visible.

---

## Traffic Advisory Dataset

**Validation query:** [`src/05_validation/05_traffic_advisory_validation.ipynb`](https://github.com/cricraps/nyc-mobility-pipeline/blob/main/src/05_validation/05_traffic_advisory_validation.ipynb)


### Data Hygiene

- **Completeness (raw):** the most recent scrape must land at least one row.
- **Validity (raw):** the latest scrape should surface more than 20 distinct locations,
  against a baseline of 26 — this is the canary check. If the scraper's heading parser
  silently breaks, the location count quietly drops to a handful instead of erroring out,
  so the threshold is set to catch that failure mode without flagging normal week-to-week
  variation.
- **Uniqueness (raw):** no duplicate `advisory_sk` values within a scrape.
- **Timeliness (raw):** `scrape_date` cannot be in the future.
- **Uniqueness (clean):** `closure_bk` is unique — one row per advisory.
- **Completeness (clean):** every clean row has a non-null `closure_bk`.
- **Validity (clean):** `effective_from` cannot be after `effective_to`.
- **Validity (clean):** `borough` must be one of the five NYC boroughs or "Crossings."
- **Accuracy (clean vs. raw):** every advisory in the newest raw scrape must also exist in
  the clean table.
- **Consistency (clean):** `times_seen` can never exceed the number of scrapes present in
  raw — this is the idempotency guard, verified by comparing rows landed vs. distinct keys
  per scrape date in raw, and rows by `times_seen` in clean, before and after re-running
  the pipeline.

### Summary

| Column | Dimension | Rule | Layer | If it trips |
| --- | --- | --- | --- | --- |
| `ALL_COLUMNS` (latest scrape) | Completeness | latest scrape landed at least one row | raw | FAIL |
| `location_name` | Validity | more than 20 distinct locations vs. baseline of 26 | raw | WARN |
| `advisory_sk` | Uniqueness | no duplicate keys in raw | raw | FAIL |
| `scrape_date` | Timeliness | no scrape date in the future | raw | FAIL |
| `closure_bk` | Uniqueness | one row per advisory in clean | clean | FAIL |
| `closure_bk` | Completeness | every clean row has a business key | clean | FAIL |
| `effective_from`, `effective_to` | Validity | `effective_from` not after `effective_to` | clean | WARN |
| `borough` | Validity | borough is one of the five boroughs or Crossings | clean | WARN |
| `closure_bk` | Accuracy | every raw advisory in the newest scrape exists in clean | clean | FAIL |
| `times_seen` | Consistency | never exceeds the number of scrapes raw holds (idempotency guard) | clean | FAIL |

**Status rule.** Same shared thresholds as weather (PASS at 0%, WARN under 5%, FAIL at or
above), but the checks flagged FAIL above are treated as strict pass/fail regardless of
percentage — a duplicate key, a null business key, or a raw advisory missing from clean
breaks the downstream MERGE or joins outright, so any occurrence at all fails the check.

**Why a zero-row load fails outright.** A silent empty batch is the costliest failure
mode, so the raw notebook is designed to raise before writing rather than merging an
empty result over good data.

**Proving idempotency.** Re-running the raw and clean notebooks a second time and
re-running the validation notebook should produce identical row/key counts, with the
second raw run inserting zero new rows.
