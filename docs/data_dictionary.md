# Data Dictionary — NYC Mobility Pipeline (Mart / Gold Layer)

This document describes the fields, grain, and lineage of every table published in the
`nyc_mobility.mart` schema of the [`cricraps/nyc-mobility-pipeline`](https://github.com/cricraps/nyc-mobility-pipeline)
project. It covers what each field means, where it comes from, its unit/format, and any
rules or caveats a downstream user needs to know before joining or aggregating it — in line
with the core elements a data dictionary should carry: variable name, a human-readable
description, units/format, allowed values, and definition (per
[Harvard HMS's Data Dictionary guidance](https://datamanagement.hms.harvard.edu/collect-analyze/documentation-metadata/data-dictionary)).

## Pipeline context

The project follows a bronze / silver / gold (medallion) layout inside a single Databricks
catalog, `nyc_mobility`:

| Schema | Layer | Purpose |
|---|---|---|
| `nyc_mobility.raw` | Bronze | Raw data ingestion |
| `nyc_mobility.clean` | Silver | Data cleaning |
| `nyc_mobility.mart` | **Gold** | Dimensional model (tables documented here) |
| `nyc_mobility.bi_visualization` | — | Dashboard-ready views |
| `nyc_mobility.validation` / `nyc_mobility.dq_visualization` | — | Data quality checks |

The `mart` schema implements a star schema: one fact table (`fact_trip`) surrounded by four
dimension tables (`dim_advisory`, `dim_date`, `dim_weather`, `dim_zone`).

## Conventions

- All tables are built with `CREATE OR REPLACE TABLE ... AS SELECT`, so they are fully
  rebuilt (not incrementally loaded) on each run from their `nyc_mobility.clean` source.
- **Type** reflects an explicit `CAST` where the source SQL casts a value; otherwise it is
  the type inherited from the upstream `clean` table and is listed as *(inferred)*.
- **Nullable** reflects whether the transforming query filters the field to `NOT NULL`
  upstream — it is not a declared column constraint (Databricks SQL tables created this way
  do not enforce `NOT NULL`).
- Timestamps are **not** explicitly converted to UTC or a named timezone anywhere in these
  notebooks — they are carried through as-is from the TLC/weather source data, which is
  local Eastern time by TLC convention.
- A `date_hour_key` join key is used consistently across `dim_date`, `dim_weather`, and
  `fact_trip` in the format `yyyyMMddHH` (integer).

---

## `dim_advisory`

**Grain:** one row per traffic advisory, scoped by borough
**Source:** `nyc_mobility.clean.traffic_advisory`, filtered to `closure_bk IS NOT NULL`

| Field | Type | Nullable | Key | Definition |
|---|---|---|---|---|
| `advisory_id` | STRING *(inferred)* | No | PK (as documented — see note) | Source `closure_bk`. Business key for the advisory/closure event. |
| `section_id` | STRING *(inferred)* | Yes | | Source `section_id`, carried through unchanged. Sub-segment identifier for the advisory location. |
| `advisory_type` | STRING *(inferred)* | Yes | | Source `entry_type`, renamed. Category of the advisory (e.g. closure, restriction). |
| `description` | STRING *(inferred)* | Yes | | Source `advisory_text`, renamed. Free-text description of the advisory. |
| `borough` | STRING *(inferred)* | Yes | | NYC borough the advisory applies to. Table grain is explicitly scoped by this field. |
| `location_name` | STRING *(inferred)* | Yes | | Human-readable name of the affected location. |
| `street_name` | STRING *(inferred)* | Yes | | Primary street affected by the advisory. |
| `from_street` | STRING *(inferred)* | Yes | | Cross-street marking the start of the affected segment. |
| `to_street` | STRING *(inferred)* | Yes | | Cross-street marking the end of the affected segment. |
| `effective_from` | DATE/TIMESTAMP *(inferred)* | Yes | | `COALESCE(effective_from, advisory_week_start)` — advisory start date, falling back to the week-start value when the specific effective date is null. |
| `effective_to` | DATE/TIMESTAMP *(inferred)* | Yes | | `COALESCE(effective_to, advisory_week_end)` — advisory end date, with the same week-level fallback. |

**Note:** the grain statement ("1 row per traffic advisory, scoped by borough") implies
`advisory_id` may repeat across boroughs for advisories that span more than one — treat
`advisory_id` as a natural key only in combination with `borough`, not as a standalone PK.

---

## `dim_date`

**Grain:** documented as "1 row per date and hour"
**Source:** `nyc_mobility.clean.green_taxi`, filtered to `lpep_pickup_datetime IS NOT NULL`, via `SELECT DISTINCT`

| Field | Type | Nullable | Key | Definition |
|---|---|---|---|---|
| `date_hour_key` | INT | No | PK (see note) | `CAST(date_format(lpep_pickup_datetime, 'yyyyMMddHH') AS INT)`. Format `yyyyMMddHH`, e.g. `2026031014` = March 10, 2026, 2 PM. Used to join to `fact_trip.pickup_date_hour_key` and `dim_weather.date_hour_key`. |
| `full_date` | DATE | No | | `CAST(lpep_pickup_datetime AS DATE)` — the calendar date component. |
| `pickup_hour` | INT | No | | `HOUR(lpep_pickup_datetime)`, 0–23. |
| `dropoff_hour` | INT | No | | `HOUR(lpep_dropoff_datetime)`, 0–23. See note below — this can vary independently of `pickup_hour` for the same `date_hour_key`. |
| `year` | INT | No | | `YEAR(lpep_pickup_datetime)`. |
| `month` | INT | No | | `MONTH(lpep_pickup_datetime)`, 1–12. |
| `day` | INT | No | | `DAY(lpep_pickup_datetime)`, day of month. |
| `day_of_week` | STRING | No | | `DATE_FORMAT(lpep_pickup_datetime, 'EEEE')` — full weekday name (e.g. `Monday`). |
| `is_weekend` | BOOLEAN | No | | `TRUE` when `DAYOFWEEK(lpep_pickup_datetime)` is `1` (Sunday) or `7` (Saturday), else `FALSE`. |

**Note on grain:** because the table is built with `SELECT DISTINCT` across all ten columns
— including `dropoff_hour`, which is not part of `date_hour_key` — a single `date_hour_key`
can legitimately produce more than one row if trips with that pickup hour have different
dropoff hours. `date_hour_key` is the intended join key but is **not guaranteed unique** in
this table as built; downstream consumers joining on it should either `SELECT DISTINCT` the
date/calendar columns only, or be aware duplicate `date_hour_key` rows can inflate a join.

---

## `dim_weather`

**Grain:** one row per date and hour, city-wide NYC
**Source:** `nyc_mobility.clean.weather_silver`, filtered to `observation_time IS NOT NULL`

| Field | Type | Nullable | Key | Definition |
|---|---|---|---|---|
| `date_hour_key` | INT | No | PK | `CAST(date_format(observation_time, 'yyyyMMddHH') AS INT)`. Same `yyyyMMddHH` format as `dim_date`/`fact_trip`. |
| `observation_date` | DATE | No | | `CAST(observation_time AS DATE)`. |
| `observation_hour` | INT | No | | `HOUR(observation_time)`, 0–23. |
| `temperature_2m` | DOUBLE *(inferred)* | Yes | Measure | 2-metre air temperature, unit as provided by source (not explicitly labelled °C/°F in this query — confirm against `weather_silver` upstream). |
| `precipitation` | DOUBLE *(inferred)* | Yes | Measure | Total precipitation for the hour. **Not guaranteed** to equal `rain + snowfall` in source data (see note). |
| `rain` | DOUBLE *(inferred)* | Yes | Measure | Liquid rain component of precipitation. |
| `snowfall` | DOUBLE *(inferred)* | Yes | Measure | Snowfall component of precipitation. |
| `weather_code` | INT/STRING *(inferred)* | Yes | | Categorical weather condition code from the source weather API. |
| `wind_speed_10m` | DOUBLE *(inferred)* | Yes | Measure | Wind speed at 10 metres, unit as provided by source. |

**Note (carried from source comments):** `precipitation` is not always `>= rain + snowfall`
in the underlying data. Any downstream weather-condition logic should check `snowfall`
**before** `precipitation` when classifying conditions.

---

## `dim_zone`

**Grain:** one row per zone, mapped to a borough
**Source:** `nyc_mobility.clean.taxi_zones`, filtered to `location_id IS NOT NULL`

| Field | Type | Nullable | Key | Definition |
|---|---|---|---|---|
| `location_id` | INT *(inferred)* | No | PK | TLC Taxi Zone `LocationID`. Referenced by `fact_trip.pu_location_id` / `do_location_id`. |
| `borough` | STRING *(inferred)* | Yes | | NYC borough containing the zone (or `EWR`/`Unknown`/`N/A`-style source values where applicable). |
| `zone` | STRING *(inferred)* | Yes | | Human-readable zone name (e.g. neighborhood name). |
| `service_zone` | STRING *(inferred)* | Yes | | TLC service-zone classification (e.g. Yellow Zone, Boro Zone). |

---

## `fact_trip`

**Grain:** one row per individual trip, resolved at the borough/date/hour level
**Source:** `nyc_mobility.clean.green_taxi t`
**Filters:** `lpep_pickup_datetime IS NOT NULL`, `lpep_dropoff_datetime IS NOT NULL`, and
trip duration between 1 and 1440 minutes (1 minute to 24 hours)

> **Note:** the source (`green_taxi`) has no native trip-level identifier, so `fact_trip`
> has no declared primary key — uniqueness is not guaranteed and should be established
> downstream if required (e.g. via a hash of pickup/dropoff location + timestamps).

| Field | Type | Nullable | Key | Definition |
|---|---|---|---|---|
| `pu_location_id` | INT *(inferred)* | No | FK → `dim_zone.location_id` | Source `PULocationID`, renamed. Pickup zone. |
| `do_location_id` | INT *(inferred)* | No | FK → `dim_zone.location_id` | Source `DOLocationID`, renamed. Drop-off zone. |
| `pickup_date_hour_key` | INT | No | FK → `dim_date.date_hour_key` / `dim_weather.date_hour_key` | `CAST(date_format(t.lpep_pickup_datetime, 'yyyyMMddHH') AS INT)`. |
| `pickup_datetime` | TIMESTAMP *(inferred)* | No | | Source `lpep_pickup_datetime`, renamed. |
| `dropoff_datetime` | TIMESTAMP *(inferred)* | No | | Source `lpep_dropoff_datetime`, renamed. |
| `pickup_hour` | INT | No | | `HOUR(t.lpep_pickup_datetime)`, 0–23. |
| `dropoff_hour` | INT | No | | `HOUR(t.lpep_dropoff_datetime)`, 0–23. |
| `passenger_count` | DOUBLE/INT *(inferred)* | Yes | Measure | Source `passenger_count`, carried through unchanged. |
| `trip_distance` | DOUBLE *(inferred)* | No | Measure | Source `trip_distance`, unit as provided by source (typically miles for TLC data — confirm upstream). |
| `trip_duration_min` | DOUBLE | No | Measure, DQ-constrained | `ROUND(CAST((UNIX_TIMESTAMP(dropoff) - UNIX_TIMESTAMP(pickup)) / 60.0 AS DOUBLE), 2)`. Minutes, rounded to 2 decimal places. Filtered by the `WHERE` clause to be **between 1.0 and 1440.0** — rows outside that range are excluded from the table entirely (not flagged, dropped). |
| `fare_amount` | DECIMAL/DOUBLE *(inferred)* | No | Measure | Source `fare_amount`. Metered fare in USD (currency not explicitly labelled in source — treat as USD per TLC convention). |
| `total_amount` | DECIMAL/DOUBLE *(inferred)* | No | Measure | Source `total_amount`. Total amount charged to passenger in USD, includes fare plus surcharges/tolls/tips as captured by the source system. |

---

## Relationships

| From | To | Join |
|---|---|---|
| `fact_trip.pu_location_id` | `dim_zone.location_id` | Pickup zone lookup |
| `fact_trip.do_location_id` | `dim_zone.location_id` | Drop-off zone lookup |
| `fact_trip.pickup_date_hour_key` | `dim_date.date_hour_key` | Calendar attributes at pickup hour (see `dim_date` uniqueness note) |
| `fact_trip.pickup_date_hour_key` | `dim_weather.date_hour_key` | Weather conditions at pickup hour |
| `dim_advisory.borough` | `dim_zone.borough` | Advisory-to-zone context (no direct key column; join on shared borough value) |

## Known caveats to carry forward

- `dim_date.date_hour_key` is not enforced unique (see table note) — joins on it can
  duplicate fact rows unless deduplicated first.
- `fact_trip` has no primary/business key; trip-level uniqueness is not guaranteed.
- Timestamp fields are not explicitly timezone-labelled anywhere in the mart layer.
- Several field types are inferred from the source `clean` layer rather than an explicit
  `CAST` in the mart SQL — verify against `nyc_mobility.clean.*` table DDL if exact
  precision/scale matters downstream.
- `dim_weather.precipitation` is not guaranteed to equal `rain + snowfall`; downstream
  weather-condition logic should prioritize `snowfall` over `precipitation`.

## Maintenance

Any change that adds, renames, casts, or drops a field in a `mart` table should be reflected
here in the same change, along with the grain statement and any new join relationships.
