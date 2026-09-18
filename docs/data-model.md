# NYC Mobility Pipeline — Data Model

This document is the plain-language description of the Gold-layer (`nyc_mobility.mart`)
dimensional model and how it answers the project's business questions. For field-level
detail (types, nullability, business rules) see `docs/data_dictionary.md`.

**Catalog:** `nyc_mobility`
**Sources:** NYC TLC Green Taxi, weather (`weather_silver`), NYC Taxi Zones, NYC traffic
advisories

## Star schema overview

The model is a single-fact star schema: one fact table (`fact_trip`) surrounded by four
dimension tables (`dim_zone`, `dim_date`, `dim_weather`, `dim_advisory`). `dim_zone` is
used twice against `fact_trip` — once for pickup, once for drop-off — as a role-playing
dimension. `dim_advisory` does not have a direct foreign key into `fact_trip`; it is
joined at query time on `borough` and an effective-date range (see
[Business questions and model usage](#business-questions-and-model-usage)).

*(Insert the star-schema ERD here, e.g. `docs/model/nyc_mobility_star_schema.png`.)*

| Gold table | Type | Grain | Key |
|---|---|---|---|
| `fact_trip` | Fact | One row per individual Green Taxi trip, resolved at the borough/date/hour level | No declared PK — source has no native trip identifier |
| `dim_zone` | Dimension | One row per Taxi Zone `location_id`, mapped to a borough | `location_id` |
| `dim_date` | Dimension | One row per pickup date and hour | `date_hour_key` (not enforced unique — see caveats) |
| `dim_weather` | Dimension | One row per date and hour, city-wide NYC | `date_hour_key` |
| `dim_advisory` | Dimension | One row per traffic advisory, scoped by borough | `advisory_id` (repeats across boroughs for multi-borough advisories) |

## Fact table

### `fact_trip`

**Grain:** one row per individual trip.
**Primary key:** none — `nyc_mobility.clean.green_taxi` has no native trip-level identifier.

**Foreign keys:**

| Field | References | Role |
|---|---|---|
| `pu_location_id` | `dim_zone.location_id` | Pickup zone |
| `do_location_id` | `dim_zone.location_id` | Drop-off zone |
| `pickup_date_hour_key` | `dim_date.date_hour_key`, `dim_weather.date_hour_key` | Pickup calendar/weather context |

**Measures:** `passenger_count`, `trip_distance`, `trip_duration_min`, `fare_amount`,
`total_amount`

**Other fields:** `pickup_datetime`, `dropoff_datetime`, `pickup_hour`, `dropoff_hour`

## Dimensions

### `dim_zone`

**Grain:** one row per Taxi Zone `location_id`.
**Primary key:** `location_id`
**Other fields:** `borough`, `zone`, `service_zone`

### `dim_date`

**Grain:** one row per pickup date and hour (see caveat below).
**Primary key:** `date_hour_key` (`yyyyMMddHH`)
**Other fields:** `full_date`, `pickup_hour`, `dropoff_hour`, `year`, `month`, `day`,
`day_of_week`, `is_weekend`

### `dim_weather`

**Grain:** one row per date and hour, city-wide NYC.
**Primary key:** `date_hour_key` (`yyyyMMddHH`)
**Measures:** `temperature_2m`, `precipitation`, `rain`, `snowfall`, `wind_speed_10m`
**Other fields:** `observation_date`, `observation_hour`, `weather_code`

### `dim_advisory`

**Grain:** one row per traffic advisory, scoped by borough.
**Primary key:** `advisory_id` (business key; not unique on its own — see caveats)
**Other fields:** `section_id`, `advisory_type`, `description`, `borough`,
`location_name`, `street_name`, `from_street`, `to_street`, `effective_from`,
`effective_to`

## Business questions and model usage

These are the questions implemented in `src/04_visualisation`, each materialized as its
own table in `nyc_mobility.bi_visualization`:

| ID | Business question | Fact / dims used | Output table | Suggested chart |
|---|---|---|---|---|
| Q1a | Which borough-to-borough flows carry the highest trip volume, distance, and revenue? | `fact_trip` joined to `dim_zone` twice (pickup + drop-off role) | `borough_flow_analytics` | Heatmap — rows: pickup borough, columns: drop-off borough, color: total trips |
| Q1b | Which individual pickup zones drive the most trip volume and revenue? | `fact_trip` joined to `dim_zone` (pickup role) | `top_pickup_zones` | Horizontal bar — X: total trips, Y: pickup zone (top 10), color: pickup borough |
| Q2 | How do passenger volume, fare amounts, and trip durations vary across hours of the day and weekends vs. weekdays? | `fact_trip` joined to `dim_date` on `pickup_date_hour_key` | `temporal_patterns_and_behaviors` | Line chart — X: pickup hour, Y: total trips, series/color: `is_weekend` |
| Q3 | How do weather conditions (rain, snow, clear) impact trip demand, distance, and duration? | `fact_trip` joined to `dim_weather` on `pickup_date_hour_key` = `date_hour_key` | `weather_impact_analysis` | Combo chart — X: weather condition, bar (left Y): total trips, line (right Y): avg duration |
| Q4 | Do traffic advisories correlate with changes in pickup volume or trip duration within impacted boroughs? | `fact_trip` joined to `dim_zone` (pickup role), left-joined to `dim_advisory` on borough + effective-date range | `traffic_advisory_and_incident_impact` | Grouped bar — X: borough, Y: avg duration minutes, color/group: advisory type |
| — | Consolidated KPI summary (trips, passengers, revenue, avg fare, avg distance, avg duration, revenue/mile) | `fact_trip` only | `kpi_summary` | KPI banner |

### How each question is built

**Q1a — `borough_flow_analytics`**
`fact_trip` is joined to `dim_zone` twice — once aliased as pickup zone (`pu_location_id = pz.location_id`) and once as drop-off zone (`do_location_id = dz.location_id`) — then grouped by pickup borough and drop-off borough to get trip count, total distance, and total revenue per borough pair.

**Q1b — `top_pickup_zones`**
`fact_trip` joined to `dim_zone` on the pickup role only, grouped by pickup borough and pickup zone, ranked by trip count.

**Q2 — `temporal_patterns_and_behaviors`**
`fact_trip` joined to `dim_date` on `pickup_date_hour_key = date_hour_key`, grouped by pickup hour and `is_weekend`, aggregating trip count, total passengers, average duration, and average fare.

**Q3 — `weather_impact_analysis`**
`fact_trip` joined to `dim_weather` on `pickup_date_hour_key = date_hour_key`. A `weather_condition` label is derived inline (`snowfall > 0` → `Snow`; else `precipitation > 0` → `Rain/Precipitation`; else `Clear/Dry`) — consistent with the "check snowfall before precipitation" rule documented on `dim_weather`. Grouped by that label to get trip count, average temperature, average distance, and average duration.

**Q4 — `traffic_advisory_and_incident_impact`**
Because `dim_advisory` has no direct foreign key into `fact_trip`, this question uses a `LEFT JOIN` on `LOWER(TRIM(dim_advisory.borough)) = LOWER(TRIM(dim_zone.borough))` (via the pickup zone) **and** `pickup_datetime BETWEEN effective_from AND effective_to`. Because a trip's pickup borough can match more than one advisory in effect at that time, a `ROW_NUMBER()` window (partitioned by pickup/drop-off location and timestamps, ordered by `effective_from DESC`) picks the single most recent applicable advisory per trip before aggregating by borough and advisory type. Trips with no matching advisory are labelled `'None'` via `COALESCE`.

**KPI summary — `kpi_summary`**
A single-row aggregation directly off `fact_trip`: total trips, total passengers, total revenue, average fare, average distance, average duration, and revenue per mile (`SUM(total_amount) / NULLIF(SUM(trip_distance), 0)`, guarding against divide-by-zero).

## Modeling caveats to carry into analysis

- **`fact_trip` has no primary key.** Trip-level uniqueness is not guaranteed; joins that
  fan out (e.g. Q4's advisory match before deduplication) must be windowed down to one
  row per trip, as Q4 does.
- **`dim_date.date_hour_key` is not guaranteed unique** — the table is built with
  `SELECT DISTINCT` across all its columns, including `dropoff_hour`, which is not part
  of the key. A join on `date_hour_key` alone can duplicate fact rows; Q2 avoids this
  because it re-aggregates after the join, but any ad hoc join should be aware of it.
- **`dim_advisory` is not a clean 1:1 dimension for `fact_trip`.** It's related by
  borough and effective-date range, not a stored key, so every query that uses it (like
  Q4) needs its own dedup logic rather than a simple join.
- **Weather is city-wide, not zone-specific.** `dim_weather` has one row per hour for
  all of NYC — there is no per-borough or per-zone weather breakdown, so Q3's results
  describe NYC-wide conditions, not localized ones.
- Timestamps throughout the model are carried through as-is from source (TLC/weather
  data) with no explicit timezone conversion applied in these notebooks.

## Maintenance

Any change that adds, removes, or changes the grain/keys of a `mart` or
`bi_visualization` table — or adds a new business question — should update this document
in the same change, alongside `docs/data_dictionary.md`.
