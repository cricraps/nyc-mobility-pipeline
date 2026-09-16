# Contributing to NYC Mobility Pipeline

## 🌿 Branch names

| Branch | What it's for |
|---|---|
| `feature/green_taxi_clean` | Green Taxi silver-layer cleaning |
| `feature/weather-clean` | Weather silver-layer cleaning |
| `feature/taxi_zones-clean` | Taxi zones silver-layer cleaning |
| `feature/traffic-advisory` | Traffic advisory ingestion |
| `feature/validation` | General data-quality validation work |
| `feature/validation-weather` | Weather-specific validation |
| `docs/traffic` | Documentation for the traffic source |

**Pattern:** `<type>/<dataset>-<what-you're-doing>` (e.g. `feature/weather-clean`), or `docs/<dataset>` for documentation-only branches. One branch per piece of work.

---

## 📦 Where raw files go

Raw source files land in the shared Databricks Volume, organized by group and source:

```
/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/<source>/<filename>
```

Example:
```
/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/weather/nyc_weather_2026_03_to_05.json
```

Keep the same `groups/week-08/group-f/<source>/` structure for any new files.

---

## 🔗 Referencing tables

Always use the full path: `catalog.schema.table`.

```sql
SELECT * FROM nyc_mobility.clean.green_taxi;
```

Don't rely on a `USE CATALOG` / `USE SCHEMA` set earlier in a notebook — spell out the full reference every time.

---

## 🗂️ Table names

Catalog: `nyc_mobility`

| Schema | Layer | Holds |
|---|---|---|
| `raw` | Bronze | Untouched ingested data |
| `clean` | Silver | Cleaned/standardized data |
| `mart` | Gold | Dimension & fact tables, business logic |
| `validation` | — | Data-quality check results |
| `bi_visualization` | — | Dashboard-ready outputs |
| `dq_visualization` | — | Data-quality dashboard outputs |

**Known tables:**
```
green_03_2026, green_04_2026, green_05_2026   -- Green Taxi, one table per month
taxi_zones
traffic_advisory
weather
```

---

## 🏷️ Column naming

Lowercase `snake_case`, with these suffixes:

| Suffix | Use for |
|---|---|
| `_id` | identifiers |
| `_at` | timestamps |
| `_date` | dates |
| `_count` | counts |
| `_amount` | currency |
| `_flag` | true/false |

Keep original source column names in `raw` — apply naming conventions starting at `clean`.

---

## 👤 Who owns what

| Owner | Area |
|---|---|
| Crizza | Git & Databricks orchestration |
| Garett | Green Taxi |
| Anje | Weather |
| Kinah | Traffic |
| Cha | Taxi Zones & Gold |
| Mia & Anje | Data quality & dashboard |
