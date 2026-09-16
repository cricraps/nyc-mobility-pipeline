# Contributing to NYC Mobility Pipeline

## What this is for

This doc explains how our team works together on this pipeline — how to branch, where data goes, how tables and columns are named, and who's responsible for each part. 

---

## Project workflow

1. **Pick up an issue** from the board and confirm you're the assigned owner (see "Who owns what" below).
2. **Work in your own Databricks Git folder.** Each engineer has a personal folder synced to their own branch — never edit inside someone else's folder.
3. **Create a branch** for your task using the naming pattern below, and keep it scoped to one dataset/task.
4. **Build and test in your folder** (Raw → Clean → validation → Mart, depending on your area) before merging anything into shared notebooks.
5. **Open a PR back to `main`** once your notebook runs cleanly end-to-end; note what changed and what you tested.
6. **Review before merge** — no direct pushes to `main`.

---

## Branch names

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

## Where raw files go

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

## Referencing tables

Always use the full path: `catalog.schema.table`.

```sql
SELECT * FROM nyc_mobility.clean.green_taxi;
```

Don't rely on a `USE CATALOG` / `USE SCHEMA` set earlier in a notebook — spell out the full reference every time.

---

## Table names

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

## Approved catalog & workspace path

- **Approved catalog:** `nyc_mobility` — this is the only catalog this project should write to. Don't create new catalogs.
- **Approved workspace/volume path** for raw file drops:
  ```
  /Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/<source>/<filename>
  ```
- Don't point ingestion notebooks at any other catalog or volume path, even for testing — use a personal scratch table inside `nyc_mobility.raw` if you need to experiment.

---

## Column naming

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

## Who owns what

| Owner | Area |
|---|---|
| Crizza | Git & Documentation |
| Garett | Green Taxi Dataset |
| Anje | Weather Dataset |
| Kinah | Traffic Dataset|
| Cha | Taxi Zones & Gold fact-dim |
| Mia & Anje | Data quality & dashboard |

---

## Security

**Never commit:**
- Databricks tokens, API keys, or any credentials (including the weather/traffic API keys/R2)
- Passwords or secrets of any kind
- Raw downloaded datasets — pull from the shared Volume path instead of committing local copies
- Notebook output cells that might expose tokens, secrets, or internal paths

**Fine to commit:**
- Notebook/source code, SQL, documentation, and non-secret example configs

