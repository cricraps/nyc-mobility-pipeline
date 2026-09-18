# Naming Conventions

This document is the reference for how catalogs, schemas, storage volumes, repository folders, and files are named across the pipeline. Anything not covered here should follow the closest existing pattern rather than introducing a new one.

**Last updated:** 2026-09-18

## Catalog

| Item | Value |
| --- | --- |
| Catalog name | `nyc_mobility` |

```sql
CREATE CATALOG IF NOT EXISTS nyc_mobility;
```

**Convention:** the catalog is named after the project/domain (`nyc_mobility`), in `snake_case`, with no environment or team suffix — environment and team scoping happens at the Volume path level instead (see below), not in the catalog name.

## Schemas

One schema per pipeline layer, all created under the `nyc_mobility` catalog, in `snake_case`:

| Schema | Layer | Purpose |
| --- | --- | --- |
| `nyc_mobility.raw` | Bronze | Data ingestion — landed source data, unmodified |
| `nyc_mobility.clean` | Silver | Data cleaning — typed, standardized, flagged |
| `nyc_mobility.mart` | Gold | Dimensional modelling — `dim_*` and `fact_*` tables |
| `nyc_mobility.bi_visualization` | Dashboard | Dashboard-facing visualization outputs |
| `nyc_mobility.validation` | DQ | Data-quality check results |
| `nyc_mobility.dq_visualization` | DQ dashboard | Dashboard-facing views of data-quality results |

```sql
CREATE SCHEMA IF NOT EXISTS nyc_mobility.raw;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.clean;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.mart;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.bi_visualization;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.validation;
CREATE SCHEMA IF NOT EXISTS nyc_mobility.dq_visualization;
```

**Convention:** schema names describe the layer's *function* (`raw`, `clean`, `mart`), not a numeric prefix — ordering is implied by the pipeline stage itself and by the numbered repository folders (below), not by the schema name.

## Source volume path

Raw source files land in a Databricks Volume under this fixed path pattern:

```
/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/<dataset_name>/
```

| Path segment | Meaning |
| --- | --- |
| `workspace` / `default` | Fixed catalog/schema the Volume itself is registered under |
| `ftw-b12-r2` | Class storage environment (R2) |
| `groups/week-08` | Cohort/week identifier |
| `group-f` | Team identifier |
| `<dataset_name>` | One folder per source dataset, `snake_case`, e.g. `green_taxi` |

Example (Green Taxi):

```python
volume_path = "/Volumes/workspace/default/ftw-b12-r2/groups/week-08/group-f/green_taxi/"
```

**Convention:** every dataset gets its own trailing folder named after the dataset in `snake_case` (`green_taxi`, `weather`, `taxi_zones`, `traffic_advisory`). Do not nest multiple datasets under one shared folder — each gets its own path so retention, re-ingestion, and access can be scoped per source.

## Repository folder structure

```
nyc-mobility-pipeline/
└── src/
    ├── 00_setup/
    ├── 01_raw/
    ├── 02_clean/
    ├── 03_mart/
    └── 04_visualisation/
```

| Folder | Layer | Contains |
| --- | --- | --- |
| `00_setup` | Setup | Catalog/schema DDL and one-time environment setup |
| `01_raw` | Bronze | One ingestion file per source |
| `02_clean` | Silver | One cleaning notebook per source |
| `03_mart` | Gold | One notebook per dimension/fact table |
| `04_visualisation` | Dashboard | Notebooks that build BI/DQ dashboard queries |

**Convention:** folders are numbered in pipeline order (`00` → `04`) so they sort correctly in a file browser and so the number itself communicates where in the pipeline a file sits, matching the `raw` → `clean` → `mart` schema layering above.

## File naming within a layer folder

Pattern:

```
<NN>_<dataset_or_table_name>_<layer_suffix>.<ext>
```

- `<NN>` — two-digit sequence number. **The same number is reused for the same dataset across `01_raw` and `02_clean`**, so a dataset's raw and clean files line up at a glance:

| `NN` | Dataset |
| --- | --- |
| `01` | `green_taxi` |
| `02` | `weather` |
| `03` | `taxi_zones` |
| `04` | `traffic_advisory` |

  Examples: `01_green_taxi_raw.py` pairs with `01_green_taxi_clean.ipynb`; `04_traffic_advisory_raw.ipynb` pairs with `04_traffic_advisory_clean.ipynb`.

- `<layer_suffix>` — matches the folder's layer: `_raw` in `01_raw/`, `_clean` in `02_clean/`.
- `<ext>` — `.py` for plain ingestion scripts, `.ipynb` for notebooks with mixed SQL/markdown/exploration. Prefer `.ipynb` once a step needs more than a single linear script; keep `.py` only for straightforward, non-interactive ingestion.

### `00_setup`

Setup files are named `<step>.<subject>.ipynb`, e.g. `00_setup.dbquery.ipynb` for the catalog/schema creation notebook. There is normally only one setup file per environment concern, so no sequence number is needed beyond the leading `00`.

### `01_raw and 02_clean`

Raw and clean files follow the same `<NN>_<table_name>.<ext>` pattern

See: [Table naming (Bronze & Silver layers)](https://github.com/cricraps/nyc-mobility-pipeline/edit/main/docs/naming-conventions.md#table-naming-bronze--silver-layers) for details in convention.

### `03_mart`

Mart files follow the same `<NN>_<table_name>.<ext>` pattern, but `<table_name>` is the actual Gold table name (`dim_*` / `fact_*`), and numbering reflects build order (dimensions before the fact table that depends on them):

| File | Table |
| --- | --- |
| `01_dim_advisory_table.ipynb` | `dim_advisory` |
| `02_dim_date_table.ipynb` | `dim_date` |
| `03_dim_weather_table.ipynb` | `dim_weather` |
| `04_dim_zone_table.ipynb` | `dim_zone` |
| `05_fact_trip_table.ipynb` | `fact_trip` |

**Convention:** every dimension table file is named `<NN>_dim_<subject>_table.ipynb`; the fact table file is named `<NN>_fact_<subject>_table.ipynb`. All dimension files are numbered ahead of the fact file that consumes them.

### `04_visualisation`

Visualization notebooks are named `<NN>_<audience>_<topic>.ipynb`, where `<audience>` identifies which dashboard schema the notebook feeds (e.g. `bi` for `bi_visualization`, `dq` for `dq_visualization`) and `<topic>` is the business question or check area it covers, e.g. `01_bi_demand_and_popularity...`.

## Table naming (Bronze & Silver layers)

Tables in `nyc_mobility.raw` (Bronze) and `nyc_mobility.clean` (Silver) use the same table name in both schemas — only the schema changes between the two layers:

| Schema | Table | Dataset |
| --- | --- | --- |
| `raw` / `clean` | `green_03_2026` | Green Taxi — March 2026 |
| `raw` / `clean` | `green_04_2026` | Green Taxi — April 2026 |
| `raw` / `clean` | `green_05_2026` | Green Taxi — May 2026 |
| `raw` / `clean` | `taxi_zones` | Taxi Zones |
| `raw` / `clean` | `traffic_advisory` | Traffic Advisory |
| `raw` / `clean` | `weather` | Weather |

**Convention:**

- Green Taxi is partitioned **one table per source month**, named `green_<MM>_<YYYY>` (`green_03_2026`, `green_04_2026`, `green_05_2026`), because the source itself arrives as one file per month — the table naming mirrors the source-file cadence rather than merging months into a single table.
- Taxi Zones, Traffic Advisory, and Weather are each **one table for the whole source**, named after the dataset itself (`taxi_zones`, `traffic_advisory`, `weather`), with no month/date suffix, because those sources are not delivered as separate monthly files.
- The table name does not change between `raw` and `clean` — only the schema qualifier does (`nyc_mobility.raw.weather` → `nyc_mobility.clean.weather`), so a table's identity is traceable across layers by name alone.
- Do not add a layer indicator (`_raw`, `_bronze`, `_clean`) into the table name itself — the schema already carries that information.

## Table naming (Gold layer)

| Prefix | Meaning |
| --- | --- |
| `dim_*` | Dimension table, one row per business entity (e.g. `dim_date`, `dim_zone`, `dim_weather`, `dim_advisory`) |
| `fact_*` | Fact table, one row per measured event (e.g. `fact_trip`) |

**Convention:** table names are `snake_case`, singular subject after the prefix (`dim_zone`, not `dim_zones`), and never repeat the schema name (`mart.dim_zone`, not `mart.mart_dim_zone`).

## Commit and PR naming

Observed convention: short, plain-English, imperative-ish commit messages describing what changed, not a formal conventional-commits prefix — e.g. *"Updated green taxi"*, *"Cleaned weather table"*, *"Map the crossings section to borough Crossings"*, *"Revised the code for dim_date_table"*. Feature branches are named `feature/<short-topic>` (e.g. `feature/taxi_zones-clean`), merged via pull request into `main`.

**Convention:** keep commit messages short and descriptive of the actual change made, scoped to one file/table where possible, rather than batching unrelated changes into one commit.
