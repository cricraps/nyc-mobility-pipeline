# Source Profile

This document captures what profiling found for every upstream source feeding the pipeline: schema, size, coverage window, quality issues, and the keys used to identify a row. A source doesn't move past this stage on the strength of its URL being reachable — it moves past this stage once someone has actually looked at the data and written down what's in it.

## Status

Four of the five sources below have been profiled end-to-end, with findings carried over from the team's decision log. The fifth — Construction Closures — has been scoped (URL, format, and expected row count are known) but not yet pulled and inspected, so it's listed as open work rather than a finished profile.

## File Inventory

| Source | Pattern | Origin | Rows | Status |
| --- | --- | --- | --- | --- |
| Green taxi trips | Monthly Parquet | `d37ci6vzurychx.cloudfront.net/trip-data/` | 133,367 | Profiled |
| Taxi zone lookup | Single CSV | `d37ci6vzurychx.cloudfront.net/misc/` | 265 | Profiled |
| Open-Meteo ERA5 | REST API, JSON | `archive-api.open-meteo.com/v1/archive` | 11,040 | Profiled |
| NYC DOT weekly advisory | Scraped HTML | `nyc.gov/html/dot/html/motorist/weektraf.shtml` | 66 | Profiled |
| Construction closures | Socrata SoQL, CSV | `data.cityofnewyork.us/resource/ezy6-djsf.csv` | 11,031 | Scrapped only |

---

## Green Taxi Trip Data

### Where it comes from

Three monthly Parquet extracts pulled from the public TLC trip-record archive. Combined, the three files total 133,367 rows, and the row count landed matches what left the source exactly — nothing was lost or duplicated on the way in.

### What's in it

Twenty-one columns per file, identical across all three months — no column was added, dropped, or retyped between them. The source doesn't hand you a trip ID: `VendorID` repeats constantly and can't serve as one, so uniqueness had to be checked with full-row comparison instead of a key lookup. That check came back clean — zero exact duplicate rows in any of the three files.

### Findings Summary
<img width="424" height="408" alt="image" src="https://github.com/user-attachments/assets/c98f1480-1a80-4d54-8232-29d4ea82468d" />

---

## Taxi Zone Lookup

### Where it comes from

A single reference CSV, 265 rows, functioning as a static lookup rather than something that grows over time.

### What's in it

Four columns: a zone identifier, borough, zone name, and service-zone classification. The identifier has zero nulls and zero duplicates across all 265 rows, which makes it a safe join key on its own — with one caveat below.

### Findings Summary
<img width="420" height="187" alt="image" src="https://github.com/user-attachments/assets/069097cf-f998-4956-948d-ffc637ed0952" />




---

## Open-Meteo Weather 

### Where it comes from

Pulled from Open-Meteo's historical weather REST endpoint rather than a static file — every run is a live API call, not a download. The profiled window returned 11,040 hourly rows.

### What's in it

A small, flat schema: a timestamp column plus a handful of weather measures — temperature, precipitation, and a WMO weather-condition code. All fourteen underlying response columns were checked for type correctness; only one needed a fix.

### Findings Summary

- **The other thirteen columns** were already typed correctly; no changes made.
- **Every timestamp in the window is distinct** — no duplicate hours, confirming one row per observation hour with nothing double-counted.
- **Zero nulls anywhere in the dataset** — the hourly series has no gaps to fill or impute.

<img width="432" height="142" alt="image" src="https://github.com/user-attachments/assets/bfbd378e-71c7-4996-ad41-11aaf20d9525" />

---

## NYC DOT Weekly Traffic Advisory

### Where it comes from

`nyc.gov/html/dot/html/motorist/weektraf.shtml` — a plain HTML page, not an API. There's no key, no query parameters, and critically, no archive: the page only ever shows the current week, so any history of past advisories has to be built by scraping it repeatedly over time rather than backfilled from the source. That's the whole reason the cleaned table is built as slowly-changing history instead of a simple overwrite.

### How it's read

One request per run, sent with a descriptive user-agent and a timeout, checking the response code explicitly — a non-200 response fails the run rather than being ignored. The raw HTML is saved exactly as received before any parsing happens, so if the parsing logic turns out to be wrong later, it can be fixed and re-run against the saved page without needing to re-fetch it (useful given the page has no history to go back and re-scrape).

### Shape of the page

The markup mixes a few different patterns for what is logically the same kind of information:

| Tag | Usually means |
| --- | --- |
| A heading tag with an id | A section — a borough, or the bridge-crossings group |
| A sub-heading | A location, or the events sub-section |
| A bare bold tag | Sometimes also a location, sometimes just a field label |
| A paragraph | The advisory text itself, with dates written into the sentence rather than tagged separately |
| A bullet list | The affected street segments, written as "between X and Y" |

The inconsistency in that bold-tag usage is the main trap: treating every bold tag as a new location undercounts real locations substantially, because a chunk of them are actually field labels, not headings. Dates aren't available as clean structured data either — they're embedded in ordinary sentences and have to be pulled out with pattern matching. And not every street segment splits cleanly into a from/to pair; a few carry inconsistent source formatting and get preserved as raw text instead of being forced into a shape they don't fit.

## Construction Closures 

### What's known 

A CSV pulled via Socrata's query API from NYC Open Data, expected around 11,031 rows. Beyond the URL, format, and rough size, nothing has been verified yet — no schema check, no null analysis, no key candidate identified, and no comparison against a live pull.

---

## Exit Gate

A source is considered ready for downstream transformation only once its owner and a reviewer agree on all of the following, for that source:

- The schema and any drift across files or calls
- The date/coverage window actually observed
- The row count and whether it reconciles against what was ingested
- What serves as the row's key, and any limits on that key's uniqueness
- Every null and duplicate finding, and what was decided about each
- Any placeholder or sentinel values and how they're meant to be treated
- Documented anomalies, resolved or explicitly deferred
- For APIs and scrapes specifically: how the source behaves on a bad request, and whether repeated calls are stable

Green Taxi, Taxi Zones, Weather, and Traffic Advisory have cleared this gate.

To see what decisions were made to the data findings, see [decisions.md](https://tinyurl.com/decisions-md)
