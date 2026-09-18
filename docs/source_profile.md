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
| Construction closures | Socrata SoQL, CSV | `data.cityofnewyork.us/resource/ezy6-djsf.csv` | 11,031 | Not yet profiled |

---

## Green Taxi Trip Data

### Where it comes from

Three monthly Parquet extracts pulled from the public TLC trip-record archive. Combined, the three files total 133,367 rows, and the row count landed matches what left the source exactly — nothing was lost or duplicated on the way in.

### What's in it

Twenty-one columns per file, identical across all three months — no column was added, dropped, or retyped between them. The source doesn't hand you a trip ID: `VendorID` repeats constantly and can't serve as one, so uniqueness had to be checked with full-row comparison instead of a key lookup. That check came back clean — zero exact duplicate rows in any of the three files.

### What was decided about it

The dataset arrived with a long tail of small, specific problems rather than one big one. Two rows carry a pickup or dropoff year of 2008/2009 — almost certainly a rollover glitch at year-end rather than a real trip — and one row has a dropoff timestamp earlier than its pickup. Both categories were dropped outright; there's no way to reconstruct a trustworthy timestamp for either.

Everything else was flagged rather than removed, on the reasoning that a broken field doesn't make the whole trip fake:

| What was found | How much | What happened to it |
| --- | --- | --- |
| Corrupted year (2008/2009) | 2 rows | Removed |
| Dropoff earlier than pickup | 1 row | Removed |
| `trip_distance` extreme tail (up to ~111,000 mi) | 31 rows over 100 mi | Not yet capped — threshold still under review |
| `ehail_fee` empty in every row | 133,367 rows | Left as-is, documented as a dead column |
| VendorID 6 missing six metadata fields at once | 18,754 rows | Kept, flagged `flag_vendor6_incomplete` |
| VendorID 6 fares flat around $9, tip always $0 | 14,181 rows | Left open — looks like a broken meter feed, not confirmed |
| Zero trip duration paired with real distance | 12 rows | Kept, flagged |
| Zero duration and zero distance but still charged a fare | 87 rows | Kept, flagged (most map to the "outside zone" sentinel location) |
| Trips running longer than 3 hours (up to ~41 hrs) | 617 rows | Noted, no rule applied yet |
| Negative fare or total | 2,275 / 619 rows | Kept, flagged |
| Negative tip | 50 rows | Kept, flagged |
| Zero passengers logged | 1,727 rows | Kept, flagged |
| 7–9 passengers logged | 33 rows | Left alone — too small to matter |
| $0 tip on cash or card fares | ~34,000 rows combined | Left alone — ordinary tipping behavior |

Every categorical code (`VendorID`, `payment_type`, `RatecodeID`) checked out against the official TLC dictionary with zero invalid values once the valid-value list itself was corrected — an earlier pass had flagged `VendorID = 6` as unrecognized simply because the team's reference list was incomplete, not because the data was wrong.

Typing was standardized dataset-wide: money and distance became `DoubleType` rounded to two decimals, duration became `DecimalType(10,2)`, the coded ID fields became `IntegerType`, and every datetime became `TimestampType`.

### Still open before this feeds Gold

The distance cap and the VendorID-6 fare pattern are the two unresolved items — anything that aggregates distance or revenue should treat those two areas as provisional until a threshold is agreed on.

---

## Taxi Zone Lookup

### Where it comes from

A single reference CSV, 265 rows, functioning as a static lookup rather than something that grows over time.

### What's in it

Four columns: a zone identifier, borough, zone name, and service-zone classification. The identifier has zero nulls and zero duplicates across all 265 rows, which makes it a safe join key on its own — with one caveat below.

### What was decided about it

Nothing in this file needed correcting, but two things needed a decision on how to *treat* rather than fix:

- **Three zone names repeat.** That's legitimate — a couple of large neighborhoods genuinely span more than one taxi zone and share a name — so the name was kept as-is. The consequence is that joins and lookups have to use the zone ID, never the zone name, since the name alone isn't unique.
- **A handful of rows carry `Unknown` or `N/A`** in borough, zone, or service-zone (a few rows each, out of 265). These aren't blank cells — they're explicit placeholder values the source uses for trips that couldn't be pinned to a real zone (a GPS point outside city limits, or a location that never geocoded). They were kept rather than filtered, because dropping them would silently erase every trip that legitimately couldn't be mapped.

Column types were already correct on arrival; only text formatting (casing and whitespace) was standardized for consistency.

---

## Open-Meteo Weather (ERA5 archive)

### Where it comes from

Pulled from Open-Meteo's historical weather REST endpoint rather than a static file — every run is a live API call, not a download. The profiled window returned 11,040 hourly rows.

### What's in it

A small, flat schema: a timestamp column plus a handful of weather measures — temperature, precipitation, and a WMO weather-condition code. All fourteen underlying response columns were checked for type correctness; only one needed a fix.

### What was decided about it

- **`weather_code` arrived as a `LONG`** when a small integer type is all the field ever needs — cast to `INT`.
- **The other thirteen columns** were already typed correctly; no changes made.
- **Every timestamp in the window is distinct** — no duplicate hours, confirming one row per observation hour with nothing double-counted.
- **Zero nulls anywhere in the dataset** — the hourly series has no gaps to fill or impute.

Because this is a live API rather than a file drop, the profiling also had to establish how the endpoint behaves rather than just what it returns: it responds with a hard HTTP error (not an empty success) for a nonsensical date range, and two identical requests for the same historical window return identical data. Both of those matter for how ingestion should treat a bad or repeated call — a failure should be treated as a failure, not silently interpreted as "no data."

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

### What was decided about it

| Step | What happens | Why it's built that way |
| --- | --- | --- |
| Fetch | One request per run, timeout and status check enforced | It's a public page, not a service meant for heavy polling |
| Land | Raw HTML saved untouched, tagged with the scrape date | Preserves exactly what the page said that day, since the page itself won't |
| Parse | Sections/locations/entries/segments extracted; a hash of the entry content plus scrape date becomes the row's key | The source gives no natural ID, so one has to be constructed to compare entries across scrapes |
| Load raw | Merged in on that constructed key; already-seen rows are left untouched | Re-running the same day's scrape doesn't create duplicate rows |
| Build clean | Dates parsed properly, borough pulled from the section name, a second key built *without* the scrape date, then merged so a repeated advisory updates its "last seen" instead of inserting again, and one no longer on the page gets marked no-longer-current | The only record of "what the page said on a given date" is what gets captured here — the source itself doesn't keep one |

Measured on a live pull, the page yielded 66 advisory entries — consistent with the kind of counts this parsing approach produces once the bold-tag ambiguity above is handled correctly rather than naively.

---

## Construction Closures (not yet profiled)

### What's known so far

A CSV pulled via Socrata's query API from NYC Open Data, expected around 11,031 rows. Beyond the URL, format, and rough size, nothing has been verified yet — no schema check, no null analysis, no key candidate identified, and no comparison against a live pull.

### What still needs to happen before this is trusted as a source

- Confirm the actual column list and types returned by the endpoint
- Establish whether the API provides a natural row identifier or one needs to be constructed, the same way it did for the traffic advisory page
- Run a null and duplicate check across a real pull
- Check whether the endpoint pages results and whether one query captures the full expected row count or needs multiple calls
- Confirm rate limits and how the endpoint responds to a malformed query (loud failure vs. silent empty result)

This source should not be wired into Silver until the above is done and recorded here.

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

Green Taxi, Taxi Zones, Weather, and Traffic Advisory have cleared this gate, with two Green Taxi items (the distance cap and the VendorID-6 fare pattern) explicitly left open rather than resolved. Construction Closures has not yet reached this gate.
