# Decision Log

This document is the canonical record of data-profiling and engineering decisions made across the pipeline's four sources: **Green Taxi**, **Weather**, **Traffic Advisory**, and **Taxi Zones**. It records what was found during profiling, what was decided, why, and what the decision means for downstream layers.

**Last updated:** 2026-09-18

## Maintenance rule

When a decision changes, update this file in the same pull request as the code change it affects. If a decision is superseded, mark the old entry as **Superseded** rather than deleting it, and link to the entry that replaces it.

## Sources

| Dataset | Source |
| --- | --- |
| Green Taxi | NYC TLC Green Taxi trip record files |
| Weather | Open-Meteo historical weather API |
| Traffic Advisory | NYC DOT public traffic advisory web page (HTML, scraped) |
| Taxi Zones | NYC TLC Taxi Zone Lookup Table |

## D01: Platform

**Decision:** Use `ftw-r2` as the storage and processing environment. Use GitHub for version-controlled code and documentation.

**Reason:** `ftw-r2` is the assigned class storage environment for this project; introducing a second storage or processing platform would add setup and access risk without serving any approved requirement.

**Consequence:** All Bronze/Silver/Gold tables and Volumes for the four datasets above are built inside `ftw-r2`. Do not introduce another processing or storage platform without a new decision entry.

---

## Summary of decisions

A quick-reference table of every profiling finding and its decision, grouped by dataset. Full reasoning for each row is in the per-dataset sections below.

### Green Taxi

| Finding | Scale | Decision |
| --- | --- | --- |
| Pre-2026 timestamps (2008/2009) | 2 rows | Excluded — year field corrupted, not trustworthy |
| Dropoff before pickup | 1 row | Excluded |
| `trip_distance` extreme outlier (max 111,005.95 mi) | 31 rows | Deferred — cap at > 100 mi, not yet enforced |
| `ehail_fee` always null | 133,367 / 133,367 (100%) | No action — always-empty field, documented |
| VendorID 6 → null RatecodeID, store_and_fwd_flag, passenger_count, payment_type, trip_type, congestion_surcharge | 18,754 rows | Flagged, kept — `flag_vendor6_incomplete` |
| VendorID 6 implausible fare/tip (fare capped ~$9, tip always $0.00) | 14,181 rows | Deferred |
| Month-boundary trips (pickup late Feb 28) | 8 rows | Kept — legitimate trips |
| Zero duration + non-zero distance | 12 rows | Flagged, kept — `flag_zero_duration_nonzero_distance` |
| Zero duration + zero distance, fare charged | 87 rows | Flagged, kept — `flag_no_real_dropoff` (84 of 87 map to DOLocationID 264) |
| Trips over 3 hours (max ~41 hrs) | 617 rows | Identified, no exclusion yet |
| Vendor 2 null RatecodeID | 4,396 rows | Covered by the (deferred) distance cap |
| Negative `fare_amount` / `total_amount` | 2,275 / 619 rows | Flagged, kept — `flag_negative_fare` |
| Negative `tip_amount` | 50 rows | Flagged, kept — `flag_negative_tip` |
| `passenger_count` = 0 | 1,727 rows | Flagged, kept — `flag_zero_passengers` |
| `passenger_count` 7–9 | 33 rows | No action — negligible volume |
| $0 tip on cash | ~26,050 rows | No action — expected behavior |
| $0 tip on card | 8,168 rows | No action — plausible, noted |
| RatecodeID, payment_type, trip_type, store_and_fwd_flag, zone IDs | — | No action — all within valid TLC ranges |
| Inconsistent dtypes / decimal display | dataset-wide | Cast money/distance to `DoubleType` (2dp); duration to `DecimalType(10,2)`; categorical IDs to `IntegerType`; datetimes to `TimestampType` |

### Weather

| Finding | Scale | Decision |
| --- | --- | --- |
| `weather_code` stored as LONG instead of INT | 1 / 14 columns | Cast to `INT` |
| All other columns correctly typed | 13 / 14 columns | No action |
| `observation_time` all distinct | 100% | No action |
| Nulls | 0% | No action |

### Traffic Advisory

| Step | What happens | Why |
| --- | --- | --- |
| 1. Fetch | One `requests.get` per run, descriptive User-Agent, 30s timeout, status code checked; non-200 fails the run | Public HTML page, not an API — one polite request per run |
| 2. Land | Raw HTML written byte-for-byte to `traffic_advisory/scrape_date=YYYY-MM-DD/weektraf.html`; ledger row `STARTED` | Parser can be fixed and rerun without re-fetching; Bronze preserves what was received |
| 3. Parse | BeautifulSoup walk of sections/locations/entries/segments; dates regexed from prose; `advisory_sk` = SHA-256 of entry fields + scrape_date | One row per entry per scrape; key built because the source page has none |
| 4. Raw table | `MERGE` into `raw.traffic_advisory` on `advisory_sk`; matched rows untouched; zero parsed rows fails the load; ledger row `LOADED` | Idempotent by construction — rerunning the same scrape inserts nothing |
| 5. Clean table | Dates typed, borough parsed from `section_name`, `entry_type` set (construction/event), `closure_bk` built without scrape date, then `MERGE`: reseen rows update `last_seen_scrape`/`times_seen`, new rows get `first_seen_scrape`, gone rows flip `is_current = false` | Type 2 history — the source page overwrites itself, so this table is the only archive |
| 6. Flags | One row per check written to the DQ results table (`PASS`/`WARN`/`FAIL`) | Every claim gets a number, compiled across all engineers |

### Taxi Zones

| Finding | Scale | Decision |
| --- | --- | --- |
| Nulls | 0% | No action |
| Duplicate zone names | 3 / 265 | Retained — valid, some zones share a neighborhood name |
| Column data types | 4 / 4 correct | No action (still standardized for consistent formatting) |
| `Unknown` / `N/A` in borough, zone, service_zone | borough: 2/265, zone: 1/265, service_zone: 2/265 | Retained — represents pickups/dropoffs that don't map to a real taxi zone (e.g. GPS ping outside the five boroughs, unresolved geocode) |

---

## Green Taxi decisions

### GT-01: Exclude corrupted pre-2026 timestamps

**Finding:** 2 rows carry a pickup/dropoff year of 2008 or 2009, clustering at year-boundary dates (12-31 / 01-01).

**Decision:** Exclude.

**Reason:** Only the year field appears corrupted, consistent with a year-rollover encoding bug at the source rather than a real historical trip. The date cannot be safely derived or corrected, so the row cannot be trusted for any time-based measure.

**Consequence:** These 2 rows do not appear in any Silver or Gold table.

### GT-02: Exclude dropoff-before-pickup row

**Finding:** 1 row has a dropoff timestamp earlier than its pickup timestamp.

**Decision:** Exclude.

**Reason:** Physically impossible; there is no reliable way to determine which of the two timestamps is correct.

**Consequence:** Row does not appear downstream.

### GT-03: Defer a cap on extreme `trip_distance`

**Finding:** `trip_distance` has a dataset-wide extreme outlier tail, up to 111,005.95 miles; 31 rows exceed 100 miles.

**Decision:** Deferred. Candidate treatments under consideration: cap at `trip_distance > 100 mi`, a higher cap, a 35-mile max, a quartile-based outlier formula, or keeping outliers but filtering them out only at the dashboard layer. Only an outright `> 100,000` exclusion is treated as settled.

**Reason:** The field looks like a decimal- or unit-encoding error rather than random noise, but the right threshold hasn't been validated against enough evidence yet to commit to one rule.

**Consequence:** No distance cap is currently enforced in Silver. `trip_distance` should not be trusted at face value for aggregate distance measures until this decision is finalized. Vendor 2's null-`RatecodeID` rows (GT-11) will be covered once a cap is chosen.

### GT-04: No action on always-null `ehail_fee`

**Finding:** `ehail_fee` is null in all 133,367 rows (100%).

**Decision:** No action; documented as an always-empty field.

**Reason:** Not a data quality problem — the field is simply never populated in this source extract.

**Consequence:** Downstream consumers should not expect `ehail_fee` to ever carry a value; exclude it from any "completeness" scoring.

### GT-05: Flag, don't drop, VendorID 6's missing metadata

**Finding:** 18,754 rows have `RatecodeID`, `store_and_fwd_flag`, `passenger_count`, `payment_type`, `trip_type`, and `congestion_surcharge` null together. This traces 100% to `VendorID = 6`.

**Decision:** Flag as `flag_vendor6_incomplete`; keep the rows in the clean table.

**Reason:** The trip itself still happened — pickup/dropoff timestamps, distance, and location data are usable. Only this vendor's metadata reporting is broken. Dropping the row would discard 14,180+ real trips over one vendor's incomplete feed.

**Consequence:** Any query using `RatecodeID`, `payment_type`, `trip_type`, `store_and_fwd_flag`, `passenger_count`, or `congestion_surcharge` must filter on `NOT flag_vendor6_incomplete` (or explicitly account for the nulls) rather than assume completeness.

### GT-06: Defer VendorID 6's implausible fare/tip data

**Finding:** 14,181 rows (VendorID 6) show fares capped at roughly $9 regardless of distance, with tip always $0.00.

**Decision:** Deferred.

**Reason:** Related to GT-05's incomplete metadata, but the fare/tip implausibility needs its own validation pass before a rule (flag vs. exclude vs. re-derive) is chosen.

**Consequence:** Fare-based measures should be treated with caution for VendorID 6 until this is resolved.

### GT-07: Keep month-boundary trips

**Finding:** 8 rows have a pickup late on Feb 28.

**Decision:** Keep — legitimate trips, not errors.

**Reason:** Late-February pickups crossing into the next day/month are ordinary trip behavior, not a data defect.

**Consequence:** No special handling required.

### GT-08: Flag zero-duration trips with non-zero distance

**Finding:** 12 rows have identical pickup and dropoff timestamps but a non-zero `trip_distance`.

**Decision:** Flag as `flag_zero_duration_nonzero_distance`; keep the row.

**Reason:** Traveling a real distance in zero elapsed time is physically impossible, so the timestamp pair specifically is untrustworthy — but distance, fare, and location fields are still usable.

**Consequence:** Exclude `trip_duration_min` and any duration-derived measure for these 12 rows specifically; other measures may still use the row.

### GT-09: Flag zero-duration, zero-distance, fare-charged trips

**Finding:** 87 rows have zero duration, zero distance, and a fare charged. 84 of the 87 have `DOLocationID = 264` (Unknown zone).

**Decision:** Flag as `flag_no_real_dropoff`; keep the row.

**Reason:** These almost certainly represent real cancellation or no-show fees — a fare was genuinely charged even though no real trip/dropoff occurred.

**Consequence:** Fare/revenue analysis may use these rows; anything requiring a valid destination (zone-to-zone flow, distance, speed) must exclude them.

### GT-10: Identify long-duration trips, no exclusion yet

**Finding:** 617 rows exceed 3 hours in duration, with a maximum of roughly 41 hours.

**Decision:** Identified only; no exclusion or flag applied yet.

**Reason:** Needs a documented threshold decision (similar to the distance cap) before committing to a rule.

**Consequence:** Duration-based measures should be reviewed for outlier sensitivity until this is finalized.

### GT-11: Vendor 2 null RatecodeID covered by the distance cap

**Finding:** 4,396 rows from Vendor 2 have a null `RatecodeID`.

**Decision:** No separate rule — these rows are covered once the distance cap (GT-03) is finalized.

**Reason:** Profiling showed this null pattern overlaps with the same rows affected by the extreme-distance issue, so a standalone fix isn't needed.

**Consequence:** Resolving GT-03 resolves this finding as a side effect; revisit if the overlap turns out to be incomplete.

### GT-12: Flag negative fare/total amounts

**Finding:** 2,275 rows have negative `fare_amount`; 619 rows have negative `total_amount`.

**Decision:** Flag as `flag_negative_fare`; keep the rows.

**Reason:** Negative fares are plausible as refunds or fare corrections — legitimate financial events, not garbage. Excluding them would understate real transaction activity.

**Consequence:** A negative fare must not be averaged into normal-trip revenue as if it were a discount; revenue aggregations need separate handling for `flag_negative_fare = true` rows.

### GT-13: Flag negative tip amounts

**Finding:** 50 rows have a negative `tip_amount`.

**Decision:** Flag as `flag_negative_tip`; keep the rows.

**Reason:** Unlike fare, there's no legitimate real-world explanation for a negative tip — likely a data entry or processing error. Kept visible and documented rather than silently dropped because the volume is small and traceable.

**Consequence:** Tip-based measures should exclude `flag_negative_tip = true` rows, or treat them explicitly as anomalies.

### GT-14: Flag zero-passenger trips

**Finding:** 1,727 rows have `passenger_count = 0`.

**Decision:** Flag as `flag_zero_passengers`; keep the row.

**Reason:** TLC's own data dictionary allows 0 as a valid, if unusual, value. The rest of the row (fare, distance, locations, timestamps) is likely fine — dropping the whole trip over one questionable field would discard otherwise-good data.

**Consequence:** Passenger-count-based analysis should filter on `NOT flag_zero_passengers` where 0 doesn't make sense for the question being asked; other measures may use the row as-is.

### GT-15: No action on passenger counts 7–9

**Finding:** 33 rows have `passenger_count` between 7 and 9.

**Decision:** No action.

**Reason:** Negligible volume; within a plausible (if high) range for van-type vehicles.

### GT-16: No action on $0 tips (cash and card)

**Finding:** ~26,050 rows have $0 tip on cash payment; 8,168 rows have $0 tip on card payment.

**Decision:** No action.

**Reason:** $0 tip on cash is expected — cash tips are typically not captured in this field. $0 tip on card is plausible (rider chose not to tip) and noted, not treated as an error.

### GT-17: No action on categorical/ID fields

**Finding:** `RatecodeID`, `payment_type`, `trip_type`, `store_and_fwd_flag`, and zone ID fields.

**Decision:** No action.

**Reason:** All values fall within valid TLC-documented ranges — no corruption found.

### GT-18: Type casting and display standardization

**Finding:** Inconsistent dtypes and decimal display dataset-wide (money and distance fields showing without trailing zeros).

**Decision:**
- Money and distance fields cast to `DoubleType`, rounded to 2 decimals (accepted that doubles won't force trailing-zero display).
- `trip_duration_min` cast to `DecimalType(10,2)` for fixed 2-decimal display.
- Integer-coded fields (`VendorID`, `RatecodeID`, `PULocationID`, `DOLocationID`, `passenger_count`, `payment_type`, `trip_type`) cast to `IntegerType`.
- Datetime fields cast to `TimestampType`.

**Reason:** Consistent, queryable typing without over-engineering display formatting that the storage layer doesn't guarantee anyway.

**Consequence:** Consumers expecting fixed trailing-zero display on money/distance fields at the storage layer will need to format at presentation time, not rely on the double type to do it.

---

## Weather decisions

### W-01: Cast `weather_code` to INT

**Finding:** 1 of 14 columns (`weather_code`) is stored as `LONG` when it should be `INT`.

**Decision:** Cast `weather_code` to `INT`.

**Reason:** WMO weather codes are small integers; `LONG` is an unnecessarily wide type carried over from the raw source response.

**Consequence:** Downstream joins against a weather-code lookup/classification table should expect `INT`, not `LONG`.

### W-02: No action on the remaining 13 columns

**Finding:** All other columns are already of the correct data type.

**Decision:** No action.

### W-03: No action — `observation_time` is fully distinct

**Finding:** 100% of `observation_time` values are distinct.

**Decision:** No action.

**Reason:** Confirms one row per observation hour with no duplicate-timestamp problem.

### W-04: No action — zero nulls

**Finding:** 0% nulls across the dataset.

**Decision:** No action.

**Reason:** The hourly weather series is complete; no missing-value handling is required.

---

## Traffic Advisory decisions

### TA-01: Fetch as a scrape, not an API call

**Decision:** One `requests.get` per run with a descriptive User-Agent, a 30-second timeout, and an explicit status-code check. Any non-200 response fails the run.

**Reason:** This is a public information page, not an API — one polite, well-identified request per run is the appropriate load on the source.

**Consequence:** No retry-storming or aggressive polling of the page; a failed fetch is a failed run, not a silent skip.

### TA-02: Land raw HTML byte-for-byte

**Decision:** Write the fetched HTML unmodified to `traffic_advisory/scrape_date=YYYY-MM-DD/weektraf.html`, with a ledger row marked `STARTED`.

**Reason:** Bronze means preserving exactly what was received. Landing the raw bytes means the parser can be fixed and rerun later without re-fetching a page that may have already changed or disappeared.

**Consequence:** Any parser bug is recoverable from the landed HTML; it is not recoverable if the page has since changed and nothing was preserved.

### TA-03: Parse with a synthetic key

**Decision:** Parse sections, locations, entries, and segments with BeautifulSoup; regex dates out of prose. Build `advisory_sk` as a SHA-256 hash of the entry fields plus the scrape date.

**Reason:** The source page provides no natural entry identifier, so one row per entry per scrape needs a constructed key to be addressable and comparable across scrapes.

**Consequence:** `advisory_sk` is only stable as long as the entry fields it hashes don't change between scrapes for what is logically "the same" advisory; a genuinely edited advisory will parse as a new key.

### TA-04: Idempotent raw load via MERGE

**Decision:** `MERGE` into `raw.traffic_advisory` on `advisory_sk`; matched rows are left untouched; zero parsed rows fails the load; ledger row marked `LOADED`.

**Reason:** Running the same scrape twice must not create duplicate raw rows.

**Consequence:** Rerunning a scrape against unchanged source HTML inserts nothing new.

### TA-05: Type 2 history in the clean table

**Decision:** Type dates, parse borough from `section_name`, set `entry_type` (construction or event), build a `closure_bk` that excludes the scrape date, then `MERGE`: an advisory seen again updates `last_seen_scrape`/`times_seen`; a new advisory inserts with `first_seen_scrape`; an advisory no longer present flips `is_current = false`.

**Reason:** The source page overwrites itself on every publish — there's no other archive of what was advertised on a given date. Type 2 history is the only way to answer "what did the page say as of date X."

**Consequence:** Analyses must filter on `is_current` explicitly when they want "advisories in effect right now" versus the full historical record.

### TA-06: Every check becomes a DQ row

**Decision:** Each validation check writes one row (`PASS`/`WARN`/`FAIL`) to the shared DQ results table.

**Reason:** Every claim about this source's quality needs to be a number that can be compiled and reviewed alongside every other source's checks.

**Consequence:** No quality claim about this source should be made in documentation or a dashboard without a corresponding DQ row backing it.

---

## Taxi Zones decisions

### TZ-01: No action — zero nulls

**Finding:** 0% nulls across the lookup table.

**Decision:** No action.

### TZ-02: Retain duplicate zone names

**Finding:** 3 of 265 zone names are duplicated.

**Decision:** Retain.

**Reason:** Valid — some large neighborhoods legitimately span more than one taxi zone and therefore share a name.

**Consequence:** Zone name alone is not a unique key; joins and lookups must use `LocationID`, not zone name.

### TZ-03: No action on data types, standardize formatting

**Finding:** All 4 columns are already of the correct data type.

**Decision:** No action on typing; formatting is still standardized for consistency.

**Reason:** Types were correct on arrival, but text formatting (casing, whitespace) benefited from standardization independent of the type itself.

### TZ-04: Retain `Unknown` / `N/A` sentinel values

**Finding:** `Unknown`/`N/A` values appear in `borough` (2/265), `zone` (1/265), and `service_zone` (2/265).

**Decision:** Retain.

**Reason:** These rows represent real, expected sentinel members of the taxi zone lookup — used for pickups/dropoffs that don't map to a real taxi zone, such as a GPS ping recorded outside the five boroughs or a location geocoding couldn't resolve. They are not data-entry errors.

**Consequence:** Any zone-based join or aggregation must treat these sentinel zones as legitimate "unknown/outside" members rather than filtering them out as bad data; doing so would silently drop trips that genuinely couldn't be mapped.
