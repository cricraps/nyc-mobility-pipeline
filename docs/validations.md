## Traffic advisory (Engineer 4)

Checks run in `05_validation/04_traffic_advisory_validation`, after the raw and clean
notebooks, and are written to `nyc_mobility.validation.traffic_advisory_validation` in the
shared shape: `column`, `data_quality_check`, `failed_rows`, `total_rows`, `percentage`,
`status`. `data_quality_check` holds the dimension. Baselines are from the live page on
2026-09-14.

| Column | Dimension | Rule | Layer | If it trips |
| --- | --- | --- | --- | --- |
| `ALL_COLUMNS (raw, latest scrape)` | Completeness | the latest scrape landed at least one row | raw | FAIL |
| `location_name (raw)` | Validity | more than 20 distinct locations in the latest scrape, baseline 26 | raw | WARN |
| `advisory_sk (raw)` | Uniqueness | no duplicate keys in raw | raw | FAIL |
| `scrape_date (raw)` | Timeliness | no scrape date in the future | raw | FAIL |
| `closure_bk` | Uniqueness | one row per advisory in clean | clean | FAIL |
| `closure_bk` | Completeness | every clean row has a business key | clean | FAIL |
| `effective_from, effective_to` | Validity | `effective_from` is not after `effective_to` | clean | WARN |
| `borough` | Validity | borough is one of the five boroughs or Crossings | clean | WARN |
| `closure_bk` | Accuracy | every advisory in the newest raw scrape exists in clean, layer against layer | clean | FAIL |
| `times_seen` | Consistency | `times_seen` never exceeds the number of scrapes raw holds, the clean idempotency guard | clean | FAIL |

**Status rule.** PASS at zero failures, WARN under 5 percent, FAIL at or above, the same
as the other validation tables. The checks marked FAIL above are strict: any failure at
all is a FAIL, because a duplicate key, a null business key or a raw row missing from
clean breaks the MERGE or the downstream joins whatever the percentage says.

**Why `location_name` is the one to watch.** It is the canary for the bolded paragraph
trap described in the source profile. A parser that stops recognising bare `strong`
headings does not error, it just quietly returns about 4 locations instead of 26 and the
load still reports success. The threshold is set well under the baseline so an ordinary
quiet week does not cry wolf, but a broken parser cannot hide.

**Why a zero row count is a FAIL and not a WARN.** Returning nothing silently is the
failure mode that actually costs us, so the raw notebook raises before it writes rather
than merging an empty batch over a good one.

**Proving idempotency.** The last cell of the validation notebook prints rows landed
against distinct keys per scrape date for raw, and rows by `times_seen` for clean. Run the
raw and clean notebooks a second time and re-run it: every figure must be unchanged, and
the second raw run inserts zero rows.
