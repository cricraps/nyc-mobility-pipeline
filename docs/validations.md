## Traffic advisory (Engineer 4)

Checks run at the end of `02_clean/04_traffic_advisory_clean` and come back as one result
set, one row per check, with a PASS, WARN or FAIL. Baselines are from the live page on
2026-09-14.

| Check | Rule | Layer | If it trips |
| --- | --- | --- | --- |
| `row_count` | the latest scrape landed at least one row | raw | FAIL |
| `location_count` | at least 20 distinct locations in the latest scrape, baseline 26 | raw | WARN |
| `advisory_sk_unique` | no duplicate keys in raw | raw | FAIL |
| `closure_bk_unique` | one row per advisory in clean | clean | FAIL |
| `closure_bk_not_null` | every clean row has a business key | clean | FAIL |
| `date_order` | `effective_from` is not after `effective_to` | clean | WARN |
| `borough_in_domain` | borough is one of the five boroughs or Crossings | clean | WARN |

**Why `location_count` is the one to watch.** It is the canary for the bolded paragraph
trap described in the source profile. A parser that stops recognising bare `strong`
headings does not error, it just quietly returns about 4 locations instead of 26 and the
load still reports success. The threshold is set well under the baseline so an ordinary
quiet week does not cry wolf, but a broken parser cannot hide.

**Why a zero row count is a FAIL and not a WARN.** Returning nothing silently is the
failure mode that actually costs us, so the raw notebook raises before it writes rather
than merging an empty batch over a good one.

**Proving idempotency.** The last cell of the raw notebook prints rows landed against
distinct keys per scrape date. Running the same scrape date twice must leave both numbers
unchanged, and inserts zero rows.
