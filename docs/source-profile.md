## NYC DOT weekly traffic advisory (Engineer 4)

**Where it comes from.** https://www.nyc.gov/html/dot/html/motorist/weektraf.shtml

An HTML page, not an API. There is no key, no feed and no query string. The page carries
the current week only and the DOT keeps no archive of past weeks, so there is nothing to
backfill. Whatever history we end up with is history we accumulate ourselves, one scrape
at a time, which is why the clean table is Type 2 rather than a straight replace.

**How we read it.** One GET per run with a descriptive User-Agent, a 30 second timeout,
and the status code checked. The HTML is written to the landing volume before anything
parses it, so a parser fix can be re-run without going back to the site.

**Shape of the page.**

| Element | What it means |
| --- | --- |
| `h2[id]` | a section, either a borough or the crossings group |
| `h3` or `h4` | a location, or the marker "Festivals, Parades and Events" |
| bare `strong` | also a location, or a field label such as "Location(s):" |
| `p` | advisory prose, with the dates written into the sentence |
| `ul > li` | street segments, in the form "X between Y and Z" |

**Three things that will catch you out.**

A bolded paragraph is not a heading. A `strong` only counts as a location if it is short
and is not a field label. Reading every bold as a heading finds 11 locations on a page
that has 26, and everything under the missed ones is lost silently.

The dates are inside the prose, not in an attribute, so they are pulled out with a regex
over the sentence rather than read from the markup.

Some segments will not split into street, from and to, usually because of a typo at the
source, for example "Duane St Bet Federal Plaza and Centre Street". Those are kept as raw
text rather than dropped, so nothing disappears just because it was written oddly.

**What it yields.** Measured against the live page on 2026-09-14: 8 sections, 26
locations, 72 entries, 42 street segments, of which 39 split cleanly and 3 were kept raw.

**Tables.**

| Table | Grain |
| --- | --- |
| `nyc_mobility.raw.traffic_advisory` | one advisory entry per scrape date, all strings, nothing dropped |
| `nyc_mobility.clean.traffic_advisory` | one advisory, Type 2 across scrapes, with `first_seen_scrape`, `last_seen_scrape`, `is_current` and `times_seen` |

**Incremental and idempotent.** The scrape date is the batch. The raw key `advisory_sk`
is a SHA-256 over the entry fields plus the scrape date, so re-running the same day
matches every existing row and inserts nothing. In clean, `closure_bk` deliberately
excludes the scrape date, which is what lets the same advisory seen across three weeks
stay one row instead of three.

**One thing to agree on.** The class volume is mounted under a different name in
different workspaces, `ftw-b12-r2` in the weather and taxi zones notebooks and
`ftw-b12-de` elsewhere. The traffic notebook resolves it at runtime from
`CANDIDATE_VOLUMES` rather than hardcoding one, because a hardcoded path works for
whoever wrote it and fails for everyone else.
