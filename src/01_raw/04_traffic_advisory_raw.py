# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Traffic advisory
# TRAFFIC ADVISORY, RAW
# Source  nyc.gov/html/dot/html/motorist/weektraf.shtml, HTML, current week only, no archive
# Grain   one advisory entry per scrape date
# Output  nyc_mobility.raw.traffic_advisory
# Next    src/02_clean/04_traffic_advisory_clean

# COMMAND ----------

# MAGIC %pip install beautifulsoup4 --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import re
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# The DOT publishes one HTML page per week and keeps no archive, so there is no API
# to call and no history to backfill. Each run is one scrape of the current week.
DOT_URL = "https://www.nyc.gov/html/dot/html/motorist/weektraf.shtml"
USER_AGENT = "FTW-B12-DE-coursework/1.0 (student project)"

# Landed HTML goes beside the other group sources so the parse can be re-run without
# re-fetching. The class volume is mounted under a different name in each of our
# workspaces, ftw-b12-de in mine and ftw-b12-r2 in the paths Crizza used, so this is the
# one line to change per workspace. Nothing else here depends on the location.
LANDING_VOLUME = "/Volumes/workspace/default/ftw-b12-de"
LANDING_DIR = f"{LANDING_VOLUME}/groups/week09/traffic_advisory"

TABLE_NAME = "nyc_mobility.raw.traffic_advisory"

# COMMAND ----------

MONTH_NAMES = ("January February March April May June July August September "
               "October November December").split()

DATE_RE = re.compile(r"(" + "|".join(MONTH_NAMES) + r")\s+(\d{1,2})(?:st|nd|rd|th)?,?\s+(\d{4})",
                     re.IGNORECASE)
SEGMENT_RE = re.compile(r"^(?P<street>.+?)\s+between\s+(?P<from>.+?)\s+and\s+(?P<to>.+?)$",
                        re.IGNORECASE)

# A label is the WHOLE of a bare <strong> and ends in a colon.
LABEL_RE = re.compile(r"^(Location\(s\)|Route|Formation|Dispersal|Miscellaneous)\s*:\s*$",
                      re.IGNORECASE)
EVENT_BLOCK_RE = re.compile(r"^Festivals?,?\s*Parades and Events$", re.IGNORECASE)

# A heading is short. Anything longer than this inside a <strong> is a bolded paragraph.
# The longest real location name on the verified page is 109 characters.
MAX_HEADING_LEN = 150


def parse_dates(text):
    """Every 'September 12th, 2026' in a blob, as ISO dates, in the order they appear."""
    out = []
    for mon, day, year in DATE_RE.findall(text or ""):
        try:
            out.append(date(int(year), MONTH_NAMES.index(mon.title()) + 1, int(day)).isoformat())
        except ValueError:
            pass
    return out


def parse_advisory(html, source_url, scraped_at_iso):
    """One row per advisory entry. Three traps are handled here, see the comments."""
    soup = BeautifulSoup(html, "html.parser")

    week_h2 = next((h for h in soup.find_all("h2")
                    if "Weekly Traffic Advisory for" in h.get_text()), None)
    week_text = week_h2.get_text(" ", strip=True) if week_h2 else ""
    wd = parse_dates(week_text)
    week_start = wd[0] if wd else None
    week_end = wd[1] if len(wd) > 1 else None

    sections = soup.select("h2[id]")
    records = []

    for sec in sections:
        section_name = sec.get_text(" ", strip=True)
        section_id = sec.get("id")
        location = None
        in_event_block = False
        pending_label = None

        for sib in sec.next_siblings:
            if getattr(sib, "name", None) is None:
                continue
            if sib.name == "h2" and sib.get("id"):
                break

            # h3 and h4 are unambiguous headings, or the event block marker.
            if sib.name in ("h3", "h4"):
                heading = sib.get_text(" ", strip=True)
                if not heading or heading.startswith("*Note"):
                    continue
                if EVENT_BLOCK_RE.match(heading):
                    in_event_block = True
                else:
                    location = heading
                    pending_label = None
                continue

            # Trap 1. A bare <strong> sibling is either a field label or a location heading,
            # and a <strong> nested inside a <p> is never a heading. Reading a bolded
            # paragraph as a location finds 11 locations where the page has 26.
            if sib.name == "strong":
                heading = sib.get_text(" ", strip=True)
                if not heading or heading.startswith("*Note"):
                    continue
                m = LABEL_RE.match(heading)
                if m:
                    pending_label = m.group(1)
                elif len(heading) <= MAX_HEADING_LEN:
                    if EVENT_BLOCK_RE.match(heading):
                        in_event_block = True
                    else:
                        location = heading
                        pending_label = None
                continue

            # Trap 2. The dates live inside the sentence, not in an attribute.
            if sib.name == "p":
                txt = sib.get_text(" ", strip=True)
                if not txt or txt.startswith("*Note") or not location:
                    continue
                d = parse_dates(txt)
                records.append({
                    "section_id": section_id, "section_name": section_name,
                    "location_name": location,
                    "entry_type": "event" if in_event_block else "construction",
                    "detail_label": None, "advisory_text": txt,
                    "street_name": None, "from_street": None, "to_street": None,
                    "date_first_mentioned": d[0] if d else None,
                    "date_last_mentioned": d[-1] if d else None,
                    "n_dates_mentioned": len(d),
                })
                continue

            if sib.name in ("ul", "ol") and location:
                for li in sib.find_all("li"):
                    txt = li.get_text(" ", strip=True)
                    if not txt:
                        continue
                    seg = SEGMENT_RE.match(txt)
                    # A list with no open label and no "between" is a link list, not data.
                    if not pending_label and not seg:
                        continue
                    # Trap 3. Some segments are source typos and will not split.
                    # Keep the raw text rather than dropping the row.
                    records.append({
                        "section_id": section_id, "section_name": section_name,
                        "location_name": location,
                        "entry_type": "event" if in_event_block else "construction",
                        "detail_label": pending_label, "advisory_text": txt,
                        "street_name": seg.group("street").strip() if seg else txt,
                        "from_street": seg.group("from").strip() if seg else None,
                        "to_street": seg.group("to").strip() if seg else None,
                        "date_first_mentioned": None, "date_last_mentioned": None,
                        "n_dates_mentioned": 0,
                    })

    for r in records:
        r.update({"advisory_week_start": week_start,
                  "advisory_week_end": week_end,
                  "advisory_week_text": week_text,
                  "source_url": source_url,
                  "scraped_at_iso": scraped_at_iso})

    return records, {"sections": len(sections), "week_start": week_start, "week_end": week_end}


# COMMAND ----------

# One request per run with a descriptive User-Agent. This is a public information page,
# not an API, so we ask for it once and land it before parsing.
scraped_at = datetime.now()
scrape_date = scraped_at.strftime("%Y-%m-%d")
batch_id = scraped_at.strftime("%Y%m%d_%H%M%S")

response = requests.get(DOT_URL, timeout=30, headers={"User-Agent": USER_AGENT})
response.raise_for_status()

os.makedirs(LANDING_DIR, exist_ok=True)
landed_path = f"{LANDING_DIR}/weektraf_{scrape_date}.html"
with open(landed_path, "w", encoding="utf-8") as f:
    f.write(response.text)

print(f"Landed {len(response.text):,} chars -> {landed_path}")

# COMMAND ----------

records, shape = parse_advisory(response.text, DOT_URL, scraped_at.isoformat())

n_locations = len({r["location_name"] for r in records})
n_split = sum(1 for r in records if r["from_street"])

print(f"Sections: {shape['sections']}")
print(f"Locations: {n_locations}")
print(f"Entries: {len(records)}")
print(f"Street segments split into street/from/to: {n_split}")
print(f"Advisory week: {shape['week_start']} to {shape['week_end']}")

# A silent zero is the failure mode that actually hurts, so it stops the load here.
if not records:
    raise ValueError(f"Parsed 0 entries from {DOT_URL}. The page markup has changed.")

# COMMAND ----------

ADVISORY_SCHEMA = StructType([
    StructField("section_id", StringType()),
    StructField("section_name", StringType()),
    StructField("location_name", StringType()),
    StructField("entry_type", StringType()),
    StructField("detail_label", StringType()),
    StructField("advisory_text", StringType()),
    StructField("street_name", StringType()),
    StructField("from_street", StringType()),
    StructField("to_street", StringType()),
    StructField("date_first_mentioned", StringType()),
    StructField("date_last_mentioned", StringType()),
    StructField("n_dates_mentioned", IntegerType()),
    StructField("advisory_week_start", StringType()),
    StructField("advisory_week_end", StringType()),
    StructField("advisory_week_text", StringType()),
    StructField("source_url", StringType()),
    StructField("scraped_at_iso", StringType()),
])

# The page ships no id, so the key is built: SHA-256 over the fields that identify an
# entry plus the scrape date. Same entry on the same day gives the same key every run.
df = (spark.createDataFrame(records, schema=ADVISORY_SCHEMA)
      .withColumn("scrape_date", F.lit(scrape_date))
      .withColumn("advisory_sk", F.sha2(F.concat_ws(
          "|", F.lit(scrape_date), F.col("section_id"), F.col("location_name"),
          F.coalesce(F.col("detail_label"), F.lit("~")), F.col("advisory_text")), 256))
      .withColumns({
          "source_system": F.lit("NYC_DOT_Weekly_Traffic_Advisory"),
          "ingested_at": F.current_timestamp(),
          "batch_id": F.lit(batch_id),
      }))

# Deduplicate inside the batch first. MERGE raises if the source hands it two rows for one key.
w = Window.partitionBy("advisory_sk").orderBy(F.col("location_name").asc())
df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
df.createOrReplaceTempView("stg_traffic_advisory")

# COMMAND ----------

# MERGE rather than overwrite, because the page keeps no archive and this table is the
# only place the history will exist. Re-running the same scrape date inserts 0 rows.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME}
    AS SELECT * FROM stg_traffic_advisory WHERE 1=0
""")

spark.sql(f"""
    MERGE INTO {TABLE_NAME} AS tgt
    USING stg_traffic_advisory AS src
      ON tgt.advisory_sk = src.advisory_sk
    WHEN NOT MATCHED THEN INSERT *
""")

print(f"Saved traffic advisory data -> {TABLE_NAME}")
print(f"  Records this scrape: {df.count():,}")
print(f"  Batch ID: {batch_id}")

# COMMAND ----------

# Row count per scrape date equals distinct keys per scrape date on every run.
# That is the idempotency proof for this source.
display(spark.sql(f"""
    SELECT scrape_date,
           COUNT(*) AS rows_landed,
           COUNT(DISTINCT advisory_sk) AS distinct_keys,
           COUNT(DISTINCT location_name) AS locations
    FROM {TABLE_NAME}
    GROUP BY scrape_date
    ORDER BY scrape_date
"""))
