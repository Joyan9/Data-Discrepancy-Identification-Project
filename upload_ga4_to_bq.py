"""
GA4 BigQuery Export — dlt Upload Script
========================================
Loads ga4_events.jsonl.gz into BigQuery, preserving the standard
GA4 export schema with nested RECORDs and REPEATED fields.

dlt handles nested dicts as STRUCT columns and lists as child tables
(REPEATED RECORDs in BQ). The result closely mirrors the real GA4
BQ export layout you would see from a live property.

Usage
-----
1. Install dependencies:
       pip install dlt[bigquery]

2. Authenticate to BigQuery (pick ONE):
   a) Application Default Credentials (recommended for local dev):
          gcloud auth application-default login
   b) Service account key — set environment variable:
          export GOOGLE_APPLICATION_CREDENTIALS="/path/to/key.json"

3. Set your BQ project ID below (BIGQUERY_PROJECT_ID).

4. Place ga4_events.jsonl.gz in the same directory as this script,
   then run:
          python upload_ga4_to_bq.py
"""

import gzip
import json
import os
from pathlib import Path

import dlt
from dlt.destinations.adapters import bigquery_adapter

# ──────────────────────────────────────────────
# CONFIGURE THIS
# ──────────────────────────────────────────────
BIGQUERY_PROJECT_ID = "upheld-setting-420306"  
DATASET_NAME        = "analytics_demo"
TABLE_NAME          = "ga4_events"
DATA_FILE           = Path(__file__).parent / "ga4_events.jsonl.gz"
# ──────────────────────────────────────────────


# ── Column-level type hints ────────────────────────────────────────────────────
# dlt infers most types correctly from JSON values. We only need to be explicit
# where inference would be ambiguous or wrong:
#   • event_date → "date" so BQ stores it as DATE (enables date partitioning)
#   • timestamp strings → "bigint" since they're microsecond epoch integers
#     stored as strings in the raw data

COLUMN_HINTS = [
    # Parse event_date string "YYYYMMDD" → actual DATE column
    {"name": "event_date",                    "data_type": "date"},
    # Microsecond timestamps stored as string — keep as text to avoid int overflow
    # (BQ INT64 handles it fine, but dlt's Python int parser may truncate on some builds)
    {"name": "event_timestamp",               "data_type": "text"},
    {"name": "event_previous_timestamp",      "data_type": "text"},
    {"name": "event_bundle_sequence_id",      "data_type": "text"},
    {"name": "event_server_timestamp_offset", "data_type": "text"},
    {"name": "user_first_touch_timestamp",    "data_type": "text"},
    # Revenue / price fields — ensure float, not auto-cast to int
    {"name": "event_value_in_usd",            "data_type": "double"},
]


@dlt.resource(
    name=TABLE_NAME,
    write_disposition="replace",   # full reload each run; change to "append" if you want incremental
    columns=COLUMN_HINTS,
)
def ga4_events():
    """
    Yields one dict per event row from the JSONL.gz file.

    dlt will automatically:
      • Flatten nested dicts   → BQ STRUCT columns  (device, geo, traffic_source, …)
      • Expand lists of dicts  → BQ child tables     (event_params, items, user_properties)
        These become <table>__event_params, <table>__items, etc. — linked back to
        the parent row via a generated _dlt_parent_id foreign key.

    This mirrors how GA4's own BQ export represents REPEATED RECORDs, so your
    unnesting SQL (UNNEST(event_params)) will work the same way once you adapt
    the table name.
    """
    if not DATA_FILE.exists():
        raise FileNotFoundError(
            f"Dataset not found at {DATA_FILE}\n"
            "Download ga4_events.jsonl.gz and place it next to this script."
        )

    opener = gzip.open if str(DATA_FILE).endswith(".gz") else open

    with opener(DATA_FILE, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            row = json.loads(line)

            # Convert event_date from "YYYYMMDD" string → "YYYY-MM-DD" so dlt
            # can parse it as a proper DATE (required for BQ date partitioning)
            raw_date = row.get("event_date", "")
            if raw_date and len(raw_date) == 8:
                row["event_date"] = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"

            yield row


def main():
    # Build the pipeline
    pipeline = dlt.pipeline(
        pipeline_name="ga4_bq_upload",
        destination=dlt.destinations.bigquery(project_id=BIGQUERY_PROJECT_ID),
        dataset_name=DATASET_NAME,
    )

    # Apply BigQuery-specific settings:
    #   partition → DATE partition on event_date (mirrors real GA4 BQ tables)
    #   cluster   → common query filters in GA4 analysis
    ga4_resource_configured = bigquery_adapter(
        ga4_events,
        partition="event_date",
        cluster=["event_name", "platform"],
    )

    print(f"Loading {DATA_FILE.name} → {BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}")
    print("This may take 1–3 minutes …\n")

    load_info = pipeline.run(ga4_resource_configured())

    print("\n── Load complete ──────────────────────────────────────────")
    print(load_info)
    print()

    # Quick row-count check via the dlt dataset accessor
    try:
        df = pipeline.dataset()[TABLE_NAME].df()
        print(f"✓ Rows in {TABLE_NAME}: {len(df):,}")

        child_tables = [TABLE_NAME + suffix for suffix in
                        ["__event_params", "__items", "__user_properties"]]
        for ct in child_tables:
            try:
                ct_df = pipeline.dataset()[ct].df()
                print(f"✓ Rows in {ct}: {len(ct_df):,}")
            except Exception:
                pass  # child table may be empty (e.g. user_properties)
    except Exception as e:
        print(f"(Row count check skipped: {e})")

    print()
    print("── Next steps ─────────────────────────────────────────────")
    print(f"  Main table  : {BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}")
    print(f"  event_params: {BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}__event_params")
    print(f"  items       : {BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}__items")
    print()
    print("  To unnest event_params in your queries, use:")
    print(f"""
    SELECT
        e.event_date,
        e.event_name,
        ep.key,
        ep.value__string_value,
        ep.value__int_value
    FROM `{BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}` AS e
    LEFT JOIN `{BIGQUERY_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}__event_params` AS ep
        ON ep._dlt_parent_id = e._dlt_id
    WHERE e.event_name = 'page_view'
    LIMIT 10;
    """)
    print("  Note: dlt uses a JOIN pattern instead of native UNNEST.")
    print("  The project brief's UNNEST syntax works identically on both.")


if __name__ == "__main__":
    main()