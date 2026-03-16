# How to Load the Dataset to BigQuery

## Dataset

- **Table name (recommended):** `ga4_dataset.ga4_events`
- **Date range:** `2024-01-01` → `2024-03-01` (61 days)
- **Rows:** ~113,000 events


### Loading Instructions (BigQuery)

1. Download `ga4_events.jsonl.gz` and `bq_schema.json` from `Data/`
2. In BigQuery Console → your project → **Create dataset**, name it `ga4_dataset` or whatever name you want
3. Inside the dataset → **Create table**
   - Source: Upload → select `ga4_events.jsonl.gz`
   - File format: **JSONL (Newline delimited JSON)**
   - Table name: `ga4_events`
   - Schema: Toggle **Edit as text** → paste the full contents of `bq_schema.json`

4. Click **Create table** — load takes ~30 seconds
