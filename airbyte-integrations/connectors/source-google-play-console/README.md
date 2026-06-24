# source-google-play-console

Airbyte **source** connector that reads Google Play Console **bulk reports** directly
from each store's `pubsite_prod_rev_*` GCS bucket and loads them to BigQuery.

> **Status: SKELETON.** Structure, config spec and schemas are in place.
> `read_records` / GCS download / CSV parsing are stubbed (`NotImplementedError`, marked `TODO`).
> No data is fetched yet — this is for structure review.

## Streams (5)

| Stream | Bucket path | Grain | `app_id` from | Encoding |
|---|---|---|---|---|
| `estimated_sales` | `sales/` | account-level, monthly, zipped | column `Product ID` | UTF-8 (BOM auto) |
| `earnings` | `earnings/` | account-level, monthly, zipped | column `Package ID` | UTF-8 (BOM auto) |
| `installs_overview` | `stats/installs/` | per-app, monthly | filename | UTF-16LE |
| `ratings` | `stats/ratings/` | per-app, monthly | filename | UTF-16LE |
| `reviews` | `reviews/` | per-app, monthly | filename | UTF-16LE |

Apps are **auto-detected** (filename for stats/reviews; package column for sales/earnings) —
no per-app config. Add a store = add one entry under `stores`.

## Output

Raw passthrough: every source column kept as-is, typed `string`; cast/transform downstream
in dbt. Every record also gets injected metadata:
`store_id`, `app_id`, `_report_month`, `_source_file`, `_row_number`, `_file_modified_at`, `_synced_at`.

## Sync

Incremental, cursor = `_file_modified_at` (GCS blob last-modified), **28-day lookback**
(covers Google's 3–7 day posting + restatements). Use **Append + Deduped** at the connection.

## Primary key — user-defined at connection setup

PK is left `None` in code; pick it in the UI. **For `earnings` / `estimated_sales` the PK MUST
include `_row_number`** (e.g. `store_id, app_id, _source_file, _row_number`). These reports are
transaction-level with many rows per app per month and no natural key — a PK without `_row_number`
will collapse a whole month into one row and silently drop transactions.

## Config

```json
{
  "service_account": "<full SA key JSON as a string>",
  "stores": [
    { "store_id": "store_a", "bucket": "pubsite_prod_rev_0123456789" }
  ],
  "start_date": null,
  "lookback_days": 28
}
```

## Dev

```bash
poetry env use python3.11
poetry install
poetry run source-google-play-console spec
poetry run source-google-play-console check    --config integration_tests/sample_config.json
poetry run source-google-play-console discover --config integration_tests/sample_config.json
```

## Build & push (target VM is linux/amd64)

```bash
docker buildx build --platform linux/amd64 \
  -t <DOCKERHUB_USER>/source-google-play-console:0.1.0 . --push
```

Then in Airbyte (abctl, v2.1.0): Settings → Sources → New connector → add the image
`<DOCKERHUB_USER>/source-google-play-console:0.1.0`.

## Layout

```
source-google-play-console/
├── pyproject.toml          # Poetry, airbyte-cdk ^7.0
├── Dockerfile              # python:3.11-slim
├── metadata.yaml
├── main.py
├── README.md
└── source_google_play_console/
    ├── source.py           # AbstractSource: check_connection / streams (wiring)
    ├── streams.py          # base + 5 stream classes (no parse logic yet)
    ├── gcs_client.py       # google-cloud-storage wrapper (auth only)
    ├── spec.yaml           # config schema
    └── schemas/            # 5 JSON schemas (raw passthrough + metadata)
```
