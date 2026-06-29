# source-exchange-rate-custom

Airbyte custom source connector for [Open Exchange Rates](https://openexchangerates.org) — pulls daily forex rates (base USD, ~170 currencies including VND) into BigQuery. Replaces the Frankfurter source.

## Streams

| Stream | Sync Mode | Primary Key | Cursor |
|---|---|---|---|
| `exchange_rates` | Incremental Append+Deduped | `_date_`, `_currency_` | `_date_` |

## Config

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `app_id` | string (secret) | ✅ | — | Open Exchange Rates App ID |
| `start_date` | string (YYYY-MM-DD) | ❌ | `2024-12-31` | Backfill start date |
| `lookback_days` | integer | ❌ | `3` | Days to re-fetch on each sync (overwrite via PK dedup) |

## Output schema

| Column | Description |
|---|---|
| `_base_` | Base currency (always USD) |
| `_date_` | Rate date (YYYY-MM-DD) |
| `_currency_` | Quote currency code |
| `_rate_` | Exchange rate (string, cast in dbt) |
| `_timestamp_` | Unix timestamp from API |
| `_synced_at_` | Sync timestamp (UTC) |

## Build & Deploy

```bash
# local discover test (fake credentials)
poetry run python connectors/source-exchange-rate-custom/source_exchange_rate_custom/main.py \
  discover --config connectors/source-exchange-rate-custom/secrets/test_config.json

# Docker build (trên VM)
docker build -t dataezg/source-exchange-rate-custom:0.1.0 .

# Docker discover test
docker run --rm -v $(pwd)/secrets:/secrets \
  dataezg/source-exchange-rate-custom:0.1.0 \
  discover --config /secrets/test_config.json

# check với credentials thật
docker run --rm -v $(pwd)/secrets:/secrets \
  dataezg/source-exchange-rate-custom:0.1.0 \
  check --config /secrets/config.json

# push
docker push dataezg/source-exchange-rate-custom:0.1.0
```

## Airbyte Connection Settings

- **Sync mode:** Incremental | Append+Deduped
- **Primary key:** `_date_`, `_currency_`
- **Cursor field:** `_date_`
- **Schedule:** Daily

---

*Confidential — Internal Use Only · EasyGoing Data*
