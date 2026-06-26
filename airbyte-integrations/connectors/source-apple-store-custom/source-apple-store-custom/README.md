# source-apple-store-custom

Airbyte custom source connector for Apple App Store Connect API.

## Streams

| Stream | API | Frequency | Cursor |
|---|---|---|---|
| `summary_sales` | Sales API `/v1/salesReports` | Daily | `_Begin_Date_` |
| `financial_report` | Finance API `/v1/financeReports` | Monthly | `_Start_Date_` |
| `app_installations` | Analytics API | Daily | `_date_` |
| `app_sessions` | Analytics API | Daily | `_date_` |

## Config

| Field | Required | Description |
|---|---|---|
| `key_id` | ✅ | API key ID from App Store Connect |
| `issuer_id` | ✅ | Issuer ID from App Store Connect |
| `private_key` | ✅ | .p8 file content |
| `vendors` | ✅ | Array of `{vendor_id, vendor_name}` |
| `start_date` | ✅ | YYYY-MM-DD |
| `lookback_days` | ❌ | Default: 7. Min 3 for analytics streams |
| `get_last_x_days` | ❌ | Default: false |
| `timezone` | ❌ | Default: UTC |

## Build & Deploy

```bash
# Test syntax
poetry run python -m py_compile source_apple_store_custom/*.py

# Discover (credentials giả)
poetry run source-apple-store-custom discover --config secrets/fake_config.json

# Build image
docker build -t dataezg/source-apple-store-custom:0.1.0 .

# Verify schemas in image
docker run --rm --entrypoint ls dataezg/source-apple-store-custom:0.1.0 \
  /airbyte/integration_code/source_apple_store_custom/schemas

# Discover in Docker
docker run --rm -v $(pwd)/secrets:/secrets dataezg/source-apple-store-custom:0.1.0 \
  discover --config /secrets/fake_config.json

# Check (credentials thật)
docker run --rm -v $(pwd)/secrets:/secrets dataezg/source-apple-store-custom:0.1.0 \
  check --config /secrets/config.json
```

## Notes

- Analytics streams require **Admin role** API key for first-time ONGOING request creation.
  Subsequent syncs only need Sales and Reports role.
- `app_installations` and `app_sessions` data only includes opted-in users.
  Rows with < 5 users are omitted by Apple (not a connector error).
- Financial reports follow Apple's fiscal calendar, not calendar months.
