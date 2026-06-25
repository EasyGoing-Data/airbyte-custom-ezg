# source-google-analytics

Airbyte custom source connector kéo data từ **Google Analytics 4 Data API** về BigQuery.
Dynamic schema — mỗi report trong config = 1 stream = 1 bảng BQ.

## Config mẫu

```json
{
  "credentials": {
    "auth_type": "service",
    "credentials_json": "<SERVICE_ACCOUNT_JSON_STRING>"
  },
  "list_properties_name_and_id_as_dict": [
    { "property_name": "com.example.game", "property_id": "123456789" }
  ],
  "daily_reports": [
    {
      "report_name": "daily_revenue",
      "dimensions": ["country", "operatingSystem"],
      "metrics": ["totalRevenue", "purchaseRevenue", "adRevenue", "transactions"],
      "start_date": "2024-01-01"
    },
    {
      "report_name": "daily_installs",
      "dimensions": ["country", "firstUserSource", "firstUserMedium"],
      "metrics": ["newUsers"],
      "start_date": "2024-01-01"
    },
    {
      "report_name": "daily_engagement",
      "dimensions": ["country", "eventName", "operatingSystem"],
      "metrics": ["eventCount", "activeUsers"],
      "start_date": "2024-01-01"
    }
  ],
  "number_days_backward": 7,
  "get_last_x_days": false
}
```

Dimensions & Metrics tra cứu tại: https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/

## Columns BQ (sau normalize _Name_)

| Cột | Ý nghĩa |
|---|---|
| `_property_id_` | GA4 Property ID |
| `_property_name_` | Tên app/property từ config |
| `_synced_at_` | Thời điểm Airbyte kéo về |
| `_date_` | Ngày data (YYYYMMDD) — cursor field |
| `_<dimension>_` | Dimension từ config (đã normalize) |
| `_<metric>_` | Metric từ config (đã normalize) |

## Test checklist §8

```bash
# 1. Syntax
poetry run python -m py_compile source_google_analytics/*.py

# 2. Spec
poetry run source-google-analytics spec

# 3. Discover (fake config — phải ra đủ stream theo daily_reports)
poetry run source-google-analytics discover --config secrets/fake_config.json

# 4. Check + Read (config thật)
poetry run source-google-analytics check --config secrets/config.json
poetry run source-google-analytics read --config secrets/config.json --catalog secrets/catalog.json
```

## Deploy

```bash
docker build -t dataezg/source-google-analytics:0.1.0 .
docker push dataezg/source-google-analytics:0.1.0
```

## Airbyte UI

- Sync mode: **Incremental | Append+Deduped**
- Cursor field: `_date_`
- Primary key: `_property_id_` + tất cả `_<dimension>_` columns
