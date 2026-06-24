# source-exchange-rate (Frankfurter)

Airbyte custom source kéo tỷ giá hối đoái từ Frankfurter API (ECB reference rates, không cần API key).

- Stream `exchange_rates` — output LONG: `_date_, _base_, _currency_, _rate_, _amount_, _extracted_at_, _synced_at_`.
- Config: `base` (mặc định USD), `start_date` (mặc định 2024-01-01), `symbols` (optional, lọc đồng tiền).
- Incremental theo `_date_`, lookback 7 ngày.

PK gợi ý (Airbyte connection): `_date_, _base_, _currency_`.
