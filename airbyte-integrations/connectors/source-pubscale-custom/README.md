# source-pubscale-custom

Custom Airbyte source: Pubscale (GreedyGame) Immersive Ads Reporting API -> BigQuery.
Stream: `pubscale_report` (incremental, cursor `date`, multi-account).

Docs API: https://pubscale.gitbook.io/immersive-ads-sdk/reporting-api/api-key-documentation

## Nguon goc
- Khung code theo `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, 5 FIX cursor/slice).
- `requirements.txt` = pin cua applovin (Python 3.9, airbyte-cdk 0.51.1) bo pandas/numpy.

## API
- `POST https://go.greedygame.com/v1/chameleon/reporting/public/signal/report/generate`, header `X-Api-Key` (scope `ia-reporting.READ`).
- Body: `date_range{start_date,end_date}`, `dimensions[]`, `metrics[]`, `order_by`, `pagination{page,count}`.
- Response: `{"rows":[{date, app_id, app:{name}, country, ..., <metrics>}]}`; `204` = khong co data; metric = 0 bi luoc bo.
- Gioi han: date_range <= 90 ngay/request; 1 request / 2 giay / key; `ad_tag` khong dung cung `ad_requests/ad_responses/fill_rate/show_rate`.

## Connector
- Grain mac dinh: `date, app, country, placement_id, demand_source` x 9 metrics (co the doi trong spec).
- PK: `account_name + dimensions` (`app` -> `app_id`); them cot `app_name`, `account_name`.
- Slice = account x cua so `window_days` ngay (mac dinh 30, max 90); moi slice phan trang `page_size` (mac dinh 1000) theo `has_next` API tra ve (fallback: doc tiep khi trang day).
- Flatten: `app{}` -> `app_id` + `app_name`; `plcmnt{}` -> `placement_name` + `ad_unit_id` (vi `placement_id` chi la so).
- `order_by` = date + toan bo dimensions -> sort xac dinh, phan trang on dinh.
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Slice doc xong (ke ca 204) -> account do phu den end_date. Ly do: slice la cua so 30 ngay, neu acc1 xong ma acc2 fail thi state max toan cuc se lam acc2 mat ca cua so (> lookback).
- Rate limit: `_send` cho >= 2.1s giua 2 request cung key; 429 -> retry theo `Retry-After` (mac dinh 5s).
- Incremental: start = cursor - `number_days_backward` (mac dinh 7); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today.
- Metric thieu trong row -> fill 0.

## Test local (khong can Docker)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python main.py check    --config secrets/config.json                 # key that (gitignored)
python main.py read     --config secrets/config.json --catalog secrets/test_catalog.json
```

## Build (tren VM)
```bash
docker build -t dataezg/source-pubscale-custom:v1.0.0 .
docker run --rm dataezg/source-pubscale-custom:v1.0.0 spec | head -5
docker run --rm -v $(pwd)/secrets:/secrets dataezg/source-pubscale-custom:v1.0.0 check --config /secrets/config.json
docker push dataezg/source-pubscale-custom:v1.0.0
```
Airbyte: Settings -> Sources -> + New connector -> `dataezg/source-pubscale-custom`, tag `v1.0.0`.

## v1.0.0 — Changelog
- Khoi tao. Da test voi mock server: spec/discover/check (401 bao ro account), read (phan trang, 204, zero-fill, state), incremental lookback, clamp cursor tuong lai, rate limit.
- Review logic lay du lieu -> 3 fix: (A) cursor min theo account (acc2 fail cua so 2 -> state giu lai, khong mat data); (B/pagination) chuyen sang dung `has_next` API tra ve; (C) order_by day du dimensions.
- DA XAC NHAN voi API key that (quick_test.py, 2 ngay): response co `rows`, `total_rows`, `has_next`; `count=1000` khong bi cap. Cot that: date, app_id, app{}, placement_id, plcmnt{}, country, demand_source (HOA), revenue, ad_requests, ad_responses, impressions, clicks, ecpm, fill_rate, show_rate, click_rate. Metric = 0 bi luoc bo (vd row thieu `clicks`). Flatten them `placement_name`, `ad_unit_id` tu plcmnt. Grain PK unique 100%.
- Luu y: fill_rate/show_rate/click_rate la % (0-100), khong phai ty le.
