# source-unity-ads-custom

Custom Airbyte source: Unity Ads Monetization Stats API -> BigQuery (doanh thu quang cao lay truc tiep tu Unity, de doi soat voi so uoc tinh cua AppLovin MAX).
Stream: `unity_ads_monetization` (incremental, cursor `date`, multi-account).

Docs API: https://docs.unity.com/en-us/monetization/performance-and-analytics/api/monetization-stats-api

## Nguon goc
- Khung code theo `source-pubscale-custom` v1.0.1 / `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
- `requirements.txt` = pin cua pubscale (Python 3.9, airbyte-cdk 0.51.1).

## API
- `GET https://monetization.api.unity.com/stats/v1/operate/organizations/<organization core id>`, header `Authorization: Token <api key>`.
- API key: Unity Monetization dashboard > Settings > API management > Monetization Stats API Access. Key theo tung user nhung dung duoc cho ca organization.
- Tham so: `fields` (bat buoc), `groupBy`, `scale`, `start`, `end` (ISO 8601). Connector luon gui `scale=day`, `start=<ngay>T00:00:00Z`, `end=<ngay>T23:59:59Z`.
- Response: CSV (mac dinh, connector gui `Accept: text/csv`) hoac JSON. Ngay cat tai 00:00 UTC, API khong co tham so timezone.
- Gioi han: server timeout 60 giay / request (408); co ma 429 nhung tai lieu khong ghi nguong.
- Ma loi: 400 thieu tham so, 401 key sai, 404 khong thay organization, 408 timeout, 429 qua gioi han, 500 / 503.

## Connector
- Config `accounts[]`: `account_name`, `organization_core_id`, `api_key` (secret).
- Grain mac dinh: `date` x `country, placement, platform, game` (doi duoc qua `group_by`).
- Metric mac dinh: `adrequest_count, available_sum, revenue_sum, start_count, view_count` (doi duoc qua `fields`).
- PK: `account_name, date` + cot theo `group_by` (`game` -> `source_game_id`). Cot them: `organization_id`, `source_name`.
- Mui gio: co dinh UTC (khong co tuy chon), khop voi bao cao MAX.
- Slice = account x cua so `window_days` ngay (mac dinh 7, max 31). Request timeout (408) thi giam `window_days`.
- Incremental: start = cursor - `number_days_backward` (mac dinh **14**); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today (UTC).
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Slice doc xong (ke ca rong) -> account do phu den end_date.
- Rate limit: >= 1.1 giay giua 2 request cung key; 429 -> retry theo `Retry-After` (mac dinh 10s); 408 -> retry.
- Parser: doc CSV hoac JSON; ten cot chuan hoa (chu thuong, `_`) + alias (`game id` -> `source_game_id`, ...); cot la passthrough.
- Response thieu cot khoa (ten cot khac gia dinh) -> sync DUNG va bao ten cot nhan duoc, khong nap record nao (tranh PK null lam dedup gop nham dong).
- Response co 2 dong trung khoa chinh -> sync DUNG (grain gia dinh sai, dedup se lam mat doanh thu).
- Row co ngay ngoai cua so da xin bi loc bo (tai lieu khong ghi `end` co bao gom hay khong).
- Metric rong -> 0; `*_count` -> so nguyen, `*_sum` -> round 6 so le (BigQuery NUMERIC scale 9).
- Key khong nam trong stream_slice, khong nam trong URL, khong in ra log / thong bao loi.

## Test local (khong can Docker, khong can key that)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python integration_tests/mock_test.py        # server gia: check, read full / incremental, 429, slice rong, clamp cursor
python main.py check    --config secrets/config.json                 # key that (gitignored)
python main.py read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```
`UNITY_ADS_URL_BASE` chi dung cho test voi server gia; khong dat bien nay tren Airbyte.

## Build (tren VM: /home/data/airbyte-custom-ezg, nhanh `agent/source-unity-ads-custom`)
```bash
docker build -t dataezg/source-unity-ads-custom:v1.0.0 .
docker run --rm dataezg/source-unity-ads-custom:v1.0.0 spec | head -c 300
docker push dataezg/source-unity-ads-custom:v1.0.0
```
Airbyte: Settings -> Sources -> + New connector -> image `dataezg/source-unity-ads-custom`, tag `v1.0.0`.
Connection: stream `unity_ads_monetization`, Incremental | Append + Deduped -> `airbyte_data_export.unity_ads_monetization`.

## v1.0.0 — Changelog
- Khoi tao. Test voi server gia (Python 3.12, airbyte-cdk 0.51.1): spec / discover / check (key sai bao ro account, khong lo key) / read full / incremental lookback 14 / clamp cursor tuong lai / retry 429 / slice rong / loc row ngoai cua so / thieu cot khoa va trung khoa thi dung — 26/26 dat.
- CHUA xac nhan voi key that. Can kiem khi co key: (1) ten cot that cua response (tai lieu khong co mau; hien gia dinh `timestamp, country, placement, platform, source_game_id, source_name`), (2) `end` bao gom hay khong, (3) don vi tien cua `revenue_sum`, (4) nguong 429 thuc te, (5) PK unique tren du lieu that.
