# source-mintegral-custom

Custom Airbyte source: Mintegral Reporting API (publisher) -> BigQuery (doanh thu quang cao lay truc tiep tu Mintegral, de doi soat voi so uoc tinh cua AppLovin MAX).
Stream: `mintegral_report` (incremental, cursor `date`, multi-account).

Docs API: https://cdn-adn-https.rayjump.com/cdn-adn/reporting_api/MintegralRA.html?v=3.0

## Nguon goc
- Khung theo `source-pubscale-custom` v1.0.1 / `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
- Khac khung: KHONG dung `HttpStream`. Mintegral xac thuc bang query param (`skey`, `sign`) nen URL chua thong tin nhay cam; loi cua `HttpStream` in ca URL ra log. Connector tu goi `requests` + retry va chi bao loi da bo URL.
- `requirements.txt` = pin cua pubscale (Python 3.9, airbyte-cdk 0.51.1).

## API
- `GET https://api.mintegral.com/reporting/v2/data`.
- Xac thuc: `skey`, `time` (unix, hop le +-300 giay), `sign` = `md5(SECRET + md5(time))`. Skey + Secret: dashboard Mintegral > Account > API Tools > Report API.
- Tham so: `start`, `end` (YYYYMMDD), `timezone` (mac dinh cua API la 8), `group_by`, `page`, `limit` (toi da 10000).
- Response: `{"code": "ok", "data": {"total", "page", "total_page", "lists": [...]}}`; `code` khac `ok` la loi.
- Gioi han: **toi da 7 ngay / request; chi lay duoc du lieu 60 ngay gan nhat.** Tai lieu khong ghi rate limit.

## Connector
- Config `accounts[]`: `account_name`, `skey` (secret), `secret` (secret).
- Mui gio: luon gui `timezone=0` (UTC), khong co tuy chon — khop voi bao cao MAX.
- Grain mac dinh: `date, app_id, placement_id, unit_id, country, platform` (doi duoc qua `group_by`; them duoc `bidding_type`).
- Cot dimension kem theo: `app_name, app_package, placement_name, unit_name, ad_format`.
- Metric: `filled, request, impression, click, est_revenue, ecpm, fill_rate, ctr` (+ `hb_load, hb_load_filled` khi co `bidding_type`).
- PK: `account_name` + `group_by`.
- `date` API tra dang so `20190506` -> doi thanh `2019-05-06`. Cac id -> chuoi. Metric float round 6 so le.
- Slice = account x cua so `window_days` ngay (mac dinh 7 = toi da cua API); moi slice phan trang theo `total_page`.
- Start som hon 60 ngay -> tu clamp ve hom nay - 60 va ghi warning (du lieu cu hon KHONG lay duoc qua API).
- Incremental: start = cursor - `number_days_backward` (mac dinh **14**); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today (UTC).
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Account nao loi giua chung thi state dung o cua so cuoi account do doc xong.
- Ky lai (`time` + `sign`) o moi lan thu. Retry toi da 5 lan (5s, 10s, 20s, 40s) cho loi mang, HTTP 429, HTTP 5xx; loi khac dung ngay.
- Rate limit: >= 1.1 giay giua 2 request cung skey.
- `skey`, `secret`, `sign` khong nam trong stream_slice, khong in ra log / thong bao loi.

## Test local (khong can Docker, khong can key that)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python integration_tests/mock_test.py        # server gia: chu ky, phan trang, 60 ngay, lookback, retry 500, account loi
python main.py check    --config secrets/config.json                 # key that (gitignored)
python main.py read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```
`MINTEGRAL_URL`, `MINTEGRAL_RETRY_BASE_SEC` chi dung cho test voi server gia; khong dat cac bien nay tren Airbyte.

## Build (tren VM: /home/data/airbyte-custom-ezg, nhanh `agent/source-mintegral-custom`)
```bash
docker build -t dataezg/source-mintegral-custom:v1.0.0 .
docker run --rm dataezg/source-mintegral-custom:v1.0.0 spec | head -c 300
docker push dataezg/source-mintegral-custom:v1.0.0
```
Airbyte: Settings -> Sources -> + New connector -> image `dataezg/source-mintegral-custom`, tag `v1.0.0`.
Connection: stream `mintegral_report`, Incremental | Append + Deduped -> `airbyte_data_export.mintegral_report`.
Luu y: API chi giu 60 ngay -> bat connection cang som cang giu duoc nhieu lich su; neu connection dung qua 46 ngay thi mat du lieu (lookback 14).

## v1.0.0 — Changelog
- Khoi tao. Test voi server gia (Python 3.12, airbyte-cdk 0.51.1): spec / discover / check (secret sai bao ro account) / chu ky md5 khop vi du tai lieu / read full co phan trang / clamp 60 ngay / incremental lookback 14 / clamp cursor tuong lai / retry HTTP 500 / account loi giua chung -> state khong vuot / khong lo skey, secret, sign — 28/28 dat.
- CHUA xac nhan voi key that. Can kiem khi co key: (1) ma `code` API tra khi khong co du lieu (hien coi `ok` + `lists` rong la khong co du lieu), (2) don vi tien cua `est_revenue`, (3) gioi han 60 ngay tinh chinh xac the nao (hien clamp hom nay - 60), (4) rate limit thuc te, (5) PK unique tren du lieu that.
