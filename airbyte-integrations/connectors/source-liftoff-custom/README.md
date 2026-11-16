# source-liftoff-custom

Custom Airbyte source: Liftoff Monetize (Vungle) Publisher Reporting API 2.0 -> BigQuery (doanh thu quang cao lay truc tiep tu Liftoff / Vungle, de doi soat voi so uoc tinh cua AppLovin MAX — ad source `VUNGLE_BIDDING`).
Stream: `liftoff_report` (incremental, cursor `date`, multi-account).

Docs API: https://support.vungle.com/hc/en-us/articles/211365828-Publisher-Reporting-API-2-0

## Nguon goc
- Khung code theo `source-pubscale-custom` v1.0.1 / `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
- `requirements.txt` = pin cua pubscale (Python 3.9, airbyte-cdk 0.51.1).

## API
- `GET https://report.api.vungle.com/ext/pub/reports/performance`.
- Header: `Authorization: Bearer <api key>`, `Vungle-Version: 1`, `Accept: application/json`.
- API key: trang Reports tren dashboard Liftoff (1 key cho moi app cua tai khoan); chua co thi xin tech-support@vungle.com.
- Tham so: `start`, `end` (YYYY-MM-DD, bao gom ca 2 dau), `dimensions`, `aggregates`.
- Response: mang JSON; ten cot co dau cach (`application id`, `placement name`). Khong co du lieu -> HTTP 200 body rong.
- Chi co UTC. Lich su toi da 6 thang (cu hon phai xin Liftoff). Tai lieu khong ghi rate limit.

## Connector
- Config `accounts[]`: `account_name`, `api_key` (secret).
- Grain mac dinh: `date, application, placement, country, platform, incentivized, adType` (doi duoc qua `dimensions`; them duoc `adSize`). `incentivized` + `adType` cho tang "ad type": co thuong / khong thuong va video / mrec / banner.
- Metric mac dinh: `impressions, views, completes, clicks, revenue, ecpm` (doi duoc qua `aggregates`).
- Ten cot chuan hoa ve snake_case: `application_id, application_name, placement_id, placement_name, placement_reference_id, ad_type, ad_size`.
- PK: `account_name, date` + cot khoa cua tung dimension (`application` -> `application_id`, `placement` -> `placement_id`).
- `placement_id` co the null voi app dung SDK <= 4.1 (Liftoff khong tach placement) — giu null.
- Mui gio: UTC (API khong co lua chon khac), khop voi bao cao MAX.
- Slice = account x cua so `window_days` ngay (mac dinh 7, max 31).
- Start som hon 180 ngay -> tu clamp ve hom nay - 180 va ghi warning.
- Incremental: start = cursor - `number_days_backward` (mac dinh **14**); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today (UTC).
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Slice doc xong (ke ca rong) -> account do phu den end_date.
- Rate limit: >= 1.1 giay giua 2 request cung key; 429 -> retry theo `Retry-After` (mac dinh 10s); 408 -> retry.
- Chot chan nap sai: response thieu cot khoa, hoac co 2 dong trung khoa chinh trong cung 1 response -> sync DUNG va bao ro (tranh dedup gop nham / mat doanh thu).
- Row co ngay ngoai cua so da xin bi loc bo. Metric rong -> 0; so dem -> so nguyen; `revenue`, `ecpm` -> round 6 so le.
- Key khong nam trong stream_slice, khong nam trong URL, khong in ra log / thong bao loi.

## Test local (khong can Docker, khong can key that)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python integration_tests/mock_test.py        # server gia: check, read full / incremental, 429, slice rong, clamp, chot chan
python main.py check    --config secrets/config.json                 # key that (gitignored)
python main.py read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```
`LIFTOFF_URL_BASE` chi dung cho test voi server gia; khong dat bien nay tren Airbyte.

## Build (tren VM: /home/data/airbyte-custom-ezg, nhanh `agent/source-liftoff-custom`)
```bash
docker build -t dataezg/source-liftoff-custom:v1.0.2 .
docker run --rm dataezg/source-liftoff-custom:v1.0.2 spec | head -c 300
docker push dataezg/source-liftoff-custom:v1.0.2
```
Airbyte: Settings -> Sources -> + New connector -> image `dataezg/source-liftoff-custom`, tag `v1.0.2`.
Connection: stream `liftoff_report`, Incremental | Append + Deduped -> `airbyte_data_export.liftoff_report`.

## v1.0.2 — Changelog
- Bat mac dinh 2 dimension `incentivized` va `adType` de bang du 7 tang tach du lieu: account, app, app_id, platform, country, date, ad type. PK them `incentivized`, `ad_type`. Source da tao tu v1.0.1 voi `dimensions` nhap tay thi phai them 2 gia tri nay trong cau hinh. Test voi server gia: 28/28 dat.
- DA thu key that voi 2 dimension moi (16/11/2026, read 09/11 -> 16/11): 1.331 dong; `ad_type` toan `video`; `incentivized` = `true` o placement Reward, `false` o placement Inter; khong null; PK khong trung; so dong khong tang so voi grain cu (moi placement chi co 1 loai); tong doanh thu 09/11 -> 15/11 van 130,16 USD (khop MAX). Tang ad type o dbt: incentivized true -> rewarded, false -> interstitial.

## v1.0.1 — Changelog
- Them cot `placement_reference_id` vao schema (API that tra cot nay kem dimension `placement`; v1.0.0 chi passthrough, khong khai trong schema nen khong thanh cot o BigQuery). Khong doi PK. Test voi server gia: 27/27 dat.
- DA xac nhan voi key that (16/11/2026, read 09/11 -> 16/11, 1 account): `check` dat; 1.319 dong; ten cot that khop gia dinh (`application_id, application_name, placement_id, placement_name, country, platform` + 6 metric); PK khong trung, khong null.
- Luu y du lieu that: `platform` viet khong dong nhat (`iOS`, `android`) -> chuan hoa o dbt stage. Chua xac nhan: don vi tien cua `revenue`, hanh vi khi `start` cu hon 6 thang, rate limit.

## v1.0.0 — Changelog
- Khoi tao. Test voi server gia (Python 3.12, airbyte-cdk 0.51.1): spec / discover / check (key sai bao ro account, khong lo key) / read full / incremental lookback 14 / clamp 180 ngay / clamp cursor tuong lai / retry 429 / slice rong / loc row ngoai cua so / thieu cot khoa va trung khoa thi dung — 25/25 dat.
- CHUA xac nhan voi key that. Can kiem khi co key: (1) ten cot that cua response JSON (tai lieu chi co vi du rut gon), (2) don vi tien cua `revenue`, (3) API xu ly the nao khi `start` cu hon 6 thang, (4) rate limit thuc te, (5) PK unique tren du lieu that.
