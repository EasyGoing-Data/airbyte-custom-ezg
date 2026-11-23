# ezg-source-pangle-custom

> Thư mục: `airbyte-integrations/connectors/ezg-source-pangle-custom` (đổi tên 23/11/2026). Image Docker và connector trên Airbyte **giữ tên cũ**: `dataezg/source-pangle-custom`.

Custom Airbyte source: Pangle Reporting API 2.0 -> BigQuery (doanh thu quang cao lay truc tiep tu Pangle, de doi soat voi so uoc tinh cua AppLovin MAX — ad source `TIKTOK_BIDDING`).
Stream: `pangle_report` (incremental, cursor `date`, multi-account).

Docs API: https://www.pangleglobal.com/integration/reporting-api-v2

## Nguon goc
- Khung theo `source-pubscale-custom` v1.0.1 / `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
- Khac khung: KHONG dung `HttpStream`. Pangle xac thuc bang chu ky trong query (`user_id`, `role_id`, `sign`) nen URL chua thong tin nhay cam; loi cua `HttpStream` in ca URL ra log. Connector tu goi `requests` + retry va chi bao loi da bo URL (giong `source-mintegral-custom`).
- `requirements.txt` = pin cua pubscale (Python 3.9, airbyte-cdk 0.51.1).

## API
- `GET https://open-api.pangleglobal.com/union_pangle/open/api/rt/income` (chi du lieu ngoai Trung Quoc dai luc).
- Tham so: `user_id`, `role_id`, `version=2.0`, `sign_type=MD5`, `timestamp` (unix; lech qua 10 phut bi tu choi), `date` (**1 ngay / request**), `time_zone` (0 hoac 8; mac dinh cua API la 8), `currency` (usd hoac cny; mac dinh cua API la cny), `sign`.
- `sign` = `md5("k1=v1&k2=v2..." + security_key)`: khoa xep alphabet, bo gia tri rong. Security key + Role ID: Pangle platform > SDK Integration > Data API.
- Response: `{"Code": "100", "Message": "", "Data": {"<date>": [ ... ]}}`. Ma: `100` thanh cong, `PD0004` thanh cong nhung khong co du lieu, `101` sai chu ky, `102` sai user_id, `103` sai ngay, `106` qua 2 QPS, `114` sai tham so.
- Gioi han: 2 request / giay; 1 bao cao toi da 100.000 dong (qua thi bi cat con 100.000 dong dau).
- Du lieu UTC+0 co tu 2020/12/01. Doanh thu la so uoc tinh, can vai ngay moi on dinh. **Billing cua Pangle van tinh theo UTC+8.**
- API chi tra du lieu cua app ma role co quyen -> role phai co quyen moi app.

## Connector
- Config `accounts[]`: `account_name`, `user_id`, `role_id`, `security_key` (secret).
- Mui gio: luon gui `time_zone=0` (UTC), khong co tuy chon — khop voi bao cao MAX. `currency` mac dinh `usd` (doi duoc).
- Grain: mac dinh cua API khi khong truyen `dimensions` = ngay x ad placement x quoc gia.
- PK: `account_name, date, app_id, ad_slot_id, region`.
- Cot: `region, app_id, app_name, package_name, ad_slot_id, ad_slot_type, code_name, media_name, os, currency, time_zone, request, return, fill_rate, show, click, click_rate, revenue, ecpm, use_mediation, bidding_type, ad_request, response, ad_fill_rate, ad_impression_rate, ad_slot_type_v2, media_m_ssr, win_rate`; cot la passthrough. `app_code_type` (co trong tai lieu, API that khong tra) khong khai trong schema.
- `ad_slot_type`: 1 Native, 2 Banner, 3 App Open, 5 Rewarded Video, 6 Interstitial.
- Cac id -> chuoi; so dem -> so nguyen; `revenue`, `ecpm`, cac ti le -> round 6 so le.
- Slice = account x cua so `window_days` ngay (mac dinh 7); trong slice moi ngay 1 request. State luu sau moi slice.
- Incremental: start = cursor - `number_days_backward` (mac dinh **14**); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today (UTC).
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Account nao loi giua chung thi state dung o cua so cuoi account do doc xong.
- Ky lai (`timestamp` + `sign`) o moi lan thu. Retry toi da 5 lan (5s, 10s, 20s, 40s) cho loi mang, HTTP 429 / 5xx, Code 106; ma loi khac dung ngay.
- Rate limit: >= 0.6 giay giua 2 request cung user_id.
- Chot chan nap sai (sync DUNG va bao ro): response thieu cot khoa; 2 dong trung khoa chinh trong cung 1 ngay; bao cao cham 100.000 dong (co the da bi cat).
- `security_key` khong bao gio duoc gui di; `sign` / URL khong in ra log / thong bao loi; khong co gi nhay cam trong stream_slice.

## Test local (khong can Docker, khong can key that)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python integration_tests/mock_test.py        # server gia: chu ky, 1 ngay / request, lookback, retry 106 + 500, chot chan
python main.py check    --config secrets/config.json                 # key that (gitignored)
python main.py read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```
`PANGLE_URL`, `PANGLE_RETRY_BASE_SEC` chi dung cho test voi server gia; khong dat cac bien nay tren Airbyte.

## Build (tren VM: /home/data/airbyte-custom-ezg, nhanh `agent/source-pangle-custom`)
```bash
docker build -t dataezg/source-pangle-custom:v1.0.1 .
docker run --rm dataezg/source-pangle-custom:v1.0.1 spec | head -c 300
docker push dataezg/source-pangle-custom:v1.0.1
```
Airbyte: Settings -> Sources -> + New connector -> image `dataezg/source-pangle-custom`, tag `v1.0.1`.
Connection: stream `pangle_report`, Incremental | Append + Deduped -> `airbyte_data_export.pangle_report`.

## v1.0.1 — Changelog
- Schema khop du lieu that: them `ad_slot_type_v2`, `media_m_ssr`, `win_rate` (API tra nhung v1.0.0 chua khai nen khong thanh cot o BigQuery); bo `app_code_type` khoi schema (API khong tra -> cot toan null). Khong doi PK. Test voi server gia: 25/25 dat.
- DA xac nhan voi key that (16/11/2026, read 09/11 -> 16/11, 1 account, `user_id` = `role_id`): `check` dat; 3.328 dong; grain mac dinh dung la ngay x app x ad slot x region; PK khong trung, khong null; `currency` = usd, `time_zone` = 0.
- Doi soat voi AppLovin MAX (ad network `TIKTOK_BIDDING`) 09/11 -> 15/11: MAX 87,95 USD, Pangle 87,97 USD (lech 0,0%).
- Luu y du lieu that: `region` viet thuong (`jp`) -> viet hoa o dbt stage.

## v1.0.0 — Changelog
- Khoi tao. Test voi server gia (Python 3.12, airbyte-cdk 0.51.1): spec / discover / check (key sai bao ro account + Code 101) / chu ky md5 / time_zone=0 + currency=usd / 1 ngay 1 request / read full / incremental lookback 14 / clamp cursor tuong lai / retry Code 106 + HTTP 500 / ngay rong PD0004 / account loi giua chung -> state khong vuot / 3 chot chan / khong lo security_key, sign — 24/24 dat.
- CHUA xac nhan voi key that. Can kiem khi co key: (1) chu ky khop voi Pangle that (hien khop code mau trong tai lieu), (2) grain mac dinh co dung la ngay x ad_slot_id x region khong (sai thi connector dung va bao trung khoa), (3) kieu du lieu cua `app_id` / `ad_slot_id`, (4) role co du quyen moi app, (5) PK unique tren du lieu that.
