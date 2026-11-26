# ezg-source-meta-audience-network-custom

> Thư mục: `airbyte-integrations/connectors/ezg-source-meta-audience-network-custom` (đổi tên 23/11/2026). Image Docker và connector trên Airbyte **giữ tên cũ**: `dataezg/source-meta-audience-network-custom`.

Custom Airbyte source: Meta Audience Network Reporting API v2 -> BigQuery (doanh thu quang cao lay truc tiep tu Meta Audience Network, de doi soat voi so uoc tinh cua AppLovin MAX — ad source `FACEBOOK_NETWORK`).
Streams (incremental, cursor `date`, multi-account):
- `meta_audience_network_by_platform` — ngay x platform. Thay 100% doanh thu -> so chuan de doi chieu.
- `meta_audience_network_by_app` — ngay x property (app) x platform. Thay khoang 92% doanh thu.
- `meta_audience_network_by_app_country` — ngay x property x platform x country. Thay khoang 44% doanh thu; impression / request du 100%.
3 stream CO DINH tu v1.1.0 (khong con tuy chon `breakdowns`); stream nao khong can thi bo tick khi tao connection.

Docs API: https://developers.facebook.com/docs/audience-network/optimization/report-api/guide-v2

## Nguon goc
- Khung theo `source-pubscale-custom` v1.0.1 / `source-applovin-max-custom` v1.8.0 (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
- Khac khung: KHONG dung `HttpStream`. Response dang dai (moi dong 1 metric) va 1 o co the nam o nhieu trang -> moi slice doc het vao bo nho, gop thanh 1 record / o roi moi phat. Connector tu goi `requests` + retry (giong mintegral / pangle).
- `requirements.txt` = pin cua pubscale (Python 3.9, airbyte-cdk 0.51.1).

## API
- `GET https://graph.facebook.com/<version>/<business id>/adnetworkanalytics` (`<business id>` cung co the la property ID / app ID).
- Token: **long-lived USER access token** lay qua Facebook Login, quyen `read_audience_network_insights`, app Meta da qua App Review. **Token system user va token tao trong Monetization Manager / Business Manager bi API tu choi.**
- Tham so: `metrics` (bat buoc), `breakdowns`, `since`, `until` (khong bao gom voi aggregation theo ngay), `aggregation_period`, `limit` (toi da 2000), `after`.
- Response: `data[].results[]` = `{time, metric, breakdowns[{key, value}], value}`; `data[].omitted_results[]` = cac o KHONG duoc tra gia tri vi qua it impression; `paging.cursors.after`.
- **Ngay theo gio Pacific (PDT / PST)**, API khong co tham so timezone. So theo gio chi giu 72 gio -> khong quy ve UTC duoc.
- Gioi han: toi da 8 ngay / request dong bo; du lieu giu 540 ngay; 250 query / phut. So ngay co the dieu chinh nhe sau khi chot.

## Connector
- Config `accounts[]`: `account_name`, `business_id`, `access_token` (secret).
- Token gui bang header `Authorization: Bearer` (khong nam trong URL); KHONG di theo `paging.next` (URL do chua access_token) ma phan trang bang cursor `after`.
- Mui gio: **Pacific, khong doi duoc** (ngoai le so voi cac connector khac deu UTC). Moi record co `report_timezone` = `America/Los_Angeles` va `period_start_utc` (moc bat dau ngay theo UTC) de doi chieu.
- Ten cot metric bo tien to `fb_ad_network_`: `request, filled_request, imp, click, revenue` (+ `cpm, ctr, fill_rate, show_rate, bidding_*` neu chon).
- `revenue_omitted` = true khi Meta an doanh thu cua o do (`revenue` = null). Tach cang nhieu chieu thi cang nhieu o bi an -> **tong `revenue` cua 2 stream chi tiet THAP hon thuc te; so day du lay tu `meta_audience_network_by_platform`.**
- Nguong do tren du lieu that (26/11/2026, Meta khong cong bo): o co >= 100 impression / ngay thi co doanh thu, duoi 100 thi bi an; so dem (imp, request, click) khong bao gio bi an.
- Phan doanh thu bi an = so `by_platform` - tong phan thay duoc; phan bo (vi du theo ty le impression) lam o dbt, KHONG lam trong connector.
- `property` = ID app tren Meta (so), khong kem ten -> can bang doi chieu sang ma game o dbt.
- PK: `by_platform` = `account_name, date, platform`; `by_app` = `account_name, date, property, platform`; `by_app_country` = `account_name, date, property, platform, country`.
- Slice = account x cua so `window_days` ngay (mac dinh 7 = toi da).
- Start som hon 539 ngay -> tu clamp va ghi warning.
- Incremental: start = cursor - `number_days_backward` (mac dinh **14**); `get_last_X_days` bo qua start_date; cursor khong +1, clamp <= today (Pacific). Moi stream co state rieng.
- Cursor theo TUNG account; state emit = MIN(max_date cac account). Account nao loi giua chung thi state dung o cua so cuoi account do doc xong.
- Retry toi da 5 lan (5s, 10s, 20s, 40s) cho loi mang, HTTP 429 / 5xx, error code tam thoi cua Graph (1, 2, 4, 17, 341, 613); loi khac dung ngay.
- Loi token: code 190 -> bao "token het han / khong hop le"; thong bao co "Facebook Login" -> bao "API khong nhan token system user".
- Chot chan nap sai (sync DUNG va bao ro): trung o (time, breakdowns, metric); trung khoa chinh.
- Rate limit: >= 0.3 giay giua 2 request cung business.
- `graph_api_version` mac dinh `v27.0`, doi duoc khi Meta ngung ban cu.

## Test local (khong can Docker, khong can token that)
```bash
python main.py spec
python main.py discover --config secrets/test_config.json
python integration_tests/mock_test.py        # server gia: gop dong dai, phan trang, Pacific, omitted, lookback, loi token, chot chan
python main.py check    --config secrets/config.json                 # token that (gitignored)
python main.py read     --config secrets/config.json --catalog integration_tests/configured_catalog.json
```
`META_AN_URL_BASE`, `META_AN_RETRY_BASE_SEC` chi dung cho test voi server gia; khong dat cac bien nay tren Airbyte.

## Build (tren VM: /home/data/airbyte-custom-ezg, nhanh chinh, thu muc `airbyte-integrations/connectors/ezg-source-meta-audience-network-custom`)
```bash
docker build -t dataezg/source-meta-audience-network-custom:v1.1.0 .
docker run --rm dataezg/source-meta-audience-network-custom:v1.1.0 spec | head -c 300
docker push dataezg/source-meta-audience-network-custom:v1.1.0
```
Airbyte: Settings -> Sources -> + New connector -> image `dataezg/source-meta-audience-network-custom`, tag `v1.1.0`.
Connection: 3 stream, Incremental | Append + Deduped -> `airbyte_data_export.meta_audience_network_by_platform`, `..._by_app`, `..._by_app_country`.
Luu y van hanh: long-lived user token het han sau khoang 60 ngay neu khong co request. Tai lieu Meta ghi token duoc gia han moi ngay khi co goi Reporting API, nhung co phan anh nguoc -> theo doi; khi sync bao code 190 thi tao token moi va cap nhat source.

## v1.1.0 — Changelog
- DOI STREAM (khong tuong thich nguoc; chua co source nao tren Airbyte dung v1.0.0): bo `meta_audience_network_report` + `meta_audience_network_total`, thay bang 3 stream co dinh `by_platform`, `by_app`, `by_app_country`; bo tuy chon `breakdowns` (config cu con truong nay thi bo qua, ghi warning). Ly do: mart doanh thu can ngay x game x quoc gia x platform, va Meta an doanh thu o o nho nen phai keo nhieu muc de biet phan bi an.
- Test voi server gia: 40/40 dat.
- DA xac nhan voi token that (26/11/2026, 09/11 -> 25/11 Pacific, 1 account): `check` dat (long-lived user token dung loai); 3 stream khong trung khoa, khong null khoa; `by_platform` 34 dong, 0 dong bi an, 1.622,51 USD; `by_app` 189 dong (12 app), thay 92,1%; `by_app_country` 4.723 dong, thay 44,1%; impression 60.818 khop giua 3 stream. `platform` chi co `android`, `ios`.
- Thu them (khong thanh stream): country 68%, country x platform 65%, placement x country x platform 28% doanh thu thay duoc.
- Chua doi soat chinh xac voi AppLovin MAX (`FACEBOOK_NETWORK`): ngay Meta lech 8 gio so voi ngay UTC cua MAX; so tho tuan 09-15/11: MAX 734,42, Meta 756,69 (+3,0%). Chua build image.

## v1.0.0 — Changelog
- Khoi tao. Test voi server gia (Python 3.12, airbyte-cdk 0.51.1): spec / discover 2 stream / check (token system user va token het han bao ro, khong lo token) / gop dong dai ke ca o nam o 2 trang / ngay Pacific / revenue bi an / stream tong / incremental lookback 14 / clamp 540 ngay / clamp cursor tuong lai / retry loi tam thoi / account loi -> state khong vuot / trung o thi dung / token khong nam trong URL — 35/35 dat.
- CHUA xac nhan voi token that. Can kiem khi co token: (1) token dang co co dung loai API nhan khong (`check` se bao), (2) gia tri breakdown `placement` / `platform` that, (3) API co tra dong cho metric = 0 khong (hien: thieu dong -> so dem = 0, revenue = null), (4) `omitted_results` phan trang the nao, (5) chenh lech giua tong stream chi tiet va stream tong.
