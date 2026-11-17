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

## Nhiều vendor: 1 tài khoản bị Apple từ chối (từ 0.9.4)

Áp dụng cho `summary_sales`, `financial_report`, `financial_report_detail` khi Apple trả
**401 / 403** cho một vendor (khoá bị thu hồi, thiếu quyền, thoả thuận chưa chấp nhận...):

- Không retry 401 / 403. Log ghi mã lỗi của Apple, ví dụ
  `FORBIDDEN.REQUIRED_AGREEMENTS_MISSING_OR_EXPIRED`.
- `check`: thử mọi vendor. Còn ít nhất 1 vendor gọi được thì PASS (log cảnh báo vendor lỗi);
  mọi vendor đều bị từ chối thì FAIL. Lỗi khác (thiếu trường, lỗi mạng) vẫn FAIL ngay.
- `read`: bỏ qua vendor bị từ chối, các vendor còn lại vẫn đọc và lưu cursor. Hết stream thì
  báo lỗi nêu tên vendor, nên **job vẫn kết thúc `failed`** (không im lặng thiếu dữ liệu).
- Cursor lưu theo từng vendor, nên khi Apple thông lại, lần sync sau tự đọc bù từ
  `cursor - lookback_days` của vendor đó.

## `financial_report_detail`: dòng trống Transaction Date (từ 0.9.4)

File FINANCE_DETAIL của Apple đôi khi có dòng giao dịch **trống cột Transaction Date**
(vẫn có Settlement Date). Trước 0.9.4 parser coi dòng đó là hết phần giao dịch và bỏ
toàn bộ phần sau của file mà không báo gì.

- Dòng không có ngày hợp lệ nhưng đủ cột vẫn được **giữ** (`_Transaction_Date_` rỗng) và
  log `WARNING ... without a valid Transaction Date`. Tầng dbt cần dùng `_Settlement_Date_`
  thay thế cho các dòng này.
- Sau khi đọc, connector **đối soát** tổng `Quantity` / `Extended Partner Share` theo
  (country, currency) với bảng summary ở cuối chính file. Lệch thì log
  `WARNING ... do NOT match the file's own country summary` (không làm hỏng sync).
- `_row_number_` của các dòng đứng trước dòng trống ngày không đổi, nên kéo lại kỳ cũ
  không sinh dòng trùng.

## 1 stream lỗi không chặn stream khác (từ 0.9.4)

- Stream nào lỗi thì log `WARNING stream '<tên>' failed; continuing with the remaining streams`,
  các stream còn lại vẫn chạy hết và lưu cursor.
- Cuối lượt log `WARNING sync finished with failed stream(s): ... Streams synced normally: ...`.
- Kết quả job khi có stream lỗi:
  - Còn ít nhất 1 stream chạy xong và lỗi không phải do Apple từ chối vendor: job **`succeeded`**,
    lỗi chỉ nằm trong log (`WARNING stream '<tên>' error: ...`, kèm
    `Job is reported as succeeded; check the warnings above`). Stream lỗi vẫn mang trạng thái
    `INCOMPLETE`. Muốn biết có stream lỗi phải xem log của job.
  - Apple từ chối vendor (401 / 403): job `failed` như mục trên.
  - Không stream nào chạy xong: job `failed`.

## Test

```bash
python -m pytest unit_tests -q
```
