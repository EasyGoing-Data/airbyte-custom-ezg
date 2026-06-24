# Airbyte Custom Connector Playbook — EasyGoing Data

> **Mục đích:** Tài liệu chuẩn (context primer) để build **Airbyte custom source connector** bằng Python CDK.
> Dán toàn bộ file này vào bất kỳ AI assistant nào (Claude, GPT, Gemini...) để nó hiểu ngay **bối cảnh dự án, cấu trúc connector, quy ước, các bẫy đã gặp** → hỗ trợ build source mới theo đúng chuẩn, không phải dò lại từ đầu.
>
> Phiên bản: 2.0 · Rút ra từ quá trình build `source-google-play-console` (6 streams) chạy thành công vào BigQuery.

---

## 0. Bối cảnh dự án (đọc trước khi làm)

- **Vai trò người dùng:** Data Analyst / Data Engineer tại công ty game mobile.
- **GCP project:** `easygoing-data` (hyphenated).
- **Stack:** Airbyte (self-hosted qua `abctl`/`kind` trên GCP VM) → BigQuery → dbt → Metabase. Orchestration sắp tới: Mage AI.
- **Hạ tầng:** region `asia-southeast1`. VM build chạy Docker; Airbyte chạy trong container `airbyte-abctl-control-plane` (port 8000).
- **DockerHub user:** `dataezg`. Image đặt tên `dataezg/source-<name>`.
- **Git repo:** monorepo fork Airbyte tại `github.com/EasyGoing-Data/airbyte-custom-ezg.git`, connector nằm trong `airbyte-integrations/connectors/source-<name>/`. Branch `master`, push bằng `git push origin HEAD:master`.
- **destination-bigquery 3.x** dùng cơ chế **direct-load** (tạo cột THẬT trên BQ, không nhét vào blob JSON) → mọi tên cột phải hợp lệ BQ (xem §3.3 + §5).

### Nguyên tắc làm việc (luôn tuân thủ)
1. **Xác thực đầu vào:** confirm thông tin/thiết kế (và data thật của nguồn) trước khi build.
2. **Xác thực đầu ra:** gửi raw data / đáp số tổng quan để duyệt trước khi xuất file; sau sync luôn đối soát số dòng BQ vs file gốc.
3. **Chuẩn hóa tên task:** `DA | [Nội dung]` (vd `DA | Airbyte | Build source-xxx`).
4. **Phong cách:** ngắn gọn, có cấu trúc; fix tối giản đúng root cause, tránh over-engineering; Tiếng Việt.

---

## 1. Triết lý connector

- **Raw passthrough:** chỉ kéo dữ liệu thô, giữ nguyên cột nguồn, mọi cột kiểu `string`. Cast/transform/quy đổi tiền → để **dbt** làm.
- **Schema linh hoạt:** `additionalProperties: true` để cột mới nguồn thêm không vỡ pipeline.
- **Metadata chuẩn:** connector chèn thêm cột định danh + truy vết (§4).
- **Incremental theo cursor:** đọc gia tăng dựa trên 1 trường thời gian (file modified / extract time).
- **Mọi tên cột chuẩn hóa về `_Name_`** để an toàn BigQuery (§3.3 — quy ước cốt lõi).

---

## 2. Cấu trúc thư mục chuẩn

```
source-<name>/
├── Dockerfile
├── metadata.yaml
├── pyproject.toml
├── README.md
├── .gitignore                 # ⚠️ KHÔNG để "*.json" (§7)
├── main.py
├── source_<name>/
│   ├── __init__.py
│   ├── run.py                 # launch(SourceXxx(), sys.argv[1:])
│   ├── source.py              # AbstractSource: check_connection() + streams()
│   ├── streams.py             # Stream + IncrementalMixin, lazy client, _normalize
│   ├── <client>.py            # client gọi API / GCS (tách riêng)
│   ├── spec.yaml              # config schema → form trên UI
│   └── schemas/
│       ├── stream_a.json      # cột nguồn + metadata (tên "thô", normalize lúc discover)
│       └── stream_b.json
├── scripts/                   # helper test đọc nguồn trực tiếp (đọc cred từ argv)
└── secrets/                   # config thật để test — KHÔNG commit
```

---

## 3. Quy ước code (bài học xương máu)

### 3.1 Lazy client — KHÔNG dựng client trong `streams()`
`discover` gọi `streams()`. Nếu `streams()` dựng client (parse credentials) ngay, discover **chết khi credentials không hợp lệ** → lỗi *"No catalog found in source discovery output"*.

```python
class BaseStream(Stream, IncrementalMixin, ABC):
    def __init__(self, credentials: str, **params):
        super().__init__()
        self._credentials = credentials      # giữ string, KHÔNG dựng client
        self._client = None

    @property
    def _conn(self):
        if self._client is None:
            self._client = MyClient(self._credentials)   # dựng lazy
        return self._client

    def read_records(self, ...):
        for item in self._conn.list(...):    # client chỉ sống ở đây
            ...
```

`source.py` truyền **config string** xuống stream, KHÔNG truyền object client:
```python
def streams(self, config):
    common = dict(credentials=config["credentials"], **other_params)
    return [StreamA(**common), StreamB(**common)]
```
`check_connection()` thì được phép dựng client (để test kết nối).

### 3.2 `spec.yaml`: field optional phải nhận null
```yaml
start_date:
  title: Start Date (YYYY-MM)
  type: ["null", "string"]        # KHÔNG để type: string cứng -> lỗi "None is not of type string"
  pattern: "^[0-9]{4}-[0-9]{2}$"
```

### 3.3 ⭐ Chuẩn hóa TÊN CỘT về `_Name_` (quy ước cốt lõi — BQ-safe)
**Vấn đề:** BigQuery (direct-load) cấm tên cột:
- Bắt đầu bằng tiền tố: `_PARTITION`, `_TABLE_`, `_FILE_`, `_ROW_TIMESTAMP`, `__ROOT__`, `_COLIDENTIFIER`.
- Chứa ký tự đặc biệt: dấu cách, `(`, `)`, `!`, `"`, `$`, `*`, `,`, `.`, `/`...

→ Tên gốc của nguồn (vd Google: `Amount (Merchant Currency)`, `Service Fee %`, `Charged Amount`) sẽ **fail** khi BQ `CREATE TABLE`.

**Giải pháp — chuẩn hóa MỌI cột (gốc + metadata) về dạng `_Name_Sạch_`:**
```python
import re
def _normalize(col: str) -> str:
    # giữ chữ hoa/thường, thay [^0-9a-zA-Z]+ bằng "_", gộp "_", bọc 2 đầu
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"
```
Ví dụ:
| Tên gốc | Sau normalize |
|---|---|
| `Charged Amount` | `_Charged_Amount_` |
| `Amount (Merchant Currency)` | `_Amount_Merchant_Currency_` |
| `Service Fee %` | `_Service_Fee_` |
| `Date` | `_Date_` |
| metadata `row_number` | `_row_number_` |

Áp ở **2 chỗ** để schema và data khớp:
```python
# (1) read_records: normalize key cột nguồn (đọc field cần dùng từ tên GỐC trước!)
app_id = row.get(self.app_id_column)          # đọc theo tên gốc TRƯỚC
out = {_normalize(k): v for k, v in row.items() if k is not None}
out.update({ META_ROW_NUMBER: i, META_MODIFIED_AT: ts, ... })   # metadata đã ở dạng _Name_
yield out

# (2) get_json_schema: normalize tên property để khớp data
def get_json_schema(self):
    schema = super().get_json_schema()
    props = schema.get("properties", {})
    schema["properties"] = {_normalize(k): v for k, v in props.items()}
    return schema
```
**Lưu ý:** sau normalize phải kiểm tra **không trùng tên** (2 cột gốc khác nhau có thể ra cùng tên). Hiếm, nhưng cần test.

### 3.4 An toàn parse (downstream dbt)
- Airbyte export null thành chuỗi `"None"` → dùng `safe_cast`.
- Dùng `row_number()` thay `rank()` khi dedup.
- PK separator dùng `'|'`.

---

## 4. Cột metadata chuẩn (connector chèn, đã ở dạng `_Name_`)

| Cột | Ý nghĩa |
|---|---|
| `_store_id_` / `_<entity>_id_` | Khóa định danh do người dùng cấu hình |
| `_app_id_` | Định danh phụ (vd package name) |
| `_source_file_` / `_source_ref_` | Nguồn gốc dòng (đường dẫn file / URL) |
| `_row_number_` | Số thứ tự dòng trong file — **bắt buộc** trong PK khi nguồn không có khóa tự nhiên |
| `_report_month_` / `_period_` | Kỳ báo cáo |
| `_modified_at_` / `_extracted_at_` | Thời điểm nguồn cập nhật — thường là **cursor** |
| `_synced_at_` | Thời điểm Airbyte kéo về |

> Định nghĩa metadata bằng hằng số 1 chỗ: `META_ROW_NUMBER = _normalize("row_number")` ... và `METADATA_FIELDS = [...]`.

---

## 5. Primary Key — quy tắc chọn (tránh mất data âm thầm)

PK = tổ hợp trường **không bao giờ có 2 dòng hợp lệ trùng nhau**. Sai → Append+Deduped âm thầm bỏ dòng → sai số liệu.

- **Có khóa tự nhiên / 1 dòng mỗi (kỳ × entity):** PK = `_store_id_, _app_id_, _Date_` (vd installs_overview, ratings).
- **Thêm chiều phân rã:** thêm cột chiều đó. Vd installs theo nước: `_store_id_, _app_id_, _Date_, _Country_`.
- **KHÔNG có khóa tự nhiên** (1 logic tách nhiều dòng giống nhau — vd earnings tách Charge/fee/tax, cùng order ID lặp, có thể trùng cả loại): PK = `_store_id_, _app_id_, _source_file_, _row_number_`. **Bắt buộc `_row_number_`.**
- **Có link/ID duy nhất mỗi record** (vd review link): PK = `_store_id_, _<unique_link>_` (+ fallback `_source_file_, _row_number_`).

> **Kiểm chứng khách quan:** đếm `distinct(PK)` vs tổng dòng file. Nếu nhỏ hơn → đó là số dòng sẽ mất. (Đã verify với earnings: `Description+Transaction Type` KHÔNG đủ unique — có order lặp nhiều dòng → phải dùng `_row_number_`.)

---

## 6. File mẫu cốt lõi

### `pyproject.toml`
```toml
[tool.poetry]
name = "source-<name>"
version = "0.1.0"
packages = [{ include = "source_<name>" }]
include = ["source_<name>/schemas/*.json"]   # ⚠️ ép pip đóng gói json

[tool.poetry.dependencies]
python = ">=3.11,<3.14"     # KHÔNG "^3.11" (chặn 3.12)
airbyte-cdk = "^7.0"
# + dependency nguồn, vd: google-cloud-storage = "^2.10"

[tool.poetry.scripts]
source-<name> = "source_<name>.run:run"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
```

### `Dockerfile` (multi-stage, slim)
```dockerfile
FROM python:3.11-slim AS builder
WORKDIR /app
RUN pip install --no-cache-dir poetry
COPY pyproject.toml poetry.lock* ./
COPY source_<name> ./source_<name>
RUN poetry config virtualenvs.create false && poetry install --only main --no-root \
 && pip install --no-cache-dir .

FROM python:3.11-slim
ENV TZ=UTC
WORKDIR /airbyte/integration_code
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY main.py ./
COPY source_<name> ./source_<name>
ENV AIRBYTE_ENTRYPOINT="python /airbyte/integration_code/main.py"
ENTRYPOINT ["python", "/airbyte/integration_code/main.py"]
LABEL io.airbyte.name=dataezg/source-<name>
```

### `schemas/<stream>.json` (tên cột "thô" — sẽ normalize lúc discover)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "additionalProperties": true,
  "properties": {
    "store_id": { "type": ["null", "string"] },
    "app_id": { "type": ["null", "string"] },
    "report_month": { "type": ["null", "string"] },
    "source_file": { "type": ["null", "string"] },
    "row_number": { "type": ["null", "integer"] },
    "modified_at": { "type": ["null", "string"], "format": "date-time" },
    "synced_at": { "type": ["null", "string"], "format": "date-time" },
    "Date": { "type": ["null", "string"] },
    "Some Source Column": { "type": ["null", "string"] }
  }
}
```
> Khai tên gốc (kể cả có dấu cách) để dễ đọc/đối chiếu; `get_json_schema()` tự normalize về `_Name_`.

---

## 7. `.gitignore` — cái bẫy lớn nhất

**TUYỆT ĐỐI KHÔNG** dùng `*.json` — nó nuốt luôn `schemas/*.json` → schemas không lên Git → không vào image → lỗi *"schema not found / FileNotFoundError .../schemas/x.json"*. (`git add` không báo gì khi bỏ qua file bị ignore.)

```gitignore
__pycache__/
*.pyc
.venv/
secrets/
# Chỉ chặn credentials, KHÔNG chặn schemas
sa*.json
service_account*.json
*-credentials.json
easygoing-data-*.json
```
Đã lỡ ignore: `git check-ignore -v <file>` để tìm dòng chặn → sửa `.gitignore` → `git add -f schemas/`.

**Khi thêm SCHEMA MỚI:** luôn `git status` xác nhận file mới xuất hiện ở "Changes to be committed". Nếu không thấy → `git add -f`.

---

## 8. Checklist test (theo thứ tự)

```bash
cd .../connectors/source-<name>

# 1. cú pháp (im lặng = OK)
poetry run python -m py_compile source_<name>/*.py

# 2. spec
poetry run source-<name> spec

# 3. discover với credentials GIẢ -> PHẢI ra CATALOG đủ stream
#    (lỗi credentials = chưa lazy client §3.1; thiếu file = thiếu schema/§7)
poetry run source-<name> discover --config secrets/fake_config.json
poetry run source-<name> discover --config secrets/fake_config.json | Select-String '"name":'   # đếm số stream

# 4. schemas vào IMAGE chưa? (override entrypoint vì image mặc định chạy main.py)
docker run --rm --entrypoint ls dataezg/source-<name>:<tag> \
  /airbyte/integration_code/source_<name>/schemas

# 5. discover trong image
docker run --rm -v $(pwd)/secrets:/secrets dataezg/source-<name>:<tag> \
  discover --config /secrets/fake_config.json

# 6. check thật
docker run --rm -v $(pwd)/secrets:/secrets dataezg/source-<name>:<tag> \
  check --config /secrets/config.json
```

> **Windows PowerShell — tạo JSON không BOM** (Set-Content -Encoding utf8 chèn BOM làm hỏng JSON):
> ```powershell
> [System.IO.File]::WriteAllText("$PWD\secrets\fake.json", '{...}', (New-Object System.Text.UTF8Encoding($false)))
> ```
> **Đọc nguồn để lấy header thật / list file** (dùng venv Windows có google-cloud-storage, KHÔNG cần SA trên VM):
> ```powershell
> poetry run python -c "from google.cloud import storage; from google.oauth2 import service_account; c=service_account.Credentials.from_service_account_file(r'<SA.json>'); cl=storage.Client(credentials=c,project=c.project_id); [print(b.name) for b in cl.bucket('<bucket>').list_blobs(prefix='<prefix>/')]"
> ```

---

## 9. Workflow build → deploy

```
sửa code (VS Code, Windows)  ── KHÔNG sửa nano (dễ lệch indent)
  → py_compile + discover test local (credentials giả)
  → git add <file cụ thể> ; git status (xác nhận file mới có mặt) ; git commit ; git push origin HEAD:master
  → VM (SSH): cd repo ; git pull origin master ; ls schemas (xác nhận file về đủ)
  → docker build -t dataezg/source-<name>:<tag> .
  → docker run ... ls schemas + discover  (verify schemas + catalog trong image)
  → docker push dataezg/source-<name>:<tag>
  → Airbyte UI (http://<VM-IP>:8000):
       Settings → Sources → custom connector → đổi/đặt image + tag
       → tạo Source → Test (check + discover ra đủ stream)
       → Destination BigQuery (project easygoing-data, dataset raw_*, SA có BQ Data Editor + Job User)
       → Connection: stream, sync mode Incremental|Append+Deduped, PK (§5), cursor _modified_at_
       → đặt tên "DA | Airbyte | <Source> → BigQuery"
       → Reset + Sync
```

**Quy ước tag:** mỗi lần sửa code → **tăng tag** (`0.1.0` → `0.1.1` → ...). Cùng tag dễ bị Airbyte/kind cache image cũ.

**Lưu ý môi trường:** Docker Desktop trên Windows có thể KHÔNG chạy (lỗi virtualization) → **build trên VM** (Docker sẵn, đúng linux/amd64). Mọi lệnh `docker ...` chạy ở SSH (`data@docker-airbyte:~$`), KHÔNG phải PowerShell.

---

## 10. Thêm STREAM MỚI vào connector có sẵn (pattern nhanh)

Khi nguồn có thêm "dimension"/endpoint cùng dạng (vd installs theo country/device/language):

1. **Lấy header thật** của file/response mới (xem §8 — đọc 1 file, in header).
2. **Tạo schema** `schemas/<stream_mới>.json`: metadata (tên thô) + cột gốc theo header.
3. **Thêm class** trong `streams.py` — kế thừa mixin sẵn có, chỉ đổi `name` + `filename_regex`/endpoint:
   ```python
   class InstallsCountry(MonthlyPerAppCsvStream):
       name = "installs_country"
       report_prefix = "stats/installs/"
       filename_regex = r"^installs_(?P<package>.+?)_(?P<yyyymm>\d{6})_country\.csv$"
   ```
4. **Đăng ký** trong `source.py`: import + thêm `InstallsCountry(**common)` vào list `streams()`.
5. Test discover (đủ stream + cột mới), `git add -f` schema mới, push, rebuild, tăng tag.
6. Airbyte: refresh schema → bật stream → set PK (thêm cột chiều mới nếu cần, §5).

---

## 11. Sổ tay lỗi thường gặp

| Triệu chứng | Nguyên nhân gốc | Cách xử lý |
|---|---|---|
| `No catalog found in source discovery output` | `streams()` dựng client → discover chết khi cred không hợp lệ | Lazy client (§3.1) |
| `FileNotFoundError: .../schemas/x.json` (lúc discover) | schema thiếu trên đĩa/image: bị `.gitignore *.json` chặn, hoặc thêm stream mới chưa copy file | Sửa `.gitignore`, `git add -f schemas/`, `include` trong pyproject (§6,§7) |
| `BigQueryException: Invalid field name "_file_..." ... prefixes _FILE_ ...` | tên cột bắt đầu bằng tiền tố cấm BQ | Normalize tên cột (§3.3) — đổi `_file_modified_at` → `_modified_at_` |
| `Invalid field name` (dấu cách/ngoặc/%) | tên cột gốc có ký tự BQ cấm | Normalize `_Name_` (§3.3) |
| `Config validation error: None is not of type 'string'` | spec field optional khai `type: string` | `type: ["null","string"]` (§3.2) |
| `Could not read json file ... Expecting value: line 1 column 1` | file config có BOM (PowerShell) | Ghi UTF-8 không BOM (§8) |
| `Can't instantiate abstract class ... abstract method 'streams'` | `def streams` sai thụt lề / rớt khỏi class | Indent 4 space, cùng cột `check_connection` |
| `airbyte-cdk` resolve fail khi `poetry install` | `python = "^3.11"` chặn 3.12 | `python = ">=3.11,<3.14"` |
| `docker: permission denied` | user chưa thuộc group docker | `sudo usermod -aG docker $USER` → logout/login |
| `IsADirectoryError: ... sa.json` | `docker -v ~/sa.json:...` khi file chưa tồn tại → Docker tạo nhầm folder | `rmdir ~/sa.json`, tạo lại file SA |
| `HTTPError 403 ...compute@developer...` khi gcloud đọc bucket | VM service account không có quyền (chỉ SA được invite mới có) | dùng SA đúng, hoặc đọc bằng venv Windows |
| `request returned 500 ... dockerDesktopLinuxEngine` | chạy docker trên Windows mà Docker Desktop hỏng | chạy trên VM (SSH) |
| `git push ... src refspec master does not match` | refspec nhất thời | `git push origin HEAD:master` |

---

## 12. Khi nhờ AI hỗ trợ — nên cung cấp gì

1. File này (playbook).
2. **Mẫu data/header thật** của nguồn (1-2 dòng + header). Với nguồn tách dòng (như earnings), nói rõ để chọn PK đúng.
3. Đặc thù nguồn: pagination? auth kiểu gì? rate limit? data có lặp khóa không?
4. Lỗi gặp phải: **dán nguyên traceback**, không tóm tắt.
5. Đang ở đâu (Windows local / VM), đã push/pull/build/tag tới đâu.

---

## 13. Reference — source-google-play-console (mẫu hoàn chỉnh)

- **Nguồn:** đọc Google Play bulk reports từ GCS bucket `pubsite_prod_*` (1 SA `gpc-view@easygoing-data` đọc tất cả store). KHÔNG dùng Play Developer API.
- **Config:** `service_account` (JSON string, secret), `stores` (array `{store_id, bucket}`), `start_date` (null=backfill all), `lookback_days` (default 28).
- **6 streams:**
  - `estimated_sales` ← `sales/salesreport_YYYYMM.zip` (account-level, utf-8-sig)
  - `earnings` ← `earnings/earnings_YYYYMM_*.zip` (account-level, utf-8-sig; HKD trong `_Amount_Merchant_Currency_`, quy đổi bằng `_Currency_Conversion_Rate_`)
  - `installs_overview` ← `stats/installs/installs_<pkg>_YYYYMM_overview.csv` (per-app, utf-16)
  - `installs_country` ← `..._YYYYMM_country.csv` (per-app, utf-16, thêm `_Country_`)
  - `ratings` ← `stats/ratings/..._overview.csv` (utf-16)
  - `reviews` ← `reviews/reviews_<pkg>_YYYYMM.csv` (utf-16)
- **Cursor:** `_modified_at_` (blob.updated), incremental, lookback 28 ngày, Append+Deduped, state theo `store_id`.
- **PK:** earnings/sales = `_store_id_,_app_id_,_source_file_,_row_number_`; installs_overview/ratings = `_store_id_,_app_id_,_Date_`; installs_country = `+_Country_`; reviews = `_store_id_,_Review_Link_`.

---

*Confidential — Internal Use Only · EasyGoing Data*
