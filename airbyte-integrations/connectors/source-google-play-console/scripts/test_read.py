"""Test đọc end-to-end MỘT stream, in N record đầu (không cần configured catalog).

Cách chạy:
    poetry run python scripts/test_read.py "<sa.json>" <bucket> <stream> [limit]

stream: estimated_sales | earnings | installs_overview | ratings | reviews
ví dụ:
    poetry run python scripts/test_read.py "C:\\path\\sa.json" pubsite_prod_6267925093918500190 installs_overview 5
"""
import json
import sys

from airbyte_cdk.models import SyncMode

from source_google_play_console.source import SourceGooglePlayConsole


def main() -> None:
    sa_path, bucket, stream_name = sys.argv[1], sys.argv[2], sys.argv[3]
    limit = int(sys.argv[4]) if len(sys.argv) > 4 else 5

    config = {
        "service_account": open(sa_path, encoding="utf-8").read(),
        "stores": [{"store_id": "store_a", "bucket": bucket}],
        "start_date": None,
        "lookback_days": 28,
    }

    source = SourceGooglePlayConsole()
    ok, err = source.check_connection(None, config)
    print(f"check_connection -> {ok} {err or ''}")
    if not ok:
        sys.exit(1)

    stream = next(s for s in source.streams(config) if s.name == stream_name)
    print(f"\n--- {stream_name}: {limit} record đầu ---")
    n = 0
    for rec in stream.read_records(sync_mode=SyncMode.full_refresh):
        print(json.dumps(rec, ensure_ascii=False)[:1000])
        n += 1
        if n >= limit:
            break
    print(f"\nĐã đọc {n} record. state = {dict(stream.state)}")


if __name__ == "__main__":
    main()
