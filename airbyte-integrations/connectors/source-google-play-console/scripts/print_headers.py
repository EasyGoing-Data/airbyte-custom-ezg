import csv, io, json, re, sys, zipfile
from google.cloud import storage
from google.oauth2 import service_account

# (name, prefix, regex, is_zip)
STREAMS = [
    ("estimated_sales",  "sales/",          r"salesreport_\d{6}\.zip$",           True),
    ("earnings",         "earnings/",       r"earnings_\d{6}_[\d-]+\.zip$",        True),
    ("installs_overview","stats/installs/", r"installs_.+?_\d{6}_overview\.csv$",  False),
    ("ratings",          "stats/ratings/",  r"ratings_.+?_\d{6}_overview\.csv$",   False),
    ("reviews",          "reviews/",        r"reviews_.+?_\d{6}\.csv$",            False),
]

def decode(b, candidates):
    for enc in candidates:
        try:
            return b.decode(enc), enc
        except UnicodeDecodeError:
            continue
    return b.decode("utf-8", errors="replace"), "utf-8(replace)"

def main():
    sa_path, bucket_name = sys.argv[1], sys.argv[2]
    info = json.load(open(sa_path, encoding="utf-8"))
    creds = service_account.Credentials.from_service_account_info(info)
    client = storage.Client(credentials=creds, project=info.get("project_id"))
    bucket = client.bucket(bucket_name)

    for name, prefix, pattern, is_zip in STREAMS:
        print("\n" + "=" * 70)
        print("STREAM:", name, " prefix:", prefix)
        rgx = re.compile(pattern)
        target = None
        for blob in client.list_blobs(bucket, prefix=prefix):
            if rgx.search(blob.name):
                target = blob
                break
        if target is None:
            print("  -> khong tim thay file khop regex")
            continue
        print("  file :", target.name)
        raw = target.download_as_bytes()

        if is_zip:
            zf = zipfile.ZipFile(io.BytesIO(raw))
            inner = [n for n in zf.namelist() if n.lower().endswith(".csv")] or zf.namelist()
            print("  zip chua:", inner)
            data = zf.read(inner[0])
            text, enc = decode(data, ["utf-8-sig", "utf-16", "utf-8"])
        else:
            text, enc = decode(raw, ["utf-16", "utf-8-sig", "utf-8"])

        print("  encoding:", enc)
        reader = csv.reader(io.StringIO(text))
        rows = list(reader)
        if not rows:
            print("  -> file rong")
            continue
        header = rows[0]
        print("  so cot:", len(header), " | so dong data:", max(0, len(rows) - 1))
        print("  COLUMNS:")
        for i, c in enumerate(header):
            print("    [%02d] %s" % (i, c))

if __name__ == "__main__":
    main()
