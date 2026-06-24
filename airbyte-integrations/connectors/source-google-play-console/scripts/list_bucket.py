import json, sys
from google.cloud import storage
from google.oauth2 import service_account

SAMPLE_PREFIXES = ["sales/", "earnings/", "stats/installs/", "stats/ratings/", "reviews/"]
SAMPLES_PER_PREFIX = 8

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/list_bucket.py <sa.json> <bucket>")
        sys.exit(1)
    sa_path, bucket_name = sys.argv[1], sys.argv[2]
    with open(sa_path, "r", encoding="utf-8") as f:
        info = json.load(f)
    creds = service_account.Credentials.from_service_account_info(info)
    client = storage.Client(credentials=creds, project=info.get("project_id"))
    bucket = client.bucket(bucket_name)
    print("SA email :", info.get("client_email"))
    print("Bucket   : gs://%s/\n" % bucket_name)
    print("=== Top-level folders ===")
    top = client.list_blobs(bucket, delimiter="/")
    list(top)
    for p in sorted(top.prefixes):
        print("  " + p)
    print("\n=== File mau moi loai ===")
    for prefix in SAMPLE_PREFIXES:
        print("\n[%s]" % prefix)
        count = 0; found = False
        for blob in client.list_blobs(bucket, prefix=prefix):
            found = True
            print("  %s\t(updated=%s)" % (blob.name, blob.updated))
            count += 1
            if count >= SAMPLES_PER_PREFIX:
                print("  ..."); break
        if not found:
            print("  (trong hoac khong ton tai)")

if __name__ == "__main__":
    main()
