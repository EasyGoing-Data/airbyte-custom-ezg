#
# Test source-liftoff-custom voi server gia (khong can key that, khong goi Liftoff).
# Chay tu thu muc connector:  python integration_tests/mock_test.py
# Kiem: spec, discover, check (key dung / sai), header Bearer + Vungle-Version, read full, read incremental + lookback,
#       clamp 180 ngay, clamp cursor tuong lai, retry 429, slice rong (200 rong), loc row ngoai cua so,
#       thieu cot khoa / trung khoa thi dung, khong lo key ra stdout.
#
import json
import os
import subprocess
import sys
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STREAM = "liftoff_report"
KEYS = {"fake_key_1": "1", "fake_key_2": "2"}
TODAY = datetime.now(timezone.utc).date()
START = TODAY - timedelta(days=20)

state = {"sent_429": False, "requests": [], "mode": ""}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _reply(self, code, body="", headers=None):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/ext/pub/reports/performance":
            return self._reply(404, '{"message":"not found"}')
        auth = self.headers.get("Authorization", "")
        key = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
        if key not in KEYS or self.headers.get("Vungle-Version") != "1":
            return self._reply(401, '{"message":"unauthorized"}')
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        for required in ("start", "end", "dimensions", "aggregates"):
            if required not in q:
                return self._reply(400, '{"message":"missing %s"}' % required)
        start, end = date.fromisoformat(q["start"]), date.fromisoformat(q["end"])
        acc = KEYS[key]
        state["requests"].append({"acc": acc, "start": start, "end": end, "dimensions": q["dimensions"]})
        if q["dimensions"] == "date":  # probe cua check_connection
            return self._reply(200, json.dumps([{"date": q["start"], "views": 1, "revenue": 0.1}]))
        if acc == "1" and not state["sent_429"]:
            state["sent_429"] = True
            return self._reply(429, '{"message":"rate limit"}', {"Retry-After": "1"})
        if acc == "2" and start == START:
            return self._reply(200, "")  # khong co du lieu -> 200 rong
        rows = []
        d = start
        while d <= end and d <= TODAY:
            for country in ("US", "VN"):
                row = {
                    "platform": "android", "date": d.isoformat(), "country": country,
                    "application id": "586e201e242e3fd30800022" + acc, "application name": "Game One",
                    "placement id": None if country == "VN" else "12345678", "placement name": None if country == "VN" else "Level 3",
                    "placement reference id": None if country == "VN" else "REWARD-0000001",
                    "incentivized": True, "adType": "video",
                    "impressions": 100, "views": 90, "completes": 80, "clicks": 5, "revenue": 1.23456789, "ecpm": 13.72,
                }
                if state["mode"] == "no_app_id":
                    row["app id"] = row.pop("application id")
                rows.append(row)
            d += timedelta(days=1)
        # 1 row ngoai cua so -> connector phai loc bo
        rows.append({"platform": "android", "date": (end + timedelta(days=1)).isoformat(), "country": "US",
                     "application id": "586e201e242e3fd30800022" + acc, "application name": "Game One",
                     "placement id": "12345678", "placement name": "Level 3", "incentivized": True, "adType": "video",
                     "impressions": 1, "views": 1, "completes": 1, "clicks": 1, "revenue": 9.99, "ecpm": 1})
        if state["mode"] == "dup":
            rows.append(dict(rows[0]))
        return self._reply(200, json.dumps(rows))


def run(args, env_extra):
    env = dict(os.environ)
    env.update(env_extra)
    proc = subprocess.run([sys.executable, "main.py"] + args, cwd=ROOT, env=env, capture_output=True, text=True)
    messages = []
    for line in proc.stdout.splitlines():
        try:
            messages.append(json.loads(line))
        except ValueError:
            pass
    return proc, messages


def last_state(messages):
    states = [m["state"] for m in messages if m.get("type") == "STATE"]
    if not states:
        return None
    s = states[-1]
    if s.get("stream"):
        return s["stream"]["stream_state"].get("date")
    return (s.get("data") or {}).get(STREAM, {}).get("date")


def records(messages):
    return [m["record"]["data"] for m in messages if m.get("type") == "RECORD"]


def errored(proc, msgs):
    return proc.returncode != 0 or any(m.get("type") == "TRACE" and m["trace"].get("type") == "ERROR" for m in msgs)


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"LIFTOFF_URL_BASE": f"http://127.0.0.1:{server.server_port}/"}
    tmp = tempfile.mkdtemp()
    failures = []

    def check(name, cond, detail=""):
        print(("PASS" if cond else "FAIL") + f"  {name}" + (f"  [{detail}]" if detail and not cond else ""))
        if not cond:
            failures.append(name)

    def write(name, obj):
        path = os.path.join(tmp, name)
        with open(path, "w") as f:
            json.dump(obj, f)
        return path

    config = {
        "accounts": [
            {"account_name": "liftoff_1", "api_key": "fake_key_1"},
            {"account_name": "liftoff_2", "api_key": "fake_key_2"},
        ],
        "start_date": START.isoformat(),
        "number_days_backward": 14,
        "window_days": 7,
    }
    cfg = write("config.json", config)
    catalog = os.path.join(ROOT, "integration_tests", "configured_catalog.json")
    pk = ["account_name", "date", "application_id", "placement_id", "country", "platform", "incentivized", "ad_type"]

    # spec
    proc, msgs = run(["spec"], env)
    spec = next((m["spec"] for m in msgs if m.get("type") == "SPEC"), {})
    props = spec.get("connectionSpecification", {}).get("properties", {})
    check("spec co accounts / start_date / number_days_backward", {"accounts", "start_date", "number_days_backward"} <= set(props),
          proc.stderr[-300:])
    check("spec lookback mac dinh 14", props.get("number_days_backward", {}).get("default") == 14)
    check("spec api_key la airbyte_secret", props["accounts"]["items"]["properties"]["api_key"].get("airbyte_secret") is True)

    # discover
    proc, msgs = run(["discover", "--config", os.path.join(ROOT, "secrets", "test_config.json")], env)
    streams = next((m["catalog"]["streams"] for m in msgs if m.get("type") == "CATALOG"), [])
    check("discover 1 stream dung ten", [s["name"] for s in streams] == [STREAM], proc.stderr[-300:])
    if streams:
        check("discover schema co placement_reference_id", "placement_reference_id" in streams[0]["json_schema"]["properties"])
        check("discover PK", [p[0] for p in streams[0].get("source_defined_primary_key", [])] == pk,
              str(streams[0].get("source_defined_primary_key")))
        check("discover cursor date", streams[0].get("default_cursor_field") == ["date"])

    # check
    proc, msgs = run(["check", "--config", cfg], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key dung -> SUCCEEDED (Bearer + Vungle-Version)", status.get("status") == "SUCCEEDED", str(status))
    bad = json.loads(json.dumps(config))
    bad["accounts"][1]["api_key"] = "wrong_key_xyz"
    proc, msgs = run(["check", "--config", write("bad.json", bad)], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key sai -> FAILED, neu ro account", status.get("status") == "FAILED" and "liftoff_2" in status.get("message", ""), str(status))
    check("check key sai khong lo key", "wrong_key_xyz" not in proc.stdout + proc.stderr)

    # read full
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    recs = records(msgs)
    check("read full: so record = 70", len(recs) == 70, f"{len(recs)} | rc={proc.returncode} | {proc.stderr[-400:]}")
    keys = [tuple(r.get(k) for k in pk) for r in recs]
    check("read full: PK unique", len(set(keys)) == len(keys))
    check("read full: da retry sau 429", state["sent_429"] and proc.returncode == 0)
    check("read full: row ngoai cua so bi loc", all(r["revenue"] != 9.99 for r in recs))
    check("read full: cot co dau cach -> snake_case", all(r.get("application_id", "").startswith("586e201e") and r.get("application_name") == "Game One" for r in recs))
    check("read full: placement null giu null (SDK cu)", all(r["placement_id"] is None for r in recs if r["country"] == "VN"))
    check("read full: placement_reference_id co trong record", all(r.get("placement_reference_id") == (None if r["country"] == "VN" else "REWARD-0000001") for r in recs))
    check("read full: incentivized -> chuoi true/false, adType -> ad_type", all(r.get("incentivized") == "true" and r.get("ad_type") == "video" for r in recs))
    check("read full: revenue round 6, views int", all(r["revenue"] == 1.234568 and r["views"] == 90 for r in recs))
    check("read full: state cuoi = hom nay (UTC)", last_state(msgs) == TODAY.isoformat(), str(last_state(msgs)))
    check("read full: khong lo key ra stdout/stderr", "fake_key_" not in proc.stdout + proc.stderr)

    # clamp 180 ngay
    old = json.loads(json.dumps(config))
    old["start_date"] = (TODAY - timedelta(days=400)).isoformat()
    old["window_days"] = 31
    state["requests"].clear()
    proc, msgs = run(["read", "--config", write("old.json", old), "--catalog", catalog], env)
    first = min(r["start"] for r in state["requests"]) if state["requests"] else None
    check("start_date 400 ngay truoc -> clamp ve 180 ngay", first == TODAY - timedelta(days=180) and proc.returncode == 0,
          f"{first} rc={proc.returncode}")

    # incremental
    st = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": STREAM},
                                        "stream_state": {"date": (TODAY - timedelta(days=3)).isoformat()}}}]
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("state.json", st)], env)
    first = min(r["start"] for r in state["requests"]) if state["requests"] else None
    check("read incremental: lui 14 ngay tu cursor", first == TODAY - timedelta(days=17), str(first))
    check("read incremental: so record = 72", len(records(msgs)) == 72, str(len(records(msgs))))
    check("read incremental: state cuoi = hom nay", last_state(msgs) == TODAY.isoformat())

    # cursor tuong lai
    st[0]["stream"]["stream_state"]["date"] = (TODAY + timedelta(days=30)).isoformat()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("future.json", st)], env)
    check("cursor tuong lai: khong crash, state = hom nay", proc.returncode == 0 and last_state(msgs) == TODAY.isoformat(),
          f"rc={proc.returncode} state={last_state(msgs)}")

    # thieu cot khoa
    state["mode"] = "no_app_id"
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    out = proc.stdout + proc.stderr
    check("thieu cot khoa: sync bao loi, neu ten cot, 0 record", errored(proc, msgs) and "thieu cot khoa" in out and "app_id" in out and not records(msgs),
          f"rc={proc.returncode} recs={len(records(msgs))}")
    # trung khoa
    state["mode"] = "dup"
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    check("trung khoa trong response: sync bao loi", errored(proc, msgs) and "trung khoa chinh" in proc.stdout + proc.stderr,
          f"rc={proc.returncode}")
    state["mode"] = ""

    server.shutdown()
    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
