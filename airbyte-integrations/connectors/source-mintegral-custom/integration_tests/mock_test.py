#
# Test source-mintegral-custom voi server gia (khong can key that, khong goi Mintegral).
# Chay tu thu muc connector:  python integration_tests/mock_test.py
# Kiem: spec, discover, check (secret dung / sai), chu ky md5 + time, timezone=0, read full co phan trang,
#       gioi han 60 ngay, cua so <= 7 ngay, read incremental + lookback, clamp cursor tuong lai,
#       retry HTTP 500, slice rong, loi API giua chung -> state khong vuot, khong lo skey / secret / sign.
#
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
STREAM = "mintegral_report"
SECRETS = {"fake_skey_1": "fake_secret_1", "fake_skey_2": "fake_secret_2"}
TODAY = datetime.now(timezone.utc).date()
COUNTRIES = ["US", "VN", "JP"]

state = {"sent_500": False, "requests": [], "fail_skey2_after": None, "signs": []}


def ymd(d):
    return d.strftime("%Y%m%d")


def all_rows(start, end):
    rows = []
    d = start
    while d <= end and d <= TODAY:
        for c in COUNTRIES:
            rows.append({
                "date": int(ymd(d)), "app_id": 91458, "app_name": "Game One", "app_package": "com.ezg.one",
                "placement_id": 7001, "placement_name": "Rewarded", "unit_id": 8001, "unit_name": "RV_1",
                "ad_format": "rewarded_video", "platform": "android", "country": c,
                "filled": 90, "fill_rate": 90.0, "request": 100, "impression": 80, "click": 3,
                "est_revenue": 1.23456789, "ecpm": 15.43, "ctr": 3.75,
            })
        d += timedelta(days=1)
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, code, obj):
        data = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        if url.path != "/reporting/v2/data":
            return self._json(404, {"code": "not_found"})
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        skey, ts, sign = q.get("skey"), q.get("time"), q.get("sign")
        state["signs"].append(sign)
        if skey not in SECRETS or not ts or not ts.isdigit():
            return self._json(200, {"code": "invalid_skey", "msg": "skey invalid"})
        if abs(int(ts) - int(time.time())) > 300:
            return self._json(200, {"code": "time_expired", "msg": "time out of range"})
        expected = hashlib.md5((SECRETS[skey] + hashlib.md5(ts.encode()).hexdigest()).encode()).hexdigest()
        if sign != expected:
            return self._json(200, {"code": "sign_error", "msg": "sign verification failed"})
        start = datetime.strptime(q["start"], "%Y%m%d").date()
        end = datetime.strptime(q["end"], "%Y%m%d").date()
        page, limit = int(q.get("page", 1)), int(q.get("limit", 10000))
        state["requests"].append({"skey_ok": True, "start": start, "end": end, "page": page, "tz": q.get("timezone"),
                                  "group_by": q.get("group_by"), "which": skey[-1]})
        if (end - start).days > 6:
            return self._json(200, {"code": "date_range_error", "msg": "max 7 days"})
        if start < TODAY - timedelta(days=60):
            return self._json(200, {"code": "date_range_error", "msg": "only last 60 days"})
        if q.get("timezone") != "0":
            return self._json(200, {"code": "tz_error", "msg": "test expects timezone=0"})
        # 1 lan HTTP 500 khi doc trang 2 -> test retry (va ky lai)
        if page == 2 and not state["sent_500"]:
            state["sent_500"] = True
            return self._json(500, {"code": "server_error"})
        # account 2 loi API tu cua so co start >= fail_skey2_after -> test state khong vuot qua account loi
        if skey == "fake_skey_2" and state["fail_skey2_after"] and start >= state["fail_skey2_after"]:
            return self._json(200, {"code": "system_error", "msg": "simulated failure"})
        # account 2: cua so dau tien rong
        rows = all_rows(start, end)
        if skey == "fake_skey_2" and state.get("empty_start") == start:
            rows = []
        total_page = max(1, -(-len(rows) // limit))
        chunk = rows[(page - 1) * limit: page * limit]
        if state.get("drop_unit"):
            chunk = [{k: v for k, v in r.items() if k != "unit_id"} for r in chunk]
        return self._json(200, {"code": "ok", "data": {
            "total": len(rows) + (1 if state.get("bad_total") else 0), "page": page, "limit": limit, "total_page": total_page,
            "start": q["start"], "end": q["end"], "timezone": 0, "ad_format": "", "lists": chunk,
        }})


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


def leaked(proc):
    out = proc.stdout + proc.stderr
    if any(x in out for x in ("fake_skey_", "fake_secret_", "wrong_secret")):
        return True
    return any(s and s in out for s in state["signs"])


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"MINTEGRAL_URL": f"http://127.0.0.1:{server.server_port}/reporting/v2/data", "MINTEGRAL_RETRY_BASE_SEC": "0.2"}
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

    start = TODAY - timedelta(days=20)
    state["empty_start"] = start
    config = {
        "accounts": [
            {"account_name": "mintegral_1", "skey": "fake_skey_1", "secret": "fake_secret_1"},
            {"account_name": "mintegral_2", "skey": "fake_skey_2", "secret": "fake_secret_2"},
        ],
        "start_date": start.isoformat(),
        "number_days_backward": 14,
        "window_days": 7,
        "page_size": 10,   # ep phan trang: 7 ngay x 3 nuoc = 21 row -> 3 trang
    }
    cfg = write("config.json", config)
    catalog = os.path.join(ROOT, "integration_tests", "configured_catalog.json")
    pk = ["account_name", "date", "app_id", "placement_id", "unit_id", "country", "platform"]

    # spec
    proc, msgs = run(["spec"], env)
    spec = next((m["spec"] for m in msgs if m.get("type") == "SPEC"), {})
    props = spec.get("connectionSpecification", {}).get("properties", {})
    acc = props.get("accounts", {}).get("items", {}).get("properties", {})
    check("spec co accounts / start_date / number_days_backward", {"accounts", "start_date", "number_days_backward"} <= set(props),
          proc.stderr[-300:])
    check("spec lookback mac dinh 14", props.get("number_days_backward", {}).get("default") == 14)
    check("spec skey + secret la airbyte_secret", acc.get("skey", {}).get("airbyte_secret") is True
          and acc.get("secret", {}).get("airbyte_secret") is True)

    # discover
    proc, msgs = run(["discover", "--config", os.path.join(ROOT, "secrets", "test_config.json")], env)
    streams = next((m["catalog"]["streams"] for m in msgs if m.get("type") == "CATALOG"), [])
    check("discover 1 stream dung ten", [s["name"] for s in streams] == [STREAM], proc.stderr[-300:])
    if streams:
        check("discover PK", [p[0] for p in streams[0].get("source_defined_primary_key", [])] == pk,
              str(streams[0].get("source_defined_primary_key")))
        check("discover cursor date + incremental", streams[0].get("default_cursor_field") == ["date"]
              and "incremental" in streams[0].get("supported_sync_modes", []), str(streams[0].get("supported_sync_modes")))

    # check
    proc, msgs = run(["check", "--config", cfg], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check secret dung -> SUCCEEDED (chu ky md5 khop)", status.get("status") == "SUCCEEDED", str(status))
    bad = json.loads(json.dumps(config))
    bad["accounts"][1]["secret"] = "wrong_secret"
    proc, msgs = run(["check", "--config", write("bad.json", bad)], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check secret sai -> FAILED, neu ro account", status.get("status") == "FAILED" and "mintegral_2" in status.get("message", ""),
          str(status))
    check("check secret sai: khong lo skey / secret / sign", not leaked(proc))

    # read full
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    recs = records(msgs)
    # 21 ngay x 3 nuoc x 2 account, tru cua so dau (7 ngay x 3) cua account 2 rong = 126 - 21
    check("read full: so record = 105", len(recs) == 105, f"{len(recs)} | rc={proc.returncode} | {proc.stderr[-400:]}")
    keys = [tuple(r.get(k) for k in pk) for r in recs]
    check("read full: PK unique (phan trang khong trung / thieu)", len(set(keys)) == len(keys))
    check("read full: da retry sau HTTP 500", state["sent_500"] and proc.returncode == 0)
    check("read full: moi request timezone=0", all(r["tz"] == "0" for r in state["requests"]))
    check("read full: moi cua so <= 7 ngay", all((r["end"] - r["start"]).days <= 6 for r in state["requests"]))
    check("read full: date doi ve YYYY-MM-DD", all(len(r["date"]) == 10 and r["date"][4] == "-" for r in recs))
    check("read full: id la chuoi, revenue round 6", all(r["app_id"] == "91458" and r["est_revenue"] == 1.234568 for r in recs))
    check("read full: state cuoi = hom nay (UTC)", last_state(msgs) == TODAY.isoformat(), str(last_state(msgs)))
    check("read full: khong lo skey / secret / sign", not leaked(proc))

    # gioi han 60 ngay
    old = json.loads(json.dumps(config))
    old["start_date"] = (TODAY - timedelta(days=90)).isoformat()
    old["page_size"] = 10000
    state["requests"].clear()
    state["empty_start"] = None
    proc, msgs = run(["read", "--config", write("old.json", old), "--catalog", catalog], env)
    first = min(r["start"] for r in state["requests"]) if state["requests"] else None
    check("start_date 90 ngay truoc -> clamp ve 60 ngay", first == TODAY - timedelta(days=60) and proc.returncode == 0,
          f"{first} rc={proc.returncode} {proc.stderr[-300:]}")
    check("clamp 60 ngay: so record = 61 ngay x 3 x 2", len(records(msgs)) == 366, str(len(records(msgs))))

    # incremental: cursor = today-3 -> bat dau tu today-17
    st = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": STREAM},
                                        "stream_state": {"date": (TODAY - timedelta(days=3)).isoformat()}}}]
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("state.json", st)], env)
    first = min(r["start"] for r in state["requests"]) if state["requests"] else None
    check("read incremental: lui 14 ngay tu cursor", first == TODAY - timedelta(days=17), str(first))
    check("read incremental: so record = 108", len(records(msgs)) == 108, str(len(records(msgs))))  # 18 x 3 x 2
    check("read incremental: state cuoi = hom nay", last_state(msgs) == TODAY.isoformat())

    # account 2 loi tu cua so thu 2 -> sync fail, state khong vuot qua phan account 2 da doc
    state["fail_skey2_after"] = TODAY - timedelta(days=10)
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    st_after = last_state(msgs)
    check("account 2 loi: sync bao loi", proc.returncode != 0 or any(m.get("type") == "TRACE" and m["trace"].get("type") == "ERROR" for m in msgs))
    # cua so: [t-20..t-14] [t-13..t-7] [t-6..t]; account 2 loi o cua so 3 -> state dung o t-7 du account 1 da doc toi t
    check("account 2 loi: state dung o cuoi cua so account 2 doc xong (t-7)", st_after == (TODAY - timedelta(days=7)).isoformat(),
          str(st_after))
    check("account 2 loi: khong lo skey / secret / sign", not leaked(proc))
    state["fail_skey2_after"] = None

    def errored(proc, msgs):
        return proc.returncode != 0 or any(m.get("type") == "TRACE" and m["trace"].get("type") == "ERROR" for m in msgs)

    # total lech so dong doc duoc -> doc lai 3 lan roi dung, khong nap record nao
    state["bad_total"] = True
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    check("total lech: sync bao loi, 0 record", errored(proc, msgs) and "khong nhat quan" in (proc.stdout + proc.stderr) and not records(msgs),
          f"rc={proc.returncode} recs={len(records(msgs))}")
    check("total lech: da doc lai slice 3 lan", len([r for r in state["requests"] if r["page"] == 1]) == 3, str(len(state["requests"])))
    check("total lech: khong lo skey / secret / sign", not leaked(proc))
    state["bad_total"] = False

    # response thieu cot khoa -> dung han, khong nap record nao
    state["drop_unit"] = True
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    check("thieu cot khoa: sync bao loi, neu ten cot, 0 record",
          errored(proc, msgs) and "thieu cot khoa ['unit_id']" in (proc.stdout + proc.stderr) and not records(msgs),
          f"rc={proc.returncode} recs={len(records(msgs))}")
    state["drop_unit"] = False

    # cursor tuong lai -> clamp
    st[0]["stream"]["stream_state"]["date"] = (TODAY + timedelta(days=30)).isoformat()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("future.json", st)], env)
    check("cursor tuong lai: khong crash, state = hom nay", proc.returncode == 0 and last_state(msgs) == TODAY.isoformat(),
          f"rc={proc.returncode} state={last_state(msgs)}")

    # chu ky theo vi du trong tai lieu Mintegral
    sys.path.insert(0, ROOT)
    from source_mintegral.source import _sign
    check("sign khop vi du tai lieu", _sign("99b50cfa3e2ae9ebb276f4e16e26f04b", 1557389456) == "b5f74c77dce15a7ebecb73eb33cc5493")

    server.shutdown()
    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
