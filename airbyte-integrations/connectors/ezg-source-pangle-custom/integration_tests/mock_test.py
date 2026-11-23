#
# Test source-pangle-custom voi server gia (khong can key that, khong goi Pangle).
# Chay tu thu muc connector:  python integration_tests/mock_test.py
# Kiem: spec, discover, check (key dung / sai), chu ky md5 + timestamp, time_zone=0, currency=usd, 1 ngay / request,
#       read full, read incremental + lookback, clamp cursor tuong lai, retry Code 106 + HTTP 500, ngay rong (PD0004),
#       account loi giua chung -> state khong vuot, thieu cot khoa / trung khoa / cham 100.000 dong thi dung,
#       khong lo security_key / sign.
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
STREAM = "pangle_report"
KEYS = {"459": "fake_security_key_1", "777": "fake_security_key_2"}
TODAY = datetime.now(timezone.utc).date()
START = TODAY - timedelta(days=20)
REGIONS = ["us", "vn", "jp"]

state = {"sent_106": False, "sent_500": False, "requests": [], "signs": [], "mode": "", "fail_777_from": None}


def expected_sign(q, key):
    parts = [f"{k}={v}" for k, v in sorted(q.items()) if k != "sign" and v != ""]
    return hashlib.md5(("&".join(parts) + key).encode()).hexdigest()


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
        if url.path != "/union_pangle/open/api/rt/income":
            return self._json(404, {"Code": "404"})
        q = {k: v[0] for k, v in parse_qs(url.query, keep_blank_values=True).items()}
        state["signs"].append(q.get("sign"))
        uid = q.get("user_id")
        if uid not in KEYS:
            return self._json(200, {"Code": "102", "Message": "Invalid user_id"})
        if q.get("version") != "2.0" or q.get("sign_type") != "MD5":
            return self._json(200, {"Code": "114", "Message": "Invalid param"})
        if abs(int(q.get("timestamp", "0")) - int(time.time())) > 600:
            return self._json(200, {"Code": "114", "Message": "timestamp out of range"})
        if q.get("sign") != expected_sign(q, KEYS[uid]):
            return self._json(200, {"Code": "101", "Message": "Sign verification failed"})
        try:
            d = date.fromisoformat(q.get("date", ""))
        except ValueError:
            return self._json(200, {"Code": "103", "Message": "Invalid date format"})
        state["requests"].append({"uid": uid, "date": d, "tz": q.get("time_zone"), "currency": q.get("currency")})
        if not state["sent_106"]:
            state["sent_106"] = True
            return self._json(200, {"Code": "106", "Message": "Exceed QPS limits"})
        if not state["sent_500"]:
            state["sent_500"] = True
            return self._json(500, {"Code": "500"})
        if uid == "777" and state["fail_777_from"] and d >= state["fail_777_from"]:
            return self._json(200, {"Code": "114", "Message": "simulated failure"})
        if uid == "777" and d == START:
            return self._json(200, {"Code": "PD0004", "Message": "no data"})
        if d > TODAY:
            return self._json(200, {"Code": "100", "Message": "", "Data": {}})
        rows = []
        for region in REGIONS:
            row = {
                "ad_slot_id": 811583091, "app_code_type": 0, "app_id": 5011583, "app_name": "Game One", "ad_slot_type": 5,
                "package_name": "com.ezg.one", "media_name": "EZG", "code_name": "RV_1", "os": "android",
                "click": 2, "click_rate": 0.25, "currency": q.get("currency"), "date": d.isoformat(), "ecpm": 7.71,
                "fill_rate": 100, "region": region, "request": 8, "return": 8, "revenue": 0.06123456789, "show": 8,
                "time_zone": int(q.get("time_zone", 8)),
                "ad_slot_type_v2": 0, "media_m_ssr": 0, "win_rate": 0.1234567,
            }
            if state["mode"] == "no_slot":
                row["rit"] = row.pop("ad_slot_id")
            rows.append(row)
        if state["mode"] == "dup":
            rows.append(dict(rows[0]))
        if state["mode"] == "huge":
            rows = [dict(rows[0], ad_slot_id=i) for i in range(100000)]
        return self._json(200, {"Code": "100", "Message": "", "Data": {d.isoformat(): rows}})


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


def leaked(proc):
    out = proc.stdout + proc.stderr
    if "fake_security_key" in out or "wrong_security_key" in out:
        return True
    return any(s and s in out for s in state["signs"])


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"PANGLE_URL": f"http://127.0.0.1:{server.server_port}/union_pangle/open/api/rt/income", "PANGLE_RETRY_BASE_SEC": "0.2"}
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
            {"account_name": "pangle_1", "user_id": "459", "role_id": "459", "security_key": "fake_security_key_1"},
            {"account_name": "pangle_2", "user_id": "777", "role_id": "778", "security_key": "fake_security_key_2"},
        ],
        "start_date": START.isoformat(),
        "number_days_backward": 14,
        "window_days": 7,
    }
    cfg = write("config.json", config)
    catalog = os.path.join(ROOT, "integration_tests", "configured_catalog.json")
    pk = ["account_name", "date", "app_id", "ad_slot_id", "region"]

    # spec
    proc, msgs = run(["spec"], env)
    spec = next((m["spec"] for m in msgs if m.get("type") == "SPEC"), {})
    props = spec.get("connectionSpecification", {}).get("properties", {})
    acc = props.get("accounts", {}).get("items", {}).get("properties", {})
    check("spec co accounts / start_date / number_days_backward", {"accounts", "start_date", "number_days_backward"} <= set(props),
          proc.stderr[-300:])
    check("spec lookback mac dinh 14, currency mac dinh usd", props.get("number_days_backward", {}).get("default") == 14
          and props.get("currency", {}).get("default") == "usd")
    check("spec security_key la airbyte_secret", acc.get("security_key", {}).get("airbyte_secret") is True)

    # discover
    proc, msgs = run(["discover", "--config", os.path.join(ROOT, "secrets", "test_config.json")], env)
    streams = next((m["catalog"]["streams"] for m in msgs if m.get("type") == "CATALOG"), [])
    check("discover 1 stream dung ten", [s["name"] for s in streams] == [STREAM], proc.stderr[-300:])
    if streams:
        _props = streams[0]["json_schema"]["properties"]
        check("discover schema co 3 cot moi, khong co app_code_type",
              all(c in _props for c in ("ad_slot_type_v2", "media_m_ssr", "win_rate")) and "app_code_type" not in _props)
        check("discover PK", [p[0] for p in streams[0].get("source_defined_primary_key", [])] == pk,
              str(streams[0].get("source_defined_primary_key")))
        check("discover cursor date + incremental", streams[0].get("default_cursor_field") == ["date"]
              and "incremental" in streams[0].get("supported_sync_modes", []))

    # check
    proc, msgs = run(["check", "--config", cfg], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key dung -> SUCCEEDED (chu ky md5 khop, da retry 106 + 500)", status.get("status") == "SUCCEEDED"
          and state["sent_106"] and state["sent_500"], str(status))
    bad = json.loads(json.dumps(config))
    bad["accounts"][1]["security_key"] = "wrong_security_key"
    proc, msgs = run(["check", "--config", write("bad.json", bad)], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key sai -> FAILED, neu ro account + Code 101", status.get("status") == "FAILED" and "pangle_2" in status.get("message", "")
          and "101" in status.get("message", ""), str(status))
    check("check key sai: khong lo security_key / sign", not leaked(proc))

    # read full
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    recs = records(msgs)
    # 21 ngay x 3 region x 2 account, tru 1 ngay (START) cua account 2 khong co du lieu = 126 - 3
    check("read full: so record = 123", len(recs) == 123, f"{len(recs)} | rc={proc.returncode} | {proc.stderr[-400:]}")
    keys = [tuple(r.get(k) for k in pk) for r in recs]
    check("read full: PK unique", len(set(keys)) == len(keys))
    check("read full: moi request 1 ngay, time_zone=0, currency=usd", len(state["requests"]) == 42
          and all(r["tz"] == "0" and r["currency"] == "usd" for r in state["requests"]), str(len(state["requests"])))
    check("read full: id la chuoi, revenue round 6, so dem la int",
          all(r["app_id"] == "5011583" and r["ad_slot_id"] == "811583091" and r["revenue"] == 0.061235 and r["show"] == 8 for r in recs))
    check("read full: state cuoi = hom nay (UTC)", last_state(msgs) == TODAY.isoformat(), str(last_state(msgs)))
    check("read full: khong lo security_key / sign", not leaked(proc))

    # incremental
    st = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": STREAM},
                                        "stream_state": {"date": (TODAY - timedelta(days=3)).isoformat()}}}]
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("state.json", st)], env)
    first = min(r["date"] for r in state["requests"]) if state["requests"] else None
    check("read incremental: lui 14 ngay tu cursor", first == TODAY - timedelta(days=17), str(first))
    check("read incremental: so record = 108", len(records(msgs)) == 108, str(len(records(msgs))))  # 18 x 3 x 2
    check("read incremental: state cuoi = hom nay", last_state(msgs) == TODAY.isoformat())

    # account 2 loi tu ngay t-5 -> state dung o cuoi cua so account 2 doc xong (t-7)
    state["fail_777_from"] = TODAY - timedelta(days=5)
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    check("account 2 loi: sync bao loi, state = t-7", errored(proc, msgs) and last_state(msgs) == (TODAY - timedelta(days=7)).isoformat(),
          f"rc={proc.returncode} state={last_state(msgs)}")
    check("account 2 loi: khong lo security_key / sign", not leaked(proc))
    state["fail_777_from"] = None

    # cursor tuong lai
    st[0]["stream"]["stream_state"]["date"] = (TODAY + timedelta(days=30)).isoformat()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("future.json", st)], env)
    check("cursor tuong lai: khong crash, state = hom nay", proc.returncode == 0 and last_state(msgs) == TODAY.isoformat(),
          f"rc={proc.returncode} state={last_state(msgs)}")

    # chot chan
    for mode, needle, label in (("no_slot", "thieu cot khoa ['ad_slot_id']", "thieu cot khoa"),
                                ("dup", "trung khoa chinh", "trung khoa trong 1 ngay"),
                                ("huge", "cham gioi han 100000", "cham 100.000 dong")):
        state["mode"] = mode
        proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
        check(f"{label}: sync bao loi, 0 record", errored(proc, msgs) and needle in proc.stdout + proc.stderr and not records(msgs),
              f"rc={proc.returncode} recs={len(records(msgs))}")
    state["mode"] = ""

    server.shutdown()
    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
