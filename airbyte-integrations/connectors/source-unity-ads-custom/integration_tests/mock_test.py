#
# Test source-unity-ads-custom voi server gia (khong can key that, khong goi Unity).
# Chay tu thu muc connector:  python integration_tests/mock_test.py
# Kiem: spec, discover, check (key dung / sai), read full, read incremental + lookback,
#       clamp cursor tuong lai, retry 429, slice rong, loc row ngoai cua so, khong lo key ra stdout.
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
STREAM = "unity_ads_monetization"
KEYS = {"1111111111111": "fake_key_1", "2222222222222": "fake_key_2"}
TODAY = datetime.now(timezone.utc).date()
START = TODAY - timedelta(days=20)
HEADER = "timestamp,country,placement,platform,source_game_id,source_name,adrequest_count,available_sum,revenue_sum,start_count,view_count"

state = {"sent_429": False, "requests": []}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _reply(self, code, body="", ctype="text/csv", headers=None):
        data = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        prefix = "/stats/v1/operate/organizations/"
        if not url.path.startswith(prefix):
            return self._reply(404, '{"errors":[{"msg":"not found"}]}', "application/json")
        org = url.path[len(prefix):]
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        state["requests"].append((org, q.get("start"), q.get("end")))
        if org not in KEYS:
            return self._reply(404, '{"errors":[{"msg":"organization not found"}]}', "application/json")
        if self.headers.get("Authorization") != f"Token {KEYS[org]}":
            return self._reply(401, '{"errors":[{"msg":"invalid api key"}]}', "application/json")
        for required in ("fields", "scale", "start", "end"):
            if required not in q:
                return self._reply(400, '{"errors":[{"msg":"missing %s"}]}' % required, "application/json")
        start = date.fromisoformat(q["start"][:10])
        end = date.fromisoformat(q["end"][:10])
        # 1 lan 429 dau tien cho org 1 khi doc du lieu (co groupBy) -> test retry
        if org == "1111111111111" and "groupBy" in q and not state["sent_429"]:
            state["sent_429"] = True
            return self._reply(429, '{"errors":[{"msg":"rate limit"}]}', "application/json", {"Retry-After": "1"})
        # org 2: cua so dau tien (bat dau dung START) rong -> test slice rong van tien cursor
        if org == "2222222222222" and start == START:
            return self._reply(200, "")
        lines = [HEADER.replace("source_game_id", "sgid") if state.get("alt_header") else HEADER]
        d = start
        while d <= end and d <= TODAY:
            ts = f"{d.isoformat()}T00:00:00.000Z"
            lines.append(f'{ts},US,Rewarded_Android,android,500{org[:1]},"Game, One",100,90,1.23456789,80,70')
            lines.append(f'{ts},VN,Rewarded_Android,android,500{org[:1]},"Game, One",10,,0,,')  # metric rong -> 0
            d += timedelta(days=1)
        # 1 row ngoai cua so (ngay sau `end`) -> connector phai loc bo
        lines.append(f'{(end + timedelta(days=1)).isoformat()}T00:00:00.000Z,US,Rewarded_Android,android,500{org[:1]},"Game, One",1,1,9.99,1,1')
        return self._reply(200, "\n".join(lines) + "\n")


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


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"UNITY_ADS_URL_BASE": f"http://127.0.0.1:{server.server_port}/"}
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
            {"account_name": "unity_1", "organization_core_id": "1111111111111", "api_key": "fake_key_1"},
            {"account_name": "unity_2", "organization_core_id": "2222222222222", "api_key": "fake_key_2"},
        ],
        "start_date": START.isoformat(),
        "number_days_backward": 14,
        "window_days": 7,
    }
    cfg = write("config.json", config)
    catalog = os.path.join(ROOT, "integration_tests", "configured_catalog.json")
    pk = ["account_name", "date", "country", "placement", "platform", "source_game_id"]

    # spec
    proc, msgs = run(["spec"], env)
    spec = next((m["spec"] for m in msgs if m.get("type") == "SPEC"), {})
    props = spec.get("connectionSpecification", {}).get("properties", {})
    check("spec co accounts / start_date / number_days_backward", {"accounts", "start_date", "number_days_backward"} <= set(props))
    check("spec lookback mac dinh 14", props.get("number_days_backward", {}).get("default") == 14)
    check("spec api_key la airbyte_secret", props["accounts"]["items"]["properties"]["api_key"].get("airbyte_secret") is True)

    # discover
    proc, msgs = run(["discover", "--config", os.path.join(ROOT, "secrets", "test_config.json")], env)
    streams = next((m["catalog"]["streams"] for m in msgs if m.get("type") == "CATALOG"), [])
    check("discover 1 stream dung ten", [s["name"] for s in streams] == [STREAM], proc.stderr[-300:])
    if streams:
        check("discover PK", [p[0] for p in streams[0].get("source_defined_primary_key", [])] == pk,
              str(streams[0].get("source_defined_primary_key")))
        check("discover cursor date", streams[0].get("default_cursor_field") == ["date"])

    # check: key dung
    proc, msgs = run(["check", "--config", cfg], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key dung -> SUCCEEDED", status.get("status") == "SUCCEEDED", str(status))
    # check: key sai
    bad = json.loads(json.dumps(config))
    bad["accounts"][1]["api_key"] = "wrong_key_xyz"
    proc, msgs = run(["check", "--config", write("bad.json", bad)], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check key sai -> FAILED, neu ro account", status.get("status") == "FAILED" and "unity_2" in status.get("message", ""), str(status))
    check("check key sai khong lo key", "wrong_key_xyz" not in proc.stdout and "wrong_key_xyz" not in proc.stderr)

    # read full (khong state)
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    recs = records(msgs)
    # 21 ngay x 2 row x 2 account, tru cua so dau (7 ngay x 2 row) cua unity_2 rong
    check("read full: so record = 70", len(recs) == 70, f"{len(recs)} | rc={proc.returncode} | {proc.stderr[-400:]}")
    keys = [tuple(r.get(k) for k in pk) for r in recs]
    check("read full: PK unique", len(set(keys)) == len(keys))
    check("read full: da retry sau 429", state["sent_429"] and proc.returncode == 0)
    check("read full: row ngoai cua so bi loc", all(r["revenue_sum"] != 9.99 for r in recs))
    check("read full: metric rong -> 0", all(r["start_count"] == 0 for r in recs if r["country"] == "VN"))
    check("read full: revenue round 6", all(r["revenue_sum"] == 1.234568 for r in recs if r["country"] == "US"))
    check("read full: ten game co dau phay doc dung", all(r["source_name"] == "Game, One" for r in recs))
    check("read full: account_name + organization_id", {(r["account_name"], r["organization_id"]) for r in recs}
          == {("unity_1", "1111111111111"), ("unity_2", "2222222222222")})
    check("read full: state cuoi = hom nay (UTC)", last_state(msgs) == TODAY.isoformat(), str(last_state(msgs)))
    check("read full: khong lo key ra stdout/stderr", "fake_key_" not in proc.stdout and "fake_key_" not in proc.stderr)

    # read incremental: cursor = today-3 -> bat dau tu today-17
    st = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": STREAM},
                                        "stream_state": {"date": (TODAY - timedelta(days=3)).isoformat()}}}]
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("state.json", st)], env)
    recs = records(msgs)
    first_start = min(r[1] for r in state["requests"])[:10] if state["requests"] else None
    check("read incremental: lui 14 ngay tu cursor", first_start == (TODAY - timedelta(days=17)).isoformat(), str(first_start))
    check("read incremental: so record = 72", len(recs) == 72, f"{len(recs)} | {proc.stderr[-300:]}")  # 18 ngay x 2 x 2
    check("read incremental: state cuoi = hom nay", last_state(msgs) == TODAY.isoformat())

    # cursor tuong lai -> clamp, khong crash
    st[0]["stream"]["stream_state"]["date"] = (TODAY + timedelta(days=30)).isoformat()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("future.json", st)], env)
    check("cursor tuong lai: khong crash, state = hom nay", proc.returncode == 0 and last_state(msgs) == TODAY.isoformat(),
          f"rc={proc.returncode} state={last_state(msgs)}")

    # response thieu cot khoa (ten cot khac gia dinh) -> dung han, khong nap record nao
    state["alt_header"] = True
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    failed = proc.returncode != 0 or any(m.get("type") == "TRACE" and m["trace"].get("type") == "ERROR" for m in msgs)
    check("thieu cot khoa: sync bao loi, neu ten cot, 0 record",
          failed and "thieu cot khoa" in (proc.stdout + proc.stderr) and "sgid" in (proc.stdout + proc.stderr) and not records(msgs),
          f"rc={proc.returncode} recs={len(records(msgs))}")
    state["alt_header"] = False

    # parser JSON
    sys.path.insert(0, ROOT)
    from source_unity_ads.source import _parse_body

    class FakeResp:
        status_code = 200
        text = '{"data":[{"Timestamp":"2026-11-01T00:00:00Z","Game ID":"5001","revenue_sum":1.5}]}'

        def json(self):
            return json.loads(self.text)

    rows = _parse_body(FakeResp())
    check("parser JSON + alias cot", rows == [{"timestamp": "2026-11-01T00:00:00Z", "source_game_id": "5001", "revenue_sum": 1.5}], str(rows))

    server.shutdown()
    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
