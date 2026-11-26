#
# Test source-meta-audience-network-custom voi server gia (khong can token that, khong goi Meta).
# Chay tu thu muc connector:  python integration_tests/mock_test.py
# Kiem: spec, discover (3 stream co dinh), check (token dung / system user / het han), token chi nam trong header,
#       gop dong dai -> 1 record / o ke ca khi o nam o 2 trang, ngay theo gio Pacific, revenue bi an -> revenue_omitted,
#       stream by_platform du doanh thu, read incremental + lookback, clamp 540 ngay, clamp cursor tuong lai,
#       retry loi tam thoi, slice rong, account loi -> state khong vuot, trung o thi dung, khong lo token,
#       config cu con `breakdowns` thi bo qua.
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
from zoneinfo import ZoneInfo

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BY_PLATFORM = "meta_audience_network_by_platform"
BY_APP = "meta_audience_network_by_app"
BY_APP_COUNTRY = "meta_audience_network_by_app_country"
ALL_STREAMS = (BY_PLATFORM, BY_APP, BY_APP_COUNTRY)
PT = ZoneInfo("America/Los_Angeles")
TOKENS = {"fake_token_1": "100000000000001", "fake_token_2": "100000000000002"}
TODAY = datetime.now(PT).date()
START = TODAY - timedelta(days=20)
# 3 o chi tiet nhat; request it chieu hon thi server gop lai: platform -> 2 o, property x platform -> 2 o
CELLS = [
    {"property": "111", "platform": "android", "country": "US"},
    {"property": "111", "platform": "android", "country": "VN"},
    {"property": "222", "platform": "ios", "country": "US"},
]
VALUES = {"fb_ad_network_request": "120", "fb_ad_network_filled_request": "100", "fb_ad_network_imp": "90",
          "fb_ad_network_click": "4", "fb_ad_network_revenue": "1.23456789"}

state = {"sent_transient": False, "requests": [], "mode": "", "page_size": 37, "token_in_url": False, "fail_biz2_from": None}


def pt_midnight_utc(d):
    return datetime(d.year, d.month, d.day, tzinfo=PT).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+0000")


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

    def _error(self, status, code, message, subcode=None):
        return self._json(status, {"error": {"message": message, "type": "OAuthException", "code": code,
                                             "error_subcode": subcode, "fbtrace_id": "x"}})

    def do_GET(self):
        url = urlparse(self.path)
        parts = url.path.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "v27.0" or parts[2] != "adnetworkanalytics":
            return self._error(404, 803, "unknown path")
        biz = parts[1]
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if "access_token" in q:
            state["token_in_url"] = True
        auth = self.headers.get("Authorization", "")
        token = auth[len("Bearer "):] if auth.startswith("Bearer ") else ""
        if token == "system_user_token":
            return self._error(400, 100, "The way to access reporting API v2.0 has changed. You now need to implement Facebook Login for your app to access this API.")
        if token == "expired_token":
            return self._error(400, 190, "Error validating access token: Session has expired.", 463)
        if TOKENS.get(token) != biz:
            return self._error(400, 190, "Invalid OAuth access token.")
        metrics = json.loads(q["metrics"])
        breakdowns = json.loads(q["breakdowns"]) if "breakdowns" in q else []
        since, until = date.fromisoformat(q["since"]), date.fromisoformat(q["until"])
        state["requests"].append({"biz": biz, "since": since, "until": until, "breakdowns": breakdowns, "after": q.get("after")})
        if (until - since).days > 8:
            return self._error(400, 100, "Please reduce the amount of data you're asking for, then retry your request.")
        if since < TODAY - timedelta(days=539):
            return self._error(400, 100, "since is out of retention")
        if q.get("limit") == "1":  # probe cua check_connection
            return self._json(200, {"data": [{"query_id": "q", "results": [], "omitted_results": []}]})
        if not state["sent_transient"]:
            state["sent_transient"] = True
            return self._error(400, 4, "Application request limit reached")
        if biz.endswith("2") and state["fail_biz2_from"] and since >= state["fail_biz2_from"]:
            return self._error(400, 100, "simulated failure")
        items = []
        if not (biz.endswith("2") and since == START):  # account 2: cua so dau rong
            d = since
            while d < until and d <= TODAY:
                t = pt_midnight_utc(d)
                cells = []
                for c in CELLS:   # gop theo dung cac chieu duoc xin, giu thu tu
                    grouped = {b: c[b] for b in breakdowns}
                    if grouped not in cells:
                        cells.append(grouped)
                for cell in cells:
                    bd = [{"key": b, "value": cell[b]} for b in breakdowns]
                    for m in metrics:
                        # o nho (VN) bi an doanh thu, chi khi tach toi quoc gia
                        if cell.get("country") == "VN" and m == "fb_ad_network_revenue":
                            items.append(("omitted_results", {"time": t, "metric": m, "breakdowns": bd}))
                        else:
                            items.append(("results", {"time": t, "metric": m, "breakdowns": bd, "value": VALUES[m]}))
                d += timedelta(days=1)
        if state["mode"] == "dup" and items:
            items.append(items[0])
        offset = int(q.get("after") or 0)
        chunk = items[offset: offset + state["page_size"]]
        block = {"query_id": "q", "results": [i for k, i in chunk if k == "results"],
                 "omitted_results": [i for k, i in chunk if k == "omitted_results"]}
        body = {"data": [block]}
        nxt = offset + state["page_size"]
        if nxt < len(items):
            body["paging"] = {"cursors": {"before": str(offset), "after": str(nxt)},
                              "next": f"https://graph.facebook.com/v27.0/{biz}/adnetworkanalytics?access_token=SHOULD_NOT_BE_USED&after={nxt}"}
        return self._json(200, body)


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


def last_states(messages):
    out = {}
    for m in messages:
        if m.get("type") != "STATE":
            continue
        s = m["state"]
        if s.get("stream"):
            out[s["stream"]["stream_descriptor"]["name"]] = s["stream"]["stream_state"].get("date")
        else:
            for name, st in (s.get("data") or {}).items():
                out[name] = (st or {}).get("date")
    return out


def records(messages, stream=None):
    return [m["record"]["data"] for m in messages if m.get("type") == "RECORD" and (stream is None or m["record"]["stream"] == stream)]


def errored(proc, msgs):
    return proc.returncode != 0 or any(m.get("type") == "TRACE" and m["trace"].get("type") == "ERROR" for m in msgs)


def leaked(proc):
    out = proc.stdout + proc.stderr
    return any(x in out for x in ("fake_token_", "system_user_token", "expired_token", "SHOULD_NOT_BE_USED"))


def main():
    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    env = {"META_AN_URL_BASE": f"http://127.0.0.1:{server.server_port}/", "META_AN_RETRY_BASE_SEC": "0.2"}
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
            {"account_name": "meta_1", "business_id": "100000000000001", "access_token": "fake_token_1"},
            {"account_name": "meta_2", "business_id": "100000000000002", "access_token": "fake_token_2"},
        ],
        "start_date": START.isoformat(),
        "number_days_backward": 14,
        "window_days": 7,
    }
    cfg = write("config.json", config)
    catalog = os.path.join(ROOT, "integration_tests", "configured_catalog.json")
    pks = {
        BY_PLATFORM: ["account_name", "date", "platform"],
        BY_APP: ["account_name", "date", "property", "platform"],
        BY_APP_COUNTRY: ["account_name", "date", "property", "platform", "country"],
    }

    # spec
    proc, msgs = run(["spec"], env)
    spec = next((m["spec"] for m in msgs if m.get("type") == "SPEC"), {})
    props = spec.get("connectionSpecification", {}).get("properties", {})
    acc = props.get("accounts", {}).get("items", {}).get("properties", {})
    check("spec co accounts / start_date / number_days_backward", {"accounts", "start_date", "number_days_backward"} <= set(props),
          proc.stderr[-300:])
    check("spec lookback mac dinh 14", props.get("number_days_backward", {}).get("default") == 14)
    check("spec access_token la airbyte_secret", acc.get("access_token", {}).get("airbyte_secret") is True)
    check("spec khong con tuy chon breakdowns", "breakdowns" not in props)

    # discover
    proc, msgs = run(["discover", "--config", os.path.join(ROOT, "secrets", "test_config.json")], env)
    streams = {s["name"]: s for s in next((m["catalog"]["streams"] for m in msgs if m.get("type") == "CATALOG"), [])}
    check("discover 3 stream dung ten", set(streams) == set(ALL_STREAMS), proc.stderr[-300:])
    if len(streams) == 3:
        for name in ALL_STREAMS:
            check(f"discover PK {name}", [p[0] for p in streams[name]["source_defined_primary_key"]] == pks[name])
        check("discover cursor date + incremental", all(s.get("default_cursor_field") == ["date"]
              and "incremental" in s.get("supported_sync_modes", []) for s in streams.values()))

    # check
    proc, msgs = run(["check", "--config", cfg], env)
    status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
    check("check token dung -> SUCCEEDED", status.get("status") == "SUCCEEDED", str(status))
    for tok, needle, label in (("system_user_token", "Facebook Login", "token system user"), ("expired_token", "het han", "token het han")):
        bad = json.loads(json.dumps(config))
        bad["accounts"][1]["access_token"] = tok
        proc, msgs = run(["check", "--config", write("bad.json", bad)], env)
        status = next((m["connectionStatus"] for m in msgs if m.get("type") == "CONNECTION_STATUS"), {})
        check(f"check {label} -> FAILED, neu ro account + goi y", status.get("status") == "FAILED"
              and "meta_2" in status.get("message", "") and needle in status.get("message", ""), str(status))
        check(f"check {label}: khong lo token", not leaked(proc))

    # read full
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    plat, app, rep = records(msgs, BY_PLATFORM), records(msgs, BY_APP), records(msgs, BY_APP_COUNTRY)
    # 35 account-ngay (21 ngay x 2 account, tru 7 ngay dau cua account 2); by_platform 2 o, by_app 2 o, by_app_country 3 o
    check("read full: by_app_country = 105 record", len(rep) == 105, f"{len(rep)} | rc={proc.returncode} | {proc.stderr[-400:]}")
    check("read full: by_platform = 70, by_app = 70 record", len(plat) == 70 and len(app) == 70, f"{len(plat)} {len(app)}")
    unique = all(len({tuple(r.get(k) for k in pks[n]) for r in rs}) == len(rs)
                 for n, rs in ((BY_PLATFORM, plat), (BY_APP, app), (BY_APP_COUNTRY, rep)))
    check("read full: PK unique ca 3 stream (o nam o 2 trang van gop dung)", unique and any(r["after"] for r in state["requests"]))
    check("read full: moi stream gui dung breakdowns", {tuple(r["breakdowns"]) for r in state["requests"]} >= {
        ("platform",), ("property", "platform"), ("property", "platform", "country")})
    check("read full: by_platform chi co cot platform, by_app co property", all("property" not in r and "country" not in r for r in plat)
          and all(r.get("property") in ("111", "222") and "country" not in r for r in app))
    check("read full: du 5 metric / record, ten cot bo tien to", all(
        r["request"] == 120 and r["filled_request"] == 100 and r["imp"] == 90 and r["click"] == 4 for r in rep))
    check("read full: revenue round 6 o US", all(r["revenue"] == 1.234568 and r["revenue_omitted"] is False for r in rep if r["country"] == "US"))
    check("read full: revenue bi an -> null + revenue_omitted", all(
        r["revenue"] is None and r["revenue_omitted"] is True for r in rep if r["country"] == "VN"))
    check("read full: by_platform va by_app co revenue day du", all(
        r["revenue"] == 1.234568 and r["revenue_omitted"] is False for r in plat + app))
    dates = {r["date"] for r in rep if r["account_name"] == "meta_1"}
    check("read full: ngay theo gio Pacific, du 21 ngay", dates == {(START + timedelta(days=i)).isoformat() for i in range(21)})
    check("read full: co report_timezone + period_start_utc", all(
        r["report_timezone"] == "America/Los_Angeles" and r["period_start_utc"][11:13] in ("07", "08") for r in rep))
    check("read full: moi request <= 8 ngay", all((r["until"] - r["since"]).days <= 8 for r in state["requests"]))
    check("read full: da retry loi tam thoi (code 4)", state["sent_transient"] and proc.returncode == 0)
    check("read full: token khong nam trong URL, khong di theo paging.next", not state["token_in_url"])
    st = last_states(msgs)
    check("read full: state 3 stream = hom nay (Pacific)", all(st.get(n) == TODAY.isoformat() for n in ALL_STREAMS), str(st))
    check("read full: khong lo token", not leaked(proc))

    # config cu (tao tu v1.0.0) con truong `breakdowns` -> bo qua, van ra dung 3 stream
    legacy = dict(config, breakdowns=["placement", "country", "platform"])
    proc, msgs = run(["read", "--config", write("legacy.json", legacy), "--catalog", catalog], env)
    check("config cu con breakdowns: bo qua, van 105 / 70 / 70 record", proc.returncode == 0
          and len(records(msgs, BY_APP_COUNTRY)) == 105 and len(records(msgs, BY_PLATFORM)) == 70 and len(records(msgs, BY_APP)) == 70,
          f"rc={proc.returncode}")

    # incremental
    cur = (TODAY - timedelta(days=3)).isoformat()
    st_in = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": n}, "stream_state": {"date": cur}}} for n in ALL_STREAMS]
    state["requests"].clear()
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("state.json", st_in)], env)
    first = min(r["since"] for r in state["requests"]) if state["requests"] else None
    check("read incremental: lui 14 ngay tu cursor", first == TODAY - timedelta(days=17), str(first))
    # 36 account-ngay (18 ngay x 2 account)
    check("read incremental: so record = 108 + 72 + 72", len(records(msgs, BY_APP_COUNTRY)) == 108
          and len(records(msgs, BY_PLATFORM)) == 72 and len(records(msgs, BY_APP)) == 72,
          f"{len(records(msgs, BY_APP_COUNTRY))} {len(records(msgs, BY_PLATFORM))} {len(records(msgs, BY_APP))}")
    st = last_states(msgs)
    check("read incremental: state = hom nay", all(st.get(n) == TODAY.isoformat() for n in ALL_STREAMS))

    # account 2 loi tu cua so 3 -> state stream dung o t-7 (stream dau tien trong catalog bi loi truoc)
    state["fail_biz2_from"] = TODAY - timedelta(days=6)
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    st = last_states(msgs)
    check("account 2 loi: sync bao loi, state stream dau = t-7", errored(proc, msgs)
          and st.get(BY_PLATFORM) == (TODAY - timedelta(days=7)).isoformat(), f"rc={proc.returncode} {st}")
    check("account 2 loi: khong lo token", not leaked(proc))
    state["fail_biz2_from"] = None

    # cursor tuong lai
    fut = (TODAY + timedelta(days=30)).isoformat()
    st_in = [{"type": "STREAM", "stream": {"stream_descriptor": {"name": n}, "stream_state": {"date": fut}}} for n in ALL_STREAMS]
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog, "--state", write("future.json", st_in)], env)
    st = last_states(msgs)
    check("cursor tuong lai: khong crash, state = hom nay", proc.returncode == 0 and st.get(BY_APP_COUNTRY) == TODAY.isoformat(),
          f"rc={proc.returncode} {st}")

    # trung o (time, breakdowns, metric) -> dung
    state["mode"] = "dup"
    proc, msgs = run(["read", "--config", cfg, "--catalog", catalog], env)
    check("trung o: sync bao loi, 0 record", errored(proc, msgs) and "trung o" in proc.stdout + proc.stderr and not records(msgs),
          f"rc={proc.returncode} recs={len(records(msgs))}")
    state["mode"] = ""

    # clamp 540 ngay (chi stream by_platform, 1 account, trang lon cho nhanh)
    old = {"accounts": config["accounts"][:1], "start_date": (TODAY - timedelta(days=700)).isoformat(), "window_days": 7}
    only_total = json.load(open(catalog))
    only_total["streams"] = [s for s in only_total["streams"] if s["stream"]["name"] == BY_PLATFORM]
    state["requests"].clear()
    state["page_size"] = 5000
    proc, msgs = run(["read", "--config", write("old.json", old), "--catalog", write("total_catalog.json", only_total)], env)
    first = min(r["since"] for r in state["requests"]) if state["requests"] else None
    check("start_date 700 ngay truoc -> clamp ve 539 ngay", first == TODAY - timedelta(days=539) and proc.returncode == 0,
          f"{first} rc={proc.returncode} {proc.stderr[-300:]}")
    check("clamp 540 ngay: by_platform du 540 ngay x 2 platform = 1080 record", len(records(msgs, BY_PLATFORM)) == 1080,
          str(len(records(msgs, BY_PLATFORM))))

    server.shutdown()
    print(f"\n{'ALL PASS' if not failures else 'FAILED: ' + ', '.join(failures)}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
