#!/usr/bin/env python3
"""End-to-end smoke test: speaks JSONL to the built sidecar, verifies methods
against a real Flink JobManager (default http://10.137.154.193:8081).

Usage:
  python scripts/smoke_test.py --bin /tmp/dbx-plugin-flink-studio.exe --host 10.137.154.193 --port 8081
"""
import argparse
import json
import subprocess
import sys
import time

CONN = {
    "connection_id": "flink-smoke",
    "connection": {
        "id": "flink-smoke",
        "name": "Flink 193",
        "host": "10.137.154.193",
        "port": 8081,
        "scheme": "http",
        "read_only": True,
    },
}


def rpc(proc, req_id, method, params):
    line = json.dumps({"jsonrpc": "2.0", "id": req_id, "method": method, "params": params}, ensure_ascii=False)
    proc.stdin.write((line + "\n").encode("utf-8"))
    proc.stdin.flush()
    deadline = time.time() + 20
    while time.time() < deadline:
        raw = proc.stdout.readline()
        if not raw:
            break
        msg = json.loads(raw.decode("utf-8", "replace"))
        if msg.get("id") == req_id:
            return msg
    return {"error": {"message": "no response"}}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--host", default="10.137.154.193")
    ap.add_argument("--port", type=int, default=8081)
    args = ap.parse_args()

    conn = json.loads(json.dumps(CONN))
    conn["connection"]["host"] = args.host
    conn["connection"]["port"] = args.port

    proc = subprocess.Popen([args.bin], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    failures = []
    try:
        init = rpc(proc, 0, "plugin/initialize", {"host": {"protocolVersions": [1]}})
        assert "error" not in init, init

        def call(i, method, params=None):
            p = dict(conn)
            p.update(params or {})
            r = rpc(proc, i, method, p)
            if "error" in r:
                failures.append(f"{method}: {r['error']}")
                return None
            return r.get("result")

        ping = call(1, "flink/ping")
        print("flink/ping      =>", ping)
        assert ping and ping.get("pong"), "ping failed"

        test = call(2, "flink/test")
        print("flink/test      =>", test)
        assert test and test.get("ok"), "connection test failed"

        overview = call(3, "flink/overview")
        print("flink/overview  =>", {k: overview.get(k) for k in ("flink-version", "taskmanagers", "jobs-running")} if overview else None)
        assert overview and "flink-version" in overview

        jobs = call(4, "flink/jobs")
        print("flink/jobs      =>", len(jobs.get("jobs", [])) if jobs else 0, "jobs")

        tms = call(5, "flink/taskmanagers")
        print("flink/taskmanagers =>", len(tms.get("taskmanagers", [])) if tms else 0, "TMs")
        assert tms and tms.get("taskmanagers")

        tm = tms["taskmanagers"][0]["id"]
        logs = call(6, "flink/taskmanager-logs", {"taskManager": tm})
        out_files = [l["name"] for l in logs.get("logs", [])] if logs else []
        print("taskmanager-logs =>", out_files)
        assert any(n.endswith(".out") for n in out_files), "no .out log found"

        msgs = call(7, "flink/messages", {"limit": 5})
        print("flink/messages  =>", msgs.get("total") if msgs else None, "rows, files:", msgs.get("filesScanned") if msgs else None)
        assert msgs and msgs.get("total", 0) > 0, "no print-sink rows parsed"
        print("  sample:", msgs["rows"][-1] if msgs.get("rows") else None)

        cfg = call(8, "flink/config")
        print("flink/config    =>", len(cfg) if isinstance(cfg, list) else type(cfg).__name__)

        bad = rpc(proc, 9, "flink/request", {**conn, "path": "/../etc/passwd"})
        print("path traversal  =>", "error" in bad, "(expected True)")
        assert "error" in bad, "path traversal guard failed"
    finally:
        try:
            proc.stdin.close()
        except Exception:
            pass
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

    if failures:
        print("FAILED:")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("ALL SMOKE TESTS PASSED")


if __name__ == "__main__":
    main()
