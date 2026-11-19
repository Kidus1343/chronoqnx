#!/usr/bin/env python3
"""
ChronoQNX CI latency gate.

Reads thresholds from chrono.toml, captures a window of IPC samples from the
chrono_agent stream (127.0.0.1:9000), prints a deterministic PASS/FAIL table
and exits 0 (PASS) or 1 (FAIL).  Fails closed: no / too little data == FAIL.

  python ci_gate.py                          # evaluate whatever the target is running
  python ci_gate.py --launch normal          # start ipc_bench (via ssh) and gate on it
  python ci_gate.py --launch invert --json out.json

Standard library only (Python 3.8+; TOML via tomllib on 3.11+, tiny fallback otherwise).
License: Apache-2.0
"""
import argparse
import json
import math
import os
import re
import socket
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))

# key in [thresholds] -> (label, stat key, unit). Evaluated only when present.
CHECKS = [
    ("max_p50_us",              "p50 RTT",              "p50",        "us"),
    ("max_p95_us",              "p95 RTT",              "p95",        "us"),
    ("max_p99_us",              "p99 RTT",              "p99",        "us"),
    ("max_p999_us",             "p99.9 RTT",            "p999",       "us"),
    ("max_wcet_us",             "WCET (max RTT)",       "wcet",       "us"),
    ("max_jitter_us",           "mean RTT jitter",      "jitter",     "us"),
    ("max_priority_inversions", "priority inversions",  "inversions", ""),
]


def load_toml(path):
    try:
        import tomllib
        with open(path, "rb") as f:
            return tomllib.load(f)
    except ImportError:
        pass
    cfg, sect = {}, None                       # minimal fallback: [section] + key = scalar
    with open(path, encoding="utf-8") as f:
        for raw in f:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            m = re.match(r"\[(\w+)\]$", line)
            if m:
                sect = cfg.setdefault(m.group(1), {})
                continue
            k, _, v = line.partition("=")
            v = v.strip()
            if v.startswith('"'):
                val = v.strip('"')
            else:
                try:
                    val = int(v)
                except ValueError:
                    val = float(v)
            (sect if sect is not None else cfg)[k.strip()] = val
    return cfg


def percentile(sorted_vals, p):
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = int(math.ceil(p / 100.0 * n)) - 1
    return sorted_vals[min(max(idx, 0), n - 1)]


def capture(host, port, window_s, max_wait_s, on_connected=None):
    """Collect ipc records. Returns (records, meta). Stops at 'end' frame or window."""
    deadline_connect = time.monotonic() + max_wait_s
    s = None
    while s is None:
        try:
            s = socket.create_connection((host, port), timeout=3)
        except OSError as e:
            if time.monotonic() > deadline_connect:
                return [], {"mode": None, "pid": None, "sched_inversions": 0, "ended": False,
                            "agent": None,
                            "error": "cannot connect to %s:%d (%s)" % (host, port, e)}
            time.sleep(0.5)
    s.settimeout(1.0)
    if on_connected:
        on_connected()

    records, meta = [], {"mode": None, "pid": None, "sched_inversions": 0,
                         "ended": False, "agent": None}
    first_ts = None
    t_start = time.monotonic()
    buf = b""
    try:
        while True:
            now = time.monotonic()
            if first_ts is None and now - t_start > max_wait_s:
                meta["error"] = "no IPC samples within %.0f s (is ipc_bench running?)" % max_wait_s
                break
            if first_ts is not None and now - first_ts > window_s:
                break
            try:
                chunk = s.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                meta["error"] = "agent closed the connection"
                break
            buf += chunk
            *lines, buf = buf.split(b"\n")
            for ln in lines:
                try:
                    d = json.loads(ln)
                except ValueError:
                    continue
                t = d.get("t")
                if t == "agent":
                    meta["agent"] = d
                elif t == "hello":
                    # a fresh run starts: discard anything captured from an earlier run
                    if meta["pid"] is not None and d.get("pid") != meta["pid"]:
                        records.clear()
                        first_ts = None
                        meta["sched_inversions"] = 0
                    meta["pid"], meta["mode"] = d.get("pid"), d.get("mode")
                elif t == "ipc":
                    if first_ts is None:
                        first_ts = time.monotonic()
                    records.append(d)
                elif t == "sched":
                    meta["sched_inversions"] = max(meta["sched_inversions"],
                                                   int(d.get("inv_total", 0)))
                elif t == "end" and records:
                    meta["ended"] = True
            if meta["ended"]:
                break
    finally:
        s.close()
    return records, meta


def compute(records, warmup):
    recs = records[warmup:] if len(records) > warmup else []
    rtts = sorted(float(r["rtt_us"]) for r in recs)
    jit = [float(r.get("jitter_us", 0.0)) for r in recs]
    return {
        "samples": len(recs),
        "p50": percentile(rtts, 50), "p95": percentile(rtts, 95),
        "p99": percentile(rtts, 99), "p999": percentile(rtts, 99.9),
        "wcet": rtts[-1] if rtts else 0.0,
        "jitter": sum(jit) / len(jit) if jit else 0.0,
        "inversions": sum(1 for r in recs if int(r.get("inversion_flag", 0))),
    }


def evaluate(stats, thresholds, min_samples):
    rows, ok = [], True
    if stats["samples"] < min_samples:
        ok = False
    for key, label, stat, unit in CHECKS:
        if key not in thresholds:
            continue
        limit, val = thresholds[key], stats[stat]
        passed = stats["samples"] >= min_samples and val <= limit
        ok = ok and passed
        rows.append((label, val, limit, unit, passed))
    return rows, ok


def fmt(v, unit):
    return ("%d" % v) if unit == "" else ("%.1f %s" % (v, unit))


def report(stats, rows, ok, meta, cfg_path, min_samples):
    out = []
    w = 78
    out.append("=" * w)
    out.append(" ChronoQNX CI latency gate".ljust(w - 28) + ("mode=%s pid=%s" % (meta["mode"], meta["pid"])).rjust(28))
    out.append(" config: %s" % cfg_path)
    out.append("=" * w)
    out.append(" %-26s %16s %16s   %s" % ("CHECK", "MEASURED", "LIMIT", "RESULT"))
    out.append("-" * w)
    sample_ok = stats["samples"] >= min_samples
    out.append(" %-26s %16d %16s   %s" % ("samples captured", stats["samples"],
                                          ">= %d" % min_samples, "PASS" if sample_ok else "FAIL"))
    for label, val, limit, unit, passed in rows:
        out.append(" %-26s %16s %16s   %s" % (label, fmt(val, unit), "<= " + fmt(limit, unit),
                                              "PASS" if passed else "FAIL"))
    out.append("-" * w)
    out.append(" info: agent /proc scheduler snapshots flagged %d priority-inversion event(s)"
               % meta["sched_inversions"])
    out.append("=" * w)
    out.append(" GATE RESULT: %s" % ("PASS" if ok else "FAIL"))
    out.append("=" * w)
    return "\n".join(out)


def ssh_run(host, command, background=False):
    args = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", host, command]
    if background:
        return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    return subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def main():
    ap = argparse.ArgumentParser(description="ChronoQNX CI latency gate")
    ap.add_argument("--config", default=os.path.join(HERE, "chrono.toml"))
    ap.add_argument("--host")
    ap.add_argument("--port", type=int)
    ap.add_argument("--window", type=float, help="override capture window (s)")
    ap.add_argument("--launch", choices=["normal", "invert"],
                    help="start ipc_bench in this mode on the target via ssh, then gate on it")
    ap.add_argument("--json", help="also write the result as JSON to this file")
    args = ap.parse_args()

    try:
        cfg = load_toml(args.config)
    except (OSError, ValueError) as e:
        print("GATE RESULT: FAIL - cannot read config %s: %s" % (args.config, e))
        return 1
    th = cfg.get("thresholds", {})
    cap = cfg.get("capture", {})
    tgt = cfg.get("target", {})
    host = args.host or cap.get("host", "127.0.0.1")
    port = args.port or int(cap.get("port", 9000))
    window = args.window or float(cap.get("window_s", 20))
    max_wait = float(cap.get("max_wait_s", 25))
    min_samples = int(cap.get("min_samples", 200))
    warmup = int(cap.get("warmup_samples", 10))
    if not any(k in th for k, *_ in CHECKS):
        print("GATE RESULT: FAIL - no thresholds defined in %s" % args.config)
        return 1

    proc = None

    def launch():
        nonlocal proc
        d = tgt.get("dir", "/data/home/root")
        sh = tgt.get("ssh_host", "qnx-qemu")
        # make sure the agent is up (idempotent), then start the workload
        ssh_run(sh, "cd %s && chmod +x chrono_agent ipc_bench && "
                    "(pidin -F '%%n' | grep -q chrono_agent || "
                    "(nohup ./chrono_agent >agent.log 2>&1 </dev/null &)); sleep 1" % d)
        time.sleep(1.0)
        cmd = "cd %s && ./ipc_bench --mode %s --rate %d --duration %d --quiet" % (
            d, args.launch, int(tgt.get("rate_hz", 200)), int(tgt.get("duration_s", 10)))
        proc = ssh_run(sh, cmd, background=True)

    t0 = time.monotonic()
    if args.launch:
        # connect first so no sample is missed, then start the workload
        records, meta = capture(host, port, window, max_wait, on_connected=launch)
        if proc:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
    else:
        records, meta = capture(host, port, window, max_wait)

    stats = compute(records, warmup)
    rows, ok = evaluate(stats, th, min_samples)
    print(report(stats, rows, ok, meta, os.path.relpath(args.config), min_samples))
    if meta.get("error"):
        print(" note: %s" % meta["error"])
    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump({"pass": ok, "mode": meta["mode"], "stats": stats,
                       "checks": [{"name": r[0], "value": r[1], "limit": r[2], "pass": r[4]}
                                  for r in rows],
                       "sched_inversions": meta["sched_inversions"],
                       "elapsed_s": round(time.monotonic() - t0, 1)}, f, indent=2)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
