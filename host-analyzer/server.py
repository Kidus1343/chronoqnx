#!/usr/bin/env python3
"""
ChronoQNX host analyzer - live dashboard.

Connects to the chrono_agent telemetry stream (default 127.0.0.1:9000, which
QEMU forwards to the QNX target), reconstructs the IPC call graph
(Client -> Channel -> Server), computes rolling tail latencies / WCET / jitter /
priority-inversion alerts and serves a dark-mode dashboard on
http://127.0.0.1:8080.

Standard library only (Python 3.8+).  License: Apache-2.0
"""
import argparse
import collections
import json
import math
import socket
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HIST_BUCKETS = 32
HIST_LO_US = 50.0
HIST_HI_US = 50000.0
TIMELINE_LEN = 300


def percentile(sorted_vals, p):
    """Nearest-rank percentile (same definition as ipc_bench / ci_gate)."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    idx = int(math.ceil(p / 100.0 * n)) - 1
    return sorted_vals[min(max(idx, 0), n - 1)]


class Analyzer:
    def __init__(self, window):
        self.lock = threading.Lock()
        self.window = window
        self.connected = False
        self.agent = {}
        self.last_rx = 0.0
        self.alerts = collections.deque(maxlen=60)
        self.history = collections.deque(maxlen=12)
        self.edges = collections.Counter()      # (client_tid, server_tid) -> calls
        self.reset_run(None)

    # ------------------------------------------------------------ run state
    def reset_run(self, hello):
        self.hello = hello or {}
        self.pid = (hello or {}).get("pid")
        self.rolling = collections.deque(maxlen=self.window)   # tuples
        self.timeline = collections.deque(maxlen=TIMELINE_LEN)
        self.run_rtt = []
        self.run_n = 0
        self.wcet = 0.0
        self.wcet_seq = 0
        self.inversions = 0
        self.sched_inversions = 0
        self.rate_marks = collections.deque(maxlen=2000)
        self.sched = {}
        self.sched_rx = 0.0
        self.last_inv_rx = 0.0
        self.run_active = False
        self.run_finished = False
        self.edges = collections.Counter()
        self.prev_sched_inv_total = 0

    def snapshot_run_summary(self):
        s = sorted(self.run_rtt)
        if not s:
            return None
        return {
            "mode": self.hello.get("mode", "?"),
            "pid": self.pid,
            "samples": len(s),
            "p50": round(percentile(s, 50), 1),
            "p99": round(percentile(s, 99), 1),
            "p999": round(percentile(s, 99.9), 1),
            "wcet": round(s[-1], 1),
            "inversions": self.inversions,
            "sched_inversions": self.sched_inversions,
            "finished": time.strftime("%H:%M:%S"),
        }

    def push_history(self):
        summ = self.snapshot_run_summary()
        if summ:
            self.history.appendleft(summ)

    # ----------------------------------------------------------- ingestion
    def alert(self, kind, msg):
        self.alerts.appendleft({"ts": time.strftime("%H:%M:%S"), "kind": kind, "msg": msg})

    def feed(self, d):
        t = d.get("t")
        now = time.monotonic()
        with self.lock:
            self.last_rx = now
            if t == "agent":
                self.agent = d
            elif t == "hello":
                if d.get("pid") != self.pid:
                    if self.run_n and not self.run_finished:
                        self.push_history()
                    self.reset_run(d)
                    self.alert("info", "new run: mode=%s pid=%s rate=%s Hz"
                               % (d.get("mode"), d.get("pid"), d.get("rate")))
                self.hello = d
                self.run_active = True
            elif t == "ipc":
                self._ipc(d, now)
            elif t == "end":
                if self.run_active and not self.run_finished:
                    self.run_finished = True
                    self.run_active = False
                    self.push_history()
                    self.alert("info", "run finished: %s samples, dropped=%s"
                               % (d.get("samples"), d.get("dropped")))
            elif t == "sched":
                self._sched(d, now)

    def _ipc(self, d, now):
        try:
            rtt = float(d["rtt_us"])
        except (KeyError, TypeError, ValueError):
            return
        if self.run_finished:                       # stray datagram after 'end'
            return
        self.run_active = True
        inv = int(d.get("inversion_flag", 0))
        row = (d.get("seq", 0), rtt, float(d.get("jitter_us", 0.0)),
               float(d.get("rel_us", 0.0)), inv)
        self.rolling.append(row)
        self.timeline.append((row[0], round(rtt, 1), inv))
        if len(self.run_rtt) < 500000:
            self.run_rtt.append(rtt)
        self.run_n += 1
        self.rate_marks.append(now)
        if rtt > self.wcet:
            self.wcet, self.wcet_seq = rtt, row[0]
        self.edges[(d.get("client_tid"), d.get("server_tid"))] += 1
        if inv:
            self.inversions += 1
            self.last_inv_rx = now
            if self.inversions <= 3 or self.inversions % 25 == 0:
                self.alert("inversion",
                           "PRIORITY INVERSION seq=%s rtt=%.0f us - client prio %s blocked on "
                           "server running at prio %s" % (d.get("seq"), rtt,
                                                          d.get("client_prio"), d.get("server_prio")))

    def _sched(self, d, now):
        if d.get("gone"):
            return
        self.sched = d
        self.sched_rx = now
        tot = int(d.get("inv_total", 0))
        if tot > self.prev_sched_inv_total:
            self.sched_inversions = tot
            self.alert("sched",
                       "/proc: client tid %s in REPLY, server tid %s READY below it, hog tid %s RUNNING"
                       % (d.get("inv_client"), d.get("inv_server"), d.get("inv_hog")))
        self.prev_sched_inv_total = tot

    def set_connected(self, v):
        with self.lock:
            self.connected = v
            if not v:
                self.run_active = False

    # --------------------------------------------------------------- views
    def _histogram(self, vals):
        counts = [0] * HIST_BUCKETS
        edges = [HIST_LO_US * (HIST_HI_US / HIST_LO_US) ** (i / HIST_BUCKETS)
                 for i in range(HIST_BUCKETS + 1)]
        k = math.log(HIST_HI_US / HIST_LO_US)
        for v in vals:
            if v <= HIST_LO_US:
                i = 0
            else:
                i = min(int(math.log(v / HIST_LO_US) / k * HIST_BUCKETS), HIST_BUCKETS - 1)
            counts[i] += 1
        return {"edges": [round(e, 1) for e in edges], "counts": counts}

    def _graph(self, now):
        h = self.hello
        sched = {t["tid"]: t for t in self.sched.get("threads", [])} if self.sched else {}

        def node(tid, role, prio_hint):
            th = sched.get(tid, {})
            return {"tid": tid, "role": role, "state": th.get("state", "?"),
                    "prio": th.get("prio", prio_hint), "nom": th.get("nom", prio_hint),
                    "cpu": th.get("cpu")}
        nodes = []
        ctid, stid, htid = h.get("client_tid"), h.get("server_tid"), h.get("hog_tid")
        # prefer tids actually observed in IPC records over the (early) hello
        if self.edges:
            (ctid, stid), _ = self.edges.most_common(1)[0]
        if ctid is not None:
            nodes.append(node(ctid, "client", h.get("client_prio")))
        if stid is not None:
            nodes.append(node(stid, "server", h.get("server_prio")))
        if htid:
            nodes.append(node(htid, "hog", h.get("hog_prio")))
        calls = sum(self.edges.values())
        return {
            "pid": h.get("pid"), "chid": h.get("chid"),
            "channel_mode": "FIXED_PRIORITY (no inheritance)" if h.get("fixed_priority")
                            else "priority-inheriting",
            "nodes": nodes, "calls": calls,
        }

    def view(self):
        now = time.monotonic()
        with self.lock:
            rows = list(self.rolling)
            rtts = sorted(r[1] for r in rows)
            jit = [r[2] for r in rows]
            rel = sorted(r[3] for r in rows)
            rate = 0.0
            recent = [m for m in self.rate_marks if now - m <= 2.0]
            if len(recent) > 1:
                rate = (len(recent) - 1) / max(recent[-1] - recent[0], 1e-6)
            inv_active = (now - self.last_inv_rx < 1.0) or \
                         (self.sched.get("inversion") and now - self.sched_rx < 1.0)
            stats = {
                "n_window": len(rtts),
                "n_run": self.run_n,
                "min": rtts[0] if rtts else 0,
                "mean": sum(rtts) / len(rtts) if rtts else 0,
                "p50": percentile(rtts, 50), "p95": percentile(rtts, 95),
                "p99": percentile(rtts, 99), "p999": percentile(rtts, 99.9),
                "max_window": rtts[-1] if rtts else 0,
                "wcet": self.wcet, "wcet_seq": self.wcet_seq,
                "jitter_mean": sum(jit) / len(jit) if jit else 0,
                "jitter_max": max(jit) if jit else 0,
                "rel_p99": percentile(rel, 99), "rel_max": rel[-1] if rel else 0,
                "inversions": self.inversions,
                "sched_inversions": self.sched_inversions,
                "rate_hz": round(rate, 1),
            }
            return {
                "connected": self.connected,
                "stale": (now - self.last_rx) > 3.0 if self.last_rx else True,
                "agent": self.agent,
                "run": {"pid": self.pid, "mode": self.hello.get("mode"),
                        "rate": self.hello.get("rate"), "active": self.run_active,
                        "finished": self.run_finished,
                        "inv_threshold_us": self.hello.get("inv_threshold_us", 1000)},
                "stats": stats,
                "timeline": list(self.timeline),
                "hist": self._histogram(rtts),
                "graph": self._graph(now),
                "sched": self.sched,
                "inversion_active": bool(inv_active),
                "alerts": list(self.alerts)[:14],
                "history": list(self.history),
            }

    def reset_all(self):
        with self.lock:
            self.history.clear()
            self.alerts.clear()
            self.reset_run(None)


# ------------------------------------------------------------ agent client
def agent_loop(an, host, port, stop):
    while not stop.is_set():
        try:
            s = socket.create_connection((host, port), timeout=3)
        except OSError:
            an.set_connected(False)
            stop.wait(1.0)
            continue
        s.settimeout(5.0)
        an.set_connected(True)
        print("[analyzer] connected to agent %s:%d" % (host, port), flush=True)
        buf = b""
        try:
            while not stop.is_set():
                try:
                    chunk = s.recv(65536)
                except socket.timeout:
                    continue
                if not chunk:
                    break
                buf += chunk
                *lines, buf = buf.split(b"\n")
                for ln in lines:
                    if not ln:
                        continue
                    try:
                        an.feed(json.loads(ln))
                    except ValueError:
                        pass
        except OSError:
            pass
        finally:
            s.close()
            an.set_connected(False)
            print("[analyzer] agent connection lost - retrying", flush=True)
        stop.wait(1.0)


# --------------------------------------------------------------------- http
def make_handler(an, page):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self._send(200, page, "text/html; charset=utf-8")
            elif self.path.startswith("/api/state"):
                self._send(200, json.dumps(an.view()).encode(), "application/json")
            elif self.path == "/healthz":
                self._send(200, b"ok", "text/plain")
            else:
                self._send(404, b"not found", "text/plain")

        def do_POST(self):
            if self.path == "/api/reset":
                an.reset_all()
                self._send(200, b"{}", "application/json")
            else:
                self._send(404, b"not found", "text/plain")
    return H


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>ChronoQNX - IPC &amp; Scheduling Jitter Profiler</title>
<style>
:root{--bg:#0b0f14;--panel:#121923;--panel2:#18212d;--line:#243142;--fg:#e6edf5;--dim:#8a9bb0;
--ok:#3ecfb2;--warn:#f5b84b;--bad:#ff5d6c;--blue:#5aa9ff;--violet:#a58bff}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);font:14px/1.45 system-ui,Segoe UI,Roboto,sans-serif}
header{display:flex;gap:14px;align-items:center;flex-wrap:wrap;padding:14px 20px;border-bottom:1px solid var(--line);background:var(--panel)}
h1{font-size:17px;margin:0;font-weight:650;letter-spacing:.2px}h1 span{color:var(--blue)}
.pill{padding:2px 10px;border-radius:99px;font-size:12px;border:1px solid var(--line);background:var(--panel2);color:var(--dim)}
.pill.ok{color:var(--ok);border-color:#1f5c52}.pill.bad{color:var(--bad);border-color:#6b2a33}.pill.warn{color:var(--warn);border-color:#6b5524}
.sp{flex:1}button{background:var(--panel2);color:var(--fg);border:1px solid var(--line);border-radius:6px;padding:4px 12px;cursor:pointer}
main{padding:16px 20px;display:grid;gap:14px;grid-template-columns:repeat(12,1fr)}
.card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px;min-width:0}
.card h2{font-size:12px;margin:0 0 8px;color:var(--dim);font-weight:600;text-transform:uppercase;letter-spacing:.8px}
.kpis{grid-column:span 12;display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px}
.kpi .l{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.7px}
.kpi .v{font-size:25px;font-weight:650;font-variant-numeric:tabular-nums}.kpi .u{font-size:12px;color:var(--dim);margin-left:3px}
.kpi.bad{border-color:#6b2a33;background:#1c1217}.kpi.bad .v{color:var(--bad)}.kpi.warn .v{color:var(--warn)}
.banner{grid-column:span 12;padding:10px 14px;border-radius:10px;border:1px solid #6b2a33;background:#2a141a;color:#ffc2c8;display:none;font-weight:600}
.banner.on{display:block}
.w8{grid-column:span 8}.w4{grid-column:span 4}.w6{grid-column:span 6}.w12{grid-column:span 12}
@media(max-width:1000px){.w8,.w4,.w6{grid-column:span 12}}
canvas{width:100%;display:block}svg{width:100%;display:block}
table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}th,td{text-align:right;padding:4px 8px;border-bottom:1px solid var(--line);font-size:13px}
th:first-child,td:first-child,td.l,th.l{text-align:left}th{color:var(--dim);font-weight:600;font-size:11px;text-transform:uppercase}
.alerts{max-height:210px;overflow:auto;font-family:ui-monospace,Consolas,monospace;font-size:12px}
.alerts div{padding:3px 0;border-bottom:1px solid var(--line)}.alerts .inversion,.alerts .sched{color:var(--bad)}.alerts .info{color:var(--dim)}
.legend{display:flex;gap:14px;font-size:12px;color:var(--dim);margin-top:6px;flex-wrap:wrap}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px;vertical-align:-1px}
</style></head><body>
<header><h1><span>Chrono</span>QNX <small style="color:var(--dim);font-weight:400">Deterministic IPC &amp; Scheduling Jitter Profiler</small></h1>
<span id="conn" class="pill">connecting&hellip;</span><span id="mode" class="pill">mode: &ndash;</span>
<span id="rate" class="pill">&ndash; Hz</span><span class="sp"></span><button id="reset">Reset stats</button></header>
<main>
<div id="banner" class="banner"></div>
<div class="kpis" id="kpis"></div>
<section class="card w8"><h2>Round-trip timeline (last 300 requests, sqrt scale)</h2><canvas id="tl" height="230"></canvas>
<div class="legend"><span><i style="background:var(--ok)"></i>within budget</span><span><i style="background:var(--warn)"></i>over inversion threshold</span><span><i style="background:var(--bad)"></i>flagged priority inversion</span><span id="tlinfo"></span></div></section>
<section class="card w4"><h2>Latency distribution (rolling window)</h2><canvas id="hist" height="230"></canvas>
<div class="legend"><span>log-spaced buckets</span><span><i style="background:var(--blue)"></i>p50</span><span><i style="background:var(--bad)"></i>p99</span></div></section>
<section class="card w8"><h2>Active IPC topology &mdash; Client &rarr; Channel &rarr; Server</h2><svg id="topo" viewBox="0 0 760 262"></svg></section>
<section class="card w4"><h2>Alerts</h2><div class="alerts" id="alerts"></div></section>
<section class="card w6"><h2>Kernel threads (live /proc via chrono_agent)</h2><table id="thr"></table></section>
<section class="card w6"><h2>Run history</h2><table id="hist-tbl"></table></section>
</main>
<script>
const $=id=>document.getElementById(id);
const css=n=>getComputedStyle(document.documentElement).getPropertyValue(n).trim();
const fmt=(v,d=1)=>v>=10000?(v/1000).toFixed(1)+'k':v.toFixed(v<100?d:0);
let S=null;
function kpi(l,v,u,cls){return `<div class="kpi ${cls||''}"><div class="l">${l}</div><div class="v">${v}<span class="u">${u}</span></div></div>`}
function setup(c){const r=devicePixelRatio||1,w=c.clientWidth,h=+c.getAttribute('height');c.width=w*r;c.height=h*r;const g=c.getContext('2d');g.setTransform(r,0,0,r,0,0);return[g,w,h]}
function drawTimeline(){
  const[g,w,h]=setup($('tl'));g.clearRect(0,0,w,h);const tl=S.timeline,thr=S.run.inv_threshold_us||1000;
  const L=44,B=18,T=6,pw=w-L-4,ph=h-B-T;
  let mx=Math.max(thr*1.5,...tl.map(t=>t[1]),1);mx*=1.08;
  g.font='11px system-ui';g.fillStyle=css('--dim');g.strokeStyle=css('--line');g.lineWidth=1;
  const sc=v=>Math.sqrt(Math.min(v,mx)/mx);  /* sqrt scale: outliers don't flatten the normal band */
  for(let i=0;i<=4;i++){const y=T+ph-ph*i/4;g.beginPath();g.moveTo(L,y);g.lineTo(w,y);g.stroke();g.fillText(fmt(mx*(i/4)*(i/4),0)+' us',2,y+3)}
  const bw=pw/300;tl.forEach((t,i)=>{const hh=Math.max(1,ph*sc(t[1]));g.fillStyle=t[2]?css('--bad'):(t[1]>thr?css('--warn'):css('--ok'));g.fillRect(L+(300-tl.length+i)*bw,T+ph-hh,Math.max(1,bw-.6),hh)});
  const y=T+ph-ph*sc(thr);g.setLineDash([5,4]);g.strokeStyle=css('--warn');g.beginPath();g.moveTo(L,y);g.lineTo(w,y);g.stroke();g.setLineDash([]);g.fillStyle=css('--warn');g.fillText('threshold '+thr+' us',L+4,y-4);
  $('tlinfo').textContent=tl.length?('latest seq '+tl[tl.length-1][0]):'waiting for samples';
}
function drawHist(){
  const[g,w,h]=setup($('hist'));g.clearRect(0,0,w,h);const H=S.hist,mx=Math.max(1,...H.counts),n=H.counts.length;
  const L=6,B=22,T=6,pw=w-L-6,ph=h-B-T,bw=pw/n;
  H.counts.forEach((c,i)=>{const hh=ph*c/mx;const e=H.edges[i];g.fillStyle=e>=S.stats.p99&&S.stats.p99>0?css('--bad'):css('--blue');g.globalAlpha=.85;g.fillRect(L+i*bw,T+ph-hh,bw-1.5,hh)});g.globalAlpha=1;
  const xOf=v=>{if(v<=H.edges[0])return L;const k=Math.log(H.edges[n]/H.edges[0]);return L+Math.min(1,Math.log(v/H.edges[0])/k)*pw};
  g.font='11px system-ui';g.fillStyle=css('--dim');
  [100,1000,10000].forEach(v=>{const x=xOf(v);g.fillText(v>=1000?v/1000+' ms':v+' us',x-10,h-6);g.strokeStyle=css('--line');g.beginPath();g.moveTo(x,T);g.lineTo(x,T+ph);g.stroke()});
  [[S.stats.p50,'--blue'],[S.stats.p99,'--bad']].forEach(([v,c])=>{if(!v)return;const x=xOf(v);g.strokeStyle=css(c);g.lineWidth=2;g.beginPath();g.moveTo(x,T);g.lineTo(x,T+ph);g.stroke();g.lineWidth=1});
}
function stCol(s){return({RUNNING:'--ok',READY:'--warn',REPLY:'--violet',SEND:'--violet',RECEIVE:'--blue'})[s]||'--dim'}
function drawTopo(){
  const G=S.graph,svg=$('topo'),inv=S.inversion_active;let o='';
  const nd=r=>G.nodes.find(n=>n.role===r);
  const box=(x,y,wd,ht,title,lines,stroke,dash)=>{let s=`<rect x="${x}" y="${y}" width="${wd}" height="${ht}" rx="9" fill="${css('--panel2')}" stroke="${stroke}" stroke-width="1.6" ${dash?'stroke-dasharray="6 4"':''}/><text x="${x+12}" y="${y+22}" fill="${css('--fg')}" font-size="14" font-weight="650">${title}</text>`;lines.forEach((l,i)=>s+=`<text x="${x+12}" y="${y+42+i*17}" fill="${l[1]||css('--dim')}" font-size="12">${l[0]}</text>`);return s};
  const info=(n,pn)=>n?[['tid '+n.tid+(n.cpu!=null?'  · cpu '+n.cpu:'')],['state '+n.state,css(stCol(n.state))],['prio '+n.prio+' (nominal '+n.nom+')',n.prio<n.nom||(n.prio!==n.nom)?css('--warn'):null]]:[['waiting for agent…']];
  const c=nd('client'),s=nd('server'),hg=nd('hog'),ec=inv?css('--bad'):css('--ok');
  o+=`<defs><marker id="ar" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10z" fill="${ec}"/></marker><marker id="ar2" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto"><path d="M0 0L10 5L0 10z" fill="${css('--warn')}"/></marker></defs>`;
  o+=box(10,40,200,100,'Client (RT control)',info(c),ec);
  o+=box(280,55,190,70,'Channel '+(G.chid==null?'?':G.chid),[[G.channel_mode,G.channel_mode.startsWith('FIXED')?css('--warn'):css('--ok')],['pid '+(G.pid||'?')]],css('--line'));
  o+=box(540,40,200,100,'Server (worker)',info(s),ec);
  o+=`<line x1="212" y1="90" x2="278" y2="90" stroke="${ec}" stroke-width="2.5" marker-end="url(#ar)"/><line x1="472" y1="90" x2="538" y2="90" stroke="${ec}" stroke-width="2.5" marker-end="url(#ar)"/>`;
  o+=`<text x="245" y="80" fill="${css('--dim')}" font-size="11" text-anchor="middle">MsgSend</text><text x="505" y="80" fill="${css('--dim')}" font-size="11" text-anchor="middle">MsgReceive</text>`;
  o+=`<text x="245" y="112" fill="${css('--dim')}" font-size="11" text-anchor="middle">p50 ${fmt(S.stats.p50)} us</text><text x="505" y="112" fill="${css('--dim')}" font-size="11" text-anchor="middle">${G.calls} calls</text>`;
  o+=box(540,160,200,92,'CPU hog (medium prio)',hg?info(hg).slice(0,1).concat(info(hg).slice(1)):[['n/a']],inv?css('--warn'):css('--line'),true);
  if(inv)o+=`<path d="M640 158 L640 142" stroke="${css('--warn')}" stroke-width="2.5" marker-end="url(#ar2)"/><text x="650" y="152" fill="${css('--warn')}" font-size="11">preempts server</text>`;
  svg.innerHTML=o;
}
function render(){
  if(!S)return;const st=S.stats,R=S.run;
  $('conn').className='pill '+(S.connected?(S.stale?'warn':'ok'):'bad');
  $('conn').textContent=S.connected?(S.stale?'agent connected · idle':'agent live · '+(S.agent.os||'QNX')+' · '+(S.agent.ncpu||'?')+' cpu'):'agent disconnected';
  $('mode').textContent='mode: '+(R.mode||'–')+(R.finished?' (finished)':'');
  $('mode').className='pill '+(R.mode==='invert'?'bad':(R.mode==='normal'?'ok':''));
  $('rate').textContent=st.rate_hz+' Hz measured';
  const inv=st.inversions>0;
  $('banner').className='banner'+(S.inversion_active?' on':'');
  $('banner').textContent=S.inversion_active?'⚠ PRIORITY INVERSION ACTIVE — high-priority client is blocked on a low-priority server that a medium-priority thread is preempting.':'';
  $('kpis').innerHTML=kpi('p50',fmt(st.p50),'us')+kpi('p95',fmt(st.p95),'us')+kpi('p99',fmt(st.p99),'us',st.p99>R.inv_threshold_us?'bad':'')+kpi('p99.9',fmt(st.p999),'us',st.p999>R.inv_threshold_us?'warn':'')
   +kpi('WCET',fmt(st.wcet),'us',st.wcet>R.inv_threshold_us*3?'bad':'')+kpi('RTT jitter',fmt(st.jitter_mean),'us avg')+kpi('release jitter p99',fmt(st.rel_p99),'us')
   +kpi('inversions',st.inversions,'(/proc '+st.sched_inversions+')',inv?'bad':'')+kpi('samples',st.n_run,'');
  drawTimeline();drawHist();drawTopo();
  $('alerts').innerHTML=S.alerts.length?S.alerts.map(a=>`<div class="${a.kind}">${a.ts} · ${a.msg}</div>`).join(''):'<div class="info">no alerts</div>';
  const th=(S.sched.threads||[]);const role=t=>{const m=S.graph.nodes.find(n=>n.tid===t.tid);return m?m.role:'agent/other'};
  $('thr').innerHTML='<tr><th>tid</th><th class="l">role</th><th class="l">state</th><th>eff prio</th><th>nominal</th><th>cpu</th><th>blocked ms</th></tr>'+(th.length?th.map(t=>`<tr><td>${t.tid}</td><td class="l">${role(t)}</td><td class="l" style="color:${css(stCol(t.state))}">${t.state}</td><td>${t.prio}</td><td>${t.nom}</td><td>${t.cpu}</td><td>${t.blk_ms}</td></tr>`).join(''):'<tr><td colspan="7" style="text-align:center;color:var(--dim)">no target process attached</td></tr>');
  $('hist-tbl').innerHTML='<tr><th>mode</th><th>samples</th><th>p50</th><th>p99</th><th>p99.9</th><th>WCET</th><th>inv</th></tr>'+(S.history.length?S.history.map(h=>`<tr><td style="color:${h.mode==='invert'?css('--bad'):css('--ok')}">${h.mode}</td><td>${h.samples}</td><td>${fmt(h.p50)}</td><td>${fmt(h.p99)}</td><td>${fmt(h.p999)}</td><td>${fmt(h.wcet)}</td><td>${h.inversions}</td></tr>`).join(''):'<tr><td colspan="7" style="text-align:center;color:var(--dim)">completed runs appear here (values in us)</td></tr>');
}
async function tick(){try{const r=await fetch('/api/state');S=await r.json();render()}catch(e){$('conn').className='pill bad';$('conn').textContent='analyzer unreachable'}}
$('reset').onclick=()=>fetch('/api/reset',{method:'POST'}).then(tick);
addEventListener('resize',render);setInterval(tick,250);tick();
</script></body></html>
"""


def main():
    ap = argparse.ArgumentParser(description="ChronoQNX live dashboard")
    ap.add_argument("--agent-host", default="127.0.0.1")
    ap.add_argument("--agent-port", type=int, default=9000)
    ap.add_argument("--http-host", default="127.0.0.1")
    ap.add_argument("--http-port", type=int, default=8080)
    ap.add_argument("--window", type=int, default=2000,
                    help="rolling window size in samples (default 2000 = 10 s @ 200 Hz)")
    args = ap.parse_args()

    an = Analyzer(args.window)
    stop = threading.Event()
    threading.Thread(target=agent_loop, args=(an, args.agent_host, args.agent_port, stop),
                     daemon=True).start()
    httpd = ThreadingHTTPServer((args.http_host, args.http_port), make_handler(an, PAGE.encode()))
    print("[analyzer] dashboard: http://%s:%d   (agent %s:%d)"
          % (args.http_host, args.http_port, args.agent_host, args.agent_port), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        httpd.server_close()


if __name__ == "__main__":
    sys.exit(main())
