# ChronoQNX — Deterministic IPC & Scheduling Jitter Profiler for QNX 8.0

ChronoQNX makes **priority inversion visible, measurable and gate-able** on QNX Neutrino 8.0.
It runs a real-time client/server IPC workload on a QNX target, inspects the live kernel thread
state through `/proc`, and streams everything to a Windows host that shows a live dashboard and
fails your CI build when tail latency regresses.

| | `--mode normal` | `--mode invert` |
|---|---|---|
| Channel | `ChannelCreate(0)` — server inherits sender priority | `ChannelCreate(_NTO_CHF_FIXED_PRIORITY)` — no inheritance |
| p99 round-trip | **~0.4 ms** | **~9 ms** |
| WCET | **~1.3 ms** | **~16 ms** |
| Inversions / 1990 requests | **0** | **~378** |
| `ci_gate.py` | **PASS** (exit 0) | **FAIL** (exit 1) |

*(measured on QNX 8.0 x86_64 in QEMU, 2 vCPU, 200 Hz, 10 s — see [Verified results](#verified-results))*

## Architecture

```
 ┌────────────────────────── QNX 8.0 target (QEMU) ───────────────────────────┐
 │                                                                            │
 │   ipc_bench (one process, all threads pinned to core 0)                    │
 │   ┌──────────┐  MsgSend   ┌─────────┐  MsgReceive  ┌──────────┐            │
 │   │ client   │ ─────────► │ channel │ ───────────► │ server   │            │
 │   │ prio 25  │ ◄───────── │ chid N  │ ◄─────────── │ prio 10  │            │
 │   └──────────┘  MsgReply  └─────────┘   MsgReply   └────▲─────┘            │
 │                                                         │ preempts (invert)│
 │                                                    ┌────┴─────┐            │
 │                                                    │ CPU hog  │            │
 │                                                    │ prio 15  │            │
 │                                                    └──────────┘            │
 │        │ UDP 127.0.0.1:9001  (1 JSON record / request)                     │
 │        ▼                                                                   │
 │   chrono_agent  (core 1, prio 30)                                          │
 │     • /proc/<pid>/as  devctl DCMD_PROC_INFO + DCMD_PROC_TIDSTATUS          │
 │       → state, effective vs nominal prio, last CPU, blocked-on channel     │
 │     • inversion detector, 1 s aggregates, optional TraceEvent() markers    │
 │        │ TCP 0.0.0.0:9000  (newline-delimited JSON)                        │
 └────────┼───────────────────────────────────────────────────────────────────┘
          │  QEMU hostfwd  127.0.0.1:9000 → guest :9000
 ┌────────▼────────────────────── Windows host ───────────────────────────────┐
 │  host-analyzer/server.py   → http://127.0.0.1:8080  live dashboard         │
 │  host-analyzer/ci_gate.py  → reads chrono.toml, PASS/FAIL, exit 0/1        │
 └────────────────────────────────────────────────────────────────────────────┘
```

### Why it works (and why everything is pinned to one core)
On an SMP target the hog would simply run on the other CPU and no inversion would occur. All three
workload threads are therefore pinned to one core (`ThreadCtl(_NTO_TCTL_RUNMASK)`); the agent and the
telemetry thread run on the other core so observing the system does not disturb it.

* **normal:** the client (25) sends → the server inherits 25 while handling the request → the hog (15)
  cannot preempt it → bounded latency.
* **invert:** `_NTO_CHF_FIXED_PRIORITY` disables inheritance → the server stays at 10 → the hog (15)
  preempts it while the 25-priority client sits in `STATE_REPLY` → classic unbounded priority inversion.

## Repository layout

```
workload-sim/ipc_bench.c      QNX IPC benchmark + priority-inversion simulator
target-probe/chrono_agent.c   /proc thread inspector + telemetry server (TCP :9000)
host-analyzer/server.py       live dashboard (stdlib only)         → http://127.0.0.1:8080
host-analyzer/ci_gate.py      CI latency gate (stdlib only)
host-analyzer/chrono.toml     gate thresholds + capture settings
build_and_deploy.bat          cross-compile + scp + chmod + verify
LICENSE                       Apache-2.0
```
`target-probe/probe_check.c` is the original toolchain smoke test and is unrelated to the profiler.

## Prerequisites

* QNX SDP 8.0 at `C:\qnx800` (`qnxsdp-env.bat`)
* QNX 8.0 x86_64 QEMU VM, reachable as the SSH alias `qnx-qemu`, with forwards
  `hostfwd=tcp::2222-:22,hostfwd=tcp::9000-:9000`
* Windows OpenSSH (`ssh`, `scp`), Python 3.8+ (no pip packages)

Example `~/.ssh/config` entry:
```
Host qnx-qemu
    HostName 127.0.0.1
    Port 2222
    User root
```

## Build & deploy

```bat
build_and_deploy.bat              :: build, deploy to /data/home/root, verify
build_and_deploy.bat start-agent  :: ... and (re)start chrono_agent in the background
```

This runs, in order: `call C:\qnx800\qnxsdp-env.bat` → `qcc` both binaries → `slay` old copies →
`scp` to `/data/home/root/` → `chmod +x` → runs a 2-second smoke test on the target.
Binaries live in `/data/home/root` because `/tmp` is mounted `noexec`.

> **Note on `-lsocket`:** the build commands are exactly
> `qcc -Vgcc_ntox86_64 -O2 <src>.c -o <out> -lsocket`. QNX keeps the BSD socket API in `libsocket`, and
> both programs send/receive telemetry over sockets, so the library must be linked explicitly.

## Run

```bat
:: 1. target: start the agent (once)
ssh qnx-qemu "cd /data/home/root && (nohup ./chrono_agent > agent.log 2>&1 &)"

:: 2. host: live dashboard
python host-analyzer\server.py            :: open http://127.0.0.1:8080

:: 3. target: generate load
ssh qnx-qemu "cd /data/home/root && ./ipc_bench --mode normal --duration 15"
ssh qnx-qemu "cd /data/home/root && ./ipc_bench --mode invert --duration 15"
```

### `ipc_bench` options
| flag | default | meaning |
|---|---|---|
| `--rate <hz>` | 200 | client request rate |
| `--duration <sec>` | 15 | run time |
| `--mode normal\|invert` | normal | priority inheritance on / off |
| `--sink <host:port>` | 127.0.0.1:9001 | UDP telemetry destination (agent) |
| `--no-telemetry`, `--quiet` | | local-only run / summary only |
| `--work-us`, `--hog-burst-ms`, `--hog-period-ms` | 100, 8, 23 | simulated server work, hog duty cycle |
| `--inv-us <us>` | 1000 | RTT above this *and* server prio < client prio ⇒ flagged inversion |
| `--cpu <n>` | 0 | core all workload threads are pinned to |

### `chrono_agent` options
`--port 9000`, `--ingest-port 9001`, `--interval-ms 20`, `--watch ipc_bench` (or `--pid N`),
`--cpu`, `--prio 30`, `--trace` (emit `TraceEvent(_NTO_TRACE_INSERTUSRSTREVENT, …)` markers on every
detected inversion so they appear in the QNX System Profiler timeline), `-v`.

## Telemetry protocol

Newline-delimited JSON, one frame per line (`"t"` is the frame type):

| `t` | producer | content |
|---|---|---|
| `ipc` | ipc_bench | `seq, client_tid, server_tid, client_prio, server_prio, server_nominal, send_ns, recv_ns, reply_ns, rtt_us, jitter_us, rel_us, inversion_flag` |
| `hello` / `end` | ipc_bench | topology (`pid, chid, tids, prios, mode`) / run summary |
| `sched` | chrono_agent | per-thread `state, prio (effective), nom (nominal), cpu, blk_pid, blk_chid, blk_ms` + inversion verdict |
| `agg` | chrono_agent | 1 s aggregate (`n, mean_us, max_us, inversions`) |
| `agent` | chrono_agent | handshake: ncpu, cycles/s, watch target |

`*_ns` are nanoseconds since benchmark start, derived from `ClockCycles()` and
`SYSPAGE_ENTRY(qtime)->cycles_per_sec`. `rtt_us` = client-side `MsgSend` round trip,
`jitter_us` = `|rtt[i] − rtt[i−1]|`, `rel_us` = lateness of the client's periodic wake-up.

### Inversion detection (two independent detectors)
1. **ipc_bench** flags a request when the server's *effective* priority at `MsgReceive`
   (`SchedGet().sched_curpriority`) is below the client's **and** the RTT exceeded `--inv-us`.
2. **chrono_agent** flags a `/proc` snapshot when a thread is blocked in `SEND`/`REPLY`, the server thread
   is `READY` at a lower effective priority, and a third thread with a priority in between is `RUNNING`
   — i.e. it actually *sees* the preemption in the kernel's thread table.

The CI gate counts (1); (2) is shown as supporting evidence.

## CI latency gate

`host-analyzer/chrono.toml`:
```toml
[thresholds]
max_p99_us              = 1000
max_wcet_us             = 5000
max_priority_inversions = 0
```
```bat
python host-analyzer\ci_gate.py                     :: gate whatever the target is running
python host-analyzer\ci_gate.py --launch normal     :: start ipc_bench over ssh, then gate it
python host-analyzer\ci_gate.py --launch invert --json result.json
```
Exit code `0` = PASS, `1` = FAIL. The gate **fails closed**: no data, too few samples, an unreadable
config or a dead connection are all FAIL. Optional extra gates (`max_p50_us`, `max_p95_us`,
`max_p999_us`, `max_jitter_us`) are enabled by uncommenting them. Percentiles use nearest-rank.

Sample output (`--launch invert`):
```
==============================================================================
 ChronoQNX CI latency gate                             mode=invert pid=2269205
==============================================================================
 CHECK                              MEASURED            LIMIT   RESULT
------------------------------------------------------------------------------
 samples captured                       1990           >= 200   PASS
 p99 RTT                           8949.7 us     <= 1000.0 us   FAIL
 WCET (max RTT)                   16033.9 us     <= 5000.0 us   FAIL
 priority inversions                     378             <= 0   FAIL
------------------------------------------------------------------------------
 GATE RESULT: FAIL
```

## 3-minute demo walkthrough

| time | do | say / show |
|---|---|---|
| 0:00 | `build_and_deploy.bat start-agent` | One script cross-compiles for QNX 8.0, deploys to the QEMU target and verifies it. |
| 0:30 | `python host-analyzer\server.py`, open `http://127.0.0.1:8080` | Live dashboard is connected to the agent through the QEMU port forward; idle until load arrives. |
| 0:45 | `ssh qnx-qemu "cd /data/home/root && ./ipc_bench --mode normal --duration 20"` | Healthy system: p99 ≈ 0.4 ms, flat green timeline, topology all green, 0 inversions. |
| 1:15 | `./ipc_bench --mode invert --duration 20` | Same code, one flag changed (`_NTO_CHF_FIXED_PRIORITY`). Red spikes appear; the banner fires; the topology shows the **hog preempting the server** while the client waits in `REPLY`. |
| 1:50 | Point at *Kernel threads* + *Alerts* | Live `/proc` view: client `REPLY@25`, server `READY@10`, hog `RUNNING@15` — priority inversion caught in the kernel's own thread table. *Run history* compares both runs. |
| 2:15 | `python host-analyzer\ci_gate.py --launch normal` → `…--launch invert` | Same workload as an automated regression gate: normal **PASS (exit 0)**, inversion **FAIL (exit 1)** with the exact numbers. Drop it into any CI job. |
| 2:45 | Show `chrono.toml` | Thresholds are a 3-line file under version control. Add `--trace` to the agent and the inversions also appear in the QNX System Profiler timeline. |

## Verified results

Measured on QNX 8.0.0 x86_64 under QEMU (2 vCPU) with `ci_gate.py --launch`, 200 Hz × 10 s:

| run | p99 | WCET | inversions | gate |
|---|---|---|---|---|
| normal #1 | 390.7 µs | 1243.5 µs | 0 | PASS |
| normal #2 | 419.6 µs | 1234.5 µs | 0 | PASS |
| normal #3 | 356.9 µs | 1449.7 µs | 0 | PASS |
| invert | 9180.6 µs | 16103.3 µs | 378 | **FAIL** |

## Notes & limitations

* **Timer granularity under QEMU on Windows.** `rel_us` (periodic wake-up lateness) is typically
  milliseconds because QEMU quantises guest timer interrupts to the Windows host timer resolution
  (~15.6 ms). RTT/WCET/inversion metrics use the CPU cycle counter and are not affected, which is why
  the gate is built on those. On bare metal or KVM, `rel_us` is far smaller.
* WCET is the worst *observed* value, not a proven bound.
* The agent's inversion detector inspects threads of the watched process only; a medium-priority
  thread in a *different* process would not be seen by detector (2), though detector (1) still would.
* Thresholds in `chrono.toml` are calibrated for this QEMU setup; recalibrate on other hardware.
* Running the VM, browser and benchmarks on a loaded host adds scheduling noise (outliers of tens of ms
  were seen while a headless browser was rendering). Keep the host quiet while gating.

## License

Apache-2.0 — see [LICENSE](LICENSE).
