/*
 * ChronoQNX - chrono_agent.c
 * QNX 8.0 target telemetry & kernel thread inspector.
 *
 * Build : qcc -Vgcc_ntox86_64 -O2 target-probe/chrono_agent.c -o target-probe/chrono_agent -lsocket
 * Run   : chrono_agent [--port 9000] [--ingest-port 9001] [--interval-ms 20]
 *                      [--watch ipc_bench | --pid N] [--cpu N] [--prio N] [--trace]
 *
 * Data flow
 *   ipc_bench --UDP 127.0.0.1:9001--> [ chrono_agent ] --TCP 0.0.0.0:9000--> host analyzer
 *                                          ^
 *                  /proc/<pid>/as  (devctl DCMD_PROC_INFO / DCMD_PROC_TIDSTATUS)
 *
 * The agent
 *   1. samples every thread of the watched process through procfs: state
 *      (RUNNING/READY/SEND/RECEIVE/REPLY/...), effective vs nominal priority,
 *      last CPU, and which channel a blocked thread is waiting on;
 *   2. detects *priority inversion events* from those snapshots: a thread is
 *      blocked on a server (SEND/REPLY) while that server thread is READY at a
 *      lower effective priority than the client, and a third thread with a
 *      priority in between is RUNNING;
 *   3. forwards ipc_bench round-trip records verbatim and adds 1 s aggregates;
 *   4. optionally drops user markers into the kernel trace stream
 *      (TraceEvent(_NTO_TRACE_INSERTUSRSTREVENT, ...)) so inversions show up in
 *      the QNX System Profiler / tracelogger timeline (--trace).
 *
 * Wire format: newline-delimited JSON frames ("t" = hello|ipc|end|sched|agg|agent).
 *
 * License: Apache-2.0
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>
#include <signal.h>
#include <dirent.h>
#include <ctype.h>
#include <devctl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/neutrino.h>
#include <sys/syspage.h>
#include <sys/procfs.h>
#include <sys/states.h>
#include <sys/trace.h>
#include <pthread.h>
#include <sched.h>

#define AGENT_VERSION   "1.0"
#define MAX_CLIENTS     8
#define MAX_THREADS     64
#define CLIENT_BUF      (512 * 1024)
#define EV_INVERSION    (_NTO_TRACE_USERFIRST + 1)

/* ------------------------------------------------------------ options */
static int  o_port        = 9000;
static int  o_ingest      = 9001;
static int  o_interval_ms = 20;
static char o_watch[64]   = "ipc_bench";
static int  o_pid         = 0;
static int  o_cpu         = -1;
static int  o_prio        = 30;
static int  o_trace       = 0;
static int  o_verbose     = 0;

static volatile sig_atomic_t g_quit = 0;
static void on_sig(int s) { (void)s; g_quit = 1; }

/* ------------------------------------------------------------ helpers */
static uint64_t mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000ull + (uint64_t)ts.tv_nsec / 1000000ull;
}

static const char *state_name(unsigned s)
{
    switch (s) {
    case STATE_DEAD:        return "DEAD";
    case STATE_RUNNING:     return "RUNNING";
    case STATE_READY:       return "READY";
    case STATE_STOPPED:     return "STOPPED";
    case STATE_SEND:        return "SEND";
    case STATE_RECEIVE:     return "RECEIVE";
    case STATE_REPLY:       return "REPLY";
    case STATE_MQ_SEND:     return "MQ_SEND";
    case STATE_MQ_RECEIVE:  return "MQ_RECEIVE";
    case STATE_WAITPAGE:    return "WAITPAGE";
    case STATE_SIGSUSPEND:  return "SIGSUSPEND";
    case STATE_SIGWAITINFO: return "SIGWAITINFO";
    case STATE_NANOSLEEP:   return "NANOSLEEP";
    case STATE_MUTEX:       return "MUTEX";
    case STATE_CONDVAR:     return "CONDVAR";
    case STATE_JOIN:        return "JOIN";
    case STATE_INTR:        return "INTR";
    case STATE_SEM:         return "SEM";
    case STATE_WAITCTX:     return "WAITCTX";
    case STATE_RWLOCK_READ: return "RWLOCK_RD";
    case STATE_RWLOCK_WRITE: return "RWLOCK_WR";
    case STATE_BARRIER:     return "BARRIER";
    case STATE_PIPE:        return "PIPE";
    case STATE_SEND_NOTIFY: return "SEND_NOTIFY";
    default:                return "OTHER";
    }
}

/* --------------------------------------------------------- client fan-out */
typedef struct { int fd; char *buf; size_t len; unsigned long dropped; } cli_t;
static cli_t g_cli[MAX_CLIENTS];
static int   g_ncli;
static char  g_last_hello[2048];

static void cli_close(int i)
{
    close(g_cli[i].fd);
    free(g_cli[i].buf);
    g_cli[i] = g_cli[--g_ncli];
    fprintf(stderr, "[agent] host client disconnected (%d left)\n", g_ncli);
}

static void cli_flush(int i)
{
    cli_t *c = &g_cli[i];
    while (c->len) {
        ssize_t n = send(c->fd, c->buf, c->len, 0);
        if (n > 0) {
            memmove(c->buf, c->buf + n, c->len - (size_t)n);
            c->len -= (size_t)n;
        } else if (n == -1 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
            return;
        } else {
            cli_close(i);
            return;
        }
    }
}

static void cli_queue(cli_t *c, const char *s, size_t n)
{
    if (c->len + n + 1 > CLIENT_BUF) { c->dropped++; return; }   /* slow consumer */
    memcpy(c->buf + c->len, s, n);
    c->len += n;
    c->buf[c->len++] = '\n';
}

static void broadcast(const char *s, size_t n)
{
    for (int i = 0; i < g_ncli; i++) cli_queue(&g_cli[i], s, n);
}

static void flush_all(void)
{
    for (int i = g_ncli - 1; i >= 0; i--) cli_flush(i);
}

/* ------------------------------------------------------ ipc aggregation */
static struct {
    uint64_t n, total_n, inv, total_inv;
    double   sum, max, total_max;
} g_agg;

static void agg_add(const char *line)
{
    const char *p = strstr(line, "\"rtt_us\":");
    if (!p) return;
    double rtt = strtod(p + 9, NULL);
    int inv = (strstr(line, "\"inversion_flag\":1") != NULL);
    g_agg.n++; g_agg.total_n++;
    g_agg.sum += rtt;
    if (rtt > g_agg.max) g_agg.max = rtt;
    if (rtt > g_agg.total_max) g_agg.total_max = rtt;
    if (inv) { g_agg.inv++; g_agg.total_inv++; }
}

static void agg_emit(void)
{
    char b[512];
    int n = snprintf(b, sizeof b,
        "{\"t\":\"agg\",\"ts_ms\":%" PRIu64 ",\"n\":%" PRIu64 ",\"mean_us\":%.2f,\"max_us\":%.2f,"
        "\"inversions\":%" PRIu64 ",\"total_n\":%" PRIu64 ",\"total_max_us\":%.2f,"
        "\"total_inversions\":%" PRIu64 "}",
        mono_ms(), g_agg.n, g_agg.n ? g_agg.sum / (double)g_agg.n : 0.0, g_agg.max,
        g_agg.inv, g_agg.total_n, g_agg.total_max, g_agg.total_inv);
    broadcast(b, (size_t)n);
    g_agg.n = g_agg.inv = 0; g_agg.sum = g_agg.max = 0;
}

/* ----------------------------------------------------- procfs inspection */
typedef struct {
    int      tid, state, prio, nom, cpu;
    int      blk_pid, blk_chid;      /* SEND/REPLY: target; RECEIVE: own chid in blk_chid */
    uint64_t blk_ms;
} thr_t;

static pid_t g_pid;
static int   g_as_fd = -1;
static uint64_t g_inv_total;
static int   g_inv_latched;

static void as_close(void)
{
    if (g_as_fd >= 0) close(g_as_fd);
    g_as_fd = -1;
    g_pid = 0;
}

/* Fill name (basename of executable path) for an open /proc/<pid>/as fd. */
static int as_name(int fd, char *out, size_t cap)
{
    struct {
        procfs_debuginfo info;
        char             pad[_POSIX_PATH_MAX];
    } map;
    memset(&map, 0, sizeof map);
    if (devctl(fd, DCMD_PROC_MAPDEBUG_BASE, &map, sizeof map, NULL) != EOK) return -1;
    const char *base = strrchr(map.info.path, '/');
    base = base ? base + 1 : map.info.path;
    snprintf(out, cap, "%s", base);
    return 0;
}

static int as_open(pid_t pid)
{
    char path[64];
    snprintf(path, sizeof path, "/proc/%d/as", (int)pid);
    return open(path, O_RDONLY);
}

/* Look for the watched process in /proc. */
static void find_target(void)
{
    if (o_pid) {
        int fd = as_open(o_pid);
        if (fd >= 0) { g_as_fd = fd; g_pid = o_pid; g_inv_total = 0; }
        return;
    }
    DIR *d = opendir("/proc");
    if (!d) return;
    struct dirent *e;
    while ((e = readdir(d))) {
        if (!isdigit((unsigned char)e->d_name[0])) continue;
        pid_t pid = (pid_t)atoi(e->d_name);
        if (pid == getpid()) continue;
        int fd = as_open(pid);
        if (fd < 0) continue;
        char nm[128];
        if (as_name(fd, nm, sizeof nm) == 0 && strstr(nm, o_watch)) {
            g_as_fd = fd; g_pid = pid;
            g_inv_total = 0;                 /* inversion count is per target process */
            fprintf(stderr, "[agent] attached to %s pid %d via /proc/%d/as\n", nm, (int)pid, (int)pid);
            break;
        }
        close(fd);
    }
    closedir(d);
}

/* Snapshot all threads. Returns thread count or -1 if the process vanished. */
static int snapshot(thr_t *t, int max, debug_process_t *pinfo)
{
    if (devctl(g_as_fd, DCMD_PROC_INFO, pinfo, sizeof *pinfo, NULL) != EOK) return -1;
    int n = 0;
    procfs_status st;
    int next = 1;
    while (n < max) {
        memset(&st, 0, sizeof st);
        st.tid = (pthread_t)next;
        if (devctl(g_as_fd, DCMD_PROC_TIDSTATUS, &st, sizeof st, NULL) != EOK) break;
        thr_t *o = &t[n++];
        memset(o, 0, sizeof *o);
        o->tid   = (int)st.tid;
        o->state = st.state;
        o->prio  = st.priority;
        o->nom   = st.real_priority;
        o->cpu   = st.last_cpu;
        o->blk_ms = st.nsec_since_block / 1000000ull;
        if (st.state == STATE_SEND || st.state == STATE_REPLY) {
            o->blk_pid  = st.blocked.connect.pid;
            o->blk_chid = st.blocked.connect.chid;
        } else if (st.state == STATE_RECEIVE) {
            o->blk_chid = st.blocked.channel.chid;
        }
        next = (int)st.tid + 1;
    }
    return n;
}

/*
 * Inversion heuristic over one snapshot:
 *   client C : SEND or REPLY blocked on a server in the same process
 *   server S : READY (preempted / not dispatched), effective prio < C.prio
 *   hog    H : RUNNING, S.prio < H.prio < C.prio
 */
static int detect_inversion(const thr_t *t, int n, int *c_tid, int *s_tid, int *h_tid)
{
    for (int c = 0; c < n; c++) {
        if (t[c].state != STATE_SEND && t[c].state != STATE_REPLY) continue;
        if (t[c].blk_pid != g_pid && t[c].blk_pid != 0) continue;
        for (int s = 0; s < n; s++) {
            if (t[s].state != STATE_READY || t[s].prio >= t[c].prio) continue;
            for (int h = 0; h < n; h++) {
                if (t[h].state == STATE_RUNNING && t[h].prio > t[s].prio &&
                    t[h].prio < t[c].prio) {
                    *c_tid = t[c].tid; *s_tid = t[s].tid; *h_tid = t[h].tid;
                    return 1;
                }
            }
        }
    }
    return 0;
}

static void sample_and_emit(void)
{
    if (g_as_fd < 0) {
        find_target();
        if (g_as_fd < 0) return;
    }
    thr_t t[MAX_THREADS];
    debug_process_t pi;
    memset(&pi, 0, sizeof pi);
    int n = snapshot(t, MAX_THREADS, &pi);
    if (n < 0) {
        fprintf(stderr, "[agent] target pid %d exited\n", (int)g_pid);
        char b[128];
        int k = snprintf(b, sizeof b, "{\"t\":\"sched\",\"ts_ms\":%" PRIu64 ",\"pid\":%d,\"gone\":1}",
                         mono_ms(), (int)g_pid);
        broadcast(b, (size_t)k);
        as_close();
        g_inv_latched = 0;
        return;
    }

    int ct = 0, st = 0, ht = 0;
    int inv = detect_inversion(t, n, &ct, &st, &ht);
    if (inv && !g_inv_latched) {              /* count edges, not samples */
        g_inv_total++;
        if (o_trace) {
            char m[96];
            snprintf(m, sizeof m, "chrono: prio-inversion client=%d server=%d hog=%d", ct, st, ht);
            TraceEvent(_NTO_TRACE_INSERTUSRSTREVENT, EV_INVERSION, m);
        }
    }
    g_inv_latched = inv;

    char name[64] = "?";
    as_name(g_as_fd, name, sizeof name);

    static char b[8192];
    int k = snprintf(b, sizeof b,
        "{\"t\":\"sched\",\"ts_ms\":%" PRIu64 ",\"pid\":%d,\"name\":\"%s\",\"nthreads\":%d,"
        "\"inversion\":%d,\"inv_total\":%" PRIu64 ",\"inv_client\":%d,\"inv_server\":%d,"
        "\"inv_hog\":%d,\"threads\":[",
        mono_ms(), (int)g_pid, name, n, inv, g_inv_total, ct, st, ht);
    for (int i = 0; i < n && k < (int)sizeof b - 220; i++) {
        k += snprintf(b + k, sizeof b - (size_t)k,
            "%s{\"tid\":%d,\"state\":\"%s\",\"prio\":%d,\"nom\":%d,\"cpu\":%d,"
            "\"blk_pid\":%d,\"blk_chid\":%d,\"blk_ms\":%" PRIu64 "}",
            i ? "," : "", t[i].tid, state_name((unsigned)t[i].state), t[i].prio, t[i].nom,
            t[i].cpu, t[i].blk_pid, t[i].blk_chid, t[i].blk_ms);
    }
    k += snprintf(b + k, sizeof b - (size_t)k, "]}");
    broadcast(b, (size_t)k);
}

/* ------------------------------------------------------------- sockets */
static int listen_tcp(int port)
{
    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) return -1;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_ANY);
    a.sin_port = htons((uint16_t)port);
    if (bind(s, (struct sockaddr *)&a, sizeof a) < 0 || listen(s, 4) < 0) { close(s); return -1; }
    fcntl(s, F_SETFL, fcntl(s, F_GETFL) | O_NONBLOCK);
    return s;
}

static int bind_udp(int port)
{
    int s = socket(AF_INET, SOCK_DGRAM, 0);
    if (s < 0) return -1;
    int one = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);
    int rb = 1 << 20;
    setsockopt(s, SOL_SOCKET, SO_RCVBUF, &rb, sizeof rb);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    a.sin_port = htons((uint16_t)port);
    if (bind(s, (struct sockaddr *)&a, sizeof a) < 0) { close(s); return -1; }
    fcntl(s, F_SETFL, fcntl(s, F_GETFL) | O_NONBLOCK);
    return s;
}

static void accept_clients(int ls)
{
    for (;;) {
        int fd = accept(ls, NULL, NULL);
        if (fd < 0) return;
        if (g_ncli >= MAX_CLIENTS) { close(fd); continue; }
        fcntl(fd, F_SETFL, fcntl(fd, F_GETFL) | O_NONBLOCK);
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
        cli_t *c = &g_cli[g_ncli++];
        c->fd = fd;
        c->buf = malloc(CLIENT_BUF);
        c->len = 0;
        c->dropped = 0;
        char b[384];
        int k = snprintf(b, sizeof b,
            "{\"t\":\"agent\",\"version\":\"%s\",\"os\":\"QNX 8.0\",\"ncpu\":%d,"
            "\"cycles_per_sec\":%" PRIu64 ",\"watch\":\"%s\",\"interval_ms\":%d}",
            AGENT_VERSION, (int)_syspage_ptr->num_cpu,
            (uint64_t)SYSPAGE_ENTRY(qtime)->cycles_per_sec, o_watch, o_interval_ms);
        cli_queue(c, b, (size_t)k);
        if (g_last_hello[0]) cli_queue(c, g_last_hello, strlen(g_last_hello));
        fprintf(stderr, "[agent] host client connected (%d total)\n", g_ncli);
    }
}

static void pump_udp(int us)
{
    char b[2048];
    for (;;) {
        ssize_t n = recv(us, b, sizeof b - 1, 0);
        if (n <= 0) return;
        while (n > 0 && (b[n - 1] == '\n' || b[n - 1] == '\r')) n--;
        if (n < 8 || b[0] != '{' || b[n - 1] != '}') continue;   /* not ours */
        b[n] = 0;
        if (!strncmp(b, "{\"t\":\"ipc\"", 10)) {
            agg_add(b);
        } else if (!strncmp(b, "{\"t\":\"hello\"", 12)) {
            snprintf(g_last_hello, sizeof g_last_hello, "%s", b);
        } else if (!strncmp(b, "{\"t\":\"end\"", 10)) {
            g_last_hello[0] = 0;
        }
        broadcast(b, (size_t)n);
    }
}

/* ---------------------------------------------------------------- main */
static void usage(const char *p)
{
    printf("usage: %s [options]\n"
           "  --port <p>         TCP listen port for host analyzer (default 9000)\n"
           "  --ingest-port <p>  UDP port ipc_bench sends to       (default 9001)\n"
           "  --interval-ms <n>  /proc sampling period             (default 20)\n"
           "  --watch <name>     process name to inspect           (default ipc_bench)\n"
           "  --pid <pid>        inspect this pid instead of --watch\n"
           "  --cpu <n>          pin the agent to this core        (default: last core)\n"
           "  --prio <n>         agent priority                    (default 30)\n"
           "  --trace            emit TraceEvent() markers on inversion events\n"
           "  -v                 verbose\n", p);
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        #define NEXT() (++i < argc ? argv[i] : (usage(argv[0]), exit(2), (char *)0))
        if      (!strcmp(a, "--port"))         o_port = atoi(NEXT());
        else if (!strcmp(a, "--ingest-port"))  o_ingest = atoi(NEXT());
        else if (!strcmp(a, "--interval-ms"))  o_interval_ms = atoi(NEXT());
        else if (!strcmp(a, "--watch"))        snprintf(o_watch, sizeof o_watch, "%s", NEXT());
        else if (!strcmp(a, "--pid"))          o_pid = atoi(NEXT());
        else if (!strcmp(a, "--cpu"))          o_cpu = atoi(NEXT());
        else if (!strcmp(a, "--prio"))         o_prio = atoi(NEXT());
        else if (!strcmp(a, "--trace"))        o_trace = 1;
        else if (!strcmp(a, "-v"))             o_verbose = 1;
        else if (!strcmp(a, "--help") || !strcmp(a, "-h")) { usage(argv[0]); return 0; }
        else { fprintf(stderr, "unknown option '%s'\n", a); usage(argv[0]); return 2; }
    }
    if (o_interval_ms < 1) o_interval_ms = 1;

    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_sig);
    signal(SIGTERM, on_sig);

    /* Raise priority and stay off the core the workload is pinned to. */
    int ncpu = (int)_syspage_ptr->num_cpu;
    if (o_cpu < 0) o_cpu = ncpu - 1;
    if (ThreadCtl(_NTO_TCTL_RUNMASK, (void *)(uintptr_t)(1u << o_cpu)) == -1)
        fprintf(stderr, "[agent] warn: runmask: %s\n", strerror(errno));
    struct sched_param sp;
    memset(&sp, 0, sizeof sp);
    sp.sched_priority = o_prio;
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp) != 0)
        fprintf(stderr, "[agent] warn: could not set prio %d\n", o_prio);

    int ls = listen_tcp(o_port);
    if (ls < 0) { perror("listen tcp"); return 1; }
    int us = bind_udp(o_ingest);
    if (us < 0) { perror("bind udp"); return 1; }

    fprintf(stderr, "[agent] ChronoQNX agent %s | %d cpu(s), agent on cpu %d prio %d | "
            "tcp 0.0.0.0:%d  udp 127.0.0.1:%d | watch '%s' every %d ms%s\n",
            AGENT_VERSION, ncpu, o_cpu, o_prio, o_port, o_ingest,
            o_pid ? "(pid)" : o_watch, o_interval_ms, o_trace ? " | trace markers on" : "");

    uint64_t next_sample = mono_ms(), next_agg = mono_ms() + 1000;
    struct pollfd pf[2 + MAX_CLIENTS];

    while (!g_quit) {
        int np = 0;
        pf[np].fd = ls; pf[np++].events = POLLIN;
        pf[np].fd = us; pf[np++].events = POLLIN;
        for (int i = 0; i < g_ncli; i++) {
            pf[np].fd = g_cli[i].fd;
            pf[np++].events = POLLIN | (g_cli[i].len ? POLLOUT : 0);
        }
        uint64_t now = mono_ms();
        uint64_t due = next_sample < next_agg ? next_sample : next_agg;
        int to = due > now ? (int)(due - now) : 0;
        poll(pf, (nfds_t)np, to);

        if (pf[0].revents & POLLIN) accept_clients(ls);
        if (pf[1].revents & POLLIN) pump_udp(us);
        for (int i = g_ncli - 1; i >= 0; i--) {         /* detect hangups / drain input */
            short re = (2 + i < np) ? pf[2 + i].revents : 0;
            if (re & (POLLHUP | POLLERR)) { cli_close(i); continue; }
            if (re & POLLIN) {
                char junk[256];
                ssize_t r = recv(g_cli[i].fd, junk, sizeof junk, 0);
                if (r == 0) { cli_close(i); continue; }
            }
        }

        now = mono_ms();
        if (now >= next_sample) {
            sample_and_emit();
            next_sample = now + (uint64_t)o_interval_ms;
        }
        if (now >= next_agg) {
            agg_emit();
            next_agg = now + 1000;
            if (o_verbose)
                fprintf(stderr, "[agent] clients=%d target=%d inv_total=%" PRIu64 "\n",
                        g_ncli, (int)g_pid, g_inv_total);
        }
        flush_all();
    }

    for (int i = g_ncli - 1; i >= 0; i--) cli_close(i);
    close(ls); close(us);
    if (g_as_fd >= 0) close(g_as_fd);
    return 0;
}
