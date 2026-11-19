/*
 * ChronoQNX - ipc_bench.c
 * QNX 8.0 real-time IPC workload & priority-inversion simulator.
 *
 * Build : qcc -Vgcc_ntox86_64 -O2 workload-sim/ipc_bench.c -o workload-sim/ipc_bench
 * Run   : ipc_bench [--rate 200] [--duration 15] [--mode normal|invert]
 *
 * Threads (all pinned to ONE core so that preemption is deterministic - on an
 * SMP target the hog would otherwise simply run on the other CPU):
 *
 *      client  prio 25   real-time control loop, periodic MsgSend()
 *      hog     prio 15   medium-priority CPU burner (bursts, then sleeps)
 *      server  prio 10   low-priority worker, MsgReceive()/MsgReply()
 *
 *   --mode normal : ChannelCreate(0). The server inherits the sender's
 *                   priority (25) while it services a request, so the hog
 *                   (15) can never preempt it -> tight, bounded latency.
 *   --mode invert : ChannelCreate(_NTO_CHF_FIXED_PRIORITY). The server stays
 *                   at prio 10; the hog (15) preempts it mid-request while
 *                   the prio-25 client is blocked on it -> classic unbounded
 *                   priority inversion, visible as RTT spikes.
 *
 * Telemetry: newline-free JSON records, one per UDP datagram, sent from a
 * dedicated low-impact thread (SPSC ring buffer, never touched by the
 * measurement path) to chrono_agent at 127.0.0.1:9001 by default.
 *
 * Time base of *_ns fields: nanoseconds since benchmark start, derived from
 * ClockCycles() / SYSPAGE_ENTRY(qtime)->cycles_per_sec.
 *
 * License: Apache-2.0
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <signal.h>
#include <pthread.h>
#include <sched.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/neutrino.h>
#include <sys/syspage.h>

/* ---------------------------------------------------------------- config */
#define CLIENT_PRIO   25
#define HOG_PRIO      15
#define SERVER_PRIO   10
#define TELEM_PRIO    20
#define MSG_TYPE      0x2000u          /* > _IO_MAX: not a resmgr message  */
#define MSG_STOP      0x1u
#define RING_SZ       16384u           /* power of two                     */

enum { MODE_NORMAL = 0, MODE_INVERT = 1 };

typedef struct {
    uint16_t type;
    uint16_t pad0;
    uint32_t seq;
    uint32_t flags;
    uint32_t pad1;
    uint64_t send_cycles;
    uint8_t  payload[40];
} req_t;

typedef struct {
    uint32_t seq;
    int32_t  server_tid;
    int32_t  eff_prio;      /* server's *effective* priority at MsgReceive */
    int32_t  nom_prio;      /* server's nominal priority                   */
    uint64_t recv_cycles;
    uint64_t reply_cycles;
} rep_t;

typedef struct {
    uint32_t seq;
    int32_t  ctid, stid, cprio, sprio, snom;
    uint64_t send_ns, recv_ns, reply_ns;
    double   rtt_us, jitter_us, rel_us;
    int      inv;
} rec_t;

/* ---------------------------------------------------------------- globals */
static int      g_mode        = MODE_NORMAL;
static int      g_rate        = 200;
static int      g_duration    = 15;
static int      g_cpu         = 0;
static int      g_work_us     = 100;     /* simulated server processing time */
static int      g_hog_burst   = 8;       /* ms of CPU burn per hog cycle     */
static int      g_hog_period  = 23;      /* ms hog cycle (coprime with 5 ms) */
static double   g_inv_us      = 1000.0;  /* RTT above this + prio gap => flag */
static int      g_telemetry   = 1;
static int      g_quiet       = 0;
static char     g_sink_host[64] = "127.0.0.1";
static int      g_sink_port   = 9001;

static uint64_t g_cps;                    /* cycles per second               */
static uint64_t g_t0;                     /* ClockCycles() at start          */
static int      g_chid        = -1;
static volatile sig_atomic_t g_stop = 0;
static atomic_int g_run       = 1;        /* worker threads keep going       */
static atomic_int g_done      = 0;        /* client finished                 */

static atomic_int g_client_tid, g_server_tid, g_hog_tid;
static atomic_ullong g_hog_bursts;

static rec_t           g_ring[RING_SZ];
static atomic_uint     g_head, g_tail;
static atomic_ulong    g_dropped;

/* ---------------------------------------------------------------- helpers */
static inline uint64_t cyc2ns(uint64_t c)
{
    uint64_t s = c / g_cps, r = c % g_cps;
    return s * 1000000000ull + (r * 1000000000ull) / g_cps;
}

static inline uint64_t ns_since_start(uint64_t cycles)
{
    return cyc2ns(cycles - g_t0);
}

static void spin_us(uint64_t us)
{
    uint64_t end = ClockCycles() + (us * g_cps) / 1000000ull;
    while ((int64_t)(ClockCycles() - end) < 0) {
        __asm__ __volatile__("pause");
    }
}

static void on_sigint(int s) { (void)s; g_stop = 1; }

static void pin_to_cpu(int cpu)
{
    /* runmask bit N == CPU N (passed by value for _NTO_TCTL_RUNMASK) */
    if (ThreadCtl(_NTO_TCTL_RUNMASK, (void *)(uintptr_t)(1u << cpu)) == -1) {
        fprintf(stderr, "warn: ThreadCtl(RUNMASK cpu %d): %s\n", cpu, strerror(errno));
    }
}

static int spawn(pthread_t *t, void *(*fn)(void *), int prio)
{
    pthread_attr_t a;
    struct sched_param sp;
    pthread_attr_init(&a);
    pthread_attr_setinheritsched(&a, PTHREAD_EXPLICIT_SCHED);
    pthread_attr_setschedpolicy(&a, SCHED_FIFO);
    memset(&sp, 0, sizeof sp);
    sp.sched_priority = prio;
    pthread_attr_setschedparam(&a, &sp);
    int rc = pthread_create(t, &a, fn, NULL);
    pthread_attr_destroy(&a);
    return rc;
}

static int cur_prio(int *nominal)
{
    struct sched_param sp;
    memset(&sp, 0, sizeof sp);
    if (SchedGet(0, 0, &sp) == -1) {
        if (nominal) *nominal = -1;
        return -1;
    }
    if (nominal) *nominal = sp.sched_priority;
    return sp.sched_curpriority;
}

/* ---------------------------------------------------------------- server */
static void *server_thread(void *arg)
{
    (void)arg;
    pin_to_cpu(g_cpu);
    atomic_store(&g_server_tid, (int)pthread_self());

    req_t req;
    rep_t rep;
    for (;;) {
        struct _msg_info info;
        int rcvid = MsgReceive(g_chid, &req, sizeof req, &info);
        if (rcvid == -1) {
            if (errno == EINTR) continue;
            break;
        }
        if (rcvid == 0) continue;                 /* pulse - ignore */

        uint64_t recv_c = ClockCycles();
        int nom;
        int eff = cur_prio(&nom);

        if (!(req.flags & MSG_STOP)) {
            spin_us((uint64_t)g_work_us);         /* simulated request work */
        }

        memset(&rep, 0, sizeof rep);
        rep.seq          = req.seq;
        rep.server_tid   = (int32_t)pthread_self();
        rep.eff_prio     = eff;
        rep.nom_prio     = nom;
        rep.recv_cycles  = recv_c;
        rep.reply_cycles = ClockCycles();
        MsgReply(rcvid, EOK, &rep, sizeof rep);

        if (req.flags & MSG_STOP) break;
    }
    return NULL;
}

/* ------------------------------------------------------------------- hog */
static void *hog_thread(void *arg)
{
    (void)arg;
    pin_to_cpu(g_cpu);
    atomic_store(&g_hog_tid, (int)pthread_self());

    struct timespec gap;
    int sleep_ms = g_hog_period - g_hog_burst;
    if (sleep_ms < 0) sleep_ms = 0;
    gap.tv_sec  = sleep_ms / 1000;
    gap.tv_nsec = (long)(sleep_ms % 1000) * 1000000L;

    while (atomic_load(&g_run)) {
        uint64_t end = ClockCycles() + ((uint64_t)g_hog_burst * g_cps) / 1000ull;
        while ((int64_t)(ClockCycles() - end) < 0) {
            __asm__ __volatile__("pause");
        }
        atomic_fetch_add(&g_hog_bursts, 1);
        if (sleep_ms > 0) nanosleep(&gap, NULL);
    }
    return NULL;
}

/* ------------------------------------------------------------- telemetry */
static void ring_push(const rec_t *r)
{
    unsigned h = atomic_load_explicit(&g_head, memory_order_relaxed);
    unsigned t = atomic_load_explicit(&g_tail, memory_order_acquire);
    if (h - t >= RING_SZ) {
        atomic_fetch_add(&g_dropped, 1);
        return;
    }
    g_ring[h & (RING_SZ - 1)] = *r;
    atomic_store_explicit(&g_head, h + 1, memory_order_release);
}

static int g_sock = -1;
static struct sockaddr_in g_sink;

static void emit(const char *buf, int n)
{
    if (g_sock >= 0 && n > 0) {
        sendto(g_sock, buf, (size_t)n, 0, (struct sockaddr *)&g_sink, sizeof g_sink);
    }
}

static void emit_hello(void)
{
    char b[512];
    int n = snprintf(b, sizeof b,
        "{\"t\":\"hello\",\"pid\":%d,\"mode\":\"%s\",\"rate\":%d,\"duration\":%d,"
        "\"chid\":%d,\"cpu\":%d,\"client_tid\":%d,\"server_tid\":%d,\"hog_tid\":%d,"
        "\"client_prio\":%d,\"server_prio\":%d,\"hog_prio\":%d,"
        "\"fixed_priority\":%d,\"inv_threshold_us\":%.0f}",
        (int)getpid(), g_mode == MODE_INVERT ? "invert" : "normal", g_rate,
        g_duration, g_chid, g_cpu, atomic_load(&g_client_tid),
        atomic_load(&g_server_tid), atomic_load(&g_hog_tid), CLIENT_PRIO,
        SERVER_PRIO, HOG_PRIO, g_mode == MODE_INVERT, g_inv_us);
    emit(b, n);
}

static void emit_rec(const rec_t *r)
{
    char b[512];
    int n = snprintf(b, sizeof b,
        "{\"t\":\"ipc\",\"seq\":%u,\"client_tid\":%d,\"server_tid\":%d,"
        "\"client_prio\":%d,\"server_prio\":%d,\"server_nominal\":%d,"
        "\"send_ns\":%" PRIu64 ",\"recv_ns\":%" PRIu64 ",\"reply_ns\":%" PRIu64 ","
        "\"rtt_us\":%.2f,\"jitter_us\":%.2f,\"rel_us\":%.2f,\"inversion_flag\":%d}",
        r->seq, r->ctid, r->stid, r->cprio, r->sprio, r->snom,
        r->send_ns, r->recv_ns, r->reply_ns,
        r->rtt_us, r->jitter_us, r->rel_us, r->inv);
    emit(b, n);
}

static unsigned drain(void)
{
    unsigned sent = 0;
    unsigned t = atomic_load_explicit(&g_tail, memory_order_relaxed);
    unsigned h = atomic_load_explicit(&g_head, memory_order_acquire);
    while (t != h) {
        emit_rec(&g_ring[t & (RING_SZ - 1)]);
        t++;
        sent++;
        atomic_store_explicit(&g_tail, t, memory_order_release);
    }
    return sent;
}

static void *telemetry_thread(void *arg)
{
    (void)arg;
    int ncpu = (int)_syspage_ptr->num_cpu;
    pin_to_cpu(ncpu > 1 ? (g_cpu + 1) % ncpu : g_cpu);   /* keep off the hot core */

    struct timespec nap = { 0, 2 * 1000 * 1000 };       /* 2 ms */
    uint64_t last_hello = 0;
    uint64_t total = 0;

    while (!atomic_load(&g_done)) {
        total += drain();
        uint64_t now = ClockCycles();
        if (now - last_hello > g_cps / 2) {              /* 2 Hz hello */
            emit_hello();
            last_hello = now;
        }
        nanosleep(&nap, NULL);
    }
    total += drain();

    char b[256];
    int n = snprintf(b, sizeof b,
        "{\"t\":\"end\",\"pid\":%d,\"mode\":\"%s\",\"samples\":%" PRIu64 ",\"dropped\":%lu}",
        (int)getpid(), g_mode == MODE_INVERT ? "invert" : "normal", total,
        (unsigned long)atomic_load(&g_dropped));
    for (int i = 0; i < 3; i++) { emit(b, n); nanosleep(&nap, NULL); }
    return NULL;
}

/* ---------------------------------------------------------------- client */
static double *g_rtt;           /* all RTTs for the final summary */
static double *g_rel;
static int     g_n;
static int     g_inv_cnt;
static int     g_obs_eff, g_obs_nom;    /* last observed server priorities */

static void *client_thread(void *arg)
{
    (void)arg;
    pin_to_cpu(g_cpu);
    int ctid = (int)pthread_self();
    atomic_store(&g_client_tid, ctid);

    int coid = ConnectAttach(0, 0, g_chid, _NTO_SIDE_CHANNEL, 0);
    if (coid == -1) {
        perror("ConnectAttach");
        exit(1);
    }

    long long period_ns = 1000000000LL / g_rate;
    long long total = (long long)g_rate * g_duration;
    struct timespec next;
    clock_gettime(CLOCK_MONOTONIC, &next);
    next.tv_nsec += 50 * 1000 * 1000;                    /* 50 ms settle */
    if (next.tv_nsec >= 1000000000L) { next.tv_sec++; next.tv_nsec -= 1000000000L; }

    double prev_rtt = -1.0;
    req_t req;
    rep_t rep;
    memset(&req, 0, sizeof req);
    req.type = MSG_TYPE;

    for (long long i = 0; i < total && !g_stop; i++) {
        clock_nanosleep(CLOCK_MONOTONIC, TIMER_ABSTIME, &next, NULL);
        struct timespec woke;
        clock_gettime(CLOCK_MONOTONIC, &woke);
        double rel_us = ((woke.tv_sec - next.tv_sec) * 1e9 +
                         (woke.tv_nsec - next.tv_nsec)) / 1000.0;
        next.tv_nsec += period_ns;
        while (next.tv_nsec >= 1000000000L) { next.tv_sec++; next.tv_nsec -= 1000000000L; }

        req.seq = (uint32_t)(i + 1);
        uint64_t send_c = ClockCycles();
        req.send_cycles = send_c;
        int rc = MsgSend(coid, &req, sizeof req, &rep, sizeof rep);
        uint64_t done_c = ClockCycles();
        if (rc == -1) {
            if (errno == EINTR) continue;
            perror("MsgSend");
            break;
        }

        rec_t r;
        r.seq      = req.seq;
        r.ctid     = ctid;
        r.stid     = rep.server_tid;
        r.cprio    = CLIENT_PRIO;
        r.sprio    = rep.eff_prio;
        r.snom     = rep.nom_prio;
        r.send_ns  = ns_since_start(send_c);
        r.recv_ns  = ns_since_start(rep.recv_cycles);
        r.reply_ns = ns_since_start(rep.reply_cycles);
        r.rtt_us   = (double)cyc2ns(done_c - send_c) / 1000.0;
        r.jitter_us = prev_rtt < 0 ? 0.0 : (r.rtt_us > prev_rtt ? r.rtt_us - prev_rtt
                                                                 : prev_rtt - r.rtt_us);
        r.rel_us   = rel_us;
        r.inv      = (r.sprio < r.cprio) && (r.rtt_us > g_inv_us);
        prev_rtt   = r.rtt_us;

        g_rtt[g_n] = r.rtt_us;
        g_rel[g_n] = rel_us;
        g_n++;
        g_inv_cnt += r.inv;
        g_obs_eff = r.sprio;
        g_obs_nom = r.snom;
        if (g_telemetry) ring_push(&r);
    }

    /* tell the server to exit */
    req.flags = MSG_STOP;
    MsgSend(coid, &req, sizeof req, &rep, sizeof rep);
    ConnectDetach(coid);
    atomic_store(&g_done, 1);
    return NULL;
}

/* --------------------------------------------------------------- summary */
static int cmp_d(const void *a, const void *b)
{
    double x = *(const double *)a, y = *(const double *)b;
    return (x > y) - (x < y);
}

static double pct(const double *s, int n, double p)
{
    if (n <= 0) return 0.0;
    int idx = (int)((p / 100.0) * n + 0.999999) - 1;
    if (idx < 0) idx = 0;
    if (idx >= n) idx = n - 1;
    return s[idx];
}

static void summary(void)
{
    if (g_n == 0) { printf("no samples\n"); return; }
    double *s = malloc(sizeof(double) * (size_t)g_n);
    memcpy(s, g_rtt, sizeof(double) * (size_t)g_n);
    qsort(s, (size_t)g_n, sizeof(double), cmp_d);
    double sum = 0, rmax = 0;
    for (int i = 0; i < g_n; i++) { sum += g_rtt[i]; if (g_rel[i] > rmax) rmax = g_rel[i]; }
    printf("\n=== ipc_bench summary  mode=%s  rate=%d Hz  samples=%d ===\n",
           g_mode == MODE_INVERT ? "invert" : "normal", g_rate, g_n);
    printf("  server prio at MsgReceive : %d (server base prio %d, client %d) -> %s\n",
           g_obs_eff, SERVER_PRIO, CLIENT_PRIO,
           g_obs_eff < CLIENT_PRIO ? "NO priority inheritance" : "priority inherited from client");
    printf("  RTT us   min=%.1f  mean=%.1f  p50=%.1f  p95=%.1f  p99=%.1f  p99.9=%.1f  WCET=%.1f\n",
           s[0], sum / g_n, pct(s, g_n, 50), pct(s, g_n, 95), pct(s, g_n, 99),
           pct(s, g_n, 99.9), s[g_n - 1]);
    printf("  client release jitter max : %.1f us\n", rmax);
    printf("  priority-inversion events : %d (RTT > %.0f us with server prio < client prio)\n",
           g_inv_cnt, g_inv_us);
    printf("  hog bursts                : %llu   telemetry dropped: %lu\n",
           (unsigned long long)atomic_load(&g_hog_bursts), (unsigned long)atomic_load(&g_dropped));
    free(s);
}

/* ------------------------------------------------------------------ main */
static void usage(const char *p)
{
    printf("usage: %s [options]\n"
           "  --rate <hz>          client request rate            (default 200)\n"
           "  --duration <sec>     run time                       (default 15)\n"
           "  --mode normal|invert priority inheritance on/off    (default normal)\n"
           "  --sink <host:port>   UDP telemetry sink             (default 127.0.0.1:9001)\n"
           "  --no-telemetry       do not emit telemetry\n"
           "  --work-us <us>       simulated server work          (default 100)\n"
           "  --hog-burst-ms <ms>  hog CPU burst length           (default 8)\n"
           "  --hog-period-ms <ms> hog cycle length               (default 23)\n"
           "  --inv-us <us>        inversion RTT threshold        (default 1000)\n"
           "  --cpu <n>            core to pin all threads to     (default 0)\n"
           "  --quiet              only print the summary\n", p);
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i];
        #define NEXT() (++i < argc ? argv[i] : (usage(argv[0]), exit(2), (char *)0))
        if      (!strcmp(a, "--rate"))          g_rate       = atoi(NEXT());
        else if (!strcmp(a, "--duration"))      g_duration   = atoi(NEXT());
        else if (!strcmp(a, "--work-us"))       g_work_us    = atoi(NEXT());
        else if (!strcmp(a, "--hog-burst-ms"))  g_hog_burst  = atoi(NEXT());
        else if (!strcmp(a, "--hog-period-ms")) g_hog_period = atoi(NEXT());
        else if (!strcmp(a, "--inv-us"))        g_inv_us     = atof(NEXT());
        else if (!strcmp(a, "--cpu"))           g_cpu        = atoi(NEXT());
        else if (!strcmp(a, "--no-telemetry"))  g_telemetry  = 0;
        else if (!strcmp(a, "--quiet"))         g_quiet      = 1;
        else if (!strcmp(a, "--mode")) {
            const char *m = NEXT();
            if      (!strcmp(m, "normal")) g_mode = MODE_NORMAL;
            else if (!strcmp(m, "invert")) g_mode = MODE_INVERT;
            else { fprintf(stderr, "bad --mode '%s'\n", m); return 2; }
        } else if (!strcmp(a, "--sink")) {
            char *v = strdup(NEXT());
            char *c = strrchr(v, ':');
            if (c) { *c = 0; g_sink_port = atoi(c + 1); }
            snprintf(g_sink_host, sizeof g_sink_host, "%s", v);
            free(v);
        } else if (!strcmp(a, "--help") || !strcmp(a, "-h")) { usage(argv[0]); return 0; }
        else { fprintf(stderr, "unknown option '%s'\n", a); usage(argv[0]); return 2; }
    }
    if (g_rate < 1 || g_rate > 10000 || g_duration < 1 || g_hog_burst < 0 ||
        g_hog_period < g_hog_burst) {
        fprintf(stderr, "invalid argument value\n");
        return 2;
    }
    int ncpu = (int)_syspage_ptr->num_cpu;
    if (g_cpu < 0 || g_cpu >= ncpu) g_cpu = 0;

    g_cps = SYSPAGE_ENTRY(qtime)->cycles_per_sec;
    g_t0  = ClockCycles();
    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    long long cap = (long long)g_rate * g_duration + 16;
    g_rtt = calloc((size_t)cap, sizeof(double));
    g_rel = calloc((size_t)cap, sizeof(double));
    if (!g_rtt || !g_rel) { perror("calloc"); return 1; }

    if (g_telemetry) {
        g_sock = socket(AF_INET, SOCK_DGRAM, 0);
        memset(&g_sink, 0, sizeof g_sink);
        g_sink.sin_family = AF_INET;
        g_sink.sin_port   = htons((uint16_t)g_sink_port);
        if (inet_pton(AF_INET, g_sink_host, &g_sink.sin_addr) != 1) {
            fprintf(stderr, "bad sink host '%s'\n", g_sink_host);
            return 2;
        }
    }

    /* The whole point of the experiment is this one flag. */
    unsigned chflags = (g_mode == MODE_INVERT) ? _NTO_CHF_FIXED_PRIORITY : 0;
    g_chid = ChannelCreate(chflags);
    if (g_chid == -1) { perror("ChannelCreate"); return 1; }

    if (!g_quiet) {
        printf("ChronoQNX ipc_bench | pid %d | %d cpu(s), pinned to cpu %d | %.0f MHz cycle clock\n",
               (int)getpid(), ncpu, g_cpu, g_cps / 1e6);
        printf("mode=%s ChannelCreate(%s) | client=%d hog=%d server=%d | %d Hz x %d s | sink %s\n",
               g_mode == MODE_INVERT ? "invert" : "normal",
               g_mode == MODE_INVERT ? "_NTO_CHF_FIXED_PRIORITY" : "0",
               CLIENT_PRIO, HOG_PRIO, SERVER_PRIO, g_rate, g_duration,
               g_telemetry ? "udp" : "off");
    }

    pthread_t th_srv, th_hog, th_cli, th_tel;
    int rc;
    if ((rc = spawn(&th_srv, server_thread, SERVER_PRIO)) ||
        (rc = spawn(&th_hog, hog_thread, HOG_PRIO)) ||
        (rc = spawn(&th_tel, telemetry_thread, TELEM_PRIO))) {
        fprintf(stderr, "pthread_create: %s\n", strerror(rc));
        return 1;
    }
    usleep(100 * 1000);                 /* let server/hog publish their tids */
    if ((rc = spawn(&th_cli, client_thread, CLIENT_PRIO))) {
        fprintf(stderr, "pthread_create(client): %s\n", strerror(rc));
        return 1;
    }

    pthread_join(th_cli, NULL);
    pthread_join(th_srv, NULL);
    atomic_store(&g_run, 0);
    pthread_join(th_hog, NULL);
    pthread_join(th_tel, NULL);

    summary();
    ChannelDestroy(g_chid);
    return 0;
}
