#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/neutrino.h>
int main(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    printf("ChronoQNX Probe Active on QNX 8.0 | PID: %d | Monotonic: %lld.%09ld s\n", getpid(), (long long)ts.tv_sec, ts.tv_nsec);
    return 0;
}
