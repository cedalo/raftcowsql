#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <uv.h>

#include "../../include/raft.h"
#include "../../include/raft/uv.h"

#include "fs.h"
#include "submit.h"
#include "submit_parse.h"

static int fsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    (void)fsm;
    (void)buf;
    (void)result;
    return 0;
}

struct server
{
    struct uv_loop_s *loop;
    struct uv_timer_s timer;
    struct raft_configuration configuration;
    struct raft_uv_transport transport;
    struct raft_io io;
    struct raft_fsm fsm;
    struct raft raft;
    struct raft_buffer buf;
    struct raft_apply req;
    char *path;
    unsigned i;
    unsigned n;
    time_t start;
    time_t *latencies;
};

int serverInit(struct server *s,
               struct submitOptions *opts,
               struct uv_loop_s *loop)
{
    const char *address = "127.0.0.1:8080";
    int rv;

    s->loop = loop;
    s->transport.version = 1;
    s->transport.data = NULL;

    s->i = 0;
    s->n = opts->n;
    s->latencies = malloc(opts->n * sizeof *s->latencies);
    s->buf.len = opts->size;

    rv = FsCreateTempDir(opts->dir, &s->path);
    if (rv != 0) {
        printf("failed to create temp dir\n");
        return -1;
    }

    rv = raft_uv_tcp_init(&s->transport, loop);
    if (rv != 0) {
        printf("failed to init transport\n");
        return -1;
    }

    rv = raft_uv_init(&s->io, loop, s->path, &s->transport);
    if (rv != 0) {
        printf("failed to init io\n");
        return -1;
    }

    s->fsm.version = 1;
    s->fsm.apply = fsmApply;
    s->fsm.snapshot = NULL;
    s->fsm.restore = NULL;

    rv = raft_init(&s->raft, &s->io, &s->fsm, 1, address);
    if (rv != 0) {
        printf("failed to init raft\n");
        return -1;
    }

    raft_configuration_init(&s->configuration);
    rv = raft_configuration_add(&s->configuration, 1, address, RAFT_VOTER);
    if (rv != 0) {
        printf("failed to populate configuration\n");
        return -1;
    }
    rv = raft_bootstrap(&s->raft, &s->configuration);
    if (rv != 0) {
        printf("failed to bootstrap\n");
        return -1;
    }
    raft_configuration_close(&s->configuration);

    /* Effectively disable snapshotting. */
    raft_set_snapshot_threshold(&s->raft, 1024 * 1024);

    rv = raft_start(&s->raft);
    if (rv != 0) {
        printf("failed to start raft '%s'\n", raft_strerror(rv));
        return -1;
    }

    s->req.data = s;

    uv_timer_init(s->loop, &s->timer);

    return 0;
}

static int serverClose(struct server *s)
{
    int rv;

    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);

    rv = FsRemoveTempDir(s->path);
    if (rv != 0) {
        printf("failed to remove temp dir\n");
        return -1;
    }

    free(s->latencies);

    return 0;
}

static int submitEntry(struct server *s);

static void raftCloseCb(struct raft *r)
{
    struct server *s = r->data;
    uv_close((struct uv_handle_s *)&s->timer, NULL);
}

static void serverTimerCb(uv_timer_t *timer)
{
    struct server *s = timer->data;
    s->raft.data = s;
    raft_close(&s->raft, raftCloseCb);
}

static void submitEntryCb(struct raft_apply *req, int status, void *result)
{
    struct server *s = req->data;
    int rv;

    (void)result;

    if (status != 0) {
        printf("submission cb failed\n");
        exit(1);
    }

    s->latencies[s->i] = (time_t)uv_hrtime() - s->start;

    s->i++;
    if (s->i == s->n) {
        s->timer.data = s;
        /* Run raft_close in the next loop iteration, to avoid calling it from a
         * this commit callback, which triggers a bug in raft. */
        uv_timer_start(&s->timer, serverTimerCb, 125, 0);
        return;
    }

    rv = submitEntry(s);
    if (rv != 0) {
        printf("submission failed\n");
        exit(1);
    }
}

static int submitEntry(struct server *s)
{
    int rv;
    s->start = (time_t)uv_hrtime();
    const struct raft_buffer *bufs = &s->buf;

    s->buf.base = raft_malloc(s->buf.len);
    assert(s->buf.base != NULL);

    rv = raft_apply(&s->raft, &s->req, bufs, 1, submitEntryCb);
    if (rv != 0) {
        return -1;
    }

    return 0;
}

int SubmitRun(int argc, char *argv[], struct report *report)
{
    struct submitOptions opts;
    struct uv_loop_s loop;
    struct server server;
    struct metric *m;
    struct benchmark *benchmark;
    char *name;
    int rv;

    SubmitParse(argc, argv, &opts);

    rv = uv_loop_init(&loop);
    if (rv != 0) {
        printf("failed to init loop\n");
        return -1;
    }

    rv = serverInit(&server, &opts, &loop);
    if (rv != 0) {
        printf("failed to init server\n");
        return -1;
    }

    rv = submitEntry(&server);
    if (rv != 0) {
        printf("failed to submit entry\n");
        return -1;
    }

    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        printf("failed to run loop\n");
        return -1;
    }

    uv_loop_close(&loop);

    name = malloc(strlen("submit") + 1);
    strcpy(name, "submit");
    benchmark = ReportGrow(report, name);
    m = BenchmarkGrow(benchmark, METRIC_KIND_LATENCY);
    MetricFillLatency(m, server.latencies, server.n);

    rv = serverClose(&server);
    if (rv != 0) {
        printf("failed to cleanup\n");
        return -1;
    }

    return 0;
}