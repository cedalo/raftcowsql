#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../include/raft.h"
#include "../include/raft/uv.h"

#define N_SERVERS 3    /* Number of servers in the example cluster */
#define APPLY_RATE 125 /* Apply a new entry every 125 milliseconds */

#define Log(SERVER_ID, FORMAT) printf("%d: " FORMAT "\n", SERVER_ID)
#define Logf(SERVER_ID, FORMAT, ...) \
    printf("%d: " FORMAT "\n", SERVER_ID, __VA_ARGS__)

/********************************************************************
 *
 * Sample application FSM that just increases a counter.
 *
 ********************************************************************/

struct Fsm
{
    unsigned long long count;
};

static int FsmApply(struct raft_fsm *fsm,
                    const struct raft_buffer *buf,
                    void **result)
{
    struct Fsm *f = fsm->data;
    if (buf->len != 8) {
        return RAFT_MALFORMED;
    }
    f->count += *(uint64_t *)buf->base;
    *result = &f->count;
    return 0;
}

static int FsmSnapshot(struct raft_fsm *fsm,
                       struct raft_buffer *bufs[],
                       unsigned *n_bufs)
{
    struct Fsm *f = fsm->data;
    *n_bufs = 1;
    *bufs = raft_malloc(sizeof **bufs);
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = sizeof(uint64_t);
    (*bufs)[0].base = raft_malloc((*bufs)[0].len);
    if ((*bufs)[0].base == NULL) {
        return RAFT_NOMEM;
    }
    *(uint64_t *)(*bufs)[0].base = f->count;
    return 0;
}

static int FsmRestore(struct raft_fsm *fsm, struct raft_buffer *buf)
{
    struct Fsm *f = fsm->data;
    if (buf->len != sizeof(uint64_t)) {
        return RAFT_MALFORMED;
    }
    f->count = *(uint64_t *)buf->base;
    raft_free(buf->base);
    return 0;
}

static int FsmInit(struct raft_fsm *fsm)
{
    struct Fsm *f = raft_malloc(sizeof *f);
    if (f == NULL) {
        return RAFT_NOMEM;
    }
    f->count = 0;
    fsm->version = 2;
    fsm->data = f;
    fsm->apply = FsmApply;
    fsm->snapshot = FsmSnapshot;
    fsm->snapshot_finalize = NULL;
    fsm->restore = FsmRestore;
    return 0;
}

static void FsmClose(struct raft_fsm *f)
{
    if (f->data != NULL) {
        raft_free(f->data);
    }
}

/********************************************************************
 *
 * Example struct holding a single raft server instance and all its
 * dependencies.
 *
 ********************************************************************/

struct Server;
typedef void (*ServerCloseCb)(struct Server *server);

typedef int (*raft_io_start_fn)(struct raft_io *io,
                                unsigned msecs,
                                raft_io_tick_cb tick,
                                raft_io_recv_cb recv);

struct Server
{
    void *data;                         /* User data context. */
    struct uv_loop_s *loop;             /* UV loop. */
    struct uv_timer_s timer;            /* To periodically apply a new entry. */
    const char *dir;                    /* Data dir of UV I/O backend. */
    struct raft_uv_transport transport; /* UV I/O backend transport. */
    struct raft_io io;                  /* UV I/O backend. */
    struct raft_fsm fsm;                /* Sample application FSM. */
    unsigned id;                        /* Raft instance ID. */
    char address[64];                   /* Raft instance address. */
    struct raft raft;                   /* Raft instance. */
    struct raft_transfer transfer;      /* Transfer leadership request. */
    ServerCloseCb close_cb;             /* Optional close callback. */
    raft_io_start_fn raft_start_fn; /* The original raft_io start function */
    raft_io_recv_cb raft_recv_cb;   /* Original raft recv cb */
};

static void serverRaftCloseCb(struct raft *raft)
{
    struct Server *s = raft->data;
    raft_uv_close(&s->io);
    raft_uv_tcp_close(&s->transport);
    FsmClose(&s->fsm);
    if (s->close_cb != NULL) {
        s->close_cb(s);
    }
}

static void serverTransferCb(struct raft_transfer *req)
{
    struct Server *s = req->data;
    raft_id id;
    const char *address;
    raft_leader(&s->raft, &id, &address);
    raft_close(&s->raft, serverRaftCloseCb);
}

/* Final callback in the shutdown sequence, invoked after the timer handle has
 * been closed. */
static void serverTimerCloseCb(struct uv_handle_s *handle)
{
    struct Server *s = handle->data;
    if (s->raft.data != NULL) {
        if (s->raft.state == RAFT_LEADER) {
            int rv;
            rv = raft_transfer(&s->raft, &s->transfer, 0, serverTransferCb);
            if (rv == 0) {
                return;
            }
        }
        raft_close(&s->raft, serverRaftCloseCb);
    }
}

static void custom_recv_cb(struct raft_io *io, struct raft_message *msg)
{
    struct raft *r = io->data;
    struct Server *s = r->data;

    if (msg->type == RAFT_REQUEST_CUSTOM) {
        Logf(s->id, "Received custom request from %llu: %s", msg->server_id,
             msg->server_address ? msg->server_address : "<NULL>");
        return;
    }

    s->raft_recv_cb(io, msg);
}

static int custom_uv_start(struct raft_io *io,
                           unsigned msecs,
                           raft_io_tick_cb tick_cb,
                           raft_io_recv_cb recv_cb)
{
    struct raft *r = io->data;
    struct Server *s = r->data;
    s->raft_recv_cb = recv_cb;

    return s->raft_start_fn(io, msecs, tick_cb, &custom_recv_cb);
}

/* Initialize the example server struct, without starting it yet. */
static int ServerInit(struct Server *s,
                      struct uv_loop_s *loop,
                      const char *dir,
                      unsigned id)
{
    struct raft_configuration configuration;
    struct timespec now;
    unsigned i;
    int rv;

    memset(s, 0, sizeof *s);

    /* Seed the random generator */
    timespec_get(&now, TIME_UTC);
    srandom((unsigned)(now.tv_nsec ^ now.tv_sec));

    s->loop = loop;

    /* Add a timer to periodically try to propose a new entry. */
    rv = uv_timer_init(s->loop, &s->timer);
    if (rv != 0) {
        Logf(s->id, "uv_timer_init(): %s", uv_strerror(rv));
        goto err;
    }
    s->timer.data = s;

    /* Initialize the TCP-based RPC transport. */
    s->transport.version = 1;
    s->transport.data = NULL;
    rv = raft_uv_tcp_init(&s->transport, s->loop);
    if (rv != 0) {
        goto err;
    }

    /* Initialize the libuv-based I/O backend. */
    rv = raft_uv_init(&s->io, s->loop, dir, &s->transport);
    if (rv != 0) {
        Logf(s->id, "raft_uv_init(): %s", s->io.errmsg);
        goto err_after_uv_tcp_init;
    }
    s->raft_start_fn = s->io.start;
    s->io.start = custom_uv_start;

    /* Initialize the finite state machine. */
    rv = FsmInit(&s->fsm);
    if (rv != 0) {
        Logf(s->id, "FsmInit(): %s", raft_strerror(rv));
        goto err_after_uv_init;
    }

    /* Save the server ID. */
    s->id = id;

    /* Render the address. */
    sprintf(s->address, "127.0.0.1:900%d", id);

    /* Initialize and start the engine, using the libuv-based I/O backend. */
    rv = raft_init(&s->raft, &s->io, &s->fsm, id, s->address);
    if (rv != 0) {
        Logf(s->id, "raft_init(): %s", raft_errmsg(&s->raft));
        goto err_after_fsm_init;
    }
    s->raft.data = s;

    /* Bootstrap the initial configuration if needed. */
    raft_configuration_init(&configuration);
    for (i = 0; i < N_SERVERS; i++) {
        char address[64];
        unsigned server_id = i + 1;
        sprintf(address, "127.0.0.1:900%d", server_id);
        rv = raft_configuration_add(&configuration, server_id, address,
                                    RAFT_VOTER);
        if (rv != 0) {
            Logf(s->id, "raft_configuration_add(): %s", raft_strerror(rv));
            goto err_after_configuration_init;
        }
    }
    rv = raft_bootstrap(&s->raft, &configuration);
    if (rv != 0 && rv != RAFT_CANTBOOTSTRAP) {
        goto err_after_configuration_init;
    }
    raft_configuration_close(&configuration);

    raft_set_snapshot_threshold(&s->raft, 64);
    raft_set_snapshot_trailing(&s->raft, 16);
    raft_set_pre_vote(&s->raft, true);

    s->transfer.data = s;

    return 0;

err_after_configuration_init:
    raft_configuration_close(&configuration);
err_after_fsm_init:
    FsmClose(&s->fsm);
err_after_uv_init:
    raft_uv_close(&s->io);
err_after_uv_tcp_init:
    raft_uv_tcp_close(&s->transport);
err:
    return rv;
}

/* Called after a request to apply a new command to the FSM has been
 * completed. */
static void serverApplyCb(struct raft_apply *req, int status, void *result)
{
    struct Server *s = req->data;
    int count;
    raft_free(req);
    if (status != 0) {
        if (status != RAFT_LEADERSHIPLOST) {
            Logf(s->id, "raft_apply() callback: %s (%d)", raft_errmsg(&s->raft),
                 status);
        }
        return;
    }
    count = *(int *)result;
    if (count % 100 == 0) {
        Logf(s->id, "count %d", count);
    }
}

static void sendCustomMessageCb(struct raft_io_send *send, int status)
{
    struct raft_message *message = send->data;

    (void)status;

    if (message) {
        raft_free(message->request_custom.data.base);
        raft_free(message);
    }
    raft_free(send);
}

static void sendCustomMessageToLeader(struct Server *s)
{
    int rv;
    raft_id leader_id;
    const char *leader_address = NULL;
    raft_leader(&s->raft, &leader_id, &leader_address);
    if (!leader_address) {
        return;
    }

    struct raft_io_send *custom_req = raft_malloc(sizeof(struct raft_io_send));
    struct raft_message *message = raft_malloc(sizeof(struct raft_message));

    if (!custom_req || !message) {
        raft_free(custom_req);
        raft_free(message);
        Log(s->id, "serverTimerCb(): out of memory for custom request");
        return;
    }
    custom_req->data = message;
    message->type = RAFT_REQUEST_CUSTOM;
    message->server_id = leader_id;
    message->server_address = leader_address;
    message->request_custom.version = 1;
    message->request_custom.term = 0;
    message->request_custom.data.len = sizeof(uint64_t);
    message->request_custom.data.base = raft_malloc(sizeof(uint64_t));
    if (!message->request_custom.data.base) {
        sendCustomMessageCb(custom_req, 0);
        Log(s->id, "serverTimerCb(): out of memory for custom request data");
        return;
    }

    if (!s->io.send) {
        Log(s->id, "serverTimerCb(): cannot send custom message");
        sendCustomMessageCb(custom_req, 0);
        return;
    }
    rv = s->io.send(&s->io, custom_req, message, &sendCustomMessageCb);
    if (rv != 0) {
        Logf(s->id, "io.send(customMessage): %s", raft_errmsg(&s->raft));
    }
}

/* Called periodically every APPLY_RATE milliseconds. */
static void serverTimerCb(uv_timer_t *timer)
{
    struct Server *s = timer->data;
    struct raft_buffer buf;
    struct raft_apply *req;
    int rv;

    if (s->raft.state != RAFT_LEADER) {
        sendCustomMessageToLeader(s);
        return;
    }

    buf.len = sizeof(uint64_t);
    buf.base = raft_malloc(buf.len);
    if (buf.base == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }

    *(uint64_t *)buf.base = 1;

    req = raft_malloc(sizeof *req);
    if (req == NULL) {
        Log(s->id, "serverTimerCb(): out of memory");
        return;
    }
    req->data = s;

    rv = raft_apply(&s->raft, req, &buf, 1, serverApplyCb);
    if (rv != 0) {
        Logf(s->id, "raft_apply(): %s", raft_errmsg(&s->raft));
        return;
    }
}

/* Start the example server. */
static int ServerStart(struct Server *s)
{
    int rv;

    Log(s->id, "starting");

    rv = raft_start(&s->raft);
    if (rv != 0) {
        Logf(s->id, "raft_start(): %s", raft_errmsg(&s->raft));
        goto err;
    }
    rv = uv_timer_start(&s->timer, serverTimerCb, 0, APPLY_RATE);
    if (rv != 0) {
        Logf(s->id, "uv_timer_start(): %s", uv_strerror(rv));
        goto err;
    }

    return 0;

err:
    return rv;
}

/* Release all resources used by the example server. */
static void ServerClose(struct Server *s, ServerCloseCb cb)
{
    s->close_cb = cb;

    Log(s->id, "stopping");

    /* Close the timer asynchronously if it was successfully
     * initialized. Otherwise invoke the callback immediately. */
    if (s->timer.data != NULL) {
        uv_close((struct uv_handle_s *)&s->timer, serverTimerCloseCb);
    } else {
        s->close_cb(s);
    }
}

/********************************************************************
 *
 * Top-level main loop.
 *
 ********************************************************************/

static void mainServerCloseCb(struct Server *server)
{
    struct uv_signal_s *sigint = server->data;
    uv_close((struct uv_handle_s *)sigint, NULL);
}

/* Handler triggered by SIGINT. It will initiate the shutdown sequence. */
static void mainSigintCb(struct uv_signal_s *handle, int signum)
{
    (void)signum;
    struct Server *server = handle->data;
    assert(signum == SIGINT);
    uv_signal_stop(handle);
    server->data = handle;
    ServerClose(server, mainServerCloseCb);
}

int main(int argc, char *argv[])
{
    struct uv_loop_s loop;
    struct uv_signal_s sigint; /* To catch SIGINT and exit. */
    struct Server server;
    const char *dir;
    unsigned id;
    int rv;

    if (argc != 3) {
        printf("usage: example-server <dir> <id>\n");
        return 1;
    }
    dir = argv[1];
    id = (unsigned)atoi(argv[2]);

    /* Ignore SIGPIPE, see https://github.com/joyent/libuv/issues/1254 */
    signal(SIGPIPE, SIG_IGN);

    /* Initialize the libuv loop. */
    rv = uv_loop_init(&loop);
    if (rv != 0) {
        Logf(id, "uv_loop_init(): %s", uv_strerror(rv));
        goto err;
    }

    /* Initialize the example server. */
    rv = ServerInit(&server, &loop, dir, id);
    if (rv != 0) {
        goto err_after_server_init;
    }

    /* Add a signal handler to stop the example server upon SIGINT. */
    rv = uv_signal_init(&loop, &sigint);
    if (rv != 0) {
        Logf(id, "uv_signal_init(): %s", uv_strerror(rv));
        goto err_after_server_init;
    }
    sigint.data = &server;
    rv = uv_signal_start(&sigint, mainSigintCb, SIGINT);
    if (rv != 0) {
        Logf(id, "uv_signal_start(): %s", uv_strerror(rv));
        goto err_after_signal_init;
    }

    /* Start the server. */
    rv = ServerStart(&server);
    if (rv != 0) {
        goto err_after_signal_init;
    }

    /* Run the event loop until we receive SIGINT. */
    rv = uv_run(&loop, UV_RUN_DEFAULT);
    if (rv != 0) {
        Logf(id, "uv_run_start(): %s", uv_strerror(rv));
    }

    uv_loop_close(&loop);

    return rv;

err_after_signal_init:
    uv_close((struct uv_handle_s *)&sigint, NULL);
err_after_server_init:
    ServerClose(&server, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
err:
    return rv;
}
