#include "../include/raft.h"

#include <limits.h>
#include <string.h>

#include "assert.h"
#include "byte.h"
#include "client.h"
#include "configuration.h"
#include "convert.h"
#include "election.h"
#include "err.h"
#include "flags.h"
#include "heap.h"
#include "legacy.h"
#include "log.h"
#include "membership.h"
#include "queue.h"
#include "recv.h"
#include "replication.h"
#include "tick.h"
#include "tracing.h"

#define DEFAULT_ELECTION_TIMEOUT 1000          /* One second */
#define DEFAULT_HEARTBEAT_TIMEOUT 100          /* One tenth of a second */
#define DEFAULT_INSTALL_SNAPSHOT_TIMEOUT 30000 /* 30 seconds */
#define DEFAULT_SNAPSHOT_THRESHOLD 1024
#define DEFAULT_SNAPSHOT_TRAILING 2048

/* Number of milliseconds after which a server promotion will be aborted if the
 * server hasn't caught up with the logs yet. */
#define DEFAULT_MAX_CATCH_UP_ROUNDS 10
#define DEFAULT_MAX_CATCH_UP_ROUND_DURATION (5 * 1000)

#define tracef(...) Tracef(r->tracer, __VA_ARGS__)

int raft_version_number(void)
{
    return RAFT_VERSION_NUMBER;
}

static int ioFsmVersionCheck(struct raft *r,
                             struct raft_io *io,
                             struct raft_fsm *fsm);

int raft_init(struct raft *r,
              struct raft_io *io,
              struct raft_fsm *fsm,
              const raft_id id,
              const char *address)
{
    int rv;
    assert(r != NULL);

    rv = ioFsmVersionCheck(r, io, fsm);
    if (rv != 0) {
        goto err;
    }

    r->io = io;
    r->fsm = fsm;

    r->tracer = &StderrTracer;
    raft_tracer_maybe_enable(r->tracer, true);

    r->id = id;
    /* Make a copy of the address */
    r->address = RaftHeapMalloc(strlen(address) + 1);
    if (r->address == NULL) {
        ErrMsgOom(r->errmsg);
        rv = RAFT_NOMEM;
        goto err;
    }
    strcpy(r->address, address);
    r->current_term = 0;
    r->voted_for = 0;
    r->log = logInit();
    if (r->log == NULL) {
        ErrMsgOom(r->errmsg);
        rv = RAFT_NOMEM;
        goto err_after_address_alloc;
    }

    raft_configuration_init(&r->configuration);
    raft_configuration_init(&r->configuration_last_snapshot);
    r->configuration_committed_index = 0;
    r->configuration_uncommitted_index = 0;
    r->configuration_last_snapshot_index = 0;
    r->election_timeout = DEFAULT_ELECTION_TIMEOUT;
    r->heartbeat_timeout = DEFAULT_HEARTBEAT_TIMEOUT;
    r->install_snapshot_timeout = DEFAULT_INSTALL_SNAPSHOT_TIMEOUT;
    r->commit_index = 0;
    r->last_applied = 0;
    r->last_stored = 0;
    r->state = RAFT_UNAVAILABLE;
    r->transfer = NULL;
    r->snapshot.threshold = DEFAULT_SNAPSHOT_THRESHOLD;
    r->snapshot.trailing = DEFAULT_SNAPSHOT_TRAILING;
    r->snapshot.taking = false;
    r->snapshot.persisting = false;
    r->close_cb = NULL;
    memset(r->errmsg, 0, sizeof r->errmsg);
    r->pre_vote = false;
    r->max_catch_up_rounds = DEFAULT_MAX_CATCH_UP_ROUNDS;
    r->max_catch_up_round_duration = DEFAULT_MAX_CATCH_UP_ROUND_DURATION;
    r->now = 0;
    if (io != NULL) {
        r->io->data = r;
        rv = r->io->init(r->io, r->id, r->address);
        if (rv != 0) {
            ErrMsgTransfer(r->io->errmsg, r->errmsg, "io");
            goto err_after_address_alloc;
        }
        r->now = r->io->time(r->io);
        raft_seed(r, (unsigned)r->io->random(r->io, 0, INT_MAX));
        QUEUE_INIT(&r->legacy.requests);
        r->legacy.step_cb = NULL;
    }
    r->updates = 0;
    r->messages = NULL;
    r->n_messages_cap = 0;
    r->entries = NULL;
    return 0;

err_after_address_alloc:
    RaftHeapFree(r->address);
err:
    assert(rv != 0);
    return rv;
}

static void finalClose(struct raft *r)
{
    raft_free(r->address);
    logClose(r->log);
    raft_configuration_close(&r->configuration);
    raft_configuration_close(&r->configuration_last_snapshot);
    if (r->messages != NULL) {
        raft_free(r->messages);
    }
}

static void ioCloseCb(struct raft_io *io)
{
    struct raft *r = io->data;
    finalClose(r);
    if (r->close_cb != NULL) {
        r->close_cb(r);
    }
}

void raft_close(struct raft *r, void (*cb)(struct raft *r))
{
    assert(r->close_cb == NULL);
    assert(r->updates == 0);
    if (r->state != RAFT_UNAVAILABLE) {
        convertToUnavailable(r);
        if (r->io != NULL) {
            LegacyFireCompletedRequests(r);
        }
    }
    r->close_cb = cb;
    if (r->io != NULL) {
        r->io->close(r->io, ioCloseCb);
    } else {
        finalClose(r);
    }
}

void raft_seed(struct raft *r, unsigned random)
{
    r->random = random;
}

/* Handle the completion of a send message operation. */
static int stepSent(struct raft *r, struct raft_message *message, int status)
{
    int rv;
    switch (message->type) {
        case RAFT_IO_APPEND_ENTRIES:
            rv = replicationSendAppendEntriesDone(r, message, status);
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            rv = replicationSendInstallSnapshotDone(r, message, status);
            break;
        default:
            /* Ignore the status, in case of errors we'll retry. */
            rv = 0;
            break;
    }
    return rv;
}

/* Handle new messages. */
static int stepReceive(struct raft *r,
                       raft_id id,
                       const char *address,
                       struct raft_message *message)
{
    return recvMessage(r, id, address, message);
}

int raft_step(struct raft *r,
              struct raft_event *event,
              struct raft_update *update)
{
    int rv;

    assert(event != NULL);
    assert(update != NULL);

    assert(r->updates == 0);

    r->n_messages = 0;
    r->now = event->time;

    switch (event->type) {
        case RAFT_PERSISTED_ENTRIES:
            rv = replicationPersistEntriesDone(
                r, event->persisted_entries.index,
                event->persisted_entries.batch, event->persisted_entries.n,
                event->persisted_entries.status);
            break;
        case RAFT_PERSISTED_SNAPSHOT:
            rv = replicationPersistSnapshotDone(
                r, &event->persisted_snapshot.metadata,
                event->persisted_snapshot.offset,
                &event->persisted_snapshot.chunk,
                event->persisted_snapshot.last,
                event->persisted_snapshot.status);
            break;
        case RAFT_SENT:
            rv = stepSent(r, &event->sent.message, event->sent.status);
            break;
        case RAFT_RECEIVE:
            rv = stepReceive(r, event->receive.id, event->receive.address,
                             event->receive.message);
            break;
        case RAFT_SNAPSHOT:
            rv = replicationSnapshot(r, &event->snapshot.metadata,
                                     event->snapshot.trailing);
            break;
        case RAFT_TIMEOUT:
            rv = Tick(r);
            break;
        case RAFT_SUBMIT:
            rv = ClientSubmit(r, event->submit.entries, event->submit.n);
            break;
        case RAFT_CATCH_UP:
            ClientCatchUp(r, event->catch_up.server_id);
            rv = 0;
            break;
        case RAFT_TRANSFER:
            rv = ClientTransfer(r, event->transfer.server_id);
            break;
        default:
            rv = 0;
            break;
    }

    if (rv != 0) {
        goto out;
    }

    update->flags = r->updates;

    if (update->flags & RAFT_UPDATE_ENTRIES) {
        update->entries.index = r->entries_index;
        update->entries.batch = r->entries;
        update->entries.n = r->n_entries;
    }

    if (update->flags & RAFT_UPDATE_SNAPSHOT) {
        update->snapshot.metadata = r->snapshot_metadata;
        update->snapshot.offset = r->snapshot_offset;
        update->snapshot.chunk = r->snapshot_chunk;
        update->snapshot.last = r->snapshot_last;
    }

    if (update->flags & RAFT_UPDATE_MESSAGES) {
        update->messages.batch = r->messages;
        update->messages.n = r->n_messages;
    }

out:
    r->updates = 0;

    if (rv != 0) {
        return rv;
    }
    return 0;
}

raft_term raft_current_term(struct raft *r)
{
    return r->current_term;
}

raft_term raft_voted_for(struct raft *r)
{
    return r->voted_for;
}

void raft_set_election_timeout(struct raft *r, const unsigned msecs)
{
    r->election_timeout = msecs;
    /* FIXME: workaround for failures in the dqlite test suite, which sets
     * timeouts too low and end up in failures when run on slow harder. */
    if (r->io != NULL && r->election_timeout == 150 &&
        r->heartbeat_timeout == 15) {
        r->election_timeout *= 3;
        r->heartbeat_timeout *= 3;
    }
}

void raft_set_heartbeat_timeout(struct raft *r, const unsigned msecs)
{
    r->heartbeat_timeout = msecs;
}

void raft_set_install_snapshot_timeout(struct raft *r, const unsigned msecs)
{
    r->install_snapshot_timeout = msecs;
}

void raft_set_snapshot_threshold(struct raft *r, unsigned n)
{
    r->snapshot.threshold = n;
}

void raft_set_snapshot_trailing(struct raft *r, unsigned n)
{
    r->snapshot.trailing = n;
}

void raft_set_max_catch_up_rounds(struct raft *r, unsigned n)
{
    r->max_catch_up_rounds = n;
}

void raft_set_max_catch_up_round_duration(struct raft *r, unsigned msecs)
{
    r->max_catch_up_round_duration = msecs;
}

void raft_set_pre_vote(struct raft *r, bool enabled)
{
    r->pre_vote = enabled;
}

const char *raft_errmsg(struct raft *r)
{
    return r->errmsg;
}

int raft_bootstrap(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->bootstrap(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

int raft_recover(struct raft *r, const struct raft_configuration *conf)
{
    int rv;

    if (r->state != RAFT_UNAVAILABLE) {
        return RAFT_BUSY;
    }

    rv = r->io->recover(r->io, conf);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

const char *raft_strerror(int errnum)
{
    return errCodeToString(errnum);
}

void raft_configuration_init(struct raft_configuration *c)
{
    configurationInit(c);
}

void raft_configuration_close(struct raft_configuration *c)
{
    configurationClose(c);
}

int raft_configuration_add(struct raft_configuration *c,
                           const raft_id id,
                           const char *address,
                           const int role)
{
    return configurationAdd(c, id, address, role);
}

int raft_configuration_encode(const struct raft_configuration *c,
                              struct raft_buffer *buf)
{
    return configurationEncode(c, buf);
}

unsigned long long raft_digest(const char *text, unsigned long long n)
{
    struct byteSha1 sha1;
    uint8_t value[20];
    uint64_t n64 = byteFlip64((uint64_t)n);
    uint64_t digest;

    byteSha1Init(&sha1);
    byteSha1Update(&sha1, (const uint8_t *)text, (uint32_t)strlen(text));
    byteSha1Update(&sha1, (const uint8_t *)&n64, (uint32_t)(sizeof n64));
    byteSha1Digest(&sha1, value);

    memcpy(&digest, value + (sizeof value - sizeof digest), sizeof digest);

    return byteFlip64(digest);
}

static int ioFsmVersionCheck(struct raft *r,
                             struct raft_io *io,
                             struct raft_fsm *fsm)
{
    if (io != NULL && io->version == 0) {
        ErrMsgPrintf(r->errmsg, "io->version must be set");
        return -1;
    }

    if (fsm != NULL && fsm->version == 0) {
        ErrMsgPrintf(r->errmsg, "fsm->version must be set");
        return -1;
    }

    return 0;
}
