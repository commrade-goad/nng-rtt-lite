#ifndef NNG_WRAPPER_H
#define NNG_WRAPPER_H

#include <nng/nng.h>

#if defined(NNG_PUBSUB_SURVEY) || defined(NNG_PUBSUB_RELIABLE) || \
    defined(NNG_PUBSUB_SACK)

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>

// NOTE on macros: the alias #defines live at the BOTTOM of this header,
// after every function body. They are object-like macros
// (#define nng_send X), which expand everywhere regardless of
// parentheses, so placing them earlier would make the inner "real"
// calls below recursively expand into the wrappers themselves.

#if defined(NNG_PUBSUB_SURVEY)

// Surveyor/respondent bridge: true 1-to-N broadcast where every
// subscriber replies, so the publisher can observe all of them.
// Adaptive RTT/AIMD lives in surveyor.c (per-pipe estimators, adaptive
// survey deadline, MD on missed replies) and is enabled here, mirroring
// the req/rep bridge below.
//
// Collection policy ( pacing ): the publisher collects up to
// nng_survey_quorum replies per survey, but EVERY wait is bounded by
// the (adaptive) survey deadline, so a slow or dead subscriber can
// delay the publisher by at most one deadline, never stall it.
// Recommended thesis policy is quorum = 1 ("pace to fastest"): the
// publisher moves on after the first reply and can never be
// head-of-line blocked by one slow device. Subscriber-side metrics
// stay valid regardless, because every subscriber timestamps on
// receipt, independent of the publisher's pacing. Strict quorum = N
// ("wait for all") is available but collapses past a loss threshold
// (each miss burns a full deadline; timed-out subscribers exit and
// make quorum permanently unfillable) -- documented limitation.
//
// Quorum/deadline are configured by the application before opening
// the publisher (see ops_nng.c): NNG_SURVEY_QUORUM env (expected
// replies per message, default 1), NNG_SURVEY_TIME_MS env (survey
// deadline cap in ms, default 2000).
static int          nng_survey_quorum      = 1;
static nng_duration nng_survey_deadline_ms = 2000;

// Sequencing for deliver-once + resend dedup (body-embedded).
// The publisher prepends "<seq>|" to every outgoing body; the
// subscriber strips it before handing the body to the app and delivers
// each seq exactly once (high-water mark per socket).
// NOTE: the seq lives in the BODY, not the header, so cooked sockets
// work unchanged and dp-len accounting (measured on the stripped body)
// stays exact. The benchmark body format "<ts>|<payload>" is preserved
// after stripping.
static uint32_t nng_survey_seq_next = 1; // pub side; one pub thread

// Per-subscriber high-water marks. One benchmark process can hold
// several sub sockets (inproc threads), and broadcast reaches each
// independently, so sharing one mark would drop lagging subscribers'
// fresh messages. Tiny linear table; benchmark scale.
#define NNG_SURVEY_DEDUP_SLOTS 16
static struct {
    bool     used;
    int      sock_id;
    uint32_t last;
} nng_survey_seen[NNG_SURVEY_DEDUP_SLOTS] = { { 0 } };

// Look up (or create) the high-water mark for a socket.
// Returns NULL when the table is full: caller must fail open
// (deliver) rather than stall.
static inline uint32_t *
nng_survey_mark_for(nng_socket sock)
{
    int id = nng_socket_id(sock);
    for (int i = 0; i < NNG_SURVEY_DEDUP_SLOTS; i++) {
        if (nng_survey_seen[i].used && nng_survey_seen[i].sock_id == id) {
            return &nng_survey_seen[i].last;
        }
    }
    for (int i = 0; i < NNG_SURVEY_DEDUP_SLOTS; i++) {
        if (!nng_survey_seen[i].used) {
            nng_survey_seen[i].used    = true;
            nng_survey_seen[i].sock_id = id;
            nng_survey_seen[i].last    = 0;
            return &nng_survey_seen[i].last;
        }
    }
    return NULL;
}

// Split "<seq>|<rest>" at the first '|'. Returns the prefix length
// (including '|', always >0) with *seq set on success, 0 when the body
// has no numeric prefix (legacy/foreign peer: deliver as-is).
static inline int
nng_survey_split_seq(const char *body, size_t len, uint32_t *seq)
{
    size_t i = 0;
    uint32_t v = 0;
    if (len == 0) {
        return 0;
    }
    while (i < len && body[i] >= '0' && body[i] <= '9') {
        v = v * 10 + (uint32_t) (body[i] - '0');
        i++;
    }
    if (i == 0 || i >= len || body[i] != '|') {
        return 0;
    }
    *seq = v;
    return (int) (i + 1); // prefix length including '|', >0
}

// Debug hook: NNG_SURVEY_DEBUG=1 logs every dropped duplicate to stderr.
static inline bool
nng_survey_debug(void)
{
    static int cached = -1;
    if (cached < 0) {
        cached = getenv("NNG_SURVEY_DEBUG") != NULL ? 1 : 0;
    }
    return cached == 1;
}

// Test hook: if NNG_RESPOND_DELAY_MS is set, respondent paths sleep
// that many ms before sending the ACK. Used to prove a slow subscriber
// cannot stall the publisher (quorum = 1). Never set in real runs.
static inline void
nng_survey_test_delay(void)
{
    const char *d = getenv("NNG_RESPOND_DELAY_MS");
    if (d != NULL && atoi(d) > 0) {
        nng_msleep((nng_duration) atoi(d));
    }
}

/**
 * @brief Wrapper for nng_pub0_open that redirects to surveyor.
 */
static inline int nng_pub0_open_survey(nng_socket *sock) {
    int rv = nng_surveyor0_open(sock);
    if (rv == 0) {
        nng_socket_set_ms(
            *sock, NNG_OPT_SURVEYOR_SURVEYTIME, nng_survey_deadline_ms);
        // Enable adaptive congestion control (RTT-lite for surveyor).
        nng_socket_set_bool(*sock, "surveyor:adaptive", true);
    }
    return rv;
}

/**
 * @brief Wrapper for nng_sub0_open that redirects to respondent.
 */
static inline int nng_sub0_open_survey(nng_socket *sock) {
    return nng_respondent0_open(sock);
}

/**
 * @brief Wrapper for nng_send: broadcast a survey, collect replies.
 *
 * Prepends "<seq>|" to the body (see sequencing NOTE above), sends
 * once, then receives up to nng_survey_quorum replies (stops
 * early on deadline expiry). Returns 0 if at least one subscriber
 * replied, NNG_ETIMEDOUT otherwise.
 */
static inline int nng_send_survey(
    nng_socket sock, void *data, size_t size, int flags) {
    int     rv;
    uint32_t seq = nng_survey_seq_next++;
    char     prefix[16];
    int      plen = snprintf(prefix, sizeof(prefix), "%u|", seq);
    char    *buf  = (char *) malloc((size_t) plen + size);
    if (buf == NULL) {
        return NNG_ENOMEM;
    }
    memcpy(buf, prefix, (size_t) plen);
    if (size > 0) {
        memcpy(buf + plen, data, size);
    }
    rv = nng_send(sock, buf, (size_t) plen + size, flags);
    free(buf);
    if (rv != 0) {
        return rv;
    }
    int got = 0;
    for (int i = 0; i < nng_survey_quorum; i++) {
        char   ack_buf[16];
        size_t ack_sz = sizeof(ack_buf);
        if (nng_recv(sock, ack_buf, &ack_sz, 0) != 0) {
            break; // survey deadline expired or error
        }
        got++;
    }
    return got > 0 ? 0 : NNG_ETIMEDOUT;
}

/**
 * @brief Wrapper for nng_recvmsg: receive a survey, reply with an ACK.
 *
 * Strips the "<seq>|" prefix and delivers each seq exactly once per
 * socket (duplicates from timer resends are dropped silently after
 * replying, so the publisher's books stay fed but the app never sees
 * them twice). Bodies without a numeric prefix are delivered as-is.
 */
static inline int nng_recvmsg_survey(
    nng_socket sock, nng_msg **msgp, int flags) {
    uint32_t *mark = nng_survey_mark_for(sock);
    for (;;) {
        nng_msg *m   = NULL;
        int      rv  = nng_recvmsg(sock, &m, flags);
        uint32_t seq = 0;
        int      pre = 0;
        if (rv != 0) {
            return rv;
        }
        pre = nng_survey_split_seq(
            (const char *) nng_msg_body(m), nng_msg_len(m), &seq);
        if (pre > 0 && mark != NULL && seq <= *mark) {
            // Duplicate (resend of an already delivered survey).
            if (nng_survey_debug()) {
                fprintf(stderr, "survey: drop dup seq=%u sock=%d\n",
                    seq, nng_socket_id(sock));
            }
            nng_survey_test_delay();
            nng_send(sock, (void *) "ACK", 3, 0);
            nng_msg_free(m);
            continue;
        }
        if (pre > 0) {
            if (mark != NULL) {
                *mark = seq;
            }
            nng_msg_trim(m, (size_t) pre);
        }
        nng_survey_test_delay();
        int send_rv = nng_send(sock, (void *) "ACK", 3, 0);
        if (send_rv != 0) {
            nng_msg_free(m);
            *msgp = NULL;
            return send_rv;
        }
        *msgp = m;
        return 0;
    }
}

/**
 * @brief Wrapper for nng_recv: receive data, reply with an ACK.
 *
 * Same deliver-once rule as nng_recvmsg_survey, applied to a flat
 * caller buffer (prefix stripped via memmove, *sizep adjusted).
 */
static inline int nng_recv_survey(
    nng_socket sock, void *data, size_t *sizep, int flags) {
    uint32_t *mark = nng_survey_mark_for(sock);
    for (;;) {
        char    *buf = (char *) data;
        int      rv  = nng_recv(sock, data, sizep, flags);
        uint32_t seq = 0;
        int      pre = 0;
        if (rv != 0) {
            return rv;
        }
        pre = nng_survey_split_seq(buf, *sizep, &seq);
        if (pre > 0 && mark != NULL && seq <= *mark) {
            if (nng_survey_debug()) {
                fprintf(stderr, "survey: drop dup seq=%u sock=%d\n",
                    seq, nng_socket_id(sock));
            }
            nng_survey_test_delay();
            nng_send(sock, (void *) "ACK", 3, 0);
            continue;
        }
        if (pre > 0) {
            if (mark != NULL) {
                *mark = seq;
            }
            memmove(buf, buf + pre, *sizep - (size_t) pre);
            *sizep -= (size_t) pre;
        }
        nng_survey_test_delay();
        int send_rv = nng_send(sock, (void *) "ACK", 3, 0);
        if (send_rv != 0) {
            return send_rv;
        }
        return 0;
    }
}

#elif defined(NNG_PUBSUB_RELIABLE)

/**
 * @brief Wrapper for nng_pub0_open that redirects to req0 and sets adaptive options.
 */
static inline int nng_pub0_open_reliable(nng_socket *sock) {
    int rv = nng_req0_open(sock);
    if (rv == 0) {
        // Enable adaptive congestion control (RTT-lite)
        nng_socket_set_bool(*sock, "req:adaptive", true);
    }
    return rv;
}

/**
 * @brief Wrapper for nng_sub0_open that redirects to rep0.
 */
static inline int nng_sub0_open_reliable(nng_socket *sock) {
    return nng_rep0_open(sock);
}

/**
 * @brief Wrapper for nng_send that handles the synchronous request-reply ACK cycle.
 */
static inline int nng_send_reliable(nng_socket sock, void *data, size_t size, int flags) {
    // 1. Send the request message
    int rv = nng_send(sock, data, size, flags);
    if (rv != 0) {
        return rv;
    }
    // 2. Receive the ACK reply back from the receiver to complete the req/rep cycle.
    // We use a small local buffer because the ACK is tiny ("ACK") and NNG_FLAG_ALLOC is deprecated.
    char ack_buf[16];
    size_t ack_sz = sizeof(ack_buf);
    rv = nng_recv(sock, ack_buf, &ack_sz, 0);
    return rv;
}

/**
 * @brief Wrapper for nng_recvmsg that receives a request and automatically replies with an ACK.
 */
static inline int nng_recvmsg_reliable(nng_socket sock, nng_msg **msgp, int flags) {
    // 1. Receive the incoming request message
    int rv = nng_recvmsg(sock, msgp, flags);
    if (rv != 0) {
        return rv;
    }
    // 2. Send the ACK reply back to the sender
    int send_rv = nng_send(sock, (void*)"ACK", 3, 0);
    if (send_rv != 0) {
        nng_msg_free(*msgp);
        *msgp = NULL;
        return send_rv;
    }
    return 0;
}

/**
 * @brief Wrapper for nng_recv that receives data and automatically replies with an ACK.
 */
static inline int nng_recv_reliable(nng_socket sock, void *data, size_t *sizep, int flags) {
    // 1. Receive the incoming data
    int rv = nng_recv(sock, data, sizep, flags);
    if (rv != 0) {
        return rv;
    }
    // 2. Send the ACK reply back to the sender
    int send_rv = nng_send(sock, (void*)"ACK", 3, 0);
    if (send_rv != 0) {
        return send_rv;
    }
    return 0;
}

#endif // mode selection (survey/reliable)

#if defined(NNG_PUBSUB_SACK)

// Cumulative-ACK bridge over the sack/sackresp protocol: the publisher
// pipelines surveys WITHOUT waiting (window SACK0_RING_MAX = 32, one
// #define in sack.c), and each subscriber answers once per batch with
// "C<next>[:mask]" ("everything below <next> done, plus SACK bits").
// 10 msgs cost ~2 uplink ACKs instead of 10. RTT-lite AIMD lives in
// sack.c and is enabled here. All subscribers get everything: the
// publisher resends per-pipe holes until each pipe cumulatively acks.

static nng_duration nng_sack_deadline_ms = 2000;
static uint32_t     nng_sack_seq_next    = 1; // pub side; one pub thread

// Batch + delayed-ACK tunables. NNG_SACK_BATCH must stay <= the
// protocol window SACK0_RING_MAX (32, one #define in sack.c): one
// cumulative per batch. NNG_SACK_ACK_DELAY_MS bounds tail latency:
// a partial batch flushes at most this long after it stopped growing,
// even while the app is blocked in nng_recv.
#define NNG_SACK_BATCH 8
#define NNG_SACK_ACK_DELAY_MS 50
// Per-subscriber cumulative state: last = highest contiguous wrapper
// seq delivered, bits = SACK for last+1..last+16 (LSB = last+1),
// pending = new deliveries since the last flushed C.
#define NNG_SACK_SLOTS 16
struct nng_sack_slot {
    bool     used;
    int      sock_id;
    uint32_t last;
    uint32_t bits;
    int      pending;
};
static struct nng_sack_slot nng_sack_seen[NNG_SACK_SLOTS] = { { 0 } };

// Flush telemetry: how many cumulative ACKs actually left the socket
// vs failed to send. Read them after a run to tell "C never sent"
// apart from "C sent but lost/unprocessed". Zero behavior change.
static unsigned long nng_sack_flush_sent   = 0;
static unsigned long nng_sack_flush_failed = 0;

static inline struct nng_sack_slot *
nng_sack_state_for(nng_socket sock, bool create)
{
    int id = nng_socket_id(sock);
    for (int i = 0; i < NNG_SACK_SLOTS; i++) {
        if (nng_sack_seen[i].used && nng_sack_seen[i].sock_id == id) {
            return &nng_sack_seen[i];
        }
    }
    if (!create) {
        return NULL;
    }
    for (int i = 0; i < NNG_SACK_SLOTS; i++) {
        if (!nng_sack_seen[i].used) {
            nng_sack_seen[i].used    = true;
            nng_sack_seen[i].sock_id = id;
            nng_sack_seen[i].last    = 0;
            nng_sack_seen[i].bits    = 0;
            nng_sack_seen[i].pending = 0;
            return &nng_sack_seen[i];
        }
    }
    return NULL;
}

// Split "<seq>|<rest>" (same as survey mode). Returns prefix length or 0.
static inline int
nng_sack_split_seq(const char *body, size_t len, uint32_t *seq)
{
    size_t i = 0;
    uint32_t v = 0;
    if (len == 0) {
        return 0;
    }
    while (i < len && body[i] >= '0' && body[i] <= '9') {
        v = v * 10 + (uint32_t) (body[i] - '0');
        i++;
    }
    if (i == 0 || i >= len || body[i] != '|') {
        return 0;
    }
    *seq = v;
    return (int) (i + 1);
}

// Parse "C<next>[:maskhex]" (mirror of sack.c). Returns next, 0 = invalid.
static inline uint32_t
nng_sack_parse_next(const char *body, size_t len, uint32_t *maskp)
{
    size_t i = 1;
    uint32_t v = 0, m = 0;
    int nd = 0, hd = 0;
    if (len < 2 || body[0] != 'C') {
        return 0;
    }
    while (i < len && body[i] >= '0' && body[i] <= '9') {
        v = v * 10 + (uint32_t) (body[i] - '0');
        i++;
        nd++;
    }
    if (nd == 0 || v == 0) {
        return 0;
    }
    if (i < len && body[i] == ':') {
        i++;
        while (i < len && hd < 8) {
            char c = body[i];
            uint32_t d;
            if (c >= '0' && c <= '9') {
                d = (uint32_t) (c - '0');
            } else if (c >= 'a' && c <= 'f') {
                d = (uint32_t) (c - 'a' + 10);
            } else if (c >= 'A' && c <= 'F') {
                d = (uint32_t) (c - 'A' + 10);
            } else {
                break;
            }
            m = (m << 4) | d;
            i++;
            hd++;
        }
        if (hd == 0) {
            return 0;
        }
    }
    if (maskp != NULL) {
        *maskp = m;
    }
    return v;
}

// Publisher socket registry (SACK teardown needs the role: only pub
// sockets drain final ACKs via nng_sack_sync, sub sockets just flush
// their tail batch. Same tiny-table pattern as nng_sack_seen; benchmark
// scale, one pub per process).
#define NNG_SACK_PUB_SLOTS 16
static struct {
    bool used;
    int  sock_id;
} nng_sack_pubs[NNG_SACK_PUB_SLOTS] = { { 0 } };

static inline void
nng_sack_pub_remember(nng_socket sock)
{
    int id = nng_socket_id(sock);
    for (int i = 0; i < NNG_SACK_PUB_SLOTS; i++) {
        if (nng_sack_pubs[i].used && nng_sack_pubs[i].sock_id == id) {
            return;
        }
    }
    for (int i = 0; i < NNG_SACK_PUB_SLOTS; i++) {
        if (!nng_sack_pubs[i].used) {
            nng_sack_pubs[i].used    = true;
            nng_sack_pubs[i].sock_id = id;
            return;
        }
    }
}

static inline bool
nng_sack_is_pub(nng_socket sock)
{
    int id = nng_socket_id(sock);
    for (int i = 0; i < NNG_SACK_PUB_SLOTS; i++) {
        if (nng_sack_pubs[i].used && nng_sack_pubs[i].sock_id == id) {
            return true;
        }
    }
    return false;
}

static inline void
nng_sack_pub_forget(nng_socket sock)
{
    int id = nng_socket_id(sock);
    for (int i = 0; i < NNG_SACK_PUB_SLOTS; i++) {
        if (nng_sack_pubs[i].used && nng_sack_pubs[i].sock_id == id) {
            nng_sack_pubs[i].used = false;
            return;
        }
    }
}

/**
 * @brief Wrapper for nng_pub0_open that redirects to sack.
 */
static inline int nng_pub0_open_sack(nng_socket *sock) {
    int rv = nng_sack0_open(sock);
    if (rv == 0) {
        nng_socket_set_ms(
            *sock, NNG_OPT_SACK_SURVEYTIME, nng_sack_deadline_ms);
        nng_socket_set_bool(*sock, NNG_OPT_SACK_ADAPTIVE, true);
        nng_sack_pub_remember(*sock);
    }
    return rv;
}

/**
 * @brief Wrapper for nng_sub0_open that redirects to sackresp.
 */
static inline int nng_sub0_open_sack(nng_socket *sock) {
    return nng_sackresp0_open(sock);
}

/**
 * @brief Wrapper for nng_send: pipeline a survey, do NOT wait.
 *
 * Prepends "<seq>|" and returns once queued. ACKs arrive async as
 * "C<next>[:mask]" batches (one per ~8 surveys); drain them with
 * nng_sack_sync() or plain nng_recv on the pub socket.
 */
static inline int nng_send_sack(
    nng_socket sock, void *data, size_t size, int flags) {
    uint32_t seq = nng_sack_seq_next++;
    char     prefix[16];
    int      plen = snprintf(prefix, sizeof(prefix), "%u|", seq);
    char    *buf  = (char *) malloc((size_t) plen + size);
    int      rv;
    if (buf == NULL) {
        return NNG_ENOMEM;
    }
    memcpy(buf, prefix, (size_t) plen);
    if (size > 0) {
        memcpy(buf + plen, data, size);
    }
    rv = nng_send(sock, buf, (size_t) plen + size, flags);
    free(buf);
    return rv;
}

/**
 * @brief Drain pending cumulative ACKs; optionally wait for a target.
 *
 * Reads "C<next>" replies until one with next > target arrives (or the
 * timeout expires). Returns the highest next observed (0 = none).
 * target = 0 just drains whatever is already queued (non-blocking).
 */
static inline uint32_t nng_sack_sync(
    nng_socket sock, uint32_t target, nng_duration timeout_ms) {
    uint32_t    best = 0;
    nng_duration old  = 0;
    bool         have_old = false;
    nng_time     deadline = 0;
    if (timeout_ms > 0) {
        if (nng_socket_get_ms(
                sock, NNG_OPT_RECVTIMEO, &old) == 0) {
            have_old = true;
        }
        deadline = nng_clock() + (nng_time) timeout_ms;
        nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, 50);
    }
    for (;;) {
        char     buf[64];
        size_t   sz = sizeof(buf) - 1;
        uint32_t next, mask = 0;
        int      flags = (timeout_ms > 0) ? 0 : NNG_FLAG_NONBLOCK;
        int      rv    = nng_recv(sock, buf, &sz, flags);
        if (rv != 0) {
            if (timeout_ms > 0 && rv == NNG_ETIMEDOUT &&
                nng_clock() < deadline) {
                continue; // short poll slice, overall deadline stands
            }
            break;
        }
        if (sz >= sizeof(buf)) {
            sz = sizeof(buf) - 1;
        }
        buf[sz] = '\0';
        next    = nng_sack_parse_next(buf, sz, &mask);
        if (next > best) {
            best = next;
        }
        if (target != 0 && best > target) {
            break;
        }
        if (timeout_ms > 0 && nng_clock() >= deadline) {
            break;
        }
    }
    if (have_old) {
        nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, old);
    }
    return best;
}

// Shared sub-side core: classify seq, maintain cumulative state, decide
// whether a "C" flush is due. Returns 1 = deliver, 0 = duplicate-drop,
// and sets *flush when a cumulative ACK must be sent.
static inline int
nng_sack_track(nng_socket sock, uint32_t seq, int have_seq, int *flush)
{
    struct nng_sack_slot *st = nng_sack_state_for(sock, true);

    *flush = 0;
    if (!have_seq) {
        return 1; // legacy body: deliver as-is, no tracking, no ACK change
    }
    if (st == NULL) {
        return 1; // table full: fail open (deliver), no batching
    }
    if (st->last != 0 && seq <= st->last) {
        *flush = 1; // duplicate resend: drop, but fast-ACK now
        return 0;
    }
    if (st->last == 0) {
        // First seq seen on this socket: adopt as base. (Joining
        // mid-stream credits older seqs as done; the publisher frees
        // them. Streaming benchmarks always start at seq 1.)
        st->last = seq;
    } else if (seq == st->last + 1) {
        st->last = seq;
        // Consume the bit position of the newly contiguous seq first:
        // bit0 always described (old) last+1, which just arrived, so it
        // must not survive the base advance. Then drain any further
        // contiguous SACK bits. Without this shift the mask goes stale
        // by one (claims the next seq missing and a future seq present).
        st->bits >>= 1;
        while ((st->bits & 1u) != 0) {
            st->last++;
            st->bits >>= 1;
        }
    } else {
        uint32_t d = seq - st->last - 1;
        if (d < 16) {
            st->bits |= (1u << d);
        }
        // else beyond mask horizon: deliver, count, pub will resend
    }
    st->pending++;
    if (st->pending >= NNG_SACK_BATCH) {
        *flush = 1;
    }
    return 1;
}

// Send the current cumulative "C<next>[:mask]" for a socket.
static inline void
nng_sack_flush(nng_socket sock)
{
    struct nng_sack_slot *st = nng_sack_state_for(sock, false);
    char                  ack[32];
    int                   n;
    int                   rv;

    if (st == NULL || st->last == 0) {
        return;
    }
    if (st->bits != 0) {
        n = snprintf(ack, sizeof(ack), "C%u:%x", st->last + 1, st->bits);
    } else {
        n = snprintf(ack, sizeof(ack), "C%u", st->last + 1);
    }
    if (n > 0) {
        rv = nng_send(sock, ack, (size_t) n + 1, 0);
        if (rv == 0) {
            nng_sack_flush_sent++;
        } else {
            nng_sack_flush_failed++;
            fprintf(stderr, "sack: flush send failed: %s\n",
                nng_strerror(rv));
        }
    }
    st->pending = 0;
}

/**
 * @brief Wrapper for nng_recvmsg: deliver once, batch ACKs.
 *
 * Blocking waits are sliced into NNG_SACK_ACK_DELAY_MS chunks so a
 * partial tail batch flushes promptly even while no new data arrives.
 * The socket's NNG_OPT_RECVTIMEO is honored as the overall deadline
 * (single thread per socket assumed, as in the benchmark).
 */
static inline int nng_recvmsg_sack(
    nng_socket sock, nng_msg **msgp, int flags) {
    nng_duration tmo      = -1;
    nng_time     deadline = 0;
    bool         infinite = true;
    bool         sliced   = false;
    bool         nonblock = ((flags & NNG_FLAG_NONBLOCK) != 0);
    nng_duration saved    = 0;
    bool         have_saved = false;
    int          inner    = 0; // inner call flags (0 = blocking slice)
    int          first    = 1; // first pass probes nonblocking (below)

    if (!nonblock) {
        if (nng_socket_get_ms(sock, NNG_OPT_RECVTIMEO, &tmo) == 0 &&
            tmo >= 0) {
            infinite = false;
            deadline = nng_clock() + (nng_time) tmo;
        }
        if (nng_socket_get_ms(sock, NNG_OPT_RECVTIMEO, &saved) == 0) {
            have_saved = true;
        }
        nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, NNG_SACK_ACK_DELAY_MS);
        sliced = true;
    } else {
        inner = NNG_FLAG_NONBLOCK;
    }
    for (;;) {
        nng_msg *m   = NULL;
        int      rv  = nng_recvmsg(sock, &m,
            (!nonblock && first) ? (int) NNG_FLAG_NONBLOCK : inner);
        uint32_t seq = 0;
        int      pre = 0;
        int      flush = 0;
        int      keep;
        first = 0;
        if (rv == NNG_EAGAIN && !nonblock) {
            // Probe miss in blocking mode: nothing ready right now.
            // Flush pending first so a starved publisher unblocks now
            // instead of at the next 50 ms slice, then fall into the
            // normal blocking slices below. At most one extra flush
            // per call; streaming (queue non-empty) never takes it.
            struct nng_sack_slot *pst =
                nng_sack_state_for(sock, false);
            if (pst != NULL && pst->pending > 0) {
                nng_sack_flush(sock);
            }
            continue;
        }
        if (rv == NNG_ETIMEDOUT && sliced) {
            struct nng_sack_slot *st =
                nng_sack_state_for(sock, false);
            if (st != NULL && st->pending > 0) {
                nng_sack_flush(sock); // tail: partial batch, send it now
            }
            if (!infinite && nng_clock() >= deadline) {
                if (have_saved) {
                    nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
                }
                return NNG_ETIMEDOUT;
            }
            continue;
        }
        if (rv != 0) {
            if (sliced && have_saved) {
                nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
            }
            return rv;
        }
        pre = nng_sack_split_seq(
            (const char *) nng_msg_body(m), nng_msg_len(m), &seq);
        keep = nng_sack_track(sock, seq, pre > 0, &flush);
        if (!keep) {
            nng_sack_flush(sock); // dup: feed publisher's books now
            nng_msg_free(m);
            continue;
        }
        if (pre > 0) {
            nng_msg_trim(m, (size_t) pre);
        }
        if (flush) {
            nng_sack_flush(sock);
        }
        if (sliced && have_saved) {
            nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
        }
        *msgp = m;
        return 0;
    }
}

/**
 * @brief Wrapper for nng_recv: deliver once, batch ACKs (same slicing).
 */
static inline int nng_recv_sack(
    nng_socket sock, void *data, size_t *sizep, int flags) {
    nng_duration tmo      = -1;
    nng_time     deadline = 0;
    bool         infinite = true;
    bool         sliced   = false;
    bool         nonblock = ((flags & NNG_FLAG_NONBLOCK) != 0);
    nng_duration saved    = 0;
    bool         have_saved = false;
    int          inner    = 0;
    int          first    = 1; // first pass probes nonblocking (below)

    if (!nonblock) {
        if (nng_socket_get_ms(sock, NNG_OPT_RECVTIMEO, &tmo) == 0 &&
            tmo >= 0) {
            infinite = false;
            deadline = nng_clock() + (nng_time) tmo;
        }
        if (nng_socket_get_ms(sock, NNG_OPT_RECVTIMEO, &saved) == 0) {
            have_saved = true;
        }
        nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, NNG_SACK_ACK_DELAY_MS);
        sliced = true;
    } else {
        inner = NNG_FLAG_NONBLOCK;
    }
    for (;;) {
        char    *buf = (char *) data;
        size_t   cap = *sizep;
        int      rv  = nng_recv(sock, data, sizep,
            (!nonblock && first) ? (int) NNG_FLAG_NONBLOCK : inner);
        uint32_t seq = 0;
        int      pre = 0;
        int      flush = 0;
        int      keep;
        first = 0;
        if (rv == NNG_EAGAIN && !nonblock) {
            // Probe miss in blocking mode: see nng_recvmsg_sack.
            struct nng_sack_slot *pst =
                nng_sack_state_for(sock, false);
            if (pst != NULL && pst->pending > 0) {
                nng_sack_flush(sock);
            }
            *sizep = cap;
            continue;
        }
        if (rv == NNG_ETIMEDOUT && sliced) {
            struct nng_sack_slot *st =
                nng_sack_state_for(sock, false);
            if (st != NULL && st->pending > 0) {
                nng_sack_flush(sock);
            }
            if (!infinite && nng_clock() >= deadline) {
                if (have_saved) {
                    nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
                }
                *sizep = 0;
                return NNG_ETIMEDOUT;
            }
            *sizep = cap;
            continue;
        }
        if (rv != 0) {
            if (sliced && have_saved) {
                nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
            }
            return rv;
        }
        pre  = nng_sack_split_seq(buf, *sizep, &seq);
        keep = nng_sack_track(sock, seq, pre > 0, &flush);
        if (!keep) {
            nng_sack_flush(sock);
            *sizep = cap;
            continue;
        }
        if (pre > 0) {
            memmove(buf, buf + pre, *sizep - (size_t) pre);
            *sizep -= (size_t) pre;
        }
        if (flush) {
            nng_sack_flush(sock);
        }
        if (sliced && have_saved) {
            nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, saved);
        }
        return 0;
    }
}

#endif // NNG_PUBSUB_SACK

// Override pub/sub functions with the selected bridge.
// Must stay after all definitions above (see NOTE at top).
// SACK mode wins if several flags are (mistakenly) defined.
#if defined(NNG_PUBSUB_SACK)
#define nng_pub0_open nng_pub0_open_sack
#define nng_sub0_open nng_sub0_open_sack
#define nng_send nng_send_sack
#define nng_recvmsg nng_recvmsg_sack
#define nng_recv nng_recv_sack
#elif defined(NNG_PUBSUB_SURVEY)
#define nng_pub0_open nng_pub0_open_survey
#define nng_sub0_open nng_sub0_open_survey
#define nng_send nng_send_survey
#define nng_recvmsg nng_recvmsg_survey
#define nng_recv nng_recv_survey
#elif defined(NNG_PUBSUB_RELIABLE)
#define nng_pub0_open nng_pub0_open_reliable
#define nng_sub0_open nng_sub0_open_reliable
#define nng_send nng_send_reliable
#define nng_recvmsg nng_recvmsg_reliable
#define nng_recv nng_recv_reliable
#endif

#endif // NNG_PUBSUB_SURVEY || NNG_PUBSUB_RELIABLE || NNG_PUBSUB_SACK

#endif // NNG_WRAPPER_H
