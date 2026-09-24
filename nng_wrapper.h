#ifndef NNG_WRAPPER_H
#define NNG_WRAPPER_H

#include <nng/nng.h>

#if defined(NNG_PUBSUB_SURVEY) || defined(NNG_PUBSUB_RELIABLE)

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

#endif // mode selection

// Override pub/sub functions with the selected bridge.
// Must stay after all definitions above (see NOTE at top).
// Survey mode wins if both flags are (mistakenly) defined.
#if defined(NNG_PUBSUB_SURVEY)
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

#endif // NNG_PUBSUB_SURVEY || NNG_PUBSUB_RELIABLE

#endif // NNG_WRAPPER_H
