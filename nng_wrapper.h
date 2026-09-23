#ifndef NNG_WRAPPER_H
#define NNG_WRAPPER_H

#include <nng/nng.h>

#if defined(NNG_PUBSUB_SURVEY) || defined(NNG_PUBSUB_RELIABLE)

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
// Quorum/deadline are configured by the application before opening
// the publisher (see ops_nng.c): NNG_SURVEY_QUORUM env (expected
// replies per message, default 1), NNG_SURVEY_TIME_MS env (survey
// deadline cap in ms, default 2000).
static int          nng_survey_quorum      = 1;
static nng_duration nng_survey_deadline_ms = 2000;

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
 * Sends once, then receives up to nng_survey_quorum replies (stops
 * early on deadline expiry). Returns 0 if at least one subscriber
 * replied, NNG_ETIMEDOUT otherwise.
 */
static inline int nng_send_survey(
    nng_socket sock, void *data, size_t size, int flags) {
    int rv = nng_send(sock, data, size, flags);
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
 */
static inline int nng_recvmsg_survey(
    nng_socket sock, nng_msg **msgp, int flags) {
    int rv = nng_recvmsg(sock, msgp, flags);
    if (rv != 0) {
        return rv;
    }
    int send_rv = nng_send(sock, (void *) "ACK", 3, 0);
    if (send_rv != 0) {
        nng_msg_free(*msgp);
        *msgp = NULL;
        return send_rv;
    }
    return 0;
}

/**
 * @brief Wrapper for nng_recv: receive data, reply with an ACK.
 */
static inline int nng_recv_survey(
    nng_socket sock, void *data, size_t *sizep, int flags) {
    int rv = nng_recv(sock, data, sizep, flags);
    if (rv != 0) {
        return rv;
    }
    int send_rv = nng_send(sock, (void *) "ACK", 3, 0);
    if (send_rv != 0) {
        return send_rv;
    }
    return 0;
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
