#ifndef NNG_WRAPPER_H
#define NNG_WRAPPER_H

#include <nng/nng.h>

#ifdef NNG_PUBSUB_RELIABLE

#include <string.h>

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

// Override open functions
#define nng_pub0_open nng_pub0_open_reliable
#define nng_sub0_open nng_sub0_open_reliable

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
#define nng_send nng_send_reliable

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
#define nng_recvmsg nng_recvmsg_reliable

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
#define nng_recv nng_recv_reliable

#endif // NNG_PUBSUB_RELIABLE

#endif // NNG_WRAPPER_H
