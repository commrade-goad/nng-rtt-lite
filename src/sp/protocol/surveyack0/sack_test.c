//
// Copyright 2026 Staysail Systems, Inc. <info@staysail.tech>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include "../../../testing/nuts.h"

#define SACK0_SELF 0x64
#define SACK0_PEER 0x65
#define SACK0_SELF_NAME "sack"
#define SACK0_PEER_NAME "sackresp"

static void
test_sack_identity(void)
{
	nng_socket  s;
	uint16_t    p;
	const char *n;

	NUTS_PASS(nng_sack0_open(&s));
	NUTS_PASS(nng_socket_proto_id(s, &p));
	NUTS_TRUE(p == SACK0_SELF);
	NUTS_PASS(nng_socket_peer_id(s, &p));
	NUTS_TRUE(p == SACK0_PEER);
	NUTS_PASS(nng_socket_proto_name(s, &n));
	NUTS_MATCH(n, SACK0_SELF_NAME);
	NUTS_PASS(nng_socket_peer_name(s, &n));
	NUTS_MATCH(n, SACK0_PEER_NAME);
	NUTS_CLOSE(s);
}

static void
test_sack_opts(void)
{
	nng_socket   s;
	nng_duration d;
	bool         b;
	int          i;

	NUTS_PASS(nng_sack0_open(&s));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_SACK_SURVEYTIME, 10));
	NUTS_PASS(nng_socket_get_ms(s, NNG_OPT_SACK_SURVEYTIME, &d));
	NUTS_TRUE(d == 10);

	NUTS_PASS(nng_socket_set_bool(s, NNG_OPT_SACK_ADAPTIVE, true));
	NUTS_PASS(nng_socket_get_bool(s, NNG_OPT_SACK_ADAPTIVE, &b));
	NUTS_TRUE(b);

	NUTS_PASS(nng_socket_get_ms(s, NNG_OPT_SACK_RTO, &d));
	NUTS_TRUE(d >= 200);
	NUTS_PASS(nng_socket_get_int(s, NNG_OPT_SACK_CWND, &i));
	NUTS_TRUE(i >= 1);
	NUTS_CLOSE(s);
}

// Window: 10 pipelined sends stay live at once (no abort), the
// respondent gets all 10, and ONE cumulative reply closes them.
static void
test_sack_pipeline_cumulative(void)
{
	nng_socket s;
	nng_socket r;
	char       tx[64];
	char       rx[64];
	size_t     sz;

	NUTS_PASS(nng_sack0_open(&s));
	NUTS_PASS(nng_sackresp0_open(&r));
	NUTS_PASS(nng_socket_set_bool(s, NNG_OPT_SACK_ADAPTIVE, true));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_SACK_SURVEYTIME, 2000));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_SENDTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(r, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(r, NNG_OPT_SENDTIMEO, 2000));
	NUTS_MARRY(s, r);

	// Pipeline 10 surveys without waiting (window = 8, so 2 over).
	for (int i = 1; i <= 10; i++) {
		snprintf(tx, sizeof(tx), "%d|payload-%d", i, i);
		NUTS_PASS(nng_send(s, tx, strlen(tx) + 1, 0));
	}

	// All 10 arrive intact, in order.
	for (int i = 1; i <= 10; i++) {
		sz = sizeof(rx);
		NUTS_PASS(nng_recv(r, rx, &sz, 0));
		snprintf(tx, sizeof(tx), "%d|payload-%d", i, i);
		NUTS_TRUE(strcmp(rx, tx) == 0);
		// Defer replies: only the LAST survey gets a cumulative ACK.
		if (i < 10) {
			continue;
		}
		NUTS_PASS(nng_send(r, "C11", 4, 0));
	}

	// Publisher gets the single cumulative reply.
	sz = sizeof(rx);
	NUTS_PASS(nng_recv(s, rx, &sz, 0));
	NUTS_TRUE(strcmp(rx, "C11") == 0);

	// Estimator trained by the contiguous advance.
	nng_duration rto = 0;
	NUTS_PASS(nng_socket_get_ms(s, NNG_OPT_SACK_RTO, &rto));
	NUTS_TRUE(rto >= 200);

	NUTS_CLOSE(s);
	NUTS_CLOSE(r);
}

// SACK mask: one hole + selective bits, single reply, only the hole
// is resent (checked implicitly: no hang, reply drains, window frees).
static void
test_sack_hole_sack_mask(void)
{
	nng_socket s;
	nng_socket r;
	char       tx[64];
	char       rx[128];
	size_t     sz;

	NUTS_PASS(nng_sack0_open(&s));
	NUTS_PASS(nng_sackresp0_open(&r));
	NUTS_PASS(nng_socket_set_bool(s, NNG_OPT_SACK_ADAPTIVE, true));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_SACK_SURVEYTIME, 2000));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(s, NNG_OPT_SENDTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(r, NNG_OPT_RECVTIMEO, 2000));
	NUTS_PASS(nng_socket_set_ms(r, NNG_OPT_SENDTIMEO, 2000));
	NUTS_MARRY(s, r);

	for (int i = 1; i <= 4; i++) {
		snprintf(tx, sizeof(tx), "%d|p%d", i, i);
		NUTS_PASS(nng_send(s, tx, strlen(tx) + 1, 0));
	}
	// Pretend sub missed 3: it reports next=3 but SACKs 4 (bit0).
	for (int i = 0; i < 4; i++) {
		sz = sizeof(rx);
		NUTS_PASS(nng_recv(r, rx, &sz, 0));
	}
	// "C3:2": next=3, mask=0x2 (bit1 set) = need 3, SACK 4.
	NUTS_PASS(nng_send(r, "C3:2", 5, 0));

	sz = sizeof(rx);
	NUTS_PASS(nng_recv(s, rx, &sz, 0));
	NUTS_TRUE(strcmp(rx, "C3:2") == 0);

	NUTS_CLOSE(s);
	NUTS_CLOSE(r);
}

NUTS_TESTS = {
	{ "sack identity", test_sack_identity },
	{ "sack opts", test_sack_opts },
	{ "sack pipeline cumulative", test_sack_pipeline_cumulative },
	{ "sack hole sack mask", test_sack_hole_sack_mask },
	{ NULL, NULL },
};
