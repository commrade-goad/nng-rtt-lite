//
// Copyright 2025 Staysail Systems, Inc. <info@staysail.tech>
// Copyright 2018 Capitar IT Group BV <info@capitar.com>
//
// This software is supplied under the terms of the MIT License, a
// copy of which should be located in the distribution where this
// file was obtained (LICENSE.txt).  A copy of the license may also be
// found online at https://opensource.org/licenses/MIT.
//

#include "../../../core/aio.h"
#include "../../../core/defs.h"
#include "../../../core/idhash.h"
#include "../../../core/list.h"
#include "../../../core/lmq.h"
#include "../../../core/message.h"
#include "../../../core/pipe.h"
#include "../../../core/platform.h"
#include "../../../core/pollable.h"
#include "../../../core/protocol.h"

// Surveyor protocol.  The SURVEYOR protocol is the "survey" side of the
// survey pattern.  This is useful for building service discovery, voting, etc.
// Note that this pattern is not optimized for extreme low latency, as it makes
// multiple use of queues for simplicity.  Typically this is used in cases
// where a few dozen extra microseconds does not matter.

#define SURVEYOR0_SELF 0x62
#define SURVEYOR0_PEER 0x63
#define SURVEYOR0_SELF_NAME "surveyor"
#define SURVEYOR0_PEER_NAME "respondent"

// Adaptive RTT/AIMD constants, mirroring req0 (see reqrep0/req.c).
// RTO estimator follows RFC 6298 EWMA; cwnd follows AIMD.
#define SURV0_RTO_INIT 3000   // ms
#define SURV0_RTO_MIN  200    // ms
#define SURV0_RTO_MAX  60000  // ms
#define SURV0_AI_STEP  100    // ms additive increase bias
#define SURV0_CWND_INIT 1
#define SURV0_CWND_MIN  1
#define SURV0_CWND_MAX  1024
#define SURV0_RING_MAX  8   // repair window: last N surveys kept for resend
#define SURV0_RESEND_TICK 100 // ms between resend scans

typedef struct surv0_pipe surv0_pipe;
typedef struct surv0_sock surv0_sock;
typedef struct surv0_ctx  surv0_ctx;

static void surv0_pipe_send_cb(void *);
static void surv0_pipe_recv_cb(void *);
static void surv0_resend_cb(void *);

struct surv0_ctx {
	surv0_sock    *sock;
	uint32_t       survey_id; // survey id
	nni_lmq        recv_lmq;
	nni_list       recv_queue;
	nni_atomic_int recv_buf;
	nni_atomic_int survey_time;
	nni_time       expire;
	int            err;
};

// surv0_sock is our per-socket protocol private structure.
struct surv0_sock {
	int            ttl;
	nni_list       pipes;
	nni_mtx        mtx;
	surv0_ctx      ctx;
	nni_id_map     surveys;
	nni_pollable   writable;
	nni_pollable   readable;
	nni_atomic_int send_buf;

	// Socket-level baseline RTT estimator (adaptive mode only).
	nni_duration srtt;
	nni_duration rttvar;
	nni_duration rto;
	bool         rtt_initialized;

	// Socket-level congestion window (tracked for AIMD dynamics and
	// observability; surveyor sends are not gated by it).
	uint32_t cwnd;

	// Adaptive mode toggle, off by default for upstream compatibility.
	bool adaptive;

	// Repair ring (adaptive only): last SURV0_RING_MAX surveys, cloned
	// with headers intact, for timer-driven republish on missed replies.
	// ring_seq holds the survey id per slot (0 = empty slot).
	nni_msg *ring[SURV0_RING_MAX];
	uint32_t ring_seq[SURV0_RING_MAX];
	uint8_t  ring_pos;

	// Resend timer: scans pipes for unreplied surveys older than RTO.
	nni_aio resend_aio;
	bool    resend_active;
};

// surv0_pipe is our per-pipe protocol private structure.
struct surv0_pipe {
	nni_pipe     *pipe;
	surv0_sock   *sock;
	nni_lmq       send_queue;
	nni_list_node node;
	nni_aio       aio_send;
	nni_aio       aio_recv;
	bool          busy;
	bool          closed;

	// Per-pipe RTT estimator (adaptive mode only).
	nni_duration srtt;
	nni_duration rttvar;
	nni_duration rto;
	bool         rtt_initialized;
	uint32_t     samples;

	// Per-pipe congestion window (tracked, not gating).
	uint32_t cwnd;

	// Attribution for the latest survey handed to this pipe:
	// its id and the transmit timestamp (0 = none outstanding).
	uint32_t survey_out;
	nni_time send_time;

	// Set when the outstanding survey was republished by the resend
	// timer. Replies to republished surveys are still delivered, but
	// skipped for RTT sampling (Karn's rule: no samples off retries).
	bool resent;
};

// Clamp RTO to [RTO_MIN, RTO_MAX].
static inline nni_duration
surv0_rto_clamp(nni_duration rto)
{
	if (rto < SURV0_RTO_MIN) {
		rto = SURV0_RTO_MIN;
	}
	if (rto > SURV0_RTO_MAX) {
		rto = SURV0_RTO_MAX;
	}
	return rto;
}

// Update RTT estimator on a timely reply (RFC 6298 EWMA with
// additive-increase bias). Late replies never reach here: they miss
// the survey id lookup and are discarded, so no Karn ambiguity arises.
//
// NOTE: the AI_STEP bias is a fixed safety margin applied to the RTO
// computation only. It must NOT accumulate into srtt (adding it per
// sample would inflate srtt without bound, ~+100ms per reply).
static void
surv0_rtt_update_success(nni_duration *srtt, nni_duration *rttvar,
    nni_duration *rto, bool *initialized, nni_duration sample)
{
	nni_duration err;

	if (!(*initialized)) {
		*srtt        = sample;
		*rttvar      = (sample > 2) ? (sample / 2) : 1;
		*initialized = true;
	} else {
		err     = sample - *srtt;
		*srtt   = *srtt + err / 8;
		*rttvar = *rttvar + ((err < 0 ? -err : err) - *rttvar) / 4;
	}
	// RTO with fixed AI safety margin (anti spurious timeout).
	*rto = *srtt + 4 * (*rttvar) + SURV0_AI_STEP;
	*rto = surv0_rto_clamp(*rto);
}

// Update RTT estimator on a missed reply (multiplicative decrease).
static void
surv0_rtt_update_timeout(nni_duration *srtt, nni_duration *rttvar,
    nni_duration *rto, bool initialized)
{
	if (!initialized) {
		return;
	}
	*srtt = *srtt / 2;
	if (*srtt < SURV0_RTO_MIN) {
		*srtt = SURV0_RTO_MIN;
	}
	*rttvar = *rttvar / 2;
	if (*rttvar < 1) {
		*rttvar = 1;
	}
	*rto = *srtt + 4 * (*rttvar);
	*rto = surv0_rto_clamp(*rto);
}

// Apply a "miss" to one pipe: drop its attribution, decay its
// estimator and halve both congestion windows (multiplicative
// decrease). Called with the socket lock held.
static void
surv0_pipe_missed(surv0_sock *s, surv0_pipe *p)
{
	p->send_time  = 0;
	p->survey_out = 0;
	surv0_rtt_update_timeout(
	    &p->srtt, &p->rttvar, &p->rto, p->rtt_initialized);
	surv0_rtt_update_timeout(
	    &s->srtt, &s->rttvar, &s->rto, s->rtt_initialized);
	p->cwnd /= 2;
	if (p->cwnd < SURV0_CWND_MIN) {
		p->cwnd = SURV0_CWND_MIN;
	}
	s->cwnd /= 2;
	if (s->cwnd < SURV0_CWND_MIN) {
		s->cwnd = SURV0_CWND_MIN;
	}
}

// Settle the books of a superseded survey: every pipe that was handed
// the survey but never replied gets MD treatment. Called with the
// socket lock held, before the survey id is retired.
static void
surv0_settle_survey(surv0_sock *sock, uint32_t survey_id)
{
	surv0_pipe *pipe;

	if (survey_id == 0) {
		return;
	}
	NNI_LIST_FOREACH (&sock->pipes, pipe) {
		if (pipe->send_time == 0 || pipe->survey_out != survey_id) {
			continue;
		}
		surv0_pipe_missed(sock, pipe);
	}
}


static void
surv0_ctx_abort(surv0_ctx *ctx, int err)
{
	nni_aio    *aio;
	surv0_sock *sock = ctx->sock;

	while ((aio = nni_list_first(&ctx->recv_queue)) != NULL) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_error(aio, err);
	}
	nni_lmq_flush(&ctx->recv_lmq);
	if (ctx->survey_id != 0) {
		nni_id_remove(&sock->surveys, ctx->survey_id);
		ctx->survey_id = 0;
	}
	if (ctx == &sock->ctx) {
		nni_pollable_clear(&sock->readable);
	}
}

static void
surv0_ctx_close(surv0_ctx *ctx)
{
	surv0_sock *sock = ctx->sock;

	nni_mtx_lock(&sock->mtx);
	surv0_ctx_abort(ctx, NNG_ECLOSED);
	nni_mtx_unlock(&sock->mtx);
}

static void
surv0_ctx_fini(void *arg)
{
	surv0_ctx *ctx = arg;

	surv0_ctx_close(ctx);
	nni_lmq_fini(&ctx->recv_lmq);
}

static void
surv0_ctx_init(void *c, void *s)
{
	surv0_ctx   *ctx  = c;
	surv0_sock  *sock = s;
	int          len;
	nng_duration tmo;

	nni_aio_list_init(&ctx->recv_queue);
	nni_atomic_init(&ctx->recv_buf);
	nni_atomic_init(&ctx->survey_time);

	if (ctx == &sock->ctx) {
		len = 128;
		tmo = NNI_SECOND; // survey timeout
	} else {
		len = nni_atomic_get(&sock->ctx.recv_buf);
		tmo = nni_atomic_get(&sock->ctx.survey_time);
	}

	nni_atomic_set(&ctx->recv_buf, len);
	nni_atomic_set(&ctx->survey_time, tmo);

	ctx->sock = sock;

	nni_lmq_init(&ctx->recv_lmq, len);
}

static void
surv0_ctx_cancel(nni_aio *aio, void *arg, nng_err rv)
{
	surv0_ctx  *ctx  = arg;
	surv0_sock *sock = ctx->sock;
	nni_mtx_lock(&sock->mtx);
	if (nni_list_active(&ctx->recv_queue, aio)) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_error(aio, rv);
	}
	if (ctx->survey_id != 0) {
		// Adaptive: a cancelled receive (e.g. survey deadline
		// expiry) ends collection for this survey. Pipes that
		// never replied get MD treatment now: the next send
		// cannot see them, because ctx_send clears survey_id
		// (via this same path) before it could settle them.
		if (sock->adaptive) {
			surv0_settle_survey(sock, ctx->survey_id);
		}
		nni_id_remove(&sock->surveys, ctx->survey_id);
		ctx->survey_id = 0;
	}
	nni_mtx_unlock(&sock->mtx);
}

static void
surv0_ctx_recv(void *arg, nni_aio *aio)
{
	surv0_ctx   *ctx  = arg;
	surv0_sock  *sock = ctx->sock;
	nni_msg     *msg;
	nni_time     now;
	nni_duration timeout;

	now = nni_clock();

	nni_mtx_lock(&sock->mtx);
	if ((ctx->survey_id == 0) || (now >= ctx->expire)) {
		nni_mtx_unlock(&sock->mtx);
		nni_aio_finish_error(aio, NNG_ESTATE);
		return;
	}

	timeout = nni_aio_get_timeout(aio);
	if ((timeout < 1) || ((now + timeout) > ctx->expire)) {
		// limit the timeout to the survey time
		nni_aio_set_expire(aio, ctx->expire);
	}

again:
	if (nni_lmq_get(&ctx->recv_lmq, &msg) != 0) {
		if (!nni_aio_start(aio, &surv0_ctx_cancel, ctx)) {
			nni_mtx_unlock(&sock->mtx);
			return;
		}
		nni_list_append(&ctx->recv_queue, aio);
		nni_mtx_unlock(&sock->mtx);
		return;
	}
	if (nni_lmq_empty(&ctx->recv_lmq) && (ctx == &sock->ctx)) {
		nni_pollable_clear(&sock->readable);
	}
	if ((msg = nni_msg_unique(msg)) == NULL) {
		goto again;
	}

	nni_mtx_unlock(&sock->mtx);
	nni_aio_finish_msg(aio, msg);
}

static void
surv0_ctx_send(void *arg, nni_aio *aio)
{
	surv0_ctx   *ctx  = arg;
	surv0_sock  *sock = ctx->sock;
	surv0_pipe  *pipe;
	nni_msg     *msg = nni_aio_get_msg(aio);
	size_t       len = nni_msg_len(msg);
	nng_duration survey_time;
	int          rv;

	survey_time = nni_atomic_get(&ctx->survey_time);

	nni_mtx_lock(&sock->mtx);

	// Adaptive: settle the superseded survey first. Pipes that never
	// replied to it get multiplicative-decrease treatment.
	if (sock->adaptive) {
		surv0_settle_survey(sock, ctx->survey_id);
	}

	// Abort everything outstanding.
	surv0_ctx_abort(ctx, NNG_ECANCELED);

	// Allocate the new ID.
	if ((rv = nni_id_alloc32(&sock->surveys, &ctx->survey_id, ctx)) != 0) {
		nni_mtx_unlock(&sock->mtx);
		nni_aio_finish_error(aio, rv);
		return;
	}
	nni_msg_header_clear(msg);
	nni_msg_header_append_u32(msg, (uint32_t) ctx->survey_id);

	// From this point, we're committed to success.  Note that we send
	// regardless of whether there are any pipes or not.  If no pipes,
	// then it just gets discarded.
	nni_aio_set_msg(aio, NULL);
	NNI_LIST_FOREACH (&sock->pipes, pipe) {

		// if the pipe isn't busy, then send this message direct.
		if (!pipe->busy) {
			pipe->busy = true;
			nni_msg_clone(msg);
			nni_aio_set_msg(&pipe->aio_send, msg);
			nni_pipe_send(pipe->pipe, &pipe->aio_send);
		} else if (!nni_lmq_full(&pipe->send_queue)) {
			nni_msg_clone(msg);
			nni_lmq_put(&pipe->send_queue, msg);
		} else {
			continue;
		}
		// Adaptive: attribute this survey to the pipe for RTT
		// sampling when its reply arrives.
		if (sock->adaptive) {
			pipe->survey_out = ctx->survey_id;
			pipe->send_time  = nni_clock();
			pipe->resent     = false;
		}
	}

	// Adaptive: file this survey in the repair ring (shared reference;
	// freed on eviction or socket teardown) and make sure the resend
	// timer is running while anything is outstanding.
	if (sock->adaptive) {
		uint8_t slot = sock->ring_pos;
		nni_msg_clone(msg);
		if (sock->ring[slot] != NULL) {
			nni_msg_free(sock->ring[slot]);
		}
		sock->ring[slot]     = msg;
		sock->ring_seq[slot] = ctx->survey_id;
		sock->ring_pos       = (uint8_t) ((slot + 1) % SURV0_RING_MAX);
		if (!sock->resend_active && !nni_list_empty(&sock->pipes)) {
			sock->resend_active = true;
			nni_sleep_aio(SURV0_RESEND_TICK, &sock->resend_aio);
		}
	}

	// save the survey time, so we know the maximum timeout to use when
	// waiting for receive. With adaptive mode and a trained estimator,
	// the RTO (capped by the configured survey time) is used instead,
	// so the survey tracks what the network actually needs.
	if (sock->adaptive && sock->rtt_initialized) {
		nni_duration rto = sock->rto;
		if (rto > survey_time) {
			rto = survey_time;
		}
		ctx->expire = nni_clock() + rto;
	} else {
		ctx->expire = nni_clock() + survey_time;
	}

	nni_mtx_unlock(&sock->mtx);
	nni_msg_free(msg);

	nni_aio_finish(aio, 0, len);
}

static void
surv0_sock_fini(void *arg)
{
	surv0_sock *sock = arg;

	for (int i = 0; i < SURV0_RING_MAX; i++) {
		if (sock->ring[i] != NULL) {
			nni_msg_free(sock->ring[i]);
			sock->ring[i] = NULL;
		}
	}
	nni_aio_fini(&sock->resend_aio);
	surv0_ctx_fini(&sock->ctx);
	nni_id_map_fini(&sock->surveys);
	nni_pollable_fini(&sock->writable);
	nni_pollable_fini(&sock->readable);
	nni_mtx_fini(&sock->mtx);
}

static void
surv0_sock_init(void *arg, nni_sock *s)
{
	surv0_sock *sock = arg;

	NNI_ARG_UNUSED(s);

	NNI_LIST_INIT(&sock->pipes, surv0_pipe, node);
	nni_mtx_init(&sock->mtx);
	nni_pollable_init(&sock->readable);
	nni_pollable_init(&sock->writable);
	// We are always writable.
	nni_pollable_raise(&sock->writable);

	// We allow for some buffering on a per-pipe basis, to allow for
	// multiple contexts to have surveys outstanding.  It is recommended
	// to increase this if many contexts will want to publish
	// at nearly the same time.
	nni_atomic_init(&sock->send_buf);
	nni_atomic_set(&sock->send_buf, 8);

	// Survey IDs are 32 bits, with the high order bit set.
	// We start at a random point, to minimize likelihood of
	// accidental collision across restarts.
	nni_id_map_init(&sock->surveys, 0x80000000u, 0xffffffffu, true);

	surv0_ctx_init(&sock->ctx, sock);

	// Adaptive RTT/AIMD state. Off by default; the RTT-lite bridge
	// opts in via NNG_OPT_SURVEYOR_ADAPTIVE.
	sock->srtt            = 0;
	sock->rttvar          = 0;
	sock->rto             = SURV0_RTO_INIT;
	sock->rtt_initialized = false;
	sock->cwnd            = SURV0_CWND_INIT;
	sock->adaptive        = false;
	for (int i = 0; i < SURV0_RING_MAX; i++) {
		sock->ring[i]     = NULL;
		sock->ring_seq[i] = 0;
	}
	sock->ring_pos      = 0;
	sock->resend_active = false;
	nni_aio_init(&sock->resend_aio, surv0_resend_cb, sock);

	sock->ttl = 8;
}

static void
surv0_sock_open(void *arg)
{
	NNI_ARG_UNUSED(arg);
}

static void
surv0_sock_close(void *arg)
{
	surv0_sock *s = arg;

	nni_aio_stop(&s->resend_aio);
	surv0_ctx_close(&s->ctx);
}

static void
surv0_pipe_stop(void *arg)
{
	surv0_pipe *p = arg;

	nni_aio_stop(&p->aio_send);
	nni_aio_stop(&p->aio_recv);
}

static void
surv0_pipe_fini(void *arg)
{
	surv0_pipe *p = arg;

	nni_aio_fini(&p->aio_send);
	nni_aio_fini(&p->aio_recv);
	nni_lmq_fini(&p->send_queue);
}

static int
surv0_pipe_init(void *arg, nni_pipe *pipe, void *s)
{
	surv0_pipe *p    = arg;
	surv0_sock *sock = s;
	int         len;

	len = nni_atomic_get(&sock->send_buf);
	nni_aio_init(&p->aio_send, surv0_pipe_send_cb, p);
	nni_aio_init(&p->aio_recv, surv0_pipe_recv_cb, p);

	// This depth could be tunable.  The deeper the queue, the more
	// concurrent surveys that can be delivered (multiple contexts).
	// Note that surveys can be *outstanding*, but not yet put on the wire.
	nni_lmq_init(&p->send_queue, len);

	p->pipe = pipe;
	p->sock = sock;

	// Adaptive per-pipe RTT/AIMD state.
	p->srtt            = 0;
	p->rttvar          = 0;
	p->rto             = SURV0_RTO_INIT;
	p->rtt_initialized = false;
	p->samples         = 0;
	p->cwnd            = SURV0_CWND_INIT;
	p->survey_out      = 0;
	p->send_time       = 0;
	p->resent          = false;
	return (0);
}

static int
surv0_pipe_start(void *arg)
{
	surv0_pipe *p = arg;
	surv0_sock *s = p->sock;

	if (nni_pipe_peer(p->pipe) != SURVEYOR0_PEER) {
		nng_log_warn("NNG-PEER-MISMATCH",
		    "Peer protocol mismatch: %d != %d, rejected.",
		    nni_pipe_peer(p->pipe), SURVEYOR0_PEER);
		return (NNG_EPROTO);
	}

	nni_mtx_lock(&s->mtx);
	nni_list_append(&s->pipes, p);
	nni_mtx_unlock(&s->mtx);

	nni_pipe_recv(p->pipe, &p->aio_recv);
	return (0);
}

static void
surv0_pipe_close(void *arg)
{
	surv0_pipe *p = arg;
	surv0_sock *s = p->sock;

	nni_aio_close(&p->aio_send);
	nni_aio_close(&p->aio_recv);

	nni_mtx_lock(&s->mtx);
	p->closed = true;
	nni_lmq_flush(&p->send_queue);
	if (nni_list_active(&s->pipes, p)) {
		nni_list_remove(&s->pipes, p);
	}
	// Adaptive: drop any outstanding attribution so a dead pipe can
	// never produce a sample later. No estimator update: a dropped
	// connection is not a congestion signal.
	p->send_time  = 0;
	p->survey_out = 0;
	p->resent     = false;
	nni_mtx_unlock(&s->mtx);
}

// Resend timer: republish surveys that went unreplied past RTO.
// Runs every SURV0_RESEND_TICK while any pipe has anything outstanding.
// Republishes use the ORIGINAL survey id (still registered: nothing was
// aborted), so replies match normally; the subscriber's deliver-once
// rule turns repeats into harmless drops. Surveys older than the repair
// ring get MD treatment once instead of a resend (window horizon).
static void
surv0_resend_cb(void *arg)
{
	surv0_sock *s = arg;
	surv0_pipe *p;
	nni_time    now;
	bool        more = false;

	nni_mtx_lock(&s->mtx);
	if (!s->adaptive || (nni_aio_result(&s->resend_aio) != 0)) {
		s->resend_active = false;
		nni_mtx_unlock(&s->mtx);
		return;
	}
	now = nni_clock();
	NNI_LIST_FOREACH (&s->pipes, p) {
		nni_msg     *found = NULL;
		nni_duration tmo;

		if (p->send_time == 0) {
			continue;
		}
		tmo = p->rtt_initialized ? p->rto : SURV0_RTO_INIT;
		if (now <= p->send_time ||
		    (now - p->send_time) <= (nni_time) tmo) {
			continue;
		}
		for (int i = 0; i < SURV0_RING_MAX; i++) {
			if (s->ring[i] != NULL && s->ring_seq[i] == p->survey_out) {
				found = s->ring[i];
				break;
			}
		}
		if (found == NULL) {
			// Older than the repair window: MD once, give up.
			surv0_pipe_missed(s, p);
			continue;
		}
		if (!p->busy) {
			p->busy = true;
			nni_msg_clone(found);
			nni_aio_set_msg(&p->aio_send, found);
			nni_pipe_send(p->pipe, &p->aio_send);
		} else if (!nni_lmq_full(&p->send_queue)) {
			nni_msg_clone(found);
			nni_lmq_put(&p->send_queue, found);
		} else {
			continue;
		}
		// Re-base the sample clock to this transmit and mark the
		// survey resent (Karn: its eventual reply is not sampled).
		// Repeats are bounded: the next resend needs another full RTO.
		p->send_time = now;
		p->resent    = true;
	}
	NNI_LIST_FOREACH (&s->pipes, p) {
		if (p->send_time != 0) {
			more = true;
			break;
		}
	}
	if (more) {
		nni_sleep_aio(SURV0_RESEND_TICK, &s->resend_aio);
	} else {
		s->resend_active = false;
	}
	nni_mtx_unlock(&s->mtx);
}

static void
surv0_pipe_send_cb(void *arg)
{
	surv0_pipe *p    = arg;
	surv0_sock *sock = p->sock;
	nni_msg    *msg;

	if (nni_aio_result(&p->aio_send) != 0) {
		nni_msg_free(nni_aio_get_msg(&p->aio_send));
		nni_aio_set_msg(&p->aio_send, NULL);
		nni_pipe_close(p->pipe);
		return;
	}

	nni_mtx_lock(&sock->mtx);
	if (p->closed) {
		nni_mtx_unlock(&sock->mtx);
		return;
	}
	if (nni_lmq_get(&p->send_queue, &msg) == 0) {
		nni_aio_set_msg(&p->aio_send, msg);
		nni_pipe_send(p->pipe, &p->aio_send);
	} else {
		p->busy = false;
	}
	nni_mtx_unlock(&sock->mtx);
}

static void
surv0_pipe_recv_cb(void *arg)
{
	surv0_pipe *p    = arg;
	surv0_sock *sock = p->sock;
	surv0_ctx  *ctx;
	nni_msg    *msg;
	uint32_t    id;
	nni_aio    *aio;

	if (nni_aio_result(&p->aio_recv) != 0) {
		nni_pipe_close(p->pipe);
		return;
	}

	msg = nni_aio_get_msg(&p->aio_recv);
	nni_aio_set_msg(&p->aio_recv, NULL);
	nni_msg_set_pipe(msg, nni_pipe_id(p->pipe));

	// We yank 4 bytes of body, and move them to the header.
	if (nni_msg_len(msg) < 4) {
		// Peer sent us garbage.  Kick it.
		nni_msg_free(msg);
		nni_pipe_close(p->pipe);
		return;
	}
	id = nni_msg_trim_u32(msg);
	nni_msg_header_append_u32(msg, id);

	nni_mtx_lock(&sock->mtx);
	// Best effort at delivery.  Discard if no context or context is
	// unable to receive it.
	if (((ctx = nni_id_get(&sock->surveys, id)) == NULL) ||
	    (nni_lmq_full(&ctx->recv_lmq))) {
		nni_msg_free(msg);
		msg = NULL; // unmatched: no RTT sample below
	} else if ((aio = nni_list_first(&ctx->recv_queue)) != NULL) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_msg(aio, msg);
	} else {
		nni_lmq_put(&ctx->recv_lmq, msg);
		if (ctx == &sock->ctx) {
			nni_pollable_raise(&sock->readable);
		}
	}

	// Adaptive RTT: sample per-pipe latency for matched replies.
	// Only the first reply per pipe per survey counts; late replies
	// never reach here (their survey id is already retired). Replies
	// to timer-resent surveys are delivered but not sampled (Karn).
	if (msg != NULL && sock->adaptive && !p->resent &&
	    p->send_time != 0 && p->survey_out == id) {
		nni_time now_ts = nni_clock();
		if (now_ts > p->send_time) {
			nni_duration sample =
			    (nni_duration) (now_ts - p->send_time);
			surv0_rtt_update_success(&sock->srtt, &sock->rttvar,
			    &sock->rto, &sock->rtt_initialized, sample);
			surv0_rtt_update_success(&p->srtt, &p->rttvar,
			    &p->rto, &p->rtt_initialized, sample);
			p->samples++;
			if (sock->cwnd < SURV0_CWND_MAX) {
				sock->cwnd++;
			}
			if (p->cwnd < SURV0_CWND_MAX) {
				p->cwnd++;
			}
		}
		p->send_time  = 0;
		p->survey_out = 0;
	}
	nni_mtx_unlock(&sock->mtx);

	nni_pipe_recv(p->pipe, &p->aio_recv);
}

static nng_err
surv0_ctx_set_survey_time(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	surv0_ctx   *ctx = arg;
	nng_duration expire;
	nng_err      rv;
	if ((rv = nni_copyin_ms(&expire, buf, sz, t)) == NNG_OK) {
		nni_atomic_set(&ctx->survey_time, expire);
	}
	return (rv);
}

static nng_err
surv0_ctx_get_survey_time(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_ctx *ctx = arg;
	return (
	    nni_copyout_ms(nni_atomic_get(&ctx->survey_time), buf, szp, t));
}

static nng_err
surv0_sock_set_max_ttl(void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	surv0_sock *s = arg;
	return (nni_copyin_int(&s->ttl, buf, sz, 1, NNI_MAX_MAX_TTL, t));
}

static nng_err
surv0_sock_get_max_ttl(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_sock *s = arg;
	return (nni_copyout_int(s->ttl, buf, szp, t));
}

static nng_err
surv0_sock_set_survey_time(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	surv0_sock *s = arg;
	return (surv0_ctx_set_survey_time(&s->ctx, buf, sz, t));
}

static nng_err
surv0_sock_get_survey_time(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_sock *s = arg;
	return (surv0_ctx_get_survey_time(&s->ctx, buf, szp, t));
}

static nng_err
surv0_sock_set_adaptive(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	surv0_sock *s = arg;
	bool        v;
	nng_err     rv;

	if ((rv = nni_copyin_bool(&v, buf, sz, t)) == NNG_OK) {
		nni_mtx_lock(&s->mtx);
		s->adaptive = v;
		nni_mtx_unlock(&s->mtx);
	}
	return (rv);
}

static nng_err
surv0_sock_get_adaptive(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_sock *s = arg;
	bool        v;

	nni_mtx_lock(&s->mtx);
	v = s->adaptive;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_bool(v, buf, szp, t));
}

static nng_err
surv0_sock_get_rto(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_sock  *s = arg;
	nng_duration rto;

	nni_mtx_lock(&s->mtx);
	rto = s->rto;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_ms(rto, buf, szp, t));
}

static nng_err
surv0_sock_get_cwnd(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	surv0_sock *s = arg;
	int         cwnd;

	nni_mtx_lock(&s->mtx);
	cwnd = (int) s->cwnd;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_int(cwnd, buf, szp, t));
}

static nng_err
surv0_sock_get_send_fd(void *arg, int *fdp)
{
	surv0_sock *sock = arg;

	return (nni_pollable_getfd(&sock->writable, fdp));
}

static nng_err
surv0_sock_get_recv_fd(void *arg, int *fdp)
{
	surv0_sock *sock = arg;

	return (nni_pollable_getfd(&sock->readable, fdp));
}

static void
surv0_sock_recv(void *arg, nni_aio *aio)
{
	surv0_sock *s = arg;
	surv0_ctx_recv(&s->ctx, aio);
}

static void
surv0_sock_send(void *arg, nni_aio *aio)
{
	surv0_sock *s = arg;
	surv0_ctx_send(&s->ctx, aio);
}

static nni_proto_pipe_ops surv0_pipe_ops = {
	.pipe_size  = sizeof(surv0_pipe),
	.pipe_init  = surv0_pipe_init,
	.pipe_fini  = surv0_pipe_fini,
	.pipe_start = surv0_pipe_start,
	.pipe_close = surv0_pipe_close,
	.pipe_stop  = surv0_pipe_stop,
};

static nni_option surv0_ctx_options[] = {
	{
	    .o_name = NNG_OPT_SURVEYOR_SURVEYTIME,
	    .o_get  = surv0_ctx_get_survey_time,
	    .o_set  = surv0_ctx_set_survey_time,
	},
	{
	    .o_name = NULL,
	}
};
static nni_proto_ctx_ops surv0_ctx_ops = {
	.ctx_size    = sizeof(surv0_ctx),
	.ctx_init    = surv0_ctx_init,
	.ctx_fini    = surv0_ctx_fini,
	.ctx_send    = surv0_ctx_send,
	.ctx_recv    = surv0_ctx_recv,
	.ctx_options = surv0_ctx_options,
};

static nni_option surv0_sock_options[] = {
	{
	    .o_name = NNG_OPT_SURVEYOR_SURVEYTIME,
	    .o_get  = surv0_sock_get_survey_time,
	    .o_set  = surv0_sock_set_survey_time,
	},
	{
	    .o_name = NNG_OPT_MAXTTL,
	    .o_get  = surv0_sock_get_max_ttl,
	    .o_set  = surv0_sock_set_max_ttl,
	},
	{
	    .o_name = NNG_OPT_SURVEYOR_ADAPTIVE,
	    .o_get  = surv0_sock_get_adaptive,
	    .o_set  = surv0_sock_set_adaptive,
	},
	{
	    .o_name = NNG_OPT_SURVEYOR_RTO,
	    .o_get  = surv0_sock_get_rto,
	},
	{
	    .o_name = NNG_OPT_SURVEYOR_CWND,
	    .o_get  = surv0_sock_get_cwnd,
	},
	// terminate list
	{
	    .o_name = NULL,
	},
};

static nni_proto_sock_ops surv0_sock_ops = {
	.sock_size         = sizeof(surv0_sock),
	.sock_init         = surv0_sock_init,
	.sock_fini         = surv0_sock_fini,
	.sock_open         = surv0_sock_open,
	.sock_close        = surv0_sock_close,
	.sock_send         = surv0_sock_send,
	.sock_recv         = surv0_sock_recv,
	.sock_send_poll_fd = surv0_sock_get_send_fd,
	.sock_recv_poll_fd = surv0_sock_get_recv_fd,
	.sock_options      = surv0_sock_options,
};

static nni_proto surv0_proto = {
	.proto_self     = { SURVEYOR0_SELF, SURVEYOR0_SELF_NAME },
	.proto_peer     = { SURVEYOR0_PEER, SURVEYOR0_PEER_NAME },
	.proto_flags    = NNI_PROTO_FLAG_SNDRCV,
	.proto_sock_ops = &surv0_sock_ops,
	.proto_pipe_ops = &surv0_pipe_ops,
	.proto_ctx_ops  = &surv0_ctx_ops,
};

int
nng_surveyor0_open(nng_socket *sock)
{
	return (nni_proto_open(sock, &surv0_proto));
}
