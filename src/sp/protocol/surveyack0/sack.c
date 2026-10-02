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

#define SACK0_SELF 0x64
#define SACK0_PEER 0x65
#define SACK0_SELF_NAME "sack"
#define SACK0_PEER_NAME "sackresp"

// Cumulative-ACK RTT-lite broadcast (cloned from surveyor0).
// Window + repair buffer size: change in ONE place via this #define.
#define SACK0_RTO_INIT 3000   // ms
#define SACK0_RTO_MIN  200    // ms
#define SACK0_RTO_MAX  60000  // ms
#define SACK0_AI_STEP  100    // ms additive increase bias
#define SACK0_CWND_INIT 1
#define SACK0_CWND_MIN  1
#define SACK0_CWND_MAX  1024
#define SACK0_RING_MAX  8   // repair window: last N surveys kept for resend
#define SACK0_RESEND_TICK 100 // ms between resend scans
// Fanout pacing: when a caught-up pipe's queue is momentarily full,
// wait (unlocked, 1ms slices) up to this long for drain instead of
// silently skipping the survey on that pipe. A skipped survey is
// unrecoverable once the window slides past it, so bursts would turn
// into permanent holes. Pipes already behind (holes outstanding) are
// never waited on: the resend machinery owns them. Bound keeps no-HOL:
// a dead pipe costs at most this per send, then MD+skip.
#define SACK0_FANOUT_WAIT_MS 20
// Per-pipe transport queue depth (burst absorption room).
#define SACK0_SEND_BUF 32
// Cumulative-ACK batching: change ONLY SACK0_RING_MAX above; the batch
// size follows it. A sub sends one "C<next>[:mask]" per batch instead
// of one ACK per survey.
#define SACK0_ACK_BATCH SACK0_RING_MAX
#define SACK0_SACK_BITS 16 // SACK mask covers next 16 wrapper seqs above base

typedef struct sack0_pipe sack0_pipe;
typedef struct sack0_sock sack0_sock;
typedef struct sack0_ctx  sack0_ctx;

static void sack0_pipe_send_cb(void *);
static void sack0_pipe_recv_cb(void *);
static void sack0_resend_cb(void *);

struct sack0_ctx {
	sack0_sock    *sock;
	uint32_t       survey_id; // survey id
	nni_lmq        recv_lmq;
	nni_list       recv_queue;
	nni_atomic_int recv_buf;
	nni_atomic_int survey_time;
	nni_time       expire;
	int            err;
};

// sack0_sock is our per-socket protocol private structure.
struct sack0_sock {
	int            ttl;
	nni_list       pipes;
	nni_mtx        mtx;
	sack0_ctx      ctx;
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

	// Repair ring (adaptive only): last SACK0_RING_MAX surveys, cloned
	// with headers intact, for timer-driven republish on missed replies.
	// ring_seq holds the survey id per slot (0 = empty slot).
	nni_msg *ring[SACK0_RING_MAX];
	uint32_t ring_seq[SACK0_RING_MAX];
	uint8_t  ring_pos;
	// Cumulative-ACK window: up to SACK0_RING_MAX surveys live at once
	// (no abort on new send). Per-slot wrapper seq (parsed from the
	// "<wseq>|" body prefix, 0 = no prefix), first-send timestamp,
	// per-slot expiry, and Karn flag (slot was resent at least once).
	uint32_t ring_wseq[SACK0_RING_MAX];
	nni_time ring_time[SACK0_RING_MAX];
	nni_time ring_expire[SACK0_RING_MAX];
	bool     ring_resent[SACK0_RING_MAX];

	// Resend timer: scans pipes for unreplied surveys older than RTO.
	nni_aio resend_aio;
	bool    resend_active;
};

// sack0_pipe is our per-pipe protocol private structure.
struct sack0_pipe {
	nni_pipe     *pipe;
	sack0_sock   *sock;
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

	// Cumulative-ACK scoreboard for this pipe (adaptive only).
	// ack_base = next wrapper seq this pipe owes ("C<ack_base>" means
	// everything below ack_base is done, 0 = nothing acked yet).
	// ack_mask = SACK bits for the next SACK0_SACK_BITS seqs above
	// ack_base (bit i set = wseq ack_base+i selectively acked).
	uint32_t ack_base;
	uint32_t ack_mask;
	// Last resend pass to this pipe (bounds repeats to ~1 per RTO).
	nni_time last_resend;
	// Last fanout skip on this pipe (proves it slow: skip the pacing
	// wait for a while so one dead pipe can't tax every send).
	nni_time last_skip;
};

// Clamp RTO to [RTO_MIN, RTO_MAX].
static inline nni_duration
sack0_rto_clamp(nni_duration rto)
{
	if (rto < SACK0_RTO_MIN) {
		rto = SACK0_RTO_MIN;
	}
	if (rto > SACK0_RTO_MAX) {
		rto = SACK0_RTO_MAX;
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
sack0_rtt_update_success(nni_duration *srtt, nni_duration *rttvar,
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
	*rto = *srtt + 4 * (*rttvar) + SACK0_AI_STEP;
	*rto = sack0_rto_clamp(*rto);
}

// Update RTT estimator on a missed reply (multiplicative decrease).
static void
sack0_rtt_update_timeout(nni_duration *srtt, nni_duration *rttvar,
    nni_duration *rto, bool initialized)
{
	if (!initialized) {
		return;
	}
	*srtt = *srtt / 2;
	if (*srtt < SACK0_RTO_MIN) {
		*srtt = SACK0_RTO_MIN;
	}
	*rttvar = *rttvar / 2;
	if (*rttvar < 1) {
		*rttvar = 1;
	}
	*rto = *srtt + 4 * (*rttvar);
	*rto = sack0_rto_clamp(*rto);
}

// Apply a "miss" to one pipe: drop its attribution, decay its
// estimator and halve both congestion windows (multiplicative
// decrease). Called with the socket lock held.
static void
sack0_pipe_missed(sack0_sock *s, sack0_pipe *p)
{
	p->send_time  = 0;
	p->survey_out = 0;
	sack0_rtt_update_timeout(
	    &p->srtt, &p->rttvar, &p->rto, p->rtt_initialized);
	sack0_rtt_update_timeout(
	    &s->srtt, &s->rttvar, &s->rto, s->rtt_initialized);
	p->cwnd /= 2;
	if (p->cwnd < SACK0_CWND_MIN) {
		p->cwnd = SACK0_CWND_MIN;
	}
	s->cwnd /= 2;
	if (s->cwnd < SACK0_CWND_MIN) {
		s->cwnd = SACK0_CWND_MIN;
	}
}

// --- Cumulative-ACK window helpers ---
// All called with the socket lock held.

// Wrap-safe sequence ordering (valid while gaps stay well below 2^31,
// always true here: the window is SACK0_RING_MAX).
static inline bool
sack0_seq_lt(uint32_t a, uint32_t b)
{
	return ((int32_t) (a - b) < 0);
}

static inline bool
sack0_seq_le(uint32_t a, uint32_t b)
{
	return ((int32_t) (a - b) <= 0);
}

// Parse the "<wseq>|..." body prefix the publisher prepends to every
// survey. Returns the wrapper seq, or 0 when there is no numeric prefix
// (legacy/foreign peer).
static uint32_t
sack0_parse_wseq(const char *body, size_t len)
{
	size_t   i = 0;
	uint32_t v = 0;

	if (len == 0) {
		return (0);
	}
	while (i < len && body[i] >= '0' && body[i] <= '9') {
		v = v * 10 + (uint32_t) (body[i] - '0');
		i++;
	}
	if (i == 0 || i >= len || body[i] != '|') {
		return (0);
	}
	return (v);
}

// Parse a cumulative reply body: "C<next>" or "C<next>:<maskhex>".
// <next> = next wrapper seq the sender still needs (everything below
// it is done). <maskhex> = SACK bits for the next SACK0_SACK_BITS seqs
// above <next> (LSB = seq <next> itself). Returns true when valid.
static bool
sack0_parse_cack(const char *body, size_t len, uint32_t *nextp,
    uint32_t *maskp)
{
	size_t   i = 0;
	uint32_t v = 0;
	uint32_t m = 0;
	int      nd = 0;
	int      hd = 0;

	if (len < 2 || body[0] != 'C') {
		return (false);
	}
	i = 1;
	while (i < len && body[i] >= '0' && body[i] <= '9') {
		v = v * 10 + (uint32_t) (body[i] - '0');
		i++;
		nd++;
	}
	if (nd == 0 || v == 0) {
		return (false);
	}
	*nextp = v;
	*maskp = 0;
	if (i < len && body[i] == ':') {
		i++;
		while (i < len && hd < 8) {
			char     c = body[i];
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
			return (false);
		}
		*maskp = m;
	}
	return (true);
}

// Find the live ring slot holding a transport survey id (-1 if none).
static int
sack0_find_slot(sack0_sock *s, uint32_t survey_id)
{
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		if (s->ring_seq[i] == survey_id) {
			return (i);
		}
	}
	return (-1);
}

// Count live window slots.
static int
sack0_live_count(sack0_sock *s)
{
	int n = 0;
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		if (s->ring_seq[i] != 0) {
			n++;
		}
	}
	return (n);
}

// Drop one window slot: unregister its survey id and free its message.
// Refreshes ctx->survey_id to the newest live survey (0 when drained).
static void
sack0_remove_slot(sack0_sock *s, int idx)
{
	uint32_t id;
	nni_time newest = 0;
	uint32_t new_id = 0;

	if (s->ring_seq[idx] == 0) {
		return;
	}
	id = s->ring_seq[idx];
	nni_id_remove(&s->surveys, id);
	if (s->ring[idx] != NULL) {
		nni_msg_free(s->ring[idx]);
		s->ring[idx] = NULL;
	}
	s->ring_seq[idx]    = 0;
	s->ring_wseq[idx]   = 0;
	s->ring_time[idx]   = 0;
	s->ring_expire[idx] = 0;
	s->ring_resent[idx] = false;
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		if (s->ring_seq[i] != 0 && s->ring_time[i] >= newest) {
			newest = s->ring_time[i];
			new_id = s->ring_seq[i];
		}
	}
	s->ctx.survey_id = new_id;
}

// True when a pipe has acked wrapper seq w (contiguous base or SACK bit).
static bool
sack0_pipe_acked(sack0_pipe *p, uint32_t w)
{
	if (w == 0) {
		return (false);
	}
	if (p->ack_base == 0) {
		return (false);
	}
	if (sack0_seq_lt(w, p->ack_base)) {
		return (true);
	}
	uint32_t d = w - p->ack_base;
	if (d < SACK0_SACK_BITS) {
		return (((p->ack_mask >> d) & 1u) != 0);
	}
	return (false);
}

// Re-point a pipe's single-outstanding clock at its oldest unacked hole
// (0 when fully caught up). Called after every scoreboard change.
static void
sack0_pipe_reclock(sack0_sock *s, sack0_pipe *p)
{
	int      best  = -1;
	uint32_t bestw = 0;

	for (int i = 0; i < SACK0_RING_MAX; i++) {
		uint32_t w;
		if (s->ring_seq[i] == 0) {
			continue;
		}
		w = s->ring_wseq[i];
		if (w != 0 && sack0_pipe_acked(p, w)) {
			continue;
		}
		if (best < 0 || (w != 0 && bestw != 0 && sack0_seq_lt(w, bestw)) ||
		    (w == 0 && bestw != 0)) {
			best  = i;
			bestw = w;
		}
	}
	if (best < 0) {
		p->send_time  = 0;
		p->survey_out = 0;
		p->resent     = false;
		return;
	}
	p->survey_out = s->ring_seq[best];
	p->send_time  = s->ring_time[best];
	p->resent     = s->ring_resent[best];
}

// Free every window slot that ALL pipes have acked (pipes that never
// sent a cumulative yet block freeing; eviction handles laggards).
static void
sack0_collect_acked(sack0_sock *s)
{
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		uint32_t    w;
		sack0_pipe *p;
		bool        all;

		if (s->ring_seq[i] == 0 || (w = s->ring_wseq[i]) == 0) {
			continue;
		}
		all = true;
		NNI_LIST_FOREACH (&s->pipes, p) {
			if (!sack0_pipe_acked(p, w)) {
				all = false;
				break;
			}
		}
		if (all) {
			sack0_remove_slot(s, i);
		}
	}
}

// Evict the oldest live slot (smallest wrapper seq, legacy w==0 first).
// Pipes that never acked it get MD treatment once (window horizon).
static void
sack0_evict_oldest(sack0_sock *s)
{
	int         best  = -1;
	uint32_t    bestw = 0;
	sack0_pipe *p;
	nni_time    now;

	for (int i = 0; i < SACK0_RING_MAX; i++) {
		uint32_t w;
		if (s->ring_seq[i] == 0) {
			continue;
		}
		w = s->ring_wseq[i];
		if (best < 0 || (bestw != 0 && w == 0) ||
		    (w != 0 && bestw != 0 && sack0_seq_lt(w, bestw))) {
			best  = i;
			bestw = w;
		}
	}
	if (best < 0) {
		return;
	}
	// Window horizon: MD only pipes this slot is genuinely late for
	// (older than their RTO). A full window sliding under healthy
	// streaming evicts without punishment; the cumulative ACK arriving
	// a moment later still advances everyone.
	now = nni_clock();
	NNI_LIST_FOREACH (&s->pipes, p) {
		nni_duration tmo;
		if (s->ring_wseq[best] != 0 &&
		    sack0_pipe_acked(p, s->ring_wseq[best])) {
			continue;
		}
		tmo = p->rtt_initialized ? p->rto : SACK0_RTO_INIT;
		if (now <= s->ring_time[best] ||
		    (now - s->ring_time[best]) <= (nni_time) tmo) {
			continue;
		}
		sack0_pipe_missed(s, p);
		sack0_pipe_reclock(s, p);
	}
	sack0_remove_slot(s, best);
}

// Clear the whole window (socket close: no MD; cancel/expiry: MD holes).
static void
sack0_clear_live(sack0_sock *s, bool md)
{
	sack0_pipe *p;

	if (md) {
		NNI_LIST_FOREACH (&s->pipes, p) {
			if (p->send_time != 0) {
				sack0_pipe_missed(s, p);
			}
		}
	}
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		if (s->ring_seq[i] != 0) {
			nni_id_remove(&s->surveys, s->ring_seq[i]);
		}
		if (s->ring[i] != NULL) {
			nni_msg_free(s->ring[i]);
			s->ring[i] = NULL;
		}
		s->ring_seq[i]    = 0;
		s->ring_wseq[i]   = 0;
		s->ring_time[i]   = 0;
		s->ring_expire[i] = 0;
		s->ring_resent[i] = false;
	}
	s->ctx.survey_id = 0;
	NNI_LIST_FOREACH (&s->pipes, p) {
		p->send_time  = 0;
		p->survey_out = 0;
		p->resent     = false;
	}
}

// Advance one pipe's scoreboard from a cumulative "C<next>[:mask]".
// Samples RTT off the oldest newly-acked slot (skipped when any of the
// covered slots was resent: Karn), grows cwnd (AI), and re-points the
// pipe clock. Returns the newly-acked contiguous count.
static uint32_t
sack0_advance_pipe(sack0_sock *s, sack0_pipe *p, uint32_t next, uint32_t mask)
{
	uint32_t start = p->ack_base;
	uint32_t newly = 0;
	int      oldest = -1;
	bool     karn   = false;
	nni_time now;

	if (next == 0) {
		return (0);
	}
	if (start != 0 && sack0_seq_le(next, start)) {
		if (next == start) {
			p->ack_mask |= (mask & 0xFFFFu);
			sack0_pipe_reclock(s, p);
		}
		return (0);
	}
	if (start == 0) {
		// First cumulative from this pipe: credit from the oldest
		// live unacked seq so the initial burst still trains RTT.
		uint32_t minw = 0;
		for (int i = 0; i < SACK0_RING_MAX; i++) {
			if (s->ring_seq[i] == 0 || s->ring_wseq[i] == 0) {
				continue;
			}
			if (sack0_seq_lt(s->ring_wseq[i], next) &&
			    (minw == 0 ||
			        sack0_seq_lt(s->ring_wseq[i], minw))) {
				minw = s->ring_wseq[i];
			}
		}
		start = minw;
	}
	now = nni_clock();
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		uint32_t w;
		if (s->ring_seq[i] == 0 || (w = s->ring_wseq[i]) == 0) {
			continue;
		}
		if (start != 0 &&
		    (sack0_seq_lt(w, start) || !sack0_seq_lt(w, next))) {
			continue;
		}
		if (oldest < 0 ||
		    sack0_seq_lt(w, s->ring_wseq[oldest])) {
			oldest = i;
		}
		if (s->ring_resent[i]) {
			karn = true;
		}
	}
	if (start != 0) {
		newly = next - start;
	}
	if (oldest >= 0 && !karn && now > s->ring_time[oldest]) {
		nni_duration sample = (nni_duration) (now - s->ring_time[oldest]);
		sack0_rtt_update_success(&s->srtt, &s->rttvar, &s->rto,
		    &s->rtt_initialized, sample);
		sack0_rtt_update_success(&p->srtt, &p->rttvar, &p->rto,
		    &p->rtt_initialized, sample);
		p->samples++;
	}
	if (s->cwnd < SACK0_CWND_MAX) {
		s->cwnd++;
	}
	if (p->cwnd < SACK0_CWND_MAX) {
		p->cwnd++;
	}
	p->ack_base = next;
	p->ack_mask = mask & 0xFFFFu;
	sack0_pipe_reclock(s, p);
	return (newly);
}


static void
sack0_ctx_abort(sack0_ctx *ctx, int err)
{
	nni_aio    *aio;
	sack0_sock *sock = ctx->sock;

	while ((aio = nni_list_first(&ctx->recv_queue)) != NULL) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_error(aio, err);
	}
	nni_lmq_flush(&ctx->recv_lmq);
	// Windowed: drop every live survey id, not just the latest.
	sack0_clear_live(sock, false);
	if (ctx == &sock->ctx) {
		nni_pollable_clear(&sock->readable);
	}
}

static void
sack0_ctx_close(sack0_ctx *ctx)
{
	sack0_sock *sock = ctx->sock;

	nni_mtx_lock(&sock->mtx);
	sack0_ctx_abort(ctx, NNG_ECLOSED);
	nni_mtx_unlock(&sock->mtx);
}

static void
sack0_ctx_fini(void *arg)
{
	sack0_ctx *ctx = arg;

	sack0_ctx_close(ctx);
	nni_lmq_fini(&ctx->recv_lmq);
}

static void
sack0_ctx_init(void *c, void *s)
{
	sack0_ctx   *ctx  = c;
	sack0_sock  *sock = s;
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
sack0_ctx_cancel(nni_aio *aio, void *arg, nng_err rv)
{
	sack0_ctx  *ctx  = arg;
	sack0_sock *sock = ctx->sock;
	nni_mtx_lock(&sock->mtx);
	if (nni_list_active(&ctx->recv_queue, aio)) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_error(aio, rv);
	}
	if (!sock->adaptive && ctx->survey_id != 0) {
		// Legacy single-outstanding path: a cancelled receive
		// retires the survey (original survey0 semantics).
		nni_id_remove(&sock->surveys, ctx->survey_id);
		ctx->survey_id = 0;
	}
	// Windowed (adaptive): a recv timeout/cancel ends only this wait,
	// never the window. Slot expiry (survey deadline) is reaped by the
	// resend timer, which MDs unacked pipes and frees the slot there.
	nni_mtx_unlock(&sock->mtx);
}

static void
sack0_ctx_recv(void *arg, nni_aio *aio)
{
	sack0_ctx   *ctx  = arg;
	sack0_sock  *sock = ctx->sock;
	nni_msg     *msg;
	nni_time     now;
	nni_duration timeout;

	now = nni_clock();

	nni_mtx_lock(&sock->mtx);

	// Buffered cumulatives are always drainable, even after the window
	// that produced them has fully closed (survey_id back to 0). This
	// is what lets the publisher pump "C" batches after the fact.
	if (nni_lmq_empty(&ctx->recv_lmq)) {
		if ((ctx->survey_id == 0) || (now >= ctx->expire)) {
			nni_mtx_unlock(&sock->mtx);
			nni_aio_finish_error(aio, NNG_ESTATE);
			return;
		}
	}

	timeout = nni_aio_get_timeout(aio);
	if (ctx->survey_id != 0 &&
	    ((timeout < 1) || ((now + timeout) > ctx->expire))) {
		// limit the timeout to the survey time
		nni_aio_set_expire(aio, ctx->expire);
	}

again:
	if (nni_lmq_get(&ctx->recv_lmq, &msg) != 0) {
		if (!nni_aio_start(aio, &sack0_ctx_cancel, ctx)) {
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
sack0_ctx_send(void *arg, nni_aio *aio)
{
	sack0_ctx   *ctx  = arg;
	sack0_sock  *sock = ctx->sock;
	sack0_pipe  *pipe;
	nni_msg     *msg = nni_aio_get_msg(aio);
	size_t       len = nni_msg_len(msg);
	nng_duration survey_time;
	uint32_t     new_id;
	uint32_t     wseq;
	nni_time     now;
	int          rv;

	survey_time = nni_atomic_get(&ctx->survey_time);

	nni_mtx_lock(&sock->mtx);

	if (!sock->adaptive) {
		// Legacy single-outstanding path (adaptive off).
		sack0_clear_live(sock, false);

		// Allocate the new ID.
		if ((rv = nni_id_alloc32(&sock->surveys, &ctx->survey_id, ctx)) !=
		    0) {
			nni_mtx_unlock(&sock->mtx);
			nni_aio_finish_error(aio, rv);
			return;
		}
		nni_msg_header_clear(msg);
		nni_msg_header_append_u32(msg, (uint32_t) ctx->survey_id);

		// From this point, we're committed to success.  Note that we
		// send regardless of whether there are any pipes or not.  If
		// no pipes, then it just gets discarded.
		nni_aio_set_msg(aio, NULL);
		NNI_LIST_FOREACH (&sock->pipes, pipe) {
			// if the pipe isn't busy, then send this message
			// direct.
			if (!pipe->busy) {
				pipe->busy = true;
				nni_msg_clone(msg);
				nni_aio_set_msg(&pipe->aio_send, msg);
				nni_pipe_send(pipe->pipe, &pipe->aio_send);
			} else if (!nni_lmq_full(&pipe->send_queue)) {
				nni_msg_clone(msg);
				nni_lmq_put(&pipe->send_queue, msg);
			}
		}

		ctx->expire = nni_clock() + survey_time;

		nni_mtx_unlock(&sock->mtx);
		nni_msg_free(msg);

		nni_aio_finish(aio, 0, len);
		return;
	}

	// Adaptive windowed path: up to SACK0_RING_MAX surveys live at once,
	// no abort. A full window evicts the oldest (MD for laggards).
	if (sack0_live_count(sock) >= SACK0_RING_MAX) {
		sack0_evict_oldest(sock);
	}

	// Allocate the new ID.
	new_id = 0;
	if ((rv = nni_id_alloc32(&sock->surveys, &new_id, ctx)) != 0) {
		nni_mtx_unlock(&sock->mtx);
		nni_aio_finish_error(aio, rv);
		return;
	}
	ctx->survey_id = new_id; // newest live (recv stays open)
	nni_msg_header_clear(msg);
	nni_msg_header_append_u32(msg, new_id);

	// Wrapper seq for cumulative ACKs ("<wseq>|" body prefix, 0 = none).
	wseq = sack0_parse_wseq(nni_msg_body(msg), nni_msg_len(msg));

	// From this point, we're committed to success.  Note that we send
	// regardless of whether there are any pipes or not.  If no pipes,
	// then it just gets discarded.
	now = nni_clock();
	// Burst pacing: if a pipe is momentarily saturated, wait briefly
	// for drain instead of skipping it into a permanent hole (a skip
	// is unrecoverable once the window slides past it). Only a boolean
	// crosses the unlock; the list is re-scanned every slice, so a
	// concurrent close is always safe. Pipes that proved slow (skipped
	// within the last second) are never waited on: the hole machinery
	// (resend/MD) owns them. Bound keeps no-HOL: a dead pipe costs at
	// most SACK0_FANOUT_WAIT_MS per second of sends, then MD+skip.
	for (int waited = 0; waited < SACK0_FANOUT_WAIT_MS; waited++) {
		bool saturated = false;
		NNI_LIST_FOREACH (&sock->pipes, pipe) {
			if (pipe->busy &&
			    nni_lmq_full(&pipe->send_queue) &&
			    (pipe->last_skip == 0 ||
			        (now - pipe->last_skip) > 1000)) {
				saturated = true;
				break;
			}
		}
		if (!saturated) {
			break;
		}
		nni_mtx_unlock(&sock->mtx);
		nni_msleep(1);
		nni_mtx_lock(&sock->mtx);
		now = nni_clock();
	}
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
			// Still saturated after the wait: skip and stamp it,
			// so pacing backs off for a while. The hole machinery
			// (resend/MD) owns recovery from here.
			pipe->last_skip = nni_clock();
			continue;
		}
	}
	// Re-point every pipe clock at its oldest hole below via the
	// ring store; reclock after filing keeps legacy fields coherent.

	// File this survey in the repair window (shared reference; freed
	// on eviction, collect, or socket teardown) and make sure the
	// resend timer is running while anything is outstanding.
	{
		uint8_t slot = sock->ring_pos;
		nni_msg_clone(msg);
		if (sock->ring[slot] != NULL) {
			nni_msg_free(sock->ring[slot]);
		}
		nni_duration eff = survey_time;
		if (sock->rtt_initialized && sock->rto < eff) {
			eff = sock->rto;
		}
		sock->ring[slot]        = msg;
		sock->ring_seq[slot]    = new_id;
		sock->ring_wseq[slot]   = wseq;
		sock->ring_time[slot]   = now;
		sock->ring_expire[slot] = now + eff;
		sock->ring_resent[slot] = false;
		sock->ring_pos = (uint8_t) ((slot + 1) % SACK0_RING_MAX);
		// Window expiry. honors the slowest hole, not just the newest.
		nni_time latest = 0;
		for (int i = 0; i < SACK0_RING_MAX; i++) {
			if (sock->ring_seq[i] != 0 &&
			    sock->ring_expire[i] > latest) {
				latest = sock->ring_expire[i];
			}
		}
		ctx->expire = latest;
		if (!sock->resend_active && !nni_list_empty(&sock->pipes)) {
			sock->resend_active = true;
			nni_sleep_aio(SACK0_RESEND_TICK, &sock->resend_aio);
		}
	}
	NNI_LIST_FOREACH (&sock->pipes, pipe) {
		sack0_pipe_reclock(sock, pipe);
	}

	nni_mtx_unlock(&sock->mtx);
	nni_msg_free(msg);

	nni_aio_finish(aio, 0, len);
}

static void
sack0_sock_fini(void *arg)
{
	sack0_sock *sock = arg;

	for (int i = 0; i < SACK0_RING_MAX; i++) {
		if (sock->ring[i] != NULL) {
			nni_msg_free(sock->ring[i]);
			sock->ring[i] = NULL;
		}
	}
	nni_aio_fini(&sock->resend_aio);
	sack0_ctx_fini(&sock->ctx);
	nni_id_map_fini(&sock->surveys);
	nni_pollable_fini(&sock->writable);
	nni_pollable_fini(&sock->readable);
	nni_mtx_fini(&sock->mtx);
}

static void
sack0_sock_init(void *arg, nni_sock *s)
{
	sack0_sock *sock = arg;

	NNI_ARG_UNUSED(s);

	NNI_LIST_INIT(&sock->pipes, sack0_pipe, node);
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
	nni_atomic_set(&sock->send_buf, SACK0_SEND_BUF);

	// Survey IDs are 32 bits, with the high order bit set.
	// We start at a random point, to minimize likelihood of
	// accidental collision across restarts.
	nni_id_map_init(&sock->surveys, 0x80000000u, 0xffffffffu, true);

	sack0_ctx_init(&sock->ctx, sock);

	// Adaptive RTT/AIMD state. Off by default; the RTT-lite bridge
	// opts in via NNG_OPT_SACK_ADAPTIVE.
	sock->srtt            = 0;
	sock->rttvar          = 0;
	sock->rto             = SACK0_RTO_INIT;
	sock->rtt_initialized = false;
	sock->cwnd            = SACK0_CWND_INIT;
	sock->adaptive        = false;
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		sock->ring[i]         = NULL;
		sock->ring_seq[i]     = 0;
		sock->ring_wseq[i]    = 0;
		sock->ring_time[i]    = 0;
		sock->ring_expire[i]  = 0;
		sock->ring_resent[i]  = false;
	}
	sock->ring_pos      = 0;
	sock->resend_active = false;
	nni_aio_init(&sock->resend_aio, sack0_resend_cb, sock);

	sock->ttl = 8;
}

static void
sack0_sock_open(void *arg)
{
	NNI_ARG_UNUSED(arg);
}

static void
sack0_sock_close(void *arg)
{
	sack0_sock *s = arg;

	nni_aio_stop(&s->resend_aio);
	sack0_ctx_close(&s->ctx);
}

static void
sack0_pipe_stop(void *arg)
{
	sack0_pipe *p = arg;

	nni_aio_stop(&p->aio_send);
	nni_aio_stop(&p->aio_recv);
}

static void
sack0_pipe_fini(void *arg)
{
	sack0_pipe *p = arg;

	nni_aio_fini(&p->aio_send);
	nni_aio_fini(&p->aio_recv);
	nni_lmq_fini(&p->send_queue);
}

static int
sack0_pipe_init(void *arg, nni_pipe *pipe, void *s)
{
	sack0_pipe *p    = arg;
	sack0_sock *sock = s;
	int         len;

	len = nni_atomic_get(&sock->send_buf);
	nni_aio_init(&p->aio_send, sack0_pipe_send_cb, p);
	nni_aio_init(&p->aio_recv, sack0_pipe_recv_cb, p);

	// This depth could be tunable.  The deeper the queue, the more
	// concurrent surveys that can be delivered (multiple contexts).
	// Note that surveys can be *outstanding*, but not yet put on the wire.
	nni_lmq_init(&p->send_queue, len);

	p->pipe = pipe;
	p->sock = sock;

	// Adaptive per-pipe RTT/AIMD state.
	p->srtt            = 0;
	p->rttvar          = 0;
	p->rto             = SACK0_RTO_INIT;
	p->rtt_initialized = false;
	p->samples         = 0;
	p->cwnd            = SACK0_CWND_INIT;
	p->survey_out      = 0;
	p->send_time       = 0;
	p->resent          = false;
	// Cumulative-ACK scoreboard (adaptive only).
	p->ack_base    = 0;
	p->ack_mask    = 0;
	p->last_resend = 0;
	p->last_skip   = 0;
	return (0);
}

static int
sack0_pipe_start(void *arg)
{
	sack0_pipe *p = arg;
	sack0_sock *s = p->sock;

	if (nni_pipe_peer(p->pipe) != SACK0_PEER) {
		nng_log_warn("NNG-PEER-MISMATCH",
		    "Peer protocol mismatch: %d != %d, rejected.",
		    nni_pipe_peer(p->pipe), SACK0_PEER);
		return (NNG_EPROTO);
	}

	nni_mtx_lock(&s->mtx);
	nni_list_append(&s->pipes, p);
	nni_mtx_unlock(&s->mtx);

	nni_pipe_recv(p->pipe, &p->aio_recv);
	return (0);
}

static void
sack0_pipe_close(void *arg)
{
	sack0_pipe *p = arg;
	sack0_sock *s = p->sock;

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
	p->send_time   = 0;
	p->survey_out  = 0;
	p->resent      = false;
	p->ack_base    = 0;
	p->ack_mask    = 0;
	p->last_resend = 0;
	p->last_skip   = 0;
	nni_mtx_unlock(&s->mtx);
}

// Resend timer: republish unacked window HOLES past RTO, per pipe.
// Runs every SACK0_RESEND_TICK while any pipe has anything outstanding.
// Republishes use the ORIGINAL survey id (still registered: the window
// holds up to SACK0_RING_MAX live surveys, nothing is aborted), so
// replies match normally; the subscriber's deliver-once rule turns
// repeats into harmless drops while still feeding its books a fresh
// cumulative. Holes older than 2xRTO also take MD treatment (decay +
// halve) but stay resendable: the window horizon (eviction) is what
// finally gives up. Repeats are bounded to ~1 per RTO per pipe via
// last_resend. Selectively-acked (SACK bit) slots are never resent.
static void
sack0_resend_cb(void *arg)
{
	sack0_sock *s = arg;
	sack0_pipe *p;
	nni_time    now;
	bool        more = false;

	nni_mtx_lock(&s->mtx);
	if (!s->adaptive || (nni_aio_result(&s->resend_aio) != 0)) {
		s->resend_active = false;
		nni_mtx_unlock(&s->mtx);
		return;
	}
	now = nni_clock();
	// Reap window slots past their survey deadline: pipes that never
	// acked them get MD treatment once, then the slot is freed. This
	// is what ends collection (bounds the window in time); recv
	// timeouts/cancels deliberately do not.
	for (int i = 0; i < SACK0_RING_MAX; i++) {
		uint32_t w;
		if (s->ring_seq[i] == 0) {
			continue;
		}
		if (now <= s->ring_expire[i]) {
			continue;
		}
		w = s->ring_wseq[i];
		NNI_LIST_FOREACH (&s->pipes, p) {
			if (w != 0 && sack0_pipe_acked(p, w)) {
				continue;
			}
			sack0_pipe_missed(s, p);
			sack0_pipe_reclock(s, p);
		}
		sack0_remove_slot(s, i);
	}
	NNI_LIST_FOREACH (&s->pipes, p) {
		nni_duration tmo;
		bool         did = false;
		bool         mdone = false;

		if (p->send_time == 0) {
			continue; // fully caught up
		}
		tmo = p->rtt_initialized ? p->rto : SACK0_RTO_INIT;
		if (now <= p->send_time ||
		    (now - p->send_time) <= (nni_time) tmo) {
			continue; // oldest hole not old enough yet
		}
		if (p->last_resend != 0 &&
		    (now - p->last_resend) <= (nni_time) tmo) {
			continue; // bounded: one resend pass per RTO
		}
		for (int i = 0; i < SACK0_RING_MAX; i++) {
			uint32_t w;
			nni_msg *slot;
			if (s->ring_seq[i] == 0 ||
			    (slot = s->ring[i]) == NULL) {
				continue;
			}
			w = s->ring_wseq[i];
			if (w != 0 && sack0_pipe_acked(p, w)) {
				continue;
			}
			if (now <= s->ring_time[i] ||
			    (now - s->ring_time[i]) <= (nni_time) tmo) {
				continue;
			}
			if (!p->busy) {
				p->busy = true;
				nni_msg_clone(slot);
				nni_aio_set_msg(&p->aio_send, slot);
				nni_pipe_send(p->pipe, &p->aio_send);
			} else if (!nni_lmq_full(&p->send_queue)) {
				nni_msg_clone(slot);
				nni_lmq_put(&p->send_queue, slot);
			} else {
				continue;
			}
			// Karn: the eventual cumulative covering this
			// slot is not sampled.
			s->ring_resent[i] = true;
			p->resent         = true;
			did               = true;
			// Persistent holes also decay (MD, once per pass) but
			// stay live for further resends.
			if (!mdone &&
			    (now - s->ring_time[i]) > (nni_time) (2 * tmo)) {
				sack0_pipe_missed(s, p);
				mdone = true;
			}
		}
		if (did) {
			p->last_resend = now;
			sack0_pipe_reclock(s, p);
		}
	}
	NNI_LIST_FOREACH (&s->pipes, p) {
		if (p->send_time != 0) {
			more = true;
			break;
		}
	}
	if (more) {
		nni_sleep_aio(SACK0_RESEND_TICK, &s->resend_aio);
	} else {
		s->resend_active = false;
	}
	nni_mtx_unlock(&s->mtx);
}

static void
sack0_pipe_send_cb(void *arg)
{
	sack0_pipe *p    = arg;
	sack0_sock *sock = p->sock;
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
sack0_pipe_recv_cb(void *arg)
{
	sack0_pipe *p    = arg;
	sack0_sock *sock = p->sock;
	sack0_ctx  *ctx;
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

	// Parse the cumulative ACK BEFORE delivery (delivery may hand the
	// message to a waiter or drop it). Body is "C<next>[:mask]".
	uint32_t cnext  = 0;
	uint32_t cmask  = 0;
	bool     is_cack = false;
	int      slotidx = -1;
	if (sock->adaptive) {
		is_cack =
		    sack0_parse_cack(nni_msg_body(msg), nni_msg_len(msg),
		        &cnext, &cmask);
	}

	nni_mtx_lock(&sock->mtx);
	// Best effort at delivery.  Discard if no context or context is
	// unable to receive it. The cumulative ACK inside is still honored
	// below: even a late/duplicate transport reply can advance holes
	// for newer window slots.
	if (((ctx = nni_id_get(&sock->surveys, id)) == NULL) ||
	    (nni_lmq_full(&ctx->recv_lmq))) {
		nni_msg_free(msg);
		msg = NULL;
	} else if ((aio = nni_list_first(&ctx->recv_queue)) != NULL) {
		nni_list_remove(&ctx->recv_queue, aio);
		nni_aio_finish_msg(aio, msg);
	} else {
		nni_lmq_put(&ctx->recv_lmq, msg);
		if (ctx == &sock->ctx) {
			nni_pollable_raise(&sock->readable);
		}
	}

	if (!sock->adaptive) {
		nni_mtx_unlock(&sock->mtx);
		nni_pipe_recv(p->pipe, &p->aio_recv);
		return;
	}

	// Adaptive cumulative ACK: advance this pipe's scoreboard, free
	// slots every pipe has acked, sample RTT off the oldest
	// newly-acked slot (Karn-aware, inside advance), and re-point the
	// pipe clock. A bare/legacy (non-C) reply still counts as a SACK
	// bit for its own survey so old subscribers degrade gracefully
	// instead of stalling the window.
	if (is_cack) {
		sack0_advance_pipe(sock, p, cnext, cmask);
	} else if ((slotidx = sack0_find_slot(sock, id)) >= 0 &&
	    sock->ring_wseq[slotidx] != 0) {
		uint32_t w = sock->ring_wseq[slotidx];
		if (p->ack_base == 0) {
			p->ack_base = w;
			p->ack_mask = 0;
		} else if (w >= p->ack_base &&
		    (w - p->ack_base) < SACK0_SACK_BITS) {
			p->ack_mask |= (1u << (w - p->ack_base));
		}
		sack0_pipe_reclock(sock, p);
	}
	sack0_collect_acked(sock);
	nni_mtx_unlock(&sock->mtx);

	nni_pipe_recv(p->pipe, &p->aio_recv);
}

static nng_err
sack0_ctx_set_survey_time(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	sack0_ctx   *ctx = arg;
	nng_duration expire;
	nng_err      rv;
	if ((rv = nni_copyin_ms(&expire, buf, sz, t)) == NNG_OK) {
		nni_atomic_set(&ctx->survey_time, expire);
	}
	return (rv);
}

static nng_err
sack0_ctx_get_survey_time(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_ctx *ctx = arg;
	return (
	    nni_copyout_ms(nni_atomic_get(&ctx->survey_time), buf, szp, t));
}

static nng_err
sack0_sock_set_max_ttl(void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	sack0_sock *s = arg;
	return (nni_copyin_int(&s->ttl, buf, sz, 1, NNI_MAX_MAX_TTL, t));
}

static nng_err
sack0_sock_get_max_ttl(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_sock *s = arg;
	return (nni_copyout_int(s->ttl, buf, szp, t));
}

static nng_err
sack0_sock_set_survey_time(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	sack0_sock *s = arg;
	return (sack0_ctx_set_survey_time(&s->ctx, buf, sz, t));
}

static nng_err
sack0_sock_get_survey_time(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_sock *s = arg;
	return (sack0_ctx_get_survey_time(&s->ctx, buf, szp, t));
}

static nng_err
sack0_sock_set_adaptive(
    void *arg, const void *buf, size_t sz, nni_opt_type t)
{
	sack0_sock *s = arg;
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
sack0_sock_get_adaptive(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_sock *s = arg;
	bool        v;

	nni_mtx_lock(&s->mtx);
	v = s->adaptive;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_bool(v, buf, szp, t));
}

static nng_err
sack0_sock_get_rto(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_sock  *s = arg;
	nng_duration rto;

	nni_mtx_lock(&s->mtx);
	rto = s->rto;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_ms(rto, buf, szp, t));
}

static nng_err
sack0_sock_get_cwnd(void *arg, void *buf, size_t *szp, nni_opt_type t)
{
	sack0_sock *s = arg;
	int         cwnd;

	nni_mtx_lock(&s->mtx);
	cwnd = (int) s->cwnd;
	nni_mtx_unlock(&s->mtx);
	return (nni_copyout_int(cwnd, buf, szp, t));
}

static nng_err
sack0_sock_get_send_fd(void *arg, int *fdp)
{
	sack0_sock *sock = arg;

	return (nni_pollable_getfd(&sock->writable, fdp));
}

static nng_err
sack0_sock_get_recv_fd(void *arg, int *fdp)
{
	sack0_sock *sock = arg;

	return (nni_pollable_getfd(&sock->readable, fdp));
}

static void
sack0_sock_recv(void *arg, nni_aio *aio)
{
	sack0_sock *s = arg;
	sack0_ctx_recv(&s->ctx, aio);
}

static void
sack0_sock_send(void *arg, nni_aio *aio)
{
	sack0_sock *s = arg;
	sack0_ctx_send(&s->ctx, aio);
}

static nni_proto_pipe_ops sack0_pipe_ops = {
	.pipe_size  = sizeof(sack0_pipe),
	.pipe_init  = sack0_pipe_init,
	.pipe_fini  = sack0_pipe_fini,
	.pipe_start = sack0_pipe_start,
	.pipe_close = sack0_pipe_close,
	.pipe_stop  = sack0_pipe_stop,
};

static nni_option sack0_ctx_options[] = {
	{
	    .o_name = NNG_OPT_SACK_SURVEYTIME,
	    .o_get  = sack0_ctx_get_survey_time,
	    .o_set  = sack0_ctx_set_survey_time,
	},
	{
	    .o_name = NULL,
	}
};
static nni_proto_ctx_ops sack0_ctx_ops = {
	.ctx_size    = sizeof(sack0_ctx),
	.ctx_init    = sack0_ctx_init,
	.ctx_fini    = sack0_ctx_fini,
	.ctx_send    = sack0_ctx_send,
	.ctx_recv    = sack0_ctx_recv,
	.ctx_options = sack0_ctx_options,
};

static nni_option sack0_sock_options[] = {
	{
	    .o_name = NNG_OPT_SACK_SURVEYTIME,
	    .o_get  = sack0_sock_get_survey_time,
	    .o_set  = sack0_sock_set_survey_time,
	},
	{
	    .o_name = NNG_OPT_MAXTTL,
	    .o_get  = sack0_sock_get_max_ttl,
	    .o_set  = sack0_sock_set_max_ttl,
	},
	{
	    .o_name = NNG_OPT_SACK_ADAPTIVE,
	    .o_get  = sack0_sock_get_adaptive,
	    .o_set  = sack0_sock_set_adaptive,
	},
	{
	    .o_name = NNG_OPT_SACK_RTO,
	    .o_get  = sack0_sock_get_rto,
	},
	{
	    .o_name = NNG_OPT_SACK_CWND,
	    .o_get  = sack0_sock_get_cwnd,
	},
	// terminate list
	{
	    .o_name = NULL,
	},
};

static nni_proto_sock_ops sack0_sock_ops = {
	.sock_size         = sizeof(sack0_sock),
	.sock_init         = sack0_sock_init,
	.sock_fini         = sack0_sock_fini,
	.sock_open         = sack0_sock_open,
	.sock_close        = sack0_sock_close,
	.sock_send         = sack0_sock_send,
	.sock_recv         = sack0_sock_recv,
	.sock_send_poll_fd = sack0_sock_get_send_fd,
	.sock_recv_poll_fd = sack0_sock_get_recv_fd,
	.sock_options      = sack0_sock_options,
};

static nni_proto sack0_proto = {
	.proto_self     = { SACK0_SELF, SACK0_SELF_NAME },
	.proto_peer     = { SACK0_PEER, SACK0_PEER_NAME },
	.proto_flags    = NNI_PROTO_FLAG_SNDRCV,
	.proto_sock_ops = &sack0_sock_ops,
	.proto_pipe_ops = &sack0_pipe_ops,
	.proto_ctx_ops  = &sack0_ctx_ops,
};

int
nng_sack0_open(nng_socket *sock)
{
	return (nni_proto_open(sock, &sack0_proto));
}
