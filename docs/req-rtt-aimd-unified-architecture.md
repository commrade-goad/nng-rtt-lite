# REQ0 Adaptive RTT/AIMD Unified Architecture

## Intent

This document combines phase-1 (`req0` adaptive timeout) and phase-2 (TLS + per-pipe RTT) into one implementation architecture.

Design target:

- keep adaptation in `req0` so TLS-backed REQ benefits immediately,
- move RTT ownership to pipe-level where possible so behavior is not pinned to a single client/socket-wide latency profile,
- preserve compatibility with current REQ options and semantics.

Primary implementation file: `src/sp/protocol/reqrep0/req.c`

---

## Why this combined model

### 1) Keep control in `req0` (protocol layer)

`req0` already owns request lifecycle:

- request id assignment,
- retry scheduling,
- send queue placement,
- reply matching.

Because TLS and TCP stream transports both feed through this lifecycle, protocol-layer RTT adaptation applies to both without transport-specific congestion code.

### 2) Keep estimates near the path (`req0_pipe`)

Socket-only RTT is easy, but can be wrong when different peers have different latency/loss. Per-pipe state avoids one slow or one fast peer biasing everyone else.

This is the core of your requirement: adaptation should not be pinned to the client/global socket only.

### 3) Use hybrid fallback

Some contexts will not have reliable pipe affinity (pipe closed, failover, first send). Keep a socket-level baseline as safe fallback.

Net result:

- protocol-managed adaptation,
- pipe-accurate where possible,
- robust fallback everywhere.

---

## Architecture Summary

Two estimator scopes exist concurrently:

1. **Socket baseline estimator** (`req0_sock`)
   - always available,
   - used for bootstrap and fallback.

2. **Pipe estimator** (`req0_pipe`)
   - preferred for requests that can be attributed to a specific pipe,
   - updated from request/reply RTT on that same pipe.

3. **Congestion window** (sliding window)
   - limits number of in-flight REQ requests per scope,
   - uses AIMD from the same RTT/timeout signals,
   - prevents send bursts on slow links (the key missing piece from RTO-only).

The retry engine (`req0_retry_cb`) chooses timeout by precedence:

1. per-context explicit policy (if any),
2. last-send pipe estimator (if valid and initialized),
3. socket baseline estimator.

---

## Data Model (Proposed)

### `req0_ctx` additions

- `nni_time send_time;`
- `req0_pipe *last_send_pipe;` (or pipe id if pointer safety concerns)
- `bool rtt_sample_eligible;`
- `bool retransmitted;` (Karn's rule: do not sample RTT on retransmit)

Purpose:

- capture send timestamp,
- bind reply to the correct estimator,
- prevent invalid samples after failover or resend ambiguity.

### `req0_pipe` additions

- `nni_duration srtt;`
- `nni_duration rttvar;`
- `nni_duration rto;`
- `bool rtt_initialized;`
- `uint32_t samples;`
- `nni_time last_update;`
- `uint32_t cwnd;`
- `uint32_t inflight;`

Purpose:

- independent adaptive timeout per path/peer.

### `req0_sock` additions

- baseline estimator fields (`srtt`, `rttvar`, `rto`, `rtt_initialized`)
- socket-level congestion window fallback:
  - `uint32_t cwnd;`
  - `uint32_t inflight;`
- scope policy:
  - `socket` (default)
  - `pipe`
  - `auto`

Purpose:

- bootstrap/fallback estimator,
- rollout safety and compatibility.

---

## Algorithm Details

## Constants (initial)

- `RTO_INIT = 3000ms`
- `RTO_MIN = 200ms`
- `RTO_MAX = 60000ms`
- `AI_STEP = +100ms`
- `MD_FACTOR = 0.5`

Additional CC constants:

- `CWND_INIT = 1` request
- `CWND_MIN = 1` request
- `CWND_MAX = 1024` requests (configurable)
- `CWND_AI = +1 per RTT` (simple integer AIMD)

### Estimator update (success)

Given sample `rtt = now - send_time`:

1. If first sample:
   - `srtt = rtt`
   - `rttvar = max(1, rtt / 2)`
2. Else:
   - `err = rtt - srtt`
   - `srtt = srtt + err / 8`
   - `rttvar = rttvar + (abs(err) - rttvar) / 4`
3. AIMD additive bias:
   - `srtt = srtt + AI_STEP`
4. Compute:
   - `rto = srtt + 4 * rttvar`
5. Clamp `rto` to `[RTO_MIN, RTO_MAX]`

### Estimator update (timeout)

On retry expiration event for estimator `E`:

1. `E.srtt = max(RTO_MIN, E.srtt * MD_FACTOR)`
2. Optionally: `E.rttvar = max(1, E.rttvar * MD_FACTOR)`
3. `E.rto = clamp(E.srtt + 4 * E.rttvar, RTO_MIN, RTO_MAX)`

This follows your requested AIMD behavior.

---

## Congestion Control (Sliding Window)

### Why RTO-only is not enough

Changing resend timeout reduces duplicate retries, but does not slow new sends.
If the application creates many outstanding requests, the network can still be overloaded.
The sliding window caps concurrency and is the real congestion-control lever.

### Window semantics

- Each first-send of a request consumes one window slot.
- A request remains in-flight until its reply arrives or the context is reset.
- Retransmissions do not consume additional slots (they are part of the same in-flight request).

### AIMD for window size

On successful reply (no retransmit for that request):

- `cwnd = min(CWND_MAX, cwnd + 1)`

On timeout (retry event):

- `cwnd = max(CWND_MIN, cwnd / 2)`

This uses the same success/timeout signals as RTT estimation.

### Scope options

- **Per-pipe cwnd** (preferred): avoids fast/slow peer interference.
- **Socket cwnd** (fallback): used for initial bootstrap or when pipe affinity is unknown.

---

## Flow Integration in `req0`

### Send path (`req0_run_send_queue` / send callback)

- choose outbound pipe (existing scheduling),
- check window availability:
  - if `pipe` scope: allow first-send only if `p->inflight < p->cwnd`
  - if `socket` scope: allow first-send only if `s->inflight < s->cwnd`
  - retransmits bypass window check (already counted)
- set:
  - `ctx->send_time = nni_clock()` for first-send only
  - `ctx->last_send_pipe = selected_pipe`
  - `ctx->rtt_sample_eligible = true` only if no retransmit
  - `ctx->retransmitted = false` on first-send; set true on retry
- increment inflight counter for first-send
- set `ctx->retry_time` using effective RTO policy.

### Receive path (`req0_recv_cb`)

After matching reply to context id:

- validate sample eligibility,
- confirm attribution (reply pipe matches `last_send_pipe` when required),
- compute RTT sample,
- update selected estimator:
  - pipe estimator if valid,
  - else socket baseline.
- if no retransmit was used for this request, apply CWND AI (`+1`).
- decrement inflight counter.

### Retry path (`req0_retry_cb`)

When timeout fires for context:

- apply MD to estimator that owned the send,
- apply CWND MD (`/2`) on the same scope,
- recompute next deadline from updated effective RTO,
- mark `ctx->retransmitted = true`,
- requeue context to send queue.

### Pipe close path (`req0_pipe_close`)

- invalidate `ctx->last_send_pipe` for contexts tied to closed pipe,
- decrement inflight counters for those contexts,
- disable sample attribution for those in-flight contexts,
- fallback to socket estimator for next retry.

---

## TLS-Specific Considerations

No transport code changes are required for baseline benefit, but correctness guards are needed:

1. **Handshake contamination guard**
   - first sample after new TLS pipe can include setup jitter,
   - use conservative acceptance: do not let first sample drop `rto` below baseline floor window.

2. **Session resumption variance**
   - resumed sessions can produce very fast first replies,
   - require minimum sample count before aggressive shrink.

3. **Reconnect churn**
   - reset per-pipe estimator on pipe recreation,
   - keep socket estimator as continuity baseline.

---

## Scope Policy (`socket|pipe|auto`)

### `socket` mode

- all updates go to socket estimator,
- safest for compatibility and initial rollout.

### `pipe` mode

- updates/retries prefer per-pipe estimators,
- best for heterogeneous peer latency.

### `auto` mode

- start in socket mode,
- switch to per-pipe behavior when heterogeneity is detected.

Recommended auto trigger:

- at least 3 active pipes with >= N samples each,
- and `max(rto_pipe) / min(rto_pipe) >= 3.0` over a sustained observation window.

---

## Option and Compatibility Contract

Preserve existing options:

- `NNG_OPT_REQ_RESENDTIME`
- `NNG_OPT_REQ_RESENDTICK`

Proposed precedence:

1. `RESENDTIME == 0` keeps no-retry behavior.
2. If `RESENDTIME > 0`, treat as configurable cap/floor policy (document exact rule).
3. Adaptive estimator still runs unless explicitly disabled by a future option.

Reason:

- avoids breaking current applications,
- still enables adaptive gains in slow networks.

---

## Concurrency and Safety Invariants

All estimator reads/writes stay under existing `s->mtx`.

Invariants:

- `ctx->last_send_pipe` must be validated before dereference.
- no RTT update from unmatched/malformed replies.
- no negative/overflow duration math.
- estimator clamped at every write.

---

## Implementation Sequence (Recommended)

1. Add socket baseline estimator only (`socket` mode default).
2. Add per-pipe fields + attribution plumbing (`last_send_pipe`, `send_time`).
3. Add `pipe` mode and retry selection by affinity.
4. Add `auto` mode with heterogeneity detection.
5. Add TLS-focused regression tests and mixed-peer stress tests.

This sequence keeps the system always shippable and debuggable.

---

## Test Strategy (Deep)

### Functional

1. single peer TCP: adaptive retry shrinks/grows correctly.
2. single peer TLS: same behavior as TCP within expected jitter.
3. retry disabled (`RESENDTIME=0`) unchanged.
4. explicit resend option still honored.

### Heterogeneous peers

1. one fast REP, one slow REP on same REQ socket.
2. socket mode shows shared compromise timeout.
3. pipe mode converges distinct timeouts.
4. auto mode transitions only when threshold sustained.

### Failure/recovery

1. pipe closes mid-flight: fallback estimator used safely.
2. repeated timeout bursts: MD bounded at min.
3. delayed duplicate replies: ignored or safely handled.

### Performance/soak

1. lock contention under high context count.
2. retry storm prevention under induced latency/loss.
3. long-run drift: no runaway `rto` or stuck min-floor.

---

## Observability (Recommended)

Add internal debug counters (or tracepoints) for:

- RTT samples accepted/rejected,
- retries by estimator scope,
- current socket RTO and per-pipe RTO snapshots,
- auto-mode promotion/demotion events.

These metrics are important to validate "not pinned to client" behavior.

---

## Risks and Mitigations

1. **Wrong pipe attribution**
   - Mitigation: strict affinity validation + conservative fallback.
2. **Handshake-biased first sample (TLS)**
   - Mitigation: first-sample clamp and minimum-sample gate.
3. **Over-aggressive MD in unstable links**
   - Mitigation: floor clamp and optional cooldown before repeated MD.
4. **Behavioral surprise for existing users**
   - Mitigation: default `socket` mode first; document option precedence.

---

## Final Recommendation

Implement adaptation in `req0` (so TLS gets benefit immediately), but evolve to per-pipe estimator ownership as the primary steady-state model.

This gives the best of both worlds:

- protocol-level reuse across TCP/TLS,
- path-aware retry control,
- no global client pinning for mixed-latency peers.
