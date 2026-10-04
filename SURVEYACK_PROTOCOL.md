# SURVEYACK Protocol (cumulative-ACK RTT-lite broadcast)

Plain version: one publisher sends to many subscribers. Instead of
every subscriber answering every message (expensive on the way back),
the publisher sends up to 8 messages without stopping, and each
subscriber answers roughly once per 8 messages with "everything up to
here is done, except these holes". Lost holes are re-sent one by one.

Technically: cloned from `survey0`, keeps the RTT-lite AIMD estimator
(RTT sampling + congestion window), but replaces 1-ACK-per-survey
with a window of up to 8 live surveys plus one cumulative ACK per
~8 surveys.

## Why (plain version)

Old way — one answer per message, per subscriber:

```text
pub sends seq=1 -> sub replies "ACK"
pub sends seq=2 -> sub replies "ACK"   ... 10 msgs = 10 ACKs
```

New way — the publisher sends a batch without stopping, the sub
answers once for the whole batch:

```text
pub sends 1..8 (gated by cwnd, see below)
sub got all     -> replies "C9"       (= "next I need is 9")
sub missed 6    -> replies "C6:mask" now, "C9" later -> pub resends only 6
```

`C9` is 2 bytes. 10 messages cost ~2 uplink ACKs instead of 10.
The saving matters on asymmetric links (Wi-Fi uplink) and grows with
the subscriber count, because every subscriber ACKs independently.

A second, quieter win: the publisher no longer waits for each reply
before sending the next message. Throughput is governed by the
congestion window, not by one round trip per message.

## The knobs (`src/sp/protocol/surveyack0/sack.c`)

One `#define` block controls everything. Plain meaning first,
exact semantics after.

```c
#define SACK0_RING_MAX  8   // window + repair buffer. Change ONLY here.
#define SACK0_ACK_BATCH SACK0_RING_MAX  // wrapper batch follows it
#define SACK0_SACK_BITS 16  // SACK mask horizon above base
#define SACK0_RTO_INIT 3000 / MIN 200 / MAX 60000 / AI_STEP 100
#define SACK0_CWND_INIT 1 / MIN 1 / MAX 1024
#define SACK0_RESEND_TICK 100
#define SACK0_FANOUT_WAIT_MS 200   // max pacing wait per send (no-HOL bound)
#define SACK0_SEND_BUF 128         // per-pipe transport queue depth
```

| Knob                | Default           | Plain meaning                                                                                        | Technical meaning                                               |
|---------------------|-------------------|------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------|
| `RING_MAX`          | 8                 | How many messages may be "in the air" at once.                                                       | Live window slots + repair ring size; `ACK_BATCH` follows it.   |
| `SACK_BITS`         | 16                | How many holes one ACK can describe.                                                                 | Bitmask width above the cumulative base.                        |
| `RTO_INIT/MIN/MAX`  | 3000/200/60000 ms | How long to wait before calling a message lost (starts careful, never below 200, never above 60000). | RFC 6298-style retransmission timeout bounds.                   |
| `AI_STEP`           | 100 ms            | Safety margin so normal wobble is not mistaken for congestion.                                       | Added to the RTO computation only, never accumulated into SRTT. |
| `CWND_INIT/MIN/MAX` | 1/1/1024          | Start with 1 message in flight; grow or shrink from there; never exceed 1024.                        | AIMD congestion window bounds.                                  |
| `RESEND_TICK`       | 100 ms            | How often to check for lost holes.                                                                   | Resend timer scan period.                                       |
| `FANOUT_WAIT_MS`    | 200 ms            | How long to wait for a clogged pipe before skipping it.                                              | Bounded pacing wait per send (10 ms if only one pipe is slow).  |
| `SEND_BUF`          | 128               | Per-connection waiting room.                                                                         | Per-pipe transport queue depth.                                 |

Wrapper batch/delay (`nng_wrapper.h`): `NNG_SACK_BATCH 8` (one
cumulative per 8 deliveries), `NNG_SACK_ACK_DELAY_MS 50` (a partial
batch is flushed at most 50 ms after it stopped growing, even while
the app is blocked in recv).

## Wire format

Data looks the same as survey mode, with a number stamped on the
front of the message body (so cooked sockets work unchanged):

```text
pub app:  "171111|PAYLOAD"          (benchmark timestamp|payload)
wire:     "7|171111|PAYLOAD"        (wrapper seq 7 prepended)
sub app:  "171111|PAYLOAD"          (prefix stripped, deliver-once:
                                     each number delivered exactly once)
```

ACKs are new. They travel in the reply body (the transport survey id
still routes them to the right conversation):

```text
"C9"      = everything below 9 done (cumulative)
"C6:2"    = need 6, SACK has 7 (mask bit1: have 6+1). LSB = seq <next>.
```

Plain version: `C9` means "I have 1 through 8, send 9 next".
`C6:2` means "I'm still missing 6, but I already have 7". Mask bit
`i` = "I have `next+i`". Size 2-10 bytes — about the same as a bare
`"ACK"`, but one of them can close up to 8 messages plus 16 holes.

## Files (all new code is a survey0 clone + window logic)

```text
src/sp/protocol/surveyack0/sack.c       # publisher side: window, per-pipe
                                        # scoreboards, resend timer, AIMD
src/sp/protocol/surveyack0/sack_resp.c  # subscriber side (mostly a rename;
                                        # batching lives in the wrapper)
src/sp/protocol/surveyack0/sack_test.c  # 4 unit tests
src/sp/protocol/surveyack0/CMakeLists.txt
include/nng/nng.h                       # nng_sack0_open/nng_sackresp0_open,
                                        # NNG_OPT_SACK_* knobs + observability
cmake/NNGOptions.cmake                  # NNG_PROTO_SACK0 / NNG_PROTO_SACKRESP0
nng_wrapper.h                           # NNG_PUBSUB_SACK mode (wins if mixed
                                        # with the survey/reliable modes)
```

Protocol IDs: `SACK0_SELF 0x64` / `SACK0_PEER 0x65`
(`NNI_PROTO_SACK_V0 NNI_PROTO(6,4)`, `NNI_PROTO_SACKRESP_V0
NNI_PROTO(6,5)`). The old `survey0` is untouched and all its tests
still pass.

## How it works (per piece: plain first, exact second)

- **Send (`sack0_ctx_send`)**.
  Plain: sending never cancels what is already in flight. Every send
  gets a fresh transport id and joins the window.
  Exact: allocates a new transport id per survey, keeps up to
  `SACK0_RING_MAX` live. A full window evicts the oldest slot (MD only
  if that slot is genuinely older than the pipe's RTO, so healthy
  streaming is never punished). Parses `<wseq>|` into `ring_wseq[]`.

- **Live-window gating (the RTT-lite part).**
  Plain: the publisher may only have `cwnd` messages unanswered at a
  time. On a clean link the allowance grows 1, 2, 3… up to 8; on a
  bad link it collapses back toward 1 and the sender automatically
  slows down. This is what turns "send as fast as possible" into
  congestion control.
  Exact: the adaptive path caps live slots at `min(sock->cwnd,
  SACK0_RING_MAX)`. If `live_count() >= live_limit`, the sender
  blocks in 1 ms slices until a slot frees or `survey_time` elapses,
  then force-evicts the oldest. `cwnd` rises by AI in
  `sack0_advance_pipe()` and is halved by MD on misses. Enough
  allowance survives to avoid blast-buffering on real workloads;
  bad links slow down instead of flooding.

- **Burst pacing (blast survival).**
  Plain: if a pipe's queue is momentarily full, wait a bounded moment
  for it to drain instead of silently dropping the message — because a
  skipped message can never be recovered once the window slides past
  it. But never let one dead pipe hold everyone hostage.
  Exact: after the cwnd gate, wait up to `SACK0_FANOUT_WAIT_MS` (200
  ms) when every pipe is saturated, only up to 10 ms when a single
  pipe is slow. Pipes skipped less than 1 s ago (`last_skip`) are
  never waited on. Queue depth is `SACK0_SEND_BUF` (128).

- **Scoreboard (per pipe).**
  Plain: each connection remembers "which message numbers this peer
  still owes me": the next needed number plus which of the following
  16 already arrived out of order.
  Exact: `ack_base` (next seq owed) + 16-bit `ack_mask`.
  `sack0_advance_pipe()` advances it, samples RTT off the oldest
  newly-acked slot (skipped if any covered slot was resent: Karn's
  rule — never sample a measurement you can't attribute), `cwnd++`
  (AI).

- **Freeing.**
  Plain: a message leaves the publisher's memory once every subscriber
  has confirmed it. Messages nobody confirms by their deadline are
  written off (with one penalty) and freed. A receive timing out ends
  only that one wait, never the whole window.
  Exact: `sack0_collect_acked()` frees slots every pipe acked.
  Unacked slots past deadline are reaped by the resend timer (MD
  once, free). Recv timeouts/cancels end only that wait, never the
  window (a NONBLOCK probe on an empty-but-live window used to nuke
  it; fixed). MD is also applied by `sack0_evict_oldest()` when a
  window slot is forced out while any lagging pipe is older than its
  RTO.

- **Resend (`sack0_resend_cb`, every 100 ms).**
  Plain: only the actually-missing pieces are re-sent, roughly once
  per timeout period per connection. Confirmed pieces are never
  re-sent.
  Exact: per-pipe holes only (SACK-acked bits skipped), ~1 pass per
  RTO per pipe (`last_resend` gate); holes older than 2xRTO also decay
  (MD, once per pass) but stay resendable.

- **Recv (`sack0_pipe_recv_cb`).**
  Plain: answers are understood before anything is delivered, and even
  a late or duplicate answer still helps close newer holes.
  Exact: parses `C` before delivery, advances even on id-miss
  (late/dup transport reply still closes newer holes). Bare non-`C`
  replies count as one SACK bit (old peers degrade gracefully, no
  stall).

- **Drainable.**
  Plain: answers that arrived but weren't collected yet stay
  available even after their window closed, so the publisher can make
  progress from them after the fact.
  Exact: buffered `C`s stay receivable after the window closes (recv
  checks the queue before the ESTATE gate).

- **Wrapper sub (subscriber side batching).**
  Plain: same deliver-once + prefix stripping as survey mode. Answers
  go out every 8 messages, immediately on seeing a duplicate
  (fast-ACK: "the publisher is asking again, answer now"), a partial
  batch never waits more than 50 ms — and, since the flush-before-block
  fix, never waits at all when the publisher is starving: before every
  blocking wait the subscriber first peeks without blocking, and if
  nothing is ready yet but answers are pending, it flushes them right
  away. Streaming is unaffected (when messages keep arriving the peek
  always finds one, so no extra flush happens).
  Exact: flush `C` every 8 (`NNG_SACK_BATCH`), immediate flush on
  duplicate, 50 ms sliced blocking recv so partial tails flush, plus a
  one-shot nonblocking probe at the top of every blocking
  `nng_recvmsg_sack`/`nng_recv_sack` call (blocking mode only; the
  nonblocking path is unchanged) that flushes pending once on a miss;
  `nng_sack_flush(sock)` for explicit end-of-stream flush. Every flush
  records its `nng_send` result into the `nng_sack_flush_sent` /
  `nng_sack_flush_failed` counters, so "C never sent" vs "C sent but
  lost" is distinguishable after a run.

- **Wrapper pub (publisher side pipelining).**
  Plain: sending returns once the message is queued — it never waits
  for answers. A separate call drains the arrived answers.
  Exact: `nng_send_sack` pipelines; `nng_sack_sync(sock, target,
  timeout_ms)` drains `C`s and returns the highest `next` seen
  (0 = none).

- **Teardown contract (who waits for what on close).**
  Plain: the publisher hangs around briefly to collect the last
  answers; the subscriber fires off any answers it still holds, then
  both go home. Neither ever waits on the other's schedule.
  Exact: publisher sockets are registered at open
  (`nng_sack_pubs` table, same pattern as the dedup table);
  `nng_sack_sync(target = last seq, 2000 ms)` drains tail `C`s and
  returns early as soon as they arrive. Subscriber sockets instead
  call `nng_sack_flush` once (no-op when nothing was received), so
  tail slots get acked instead of expiring — and neither side burns
  the full 2 s when there is nothing to wait for. (The pub/sub branch
  itself lives in the benchmark's socket destroy; the registry, the
  sync, and the flush are wrapper-level.)

## Why the deadline is 2000 ms (survey_time)

Plain version: 2000 ms is the longest the publisher ever waits for one
message's paperwork to finish. Think of it as a patience limit. Every
survey gets at most 2 seconds to be acknowledged, re-sent, or
accounted for — then the books are closed on it (with one MD penalty)
and the stream moves on. Nothing ever waits forever.

Technically one number bounds three things (all in `sack.c` /
`nng_wrapper.h`, default `NNG_SACK_TIME_MS=2000`, overridable via env):

1. **Slot lifetime.** `ring_expire[slot] = now + min(RTO, survey_time)`.
   A live window slot that nobody acks by then is reaped by the resend
   timer: lagging pipes take one MD and the slot is freed.
2. **Sender gate wait.** `sack0_ctx_send` blocks (1 ms slices) until a
   live slot frees or `survey_time` elapses; past that it force-evicts
   the oldest slot so the stream always makes progress.
3. **Collection horizon.** The window as a whole can never outlive its
   newest slot's expiry, so a dead subscriber stalls the publisher by
   at most one budget per message, never permanently.

Why not smaller (e.g. 500 ms)? Under loss, legitimate completion takes
multiple RTO cycles. Example from our own runs (50 ms delay + 2% loss):
round trip ~100-150 ms, trained RTO several hundred ms — one loss plus
one backoff easily exceeds 500 ms while the message is still
recoverable. Evicting at 500 ms would drop recoverable messages, spray
MDs, pin `cwnd` at 1, and collapse throughput. The sudut-berat runs
(500/500 delivered, avg ~97 ms) need that headroom.

Why not larger (e.g. 10 s)? The worst-case sender stall per message
*is* the full budget — we measured ~2020 ms outliers in IPC jitter
when one cumulative ACK went missing before RTO trained. A bigger
budget makes each such event cost more, holds 8 slots × payload in
memory longer, and multiplies benchmark wall time (500 msgs × budget).

Ordering invariant to preserve when tuning: gate budget (>=
`survey_time`) >= slot expiry (= `min(RTO, survey_time)`) >= resend
scan (100 ms tick, first resend only after one RTO). If the budget is
shorter than the expiry, the gate evicts slots that are still
legitimately alive — drops without ever giving resend a chance.

Known wrinkle (documented, not hidden): at startup `RTO_INIT` (3000 ms)
exceeds the 2000 ms budget, so before the estimator trains, a lost
cumulative ACK cannot be rescued by resend in time — the gate eats the
full budget. This is deliberate conservatism against spurious resends;
it shows up as rare ~2 s outliers in ideal-condition jitter and
vanishes once RTO trains (see IPC `max_latency` distribution).

Tuning cheat sheet (change, then rerun at least IPC + one lossy corner):

| Symptom | Knob | Direction |
|---|---|---|
| Tail stalls dominate ideal jitter | gate budget / `NNG_SACK_TIME_MS` | down, but verify loss corners still 100% |
| Drops under loss | budget, or `RTO_MIN` (200 ms floor) | up |
| Spurious resends on Wi-Fi | `RTO_INIT` / AI margin | up (more patience) |
| Benchmark too slow | message count, not the deadline | keeps semantics comparable |

## Use it

Plain version: build the benchmark with the SACK switch, point the
subscriber(s) at the publisher, and go. The deadline can be overridden
per run without rebuilding. The RTO and cwnd can be watched live.

```bash
# benchmark (bml/Makefile already has the target)
make sack
NNG_SACK_TIME_MS=2000 ./bml-pub-sub nng --sub --sub --pub --count 1000 ...

# raw API
nng_sack0_open(&pub); nng_sackresp0_open(&sub);
nng_socket_set_bool(pub, NNG_OPT_SACK_ADAPTIVE, true);
nng_socket_set_ms(pub, NNG_OPT_SACK_SURVEYTIME, 2000);
# observe: NNG_OPT_SACK_RTO / NNG_OPT_SACK_CWND
```

Tail at stream end: sub calls `nng_sack_flush(sub)` after its last recv
(or rely on resend-driven dup flush, bounded by RTO).

## Verified

- `ctest -R "sack|survey|respond"`: 5/5 pass (4 new + all old survey tests).
- Lib + benchmark build with zero new warnings.
- E2E (inproc, wrapper): 10 pipelined sends -> 10 intact in-order recvs,
  2 uplink ACKs (`C9`, `C11`), `cwnd` 1->3, RTO trains.
  Plain version: ten messages in, ten identical messages out, in the
  same order — and only two tiny answers came back up the wire while
  the congestion window grew from 1 to 3 and the timeout learned the
  network.
- Benchmark smoke: `40/40` msgs, 2 subs, inproc, SACK binary.
- Inproc gating smoke (2 sub, 2000x1000, after flush-before-block):
  std ~14.5k msg/s vs SACK ~15.0k msg/s, SACK avg ~31us, max 1.18 ms,
  jitter ~9us — the 51 ms startup stalls are gone, so on a clean link
  the gate opens almost immediately and throughput matches plain
  pub/sub. Control still bites under impairment (see transient check
  below); the gate bounds the worst case, it does not tax the best.
  Plain version: fixed the waiting, kept the safety net.
- Transient check (2000 msgs, paced 200/s, 50 ms + 1% loss injected
  mid-run): both deliver 2000/2000; std avg ~31 ms vs SACK avg ~10 ms
  at 195 vs 138 msg/s — SACK trades rate for latency under a real
  impairment, not just on paper.

## Limits (same family as before, documented)

- Horizon 8: a hole unacked past eviction+deadline is dropped with one
  MD. Late joiners get replay of unacked slots via resend (their base
  is 0).
  Plain version: the publisher only remembers the last 8 messages. A
  subscriber that shows up late gets the unconfirmed ones replayed;
  anything older than that is gone, with one penalty recorded.
- Window still slides without waiting for the slowest; laggards get hole
  resends + MD, not head-of-line blocking. No strict quorum/deadline
  abort (that semantics lives in survey0), so asymmetric loss tests show
  no sudden death of the publisher; throughput degrades instead.
  Plain version: one slow subscriber slows everyone down a bit but can
  never freeze anyone — the stream bends instead of breaking.
- The adaptiveness relies on trained RTO/cwnd; during the training phase
  (first few sends, RTO still at 3000 ms) the fanout can look like raw
  TCP pacing. Once RTT samples arrive, the live-window shrinks to cwnd
  and latency lands on the expected burst/normal boundary.
  Plain version: the first few messages fly a bit blind while the
  protocol is still learning the network; after a handful of answers it
  knows the tempo and behaves.
- One thread per sub socket assumed (wrapper slices RECVTIMEO).
  Plain version: don't share one subscriber socket between threads;
  the batching state is per socket.
