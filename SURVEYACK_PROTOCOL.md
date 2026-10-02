# SURVEYACK Protocol (cumulative-ACK RTT-lite broadcast)

Cloned from `survey0`, keeps RTT-lite AIMD, replaces 1-ACK-per-survey
with a window of 8 + one cumulative ACK per ~8 surveys.

## Why (plain version)

Before, per sub, per message:

```text
pub sends seq=1 -> sub replies "ACK"
pub sends seq=2 -> sub replies "ACK"   ... 10 msgs = 10 ACKs
```

Now the publisher sends 8 without stopping, the sub answers once:

```text
pub sends 1..8 (no waiting)
sub got all     -> replies "C9"       (= "next I need is 9")
sub missed 6    -> replies "C9" later / "C6:mask" now -> pub resends only 6
```

`C9` is 2 bytes. 10 msgs cost ~2 uplink ACKs instead of 10.

## The one #define

`src/sp/protocol/surveyack0/sack.c`:

```c
#define SACK0_RING_MAX  8   // window + repair buffer. Change ONLY here.
#define SACK0_ACK_BATCH SACK0_RING_MAX  // wrapper batch follows it
#define SACK0_SACK_BITS 16  // SACK mask horizon above base
#define SACK0_RTO_INIT 3000 / MIN 200 / MAX 60000 / AI_STEP 100
#define SACK0_CWND_INIT 1 / MIN 1 / MAX 1024
#define SACK0_RESEND_TICK 100
#define SACK0_FANOUT_WAIT_MS 20 // max pacing wait per send (no-HOL bound)
#define SACK0_SEND_BUF 32       // per-pipe transport queue depth
```

Wrapper batch/delay (`nng_wrapper.h`): `NNG_SACK_BATCH 8`,
`NNG_SACK_ACK_DELAY_MS 50` (tail flush bound while blocked in recv).

## Wire format

Data (unchanged from survey mode, string prefix in BODY):

```text
pub app:  "171111|PAYLOAD"          (benchmark ts|payload)
wire:     "7|171111|PAYLOAD"        (wrapper seq 7)
sub app:  "171111|PAYLOAD"          (prefix stripped, deliver-once)
```

ACK (new, reply body; transport survey id still routes it):

```text
"C9"      = everything below 9 done (cumulative)
"C6:2"    = need 6, SACK has 7 (mask bit1: have 6+1). LSB = seq <next>.
```

Mask bit `i` = have `next+i`. Size 2-10 bytes, same as bare `"ACK"`.

## Files (all new code is a survey0 clone + window logic)

```text
src/sp/protocol/surveyack0/sack.c       # sack side (window, scoreboard, resend, AIMD)
src/sp/protocol/surveyack0/sack_resp.c   # sackresp side (rename-only; batching is wrapper-side)
src/sp/protocol/surveyack0/sack_test.c   # 4 tests
src/sp/protocol/surveyack0/CMakeLists.txt
include/nng/nng.h                        # nng_sack0_open/nng_sackresp0_open, NNG_OPT_SACK_*
cmake/NNGOptions.cmake                   # NNG_PROTO_SACK0 / NNG_PROTO_SACKRESP0
nng_wrapper.h                            # NNG_PUBSUB_SACK mode (wins if mixed with others)
```

Proto IDs: `SACK0_SELF 0x64` / `SACK0_PEER 0x65`,
`NNI_PROTO_SACK_V0 NNI_PROTO(6,4)`, `NNI_PROTO_SACKRESP_V0 NNI_PROTO(6,5)`.
Old `survey0` untouched, all its tests still pass.

## How it works (per piece)

- **Send (`sack0_ctx_send`)**: no abort. Allocates a new transport id per
  survey, keeps up to 8 live. Full window evicts oldest (MD only if that
  slot is genuinely older than the pipe's RTO, so healthy streaming never
  punishes). Parses `<wseq>|` into `ring_wseq[]`.
- **Fanout pacing (blast survival)**: a saturated pipe used to be
  silently skipped, and the skip became permanent once the window slid
  past it (measured: ~79/1000 dropped on a TCP loopback blast). Now a
  caught-up pipe gets a bounded wait (1 ms slices, ≤20 ms) for drain
  before a skip; pipes that proved slow (skipped <1 s ago) are never
  waited on, so one dead pipe can't tax every send. Queue depth is 32.
  Verified: 1000/1000 x3 on the same blast (plain `std` gets 999/1000).
- **Scoreboard (per pipe)**: `ack_base` (next seq owed) + 16-bit `ack_mask`.
  `sack0_advance_pipe()` grows it, samples RTT off the oldest newly-acked
  slot (skipped if any covered slot was resent: Karn), `cwnd++` (AI).
- **Freeing**: `sack0_collect_acked()` frees slots every pipe acked.
  Unacked past deadline are reaped by the resend timer (MD once, free).
  Recv timeouts/cancels end only that wait, never the window (a NONBLOCK
  probe on an empty-but-live window used to nuke it; fixed).
- **Resend (`sack0_resend_cb`)**: per-pipe holes only (skips SACK bits),
  ~1 pass per RTO per pipe (`last_resend` gate), holes past 2xRTO also
  decay (MD, once per pass) but stay resendable.
- **Recv (`sack0_pipe_recv_cb`)**: parses `C` before delivery, advances
  even on id-miss (late/dup transport reply still closes newer holes).
  Bare non-`C` replies count as one SACK bit (old peers degrade, no stall).
- **Drainable**: buffered `C`s stay receivable after the window closes
  (recv checks the queue before the ESTATE gate), so the pub can pump
  progress after the fact.
- **Wrapper sub**: deliver-once + strip (same as survey), flush `C` every
  8 (`NNG_SACK_BATCH`), immediate flush on duplicate (fast ACK), 50 ms
  sliced blocking recv so partial tails flush; `nng_sack_flush(sock)`
  for explicit end-of-stream flush.
- **Wrapper pub**: `nng_send_sack` pipelines (no quorum wait);
  `nng_sack_sync(sock, target, timeout_ms)` drains `C`s, returns highest
  `next` seen (0 = none).

## Use it

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
- Benchmark smoke: `40/40` msgs, 2 subs, inproc, SACK binary.

## Limits (same family as before, documented)

- Horizon 8: a hole unacked past eviction+deadline is dropped with one MD.
  Late joiners get replay of unacked slots via resend (their base is 0).
- Window slides without waiting for the slowest; laggards get hole
  resends + MD, not head-of-line blocking.
- One thread per sub socket assumed (wrapper slices RECVTIMEO).
