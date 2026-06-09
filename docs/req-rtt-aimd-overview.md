# REQ0 Adaptive RTT/AIMD: Full Overview for Non-Experts

This document explains the changes in commit `2d98679` in plain language with lots of detail. It covers:

- what the changes are,
- why they are needed,
- how they work step by step,
- how they improve the library,
- and realistic runtime scenarios with numbers.

If you do not know the codebase or networking terms, this guide aims to make it understandable.

Primary implementation file: `src/sp/protocol/reqrep0/req.c`

## Quick Summary (Plain Language)

REQ sockets send requests and wait for replies. In the old behavior, the retry timer was mostly static. This change adds two important behaviors:

1. **Adaptive timing**: the library learns how long replies usually take and adjusts its retry timer.
2. **Adaptive pacing**: the library limits how many requests can be in flight at once, so it does not overwhelm slow peers.

The result is fewer unnecessary retries, better throughput on fast links, and more stability on slow or congested links.

## Mental Model (No Deep Background Needed)

Think of REQ as a delivery system:

- You send a package (request).
- You wait for confirmation (reply).
- If confirmation is too slow, you send another package (retry).

The old system used a fixed waiting time. The new system learns how long confirmations usually take and changes the waiting time automatically. It also limits how many packages you send at once.

## Key Terms in Simple Words

- **RTT (round trip time)**: How long it takes to send a request and get a reply.
- **RTO (retry timeout)**: How long to wait before resending the request.
- **cwnd (congestion window)**: How many requests are allowed to be "in flight" at once.
- **in flight**: Requests already sent but not yet answered.
- **peer**: The remote responder you are talking to.
- **pipe**: A specific connection to a peer.

You do not need to remember all these; the rest of the document uses plain explanations and examples.

## What Changed (Detailed but Plain)

### 1. The library now learns a better retry time

Before:

- The retry timer was mostly fixed or configured by the user.
- This was fine on stable networks but caused problems when the network was faster or slower than expected.

After:

- The library measures how long replies actually take.
- It updates the retry timer to match real network behavior.

Why this matters:

- If replies are usually fast, we should not wait a long time to retry.
- If replies are usually slow, retrying too early just creates extra traffic and confusion.

### 2. The library now limits how many requests are in flight

Before:

- If the application sends many requests quickly, the library would keep sending them even if the network was overloaded.
- This can cause congestion and make everything slower.

After:

- The library keeps a "window" of allowed in-flight requests.
- If the window is full, new requests wait in a queue.
- When replies arrive, the window opens again.

Why this matters:

- This protects slow networks from being flooded.
- It also makes performance more predictable.

### 3. Timing is tracked per peer, not just globally

Before:

- A single retry timer was shared across all peers.
- If one peer was slow, it would make the timer large for everyone.

After:

- The library keeps per-peer timing when possible.
- Fast peers stay fast; slow peers do not ruin the timing for everyone.

Why this matters:

- In real deployments, peers often have very different latency.
- One global estimate is rarely correct for all of them.

### 4. New user-facing options were added

Before:

- There was no direct way to turn off the new adaptive logic or inspect the learned retry time.

After:

- You can disable adaptive behavior.
- You can read the current retry time.

Why this matters:

- It preserves compatibility for users who want the old behavior.
- It provides observability for debugging.

## New Options in Plain Language

### `NNG_OPT_REQ_ADAPTIVE`

- **What it is**: A true/false switch.
- **What it does**: Turns the new adaptive behavior on or off.
- **Why it exists**: Allows safe rollout and easy comparisons with old behavior.

### `NNG_OPT_REQ_RTO`

- **What it is**: A read-only number.
- **What it does**: Shows the current retry timeout the library has learned.
- **Why it exists**: Helps operators see if the socket has learned a reasonable value.

## The Safety Rails (Constants)

The adaptive logic can never run wild because it is limited by safe bounds.

- `REQ0_RTO_INIT = 3000 ms`
  - Starting guess before any measurements.
- `REQ0_RTO_MIN = 200 ms`
  - Never retry faster than this.
- `REQ0_RTO_MAX = 60000 ms`
  - Never wait longer than this.
- `REQ0_AI_STEP = 100 ms`
  - On success, slowly increase the retry time a little to avoid being too aggressive.
- `REQ0_CWND_INIT = 1`
  - Start with one in-flight request.
- `REQ0_CWND_MIN = 1`
  - Always allow at least one request so progress is possible.
- `REQ0_CWND_MAX = 1024`
  - Prevent unlimited request bursts on very fast links.

## What Data Is Tracked (Simple Explanation)

### Per request

Each request now keeps track of:

- when it was first sent,
- which peer was used,
- whether it has been retried.

This is needed because the library must measure how long replies take and avoid measuring retried requests (which would give confusing timing data).

### Per socket

The socket keeps:

- a learned baseline for timing (when there is no good per-peer info),
- a count of how many requests are currently in flight,
- and a cap on how many are allowed.

This ensures the library can always fall back to a safe estimate.

### Per peer

Each peer gets:

- its own learned timing values,
- and its own pacing window.

This ensures that a slow peer does not slow down a fast peer.

## How the Learning Works (Conceptual)

The library uses a well-known algorithm for tracking RTT called EWMA (exponentially weighted moving average). The idea is simple:

- New measurements influence the estimate.
- Older measurements fade over time.

It also uses AIMD (additive increase, multiplicative decrease):

- When things succeed, timing grows slowly (additive).
- When things fail (timeouts), timing grows faster (multiplicative), and concurrency is cut.

This is a time-tested pattern used in networking.

## Why Retransmitted Requests Are Not Measured

If a request was retried, the reply might belong to the first send or the second send. That makes the timing ambiguous. To avoid bad data, the system skips RTT sampling for those requests. This is a classic safety rule called Karn's rule.

## How Congestion Window (cwnd) Works in Simple Terms

Imagine a ticket counter:

- The window size is the number of tickets.
- Each new request needs a ticket.
- When a reply arrives, the ticket returns.

If there are no tickets left, new requests wait in line. If timeouts happen, the number of tickets is reduced to slow things down.

## Step-by-Step Behavior (Simplified)

This section explains what happens when a request is sent and a reply is received.

### Step 1: A request is created

- The request is placed in a send queue.
- A retry timer is scheduled.

### Step 2: The request is sent

- The library checks if it is allowed to send (cwnd check).
- If allowed, the request is sent and the send time is recorded.
- The request is now "in flight".

### Step 3: A reply arrives

- If it was not a retried request, the reply time is measured.
- The timing estimate is updated.
- The window size is increased slightly.
- The in-flight count is reduced.

### Step 4: If no reply arrives (timeout)

- The retry timer fires.
- The window size is reduced.
- The timing estimate is adjusted to be more conservative.
- The request is retried.

## Improvements You Can Expect

- **Better responsiveness** on fast networks because timeouts shrink.
- **Fewer retries** on slow networks because timeouts grow.
- **Less overload** because too many in-flight requests are prevented.
- **More fairness** between peers because each has its own timing.

## Runtime Scenarios (Realistic Numbers)

These examples use realistic numbers to show how the new behavior works.

### Scenario 1: Client A to one stable peer

**Setup**:

- Client A sends requests to Peer P1.
- P1 replies in about 80 ms.

**Timeline (first few requests)**:

1. Request 1 sent.
   - No samples yet, so retry timeout is 3000 ms.
   - Reply arrives in 80 ms.
2. Request 2 sent.
   - Now the socket has an estimate.
   - Retry timeout drops closer to a few hundred milliseconds.
   - Reply arrives in 85 ms.
3. Request 3 sent.
   - The estimate stabilizes around ~100-200 ms for RTO.

**Result**:

- Timeouts become realistic quickly.
- cwnd grows slowly so throughput improves without overload.

### Scenario 2: Client A talking to two peers with different speeds

**Setup**:

- Peer Fast (P_fast) replies in ~30 ms.
- Peer Slow (P_slow) replies in ~700 ms.

**Behavior**:

- Requests to P_fast lead to a low per-peer RTO (around 100 ms).
- Requests to P_slow lead to a high per-peer RTO (around 900 ms).
- The socket baseline is in between, but per-peer timing overrides it.

**Result**:

- P_fast stays fast and is not penalized by P_slow.
- P_slow does not cause constant retries because it has its own timeout.

### Scenario 3: Client A floods requests

**Setup**:

- Client A tries to send 100 requests at once.
- cwnd currently allows only 4 in-flight requests.

**Behavior**:

- Only 4 requests are sent immediately.
- The remaining 96 wait in the queue.
- As replies come back, cwnd grows and the queue drains.

**Result**:

- The library avoids overwhelming the peer.
- Throughput increases safely over time.

### Scenario 4: Temporary congestion causes timeouts

**Setup**:

- Peer B normally replies in 200 ms.
- Suddenly, the network is congested and replies take 1200 ms.

**Behavior**:

1. RTO is around 300-400 ms before congestion.
2. A request times out at 400 ms.
3. The library backs off:
   - cwnd is halved.
   - RTO grows (e.g., to 600-900 ms).
4. Retries eventually succeed.

**Result**:

- The library slows down during congestion instead of amplifying it.
- Once congestion clears, the estimates adjust back.

### Scenario 5: Different clients share one server

**Setup**:

- Client A is on a LAN (fast).
- Client B is on a WAN (slow).
- Both use REQ to talk to the same service.

**Behavior**:

- Each client learns its own timing separately.
- Client A runs with short RTO and larger cwnd.
- Client B runs with longer RTO and smaller cwnd.

**Result**:

- Fast client is not held back by slow client.
- Server sees smoother request pacing from both clients.

### Scenario 6: Retries do not contaminate timing

**Setup**:

- Client sends a request with RTO set to 400 ms.
- Reply does not arrive in 400 ms.
- Client retries.
- Reply arrives at 600 ms total.

**Behavior**:

- The reply is ignored for RTT measurement because it could belong to the first or second send.
- This keeps timing estimates clean.

**Result**:

- The library avoids learning bad timing from ambiguous replies.

### Scenario 7: Adaptive behavior turned off

**Setup**:

- `NNG_OPT_REQ_ADAPTIVE` is set to false.

**Behavior**:

- Fixed retry behavior is used.
- No adaptive timing or pacing is applied.

**Result**:

- Behavior matches the old implementation.

## Simplified Call Flow (No Deep Code Needed)

These are the main steps, shown without internal detail.

### Success path

```
send request
  -> check window
  -> send on pipe
  -> receive reply
  -> update timing
  -> open window a bit more
```

### Timeout path

```
send request
  -> wait for reply
  -> timeout
  -> reduce window
  -> increase timeout
  -> retry
```

## Detailed Call Flow (Mapped to Functions)

This is for readers who want to see the exact call sequence but still keep it readable.

### Success (no retry)

```
req0_ctx_send
  -> req0_run_send_queue
       -> req0_send_cb
  -> req0_recv_cb
```

### Timeout and retry

```
req0_ctx_send
  -> req0_run_send_queue
       -> req0_send_cb
  -> req0_retry_cb
       -> req0_run_send_queue
            -> req0_send_cb
  -> req0_recv_cb
```

## File List (What Changed)

- `docs/req-rtt-aimd-unified-architecture.md`
- `include/nng/nng.h`
- `src/sp/protocol/reqrep0/req.c`

## Appendix A: Example Timelines (More Detailed)

This appendix expands on the scenarios with more explicit time values.

### Example A1: Stable peer with small jitter

Assume reply times: 80 ms, 85 ms, 75 ms, 90 ms.

Initial state:

- RTO = 3000 ms
- cwnd = 1

Timeline:

1. Request 1 sent at t=0.
   - Reply arrives at t=80 ms.
   - New RTO becomes roughly a few hundred ms.
   - cwnd increases to 2.
2. Request 2 and 3 can now be in flight.
   - Replies arrive in ~85 ms and ~75 ms.
   - RTO stabilizes near ~200 ms.
   - cwnd grows to 3 and then 4.

Effect:

- The socket quickly adapts to a realistic retry time.
- It also safely increases throughput.

### Example A2: Sudden congestion spike

Assume normal RTT is 200 ms and RTO is around 400 ms.

At t=0:

- Request sent with RTO=400 ms.

At t=400 ms:

- No reply yet, timeout occurs.
- RTO is increased (backoff), cwnd is halved.

At t=700 ms:

- Reply arrives (after retry was sent).
- RTT sample is ignored to avoid ambiguity.

Effect:

- The system becomes conservative and avoids amplifying the congestion.

### Example A3: Fast and slow peers at once

Assume:

- Peer Fast RTT = 30 ms
- Peer Slow RTT = 700 ms

After several requests:

- Fast peer RTO ~100 ms
- Slow peer RTO ~900 ms

Effect:

- Fast peer keeps low latency.
- Slow peer does not cause over-retries for fast peer.

## Appendix B: FAQ (Plain Language)

### Does this change break existing apps?

No. The adaptive behavior can be turned off. The default behavior is designed to stay within safe limits and respect existing retry settings.

### Why is the retry timeout not exactly equal to the measured RTT?

Because networks are noisy. Using a slightly larger timeout avoids false retries when there is normal jitter.

### Why does the retry timeout sometimes go up even after success?

The additive increase is a safety bias. It prevents the timeout from shrinking too aggressively when conditions fluctuate.

### Why not measure RTT after a retry?

Because you cannot be sure which send caused the reply. That would give incorrect data and lead to bad estimates.

### What if I want fixed behavior?

Set `NNG_OPT_REQ_ADAPTIVE` to false.

## Appendix C: Code-Level Map (High Level)

This section lists where the behavior lives without going into full code details.

- Adaptive fields: `req0_ctx`, `req0_sock`, `req0_pipe`.
- Timing updates on success: `req0_recv_cb` calling `req0_rtt_update_success`.
- Timing updates on timeout: `req0_retry_cb` calling `req0_rtt_update_timeout`.
- cwnd gating: `req0_run_send_queue`.
- Observability: `NNG_OPT_REQ_RTO`.
- Toggle: `NNG_OPT_REQ_ADAPTIVE`.

## Appendix D: Summary Table (Plain Language)

| Change | What it does | Why it matters |
| --- | --- | --- |
| Adaptive RTT | Learns reply time | Fewer bad retries |
| AIMD pacing | Limits in-flight requests | Prevents overload |
| Per-peer timing | Different timing per peer | Fast stays fast |
| Adaptive toggle | Turn it on or off | Compatibility |
| RTO readout | Show current timeout | Debugging |

## Appendix E: Short Glossary

- **Request**: A message sent by the client.
- **Reply**: The response from the peer.
- **Retry**: Resending a request because it seems lost.
- **Timeout**: The wait period before a retry.
- **Adaptive**: Adjusting automatically based on measurements.
- **Congestion**: When too much data is sent, causing delays.
