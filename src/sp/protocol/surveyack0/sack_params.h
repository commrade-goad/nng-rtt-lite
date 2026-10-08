// Shared SACK scale parameter: single knob S for repair window (= batch).
// Both the protocol (sack.c) and the benchmark wrapper (nng_wrapper.h)
// include this file, so one -DSACK0_RING_MAX flag (or the default below)
// drives ring size, ACK batching, and SACK mask horizon together.
#ifndef SACK0_PARAMS_H
#define SACK0_PARAMS_H

#ifndef SACK0_RING_MAX
#define SACK0_RING_MAX 16 // repair window: last N surveys kept for resend
#endif

// Cumulative-ACK batching: one "C<next>[:mask]" per batch instead of one
// ACK per survey. The batch IS the ring: batch size follows SACK0_RING_MAX.
#define SACK0_ACK_BATCH SACK0_RING_MAX

// SACK mask covers next SACK0_SACK_BITS wrapper seqs above base.
#define SACK0_SACK_BITS 32

#endif // SACK0_PARAMS_H
