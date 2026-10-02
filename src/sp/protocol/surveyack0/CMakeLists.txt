#
# Copyright 2026 Staysail Systems, Inc. <info@staysail.tech>
#
# This software is supplied under the terms of the MIT License, a
# copy of which should be located in the distribution where this
# file was obtained (LICENSE.txt).  A copy of the license may also be
# found online at https://opensource.org/licenses/MIT.
#

# Cumulative-ACK RTT-lite broadcast (cloned from survey0).
nng_directory(surveyack0)

nng_sources_if(NNG_PROTO_SACK0 sack.c)
nng_defines_if(NNG_PROTO_SACK0 NNG_HAVE_SACK0)

nng_sources_if(NNG_PROTO_SACKRESP0 sack_resp.c)
nng_defines_if(NNG_PROTO_SACKRESP0 NNG_HAVE_SACKRESP0)

nng_test(sack_test)
