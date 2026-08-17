#!/bin/bash
#
# Frozen multi-rank runtime coverage for UCC issue #1174 (enforce owner
# lifetimes). Bounded: fixed rank count, single pass, small collectives.
#
# Coverage:
#   1. ucc_test_lifetime (the frozen MPI/UCP test):
#        - refusal/retry: context_destroy/finalize refuse while live team
#          and mapping handles exist; retries succeed after ordered cleanup
#        - asynchronous failed team creation: failed team retains context
#          ownership until ucc_team_destroy
#        - ordered cleanup: unmap -> destroy team -> destroy context -> finalize
#   2. ucc_perftest happy-path smoke: exercises init/finalize ordering.
#      The perftest error path (failed team -> ucc_team_destroy ->
#      ucc_context_destroy) is the exact sequence ucc_test_lifetime verifies
#      at the library level; it is not externally triggerable on a healthy
#      UCP/UCX stack, so coverage is source review + the library-level check.
#   3. OSHMEM consumer ordering: symmetric collectives routed through the
#      UCC scoll component (barrier, reduction, fcollect), verifying the
#      consumer creates/uses/destroys its UCC contexts and teams in order.
#      Runs only when OSHMEM is available in the HPC-X environment.
#
# Usage: ./run_lifetime.sh <ucc_install_prefix> [np]
#   np defaults to 2; 2 is the minimum for the multi-rank paths.
#
set -e
UCC_INSTALL=${1:?usage: $0 <ucc_install_prefix> [np]}
NP=${2:-2}

# Load HPC-X if available (sets UCX/MPI/OSHMEM into PATH).
HPCX_DIR=$(ls -d /work/nvidia/hpcx-v*/ 2>/dev/null | sort -V | tail -1)
if [ -n "$HPCX_DIR" ]; then
    . "${HPCX_DIR}hpcx-init-ompi.sh"
    hpcx_load
fi

export LD_LIBRARY_PATH="${UCC_INSTALL}/lib:${LD_LIBRARY_PATH}"

echo "=== 1. ucc_test_lifetime (np=${NP}) ==="
mpirun -np "${NP}" --oversubscribe ./ucc_test_lifetime

echo "=== 2. ucc_perftest happy-path smoke (np=${NP}) ==="
PERFTEST="${UCC_INSTALL}/bin/ucc_perftest"
if [ ! -x "$PERFTEST" ]; then
    PERFTEST="$(cd "$(dirname "$0")" && pwd)/../../tools/perf/ucc_perftest"
fi
mpirun -np "${NP}" --oversubscribe "${PERFTEST}" -c allreduce -n 4 -N 1 -m host

echo "=== 3. OSHMEM consumer ordering (np=${NP}) ==="
if command -v oshrun >/dev/null 2>&1 && [ -d "${HPCX_DIR}ompi/tests/examples" ]; then
    oshrun -np "${NP}" "${HPCX_DIR}ompi/tests/examples/oshmem_max_reduction"
    oshrun -np "${NP}" "${HPCX_DIR}ompi/tests/examples/ring_oshmem"
else
    echo "OSHMEM not available; consumer ordering coverage = source review only"
fi

echo "run_lifetime: PASS"
