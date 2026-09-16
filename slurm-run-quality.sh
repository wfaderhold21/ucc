#!/bin/bash
#SBATCH --job-name=ucc_quality_run
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:10:00
#SBATCH --partition=thor
#SBATCH --output=/global/home/users/faderholdt/ucc-staging/quality-run-%j.out
#SBATCH --error=/global/home/users/faderholdt/ucc-staging/quality-run-%j.out
#
# Runtime verification of the tl/ucp link-quality monitor over real IB (RoCE).
# Runs ucc_perftest allreduce with the service worker + QUALITY_MONITOR enabled
# and debug logging, so we can observe the probe RTT echo and the classifier's
# live state per peer.

set -o pipefail

UCC_DIR=/global/home/users/faderholdt/ucc-staging/ucc-shrink
HPCX=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1
OPAL_PREFIX="$HPCX/ompi"
export OPAL_PREFIX
export LD_LIBRARY_PATH="$UCC_DIR/install/lib:$HPCX/ompi/lib:$HPCX/ucx/mt/lib:${LD_LIBRARY_PATH:-}"
export PATH="$UCC_DIR/install/bin:$HPCX/ompi/bin:$PATH"

export UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_0:1}"
export UCX_TLS="${UCX_TLS:-rc,shm}"

# Enable the quality monitor (requires service worker + OOB).
# ucc_perftest initializes its UCC lib under the PERFTEST_UCC_ config prefix,
# so the TL/UCP knobs must carry that prefix to actually apply.
export UCC_TL_UCP_SERVICE_WORKER=y
export UCC_TL_UCP_QUALITY_MONITOR=y
export PERFTEST_UCC_TL_UCP_SERVICE_WORKER=y
export PERFTEST_UCC_TL_UCP_SERVICE_THROTTLING_THRESH=1
export PERFTEST_UCC_TL_UCP_QUALITY_MONITOR=y
export PERFTEST_UCC_TL_UCP_QUALITY_PROBE_INTERVAL=10000    # 10 ms
export UCC_LOG_LEVEL=trace
export UCC_LOG_PRINT_DATE=0

echo "Job $SLURM_JOB_ID nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | tr '\n' ' ')"
echo "env: SERVICE_WORKER=$UCC_TL_UCP_SERVICE_WORKER QUALITY_MONITOR=$UCC_TL_UCP_QUALITY_MONITOR"
echo "UCX_NET_DEVICES=$UCX_NET_DEVICES UCX_TLS=$UCX_TLS"
date

scontrol show hostnames "$SLURM_JOB_NODELIST" > "$SLURM_SUBMIT_DIR/hosts-quality.$$"

mpirun --map-by node --bind-to core \
    -hostfile "$SLURM_SUBMIT_DIR/hosts-quality.$$" \
    -np 2 \
    "$UCC_DIR/install/bin/ucc_perftest" \
    -c allreduce -b 8 -e 8 -n 1000 -w 10 2>&1

echo "PERFTEST rc=$?"
echo "Done: $(date)"
