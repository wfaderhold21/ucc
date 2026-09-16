#!/bin/bash
#SBATCH --job-name=ucc_quality_mpi
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=8
#SBATCH --time=00:15:00
#SBATCH --partition=thor
#SBATCH --output=/global/home/users/faderholdt/ucc-staging/quality-mpi-%j.out
#SBATCH --error=/global/home/users/faderholdt/ucc-staging/quality-mpi-%j.out
#
# Runtime verification of the tl/ucp link-quality monitor over real IB (RoCE)
# using ucc_test_mpi (default "UCC_" config prefix). The correctness suite has
# idle gaps where the service worker progresses, so the RTT probe round-trips
# and the classifier emits live per-peer state.

set -o pipefail

UCC_DIR=/global/home/users/faderholdt/ucc-staging/ucc-shrink
HPCX=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1
OPAL_PREFIX="$HPCX/ompi"
export OPAL_PREFIX
export LD_LIBRARY_PATH="$UCC_DIR/install/lib:$HPCX/ompi/lib:$HPCX/ucx/mt/lib:${LD_LIBRARY_PATH:-}"
export PATH="$UCC_DIR/install/bin:$HPCX/ompi/bin:$PATH"

export UCX_NET_DEVICES="${UCX_NET_DEVICES:-mlx5_0:1}"
export UCX_TLS="${UCX_TLS:-rc,shm}"

export UCC_TL_UCP_SERVICE_WORKER=y
export UCC_TL_UCP_SERVICE_THROTTLING_THRESH=1
export UCC_TL_UCP_QUALITY_MONITOR=y
export UCC_TL_UCP_QUALITY_PROBE_INTERVAL=10000    # 10 ms
export UCC_LOG_LEVEL=trace
export UCC_LOG_PRINT_DATE=0

echo "Job $SLURM_JOB_ID nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | tr '\n' ' ')"
date

scontrol show hostnames "$SLURM_JOB_NODELIST" > "$SLURM_SUBMIT_DIR/hosts-quality.$$"

mpirun --map-by node --bind-to core \
    -hostfile "$SLURM_SUBMIT_DIR/hosts-quality.$$" \
    -np 2 \
    "$UCC_DIR/install/bin/ucc_test_mpi" 2>&1

echo "TEST rc=$?"
echo "Done: $(date)"
