#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J hier_dbg
#
# Debug-level capture to find which hier team-create branch fails: SBGP not
# enabled vs single-node vs async team-create test. allreduce @16M, 2 nodes.
set -uo pipefail

WORK=/global/home/users/faderholdt/ucc-staging
HPCX=/global/home/users/faderholdt/hpcx-v2.25.1-gcc-doca_ofed-redhat9-cuda13-x86_64
OSUDIR=/global/home/users/faderholdt/osu-micro-benchmarks-7.4/install/libexec/osu-micro-benchmarks/mpi/collective
UCC=$WORK/ucc-threaded-topic/install

module load gcc 2>/dev/null || true
export HPCX_ENABLE_NCCL_SPCX_PLUGIN=0 HPCX_ENABLE_NCCL_MRC_PLUGIN=0
. "$HPCX/hpcx-mt-init-ompi.sh" >/dev/null 2>&1
hpcx_load
export LD_LIBRARY_PATH=$UCC/lib:$LD_LIBRARY_PATH

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
printf '%s\n' "$NODES" | head -n 2 > /tmp/hf_2
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH"
MAP="--map-by socket --bind-to socket"

timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE \
  -x UCC_CLS=hier -x UCC_LOG_LEVEL=debug \
  -np 4 $OSUDIR/osu_allreduce -f -m 16777216:16777216 \
  > /tmp/d_out 2> /tmp/d_err
echo "=== hier team/topo/sbgp messages ==="
grep -hiE "single.node|sbgp|cl_hier|hier.*team|team create|no cl team|topo|node.local" /tmp/d_err /tmp/d_out | sort -u | head -40
echo "=== UCC ERROR lines ==="
grep -hiE "ERROR|no CL teams" /tmp/d_err /tmp/d_out | sort -u | head