#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 01:30:00
#SBATCH -J splitreduce_nodesweep
#
# Node-count sweep at fixed OSU message size: tests whether split-reduce's
# relevance tracks the effective per-rank data size (= M/N per ring step).
# Placement: --map-by socket --bind-to socket, 2 PPN.
# N in {1,2,4,8}; message in {16M, 64M}; BASE vs SPLIT8 (unpinned).
# Captures timing + split/inline routing counters per config.
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

BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH"
MAPPED="--map-by socket --bind-to socket"
NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")

env_for() {
  case $1 in
    BASE)   echo "" ;;
    SPLIT8) echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
  esac
}

for N in 1 2 4 8; do
  printf '%s\n' "$NODES" | head -n "$N" > /tmp/hf_$N
  NP=$((N * 2))
  echo "===== N=$N nodes  NP=$NP ====="
  for coll in allreduce reduce; do
    OSU=$OSUDIR/osu_$coll
    for size in 16777216 67108864; do
      for cfg in BASE SPLIT8; do
        E=$(env_for $cfg)
        timeout 600 mpirun --hostfile /tmp/hf_$N $MAPPED $BASE $E \
            -x UCC_LOG_LEVEL=info -np $NP $OSU -f -m $size:$size \
            2> /tmp/ns_err_${N}_${coll}_${size}_${cfg}.log \
            | grep -E "^${size} "
        grep -hoE "reduce routing: [0-9]+ pooled, [0-9]+ split, [0-9]+ inline" \
            /tmp/ns_err_${N}_${coll}_${size}_${cfg}.log 2>/dev/null | head -1
      done
    done
  done
done
echo "DONE"
echo "End: $(date)"
