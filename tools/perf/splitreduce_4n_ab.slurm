#!/bin/bash
#SBATCH -p thor
#SBATCH -N 4
#SBATCH -t 01:30:00
#SBATCH -J splitreduce_4n_ab
#
# 8-node A/B: split-reduce OFF vs ON (unpinned) vs ON (socket-pinned).
# Placement: --map-by socket --bind-to none, 2 PPN (16 ranks).
# UCC build: ucc-threaded-topic/install with socket-aware split-worker pinning.
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
MAP="--map-by socket --bind-to socket"
NP=$((SLURM_JOB_NUM_NODES * 2))

env_for() {
  case $1 in
    BASE)      echo "" ;;
    SPLIT8)    echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
    SPLIT8PIN) echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 -x UCC_EC_CPU_PIN_THREADS=1" ;;
  esac
}

echo "nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | wc -l)  np=$NP  map='$MAP'"
for coll in reduce allreduce; do
  OSU=$OSUDIR/osu_$coll
  for cfg in BASE SPLIT8 SPLIT8PIN; do
    E=$(env_for $cfg)
    echo "---- coll=$coll cfg=$cfg np=$NP ----"
    timeout 900 mpirun $MAP $BASE $E -x UCC_LOG_LEVEL=info \
        -np $NP $OSU -f -m 4096:67108864 2>/dev/null \
        | grep -E "^(4096|16384|65536|262144|1048576|4194304|16777216|67108864) "
    echo
  done
done
echo "DONE"
echo "End: $(date)"
