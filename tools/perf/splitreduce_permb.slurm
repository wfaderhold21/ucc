#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 01:30:00
#SBATCH -J splitreduce_permb
#
# Message-size sweep at N=4 and N=8 (2 PPN, --map-by socket --bind-to socket):
# threads-per-MB vs fixed-8 vs BASE.  Lowered REDUCE_SPLIT_THRESH (64K elems)
# so the per-MB scaling actually engages across sizes.
#   SPLIT8 = fixed 8 threads
#   SPLITMB = 1 thread per 1 MiB of message, capped at 8 (REDUCE_NUM_THREADS)
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
# lower threshold so per-MB scaling engages at 1-8MB
LT="-x UCC_EC_CPU_REDUCE_SPLIT_THRESH=65536"

env_for() {
  case $1 in
    BASE)    echo "" ;;
    SPLIT8)  echo "$LT -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
    SPLITMB) echo "$LT -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_THREADS_PER_MB=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
  esac
}

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
SIZES="1048576 2097152 4194304 8388608 16777216 67108864"

for N in 4 8; do
  printf '%s\n' "$NODES" | head -n "$N" > /tmp/hf_$N
  NP=$((N * 2))
  echo "===== N=$N  NP=$NP ====="
  for coll in allreduce reduce; do
    OSU=$OSUDIR/osu_$coll
    for size in $SIZES; do
      mb=$((size / 1048576))
      for cfg in BASE SPLIT8 SPLITMB; do
        E=$(env_for $cfg)
        timeout 900 mpirun --hostfile /tmp/hf_$N $MAP $BASE $E \
            -np $NP $OSU -f -m $size:$size 2>/dev/null | grep -E "^${size} "
      done
      echo "  -- size ${mb}MB done --"
    done
  done
done
echo "DONE"
echo "End: $(date)"
