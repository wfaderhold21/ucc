#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 04:00:00
#SBATCH -J splitreduce_scale_8n
#
# 8-node scaling characterization of split-reduce on thor.
#
# Goal: find the best use case (where CPU split-reduce wins) and where it
# falls apart (small messages, high PPN, or collectives where it never
# engages). Sweeps:
#   N    node count          {1,2,4,8}   (hostfile prefix, --map-by socket --bind-to socket)
#   PPN  ranks per node      {2,4,8}
#   coll reduce | allreduce | reduce_scatter      (OSU collectives)
#   size 64K..128M          (full-sweep -m per config, greps fixed rows)
#   cfg  BASE | SPLIT        (split-reduce OFF vs ON, threads-per-MB + pin)
#
# Same binary both configs (ucc-threaded-topic/install); BASE just disables
# split-reduce via env. Per-run timeout + retry so a hung/huge case degrades,
# not stalls. Emits machine-readable ##SCALE## rows plus routing telemetry.
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

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
RUN_TIMEOUT="${RUN_TIMEOUT:-600}"
MAX_RETRIES="${MAX_RETRIES:-3}"
COLLS="${COLLS:-reduce allreduce reduce_scatter}"
SIZES="${SIZES:-65536 1048576 4194304 16777216 67108864 134217728}"
N_LIST="${N_LIST:-1 2 4 8}"
PPN_LIST="${PPN_LIST:-2 4 8}"

echo "##SCALE-HEAD## job=$SLURM_JOB_ID nodes=$(echo "$NODES" | wc -l) colls='$COLLS' sizes='$SIZES' n='$N_LIST' ppn='$PPN_LIST' ucc=$UCC"

env_for() {
  case $1 in
    BASE)  echo "" ;;
    SPLIT) echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1" ;;
  esac
}

run_one() {
  local n=$1 ppn=$2 coll=$3 size=$4 cfg=$5 np=$6
  local OSU=$OSUDIR/osu_$coll
  local E; E=$(env_for $cfg)
  local out err rc attempt=0
  out=$(mktemp); err=$(mktemp)
  while [ "$attempt" -lt "$MAX_RETRIES" ]; do
    attempt=$((attempt + 1))
    timeout "$RUN_TIMEOUT" mpirun --hostfile /tmp/hf_$n $MAP $BASE $E \
        -x UCC_LOG_LEVEL=info -np "$np" "$OSU" -f -m "$size:$size" \
        > "$out" 2> "$err"
    rc=$?
    if [ "$rc" -eq 0 ] && grep -qE "^${size} " "$out"; then
      break
    fi
    echo "  [retry $attempt] n=$n ppn=$ppn coll=$coll size=$size cfg=$cfg rc=$rc"
  done
  local row route
  row=$(grep -E "^${size} " "$out" | tail -1)
  route=$(grep -hoE "reduce routing: [0-9]+ pooled, [0-9]+ split, [0-9]+ inline" "$err" | head -1)
  if [ -z "$row" ]; then
    echo "##SCALE## n=$n ppn=$ppn coll=$coll size=$size cfg=$cfg np=$np us=NA status=fail"
  else
    echo "##SCALE## n=$n ppn=$ppn coll=$coll size=$size cfg=$cfg np=$np $row route='${route:-unknown}'"
  fi
  rm -f "$out" "$err"
}

for n in $N_LIST; do
  printf '%s\n' "$NODES" | head -n "$n" > /tmp/hf_$n
  for ppn in $PPN_LIST; do
    np=$((n * ppn))
    echo "===== N=$n  PPN=$ppn  NP=$np ====="
    for coll in $COLLS; do
      for size in $SIZES; do
        for cfg in BASE SPLIT; do
          run_one "$n" "$ppn" "$coll" "$size" "$cfg" "$np"
        done
      done
    done
  done
done
echo "##SCALE-END##"
echo "End: $(date)"