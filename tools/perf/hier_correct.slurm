#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J hier_correct
#
# Corrected hier test with proper multi-node placement: --map-by ppr:2:node
# spreads 4 ranks across 2 nodes (2 per node). Checks (a) hier team creation
# now succeeds, (b) split-reduce engages/helps under hier. reduce/allreduce
# @16M and 64M, BASE vs SPLIT.
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
MAP="--map-by ppr:2:node --bind-to core"

echo "=== placement sanity ==="
mpirun --hostfile /tmp/hf_2 --map-by ppr:2:node -np 4 hostname 2>&1 | sort | uniq -c

run_pair() {
  local coll="$1" size="$2" cls="$3" cfg="$4"
  local E=""
  [ "$cfg" = SPLIT ] && E="-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1"
  timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE -x UCC_CLS=$cls $E \
      -x UCC_LOG_LEVEL=info -np 4 $OSUDIR/osu_$coll -f -m $size:$size \
      > /tmp/c_out 2> /tmp/c_err
  local row route rc
  row=$(grep -E "^${size} " /tmp/c_out | tail -1)
  route=$(grep -h "reduce routing" /tmp/c_out /tmp/c_err | sort -u | head -1)
  rc=$(grep -hc "Error: coll_ucc_module" /tmp/c_err)
  if [ -n "$row" ]; then
    echo "RESULT coll=$coll size=$size cls=$cls cfg=$cfg $row rc_err=$rc route='${route#*INFO }'"
  else
    echo "RESULT coll=$coll size=$size cls=$cls cfg=$cfg NO_ROW rc_err=$rc"
  fi
}

for size in 16777216 67108864; do
  for coll in reduce allreduce; do
    run_pair "$coll" "$size" hier BASE
    run_pair "$coll" "$size" hier SPLIT
  done
done
echo "HIER-CORRECT-DONE"