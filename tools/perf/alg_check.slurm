#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 00:30:00
#SBATCH -J alg_check
# Determine which UCC TL/UCP allreduce algorithm actually runs and the CPU-EC
# reduce count semantics, at 16M, PPN=2, N=1 (P=2) vs N=4 (P=8). Capture the
# algorithm-selection log + split routing counters.
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
for n in 1 4; do printf '%s\n' "$NODES" | head -n "$n" > /tmp/hf_$n; done
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH \
 -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1"

run() {
  local n="$1" np=$((n*2))
  echo "############ N=$n  NP=$np (16M allreduce, split on) ############"
  timeout 180 mpirun --hostfile /tmp/hf_$n --map-by ppr:2:node --bind-to core $BASE \
    -x UCC_LOG_LEVEL=info -x UCC_COLL_TRACE=info \
    -np "$np" $OSUDIR/osu_allreduce -f -m 16777216:16777216 > /tmp/a_out 2> /tmp/a_err
  echo "--- timing ---"; grep -E "^16777216 " /tmp/a_out
  echo "--- selected allreduce algorithm ---"
  grep -hiE "allreduce.*(knomial|sra|sliding|dbt|ring)|alg.*(knomial|sra|sliding)|selected.*alg|score.*allreduce|allreduce.*score" /tmp/a_out /tmp/a_err | sort -u | head -10
  echo "--- cpu-EC routing ---"; grep -h "reduce routing" /tmp/a_out /tmp/a_err | sort -u | head -4
  echo "--- split threads enabled line ---"; grep -h "split reduce enabled" /tmp/a_out /tmp/a_err | sort -u | head -1
}
run 1
run 4
echo "ALG-DONE"