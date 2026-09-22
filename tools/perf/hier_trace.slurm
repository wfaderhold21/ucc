#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J hier_trace
#
# Trace the actual execution path of allreduce under UCC_CLS=basic vs hier,
# to determine WHY split-reduce does not engage under hier. Captures
# UCC_COLL_TRACE (which CL/TL executes the collective) + the cpu-EC routing
# finalize line, for one 64M allreduce on 2 nodes / 4 ranks.
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

for cls in basic hier; do
  echo "############ UCC_CLS=$cls ############"
  timeout 180 mpirun --hostfile /tmp/hf_2 \
      --map-by socket --bind-to socket $BASE \
      -x UCC_CLS=$cls \
      -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 \
      -x UCC_LOG_LEVEL=info -x UCC_COLL_TRACE=info \
      -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 \
      > /tmp/tr_out_$cls 2> /tmp/tr_err_$cls
  echo "--- timing row ---"
  grep -E "^67108864 " /tmp/tr_out_$cls
  echo "--- CL/TL dispatch lines (coll_trace / team / tl selection) ---"
  grep -iE "collective|cl ?=|tl ?=|selected|use.*cl|hier|sharp|self" /tmp/tr_err_$cls /tmp/tr_out_$cls | grep -viE "sharp_readiness|ucp_tl" | head -25
  echo "--- cpu EC reduce routing ---"
  grep -h "reduce routing" /tmp/tr_err_$cls /tmp/tr_out_$cls | sort -u
  echo "--- any cpu ec / sharp / reduce-kernel mentions ---"
  grep -hiE "cpu ec|sharp|reduce_kernel|executor_kernel" /tmp/tr_err_$cls /tmp/tr_out_$cls | head -8
  echo
done
echo "TRACE-DONE"