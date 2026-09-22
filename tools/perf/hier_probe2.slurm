#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J hier_probe2
#
# Corrected probe (no function-arg bug): each mpirun is one inline command.
# Test whether under true UCC_CLS=hier the reduction reaches the cpu-EC split
# path. allreduce @64M, 2 nodes / 4 ranks, full log tail captured.
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

echo "########## CLS=basic  SPLIT on  ##########"
timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE \
  -x UCC_CLS=basic -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 \
  -x UCC_LOG_LEVEL=info -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 \
  > /tmp/b_out 2> /tmp/b_err
grep -E "^67108864 " /tmp/b_out
echo "--- routing ---"; grep -h "reduce routing" /tmp/b_out /tmp/b_err | sort -u

echo "########## CLS=hier   SPLIT on  ##########"
timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE \
  -x UCC_CLS=hier -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 \
  -x UCC_LOG_LEVEL=info -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 \
  > /tmp/h_out 2> /tmp/h_err
grep -E "^67108864 " /tmp/h_out
echo "--- routing ---"; grep -h "reduce routing" /tmp/h_out /tmp/h_err | sort -u
echo "--- any cpu-ec / ec_cpu / sharp lines ---"; grep -hiE "ec_cpu|cpu ec|cpu ec INFO|SHARPD|tl_sharp|sharp" /tmp/h_out /tmp/h_err | sort -u | head -15
echo "--- stderr tail (teardown) ---"; tail -15 /tmp/h_err

echo "########## CLS=hier   SPLIT forced thresh=1 threads=8 ##########"
timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE \
  -x UCC_CLS=hier -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 \
  -x UCC_EC_CPU_REDUCE_SPLIT_THRESH=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 \
  -x UCC_LOG_LEVEL=info -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 \
  > /tmp/hf_out 2> /tmp/hf_err
grep -E "^67108864 " /tmp/hf_out
echo "--- routing ---"; grep -h "reduce routing" /tmp/hf_out /tmp/hf_err | sort -u
echo "--- stderr tail ---"; tail -15 /tmp/hf_err

echo "PROBE2-DONE"