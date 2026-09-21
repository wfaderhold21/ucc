#!/bin/bash
#SBATCH -p thor
#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH -J split_engage_confirm2
set -uo pipefail
WORK=/global/home/users/faderholdt/ucc-staging
HPCX=/global/home/users/faderholdt/hpcx-v2.25.1-gcc-doca_ofed-redhat9-cuda13-x86_64
OSU=/global/home/users/faderholdt/osu-micro-benchmarks-7.4/install/libexec/osu-micro-benchmarks/mpi/collective/osu_allreduce
UCC=$WORK/ucc-threaded-topic/install
module load gcc 2>/dev/null || true
export HPCX_ENABLE_NCCL_SPCX_PLUGIN=0 HPCX_ENABLE_NCCL_MRC_PLUGIN=0
. "$HPCX/hpcx-mt-init-ompi.sh" >/dev/null 2>&1
hpcx_load
export LD_LIBRARY_PATH=$UCC/lib:$LD_LIBRARY_PATH
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH"

# Same binary/command as the perf run, capture stderr routing telemetry to shared NFS.
echo "=== SPLIT8 (64M) ==="
timeout 400 mpirun --map-by socket --bind-to none $BASE \
    -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 \
    -x UCC_LOG_LEVEL=info -np 2 $OSU -f -m 67108864:67108864 \
    > "$WORK/eng2_out_SPLIT8.log" 2> "$WORK/eng2_err_SPLIT8.log"
echo "SPLIT8 exit=$?  rows:"; grep -E "^67108864" "$WORK/eng2_out_SPLIT8.log"
echo "routing:"; grep -iE "reduce routing|split reduce" "$WORK/eng2_err_SPLIT8.log" | sort -u | head

echo "=== BASE (64M) ==="
timeout 400 mpirun --map-by socket --bind-to none $BASE \
    -x UCC_LOG_LEVEL=info -np 2 $OSU -f -m 67108864:67108864 \
    > "$WORK/eng2_out_BASE.log" 2> "$WORK/eng2_err_BASE.log"
echo "BASE exit=$?  rows:"; grep -E "^67108864" "$WORK/eng2_out_BASE.log"
echo "routing:"; grep -iE "reduce routing|split reduce" "$WORK/eng2_err_BASE.log" | sort -u | head
echo DONE
