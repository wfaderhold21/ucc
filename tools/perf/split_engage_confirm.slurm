#!/bin/bash
#SBATCH -p thor
#SBATCH -N 1
#SBATCH -t 00:15:00
#SBATCH -J split_engage_confirm
set -uo pipefail
WORK=/global/home/users/faderholdt/ucc-staging
HPCX=/global/home/users/faderholdt/hpcx-v2.25.1-gcc-doca_ofed-redhat9-cuda13-x86_64
UCC=$WORK/ucc-threaded-topic/install
module load gcc 2>/dev/null || true
export HPCX_ENABLE_NCCL_SPCX_PLUGIN=0 HPCX_ENABLE_NCCL_MRC_PLUGIN=0
. "$HPCX/hpcx-mt-init-ompi.sh" >/dev/null 2>&1
hpcx_load
export LD_LIBRARY_PATH=$UCC/lib:$LD_LIBRARY_PATH
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH"

# Confirm split path engages at large sizes (64M bytes = 16M elements, above 4M threshold)
timeout 300 mpirun --map-by socket --bind-to none $BASE \
    -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 \
    -x UCC_LOG_LEVEL=info -np 2 $UCC/bin/ucc_test_mpi -c allreduce -m 67108864:67108864 \
    > "$WORK/split_engage_SPLIT8.log" 2>&1
echo "SPLIT8 exit=$?"
grep -iE "split|routing|sync-reduce|pool .* inline" "$WORK/split_engage_SPLIT8.log" | sort -u | head -20
echo "---- BASE (control) ----"
timeout 300 mpirun --map-by socket --bind-to none $BASE \
    -x UCC_LOG_LEVEL=info -np 2 $UCC/bin/ucc_test_mpi -c allreduce -m 67108864:67108864 \
    > "$WORK/split_engage_BASE.log" 2>&1
echo "BASE exit=$?"
grep -iE "split|routing|sync-reduce|pool .* inline" "$WORK/split_engage_BASE.log" | sort -u | head -20
echo DONE
