#!/bin/bash
#SBATCH -p thor
#SBATCH -N 4
#SBATCH -t 00:30:00
#SBATCH -J split_large_corr
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

echo "=== large-size correctness: BASE vs SPLIT(default per-MB) vs SPLIT8 ==="
for coll in allreduce reduce; do
  for cfg in BASE SPLIT SPLIT8; do
    case $cfg in
      BASE)   E="" ;;
      SPLIT)  E="-x UCC_EC_CPU_USE_SPLIT_REDUCE=1" ;;
      SPLIT8) E="-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_THREADS_PER_MB=0 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
    esac
    timeout 600 mpirun --map-by socket --bind-to socket $BASE $E \
        -np 8 $UCC/bin/ucc_test_mpi -c $coll -m 67108864:67108864 \
        > "$WORK/large_corr_${coll}_${cfg}.log" 2>&1
    echo "coll=$coll cfg=$cfg exit=$?"
    grep -iE "failed:|PASS|FAIL|error|verify" "$WORK/large_corr_${coll}_${cfg}.log" | tail -4
  done
done
echo DONE
