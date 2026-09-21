#!/bin/bash
#SBATCH -p thor
#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH -J split_pin_verify
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
MAP="--map-by socket --bind-to none"

echo "== CORRECTNESS with PIN_THREADS=1 (ucc_test_mpi allreduce) =="
timeout 600 mpirun $MAP $BASE -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 \
    -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 -x UCC_EC_CPU_PIN_THREADS=1 \
    -np 2 $UCC/bin/ucc_test_mpi -c allreduce > /tmp/pv_corr.log 2>&1
echo "exit=$?"
tail -2 /tmp/pv_corr.log

echo
echo "== PIN EVIDENCE (64M allreduce, SPLIT+PIN) =="
timeout 400 mpirun $MAP $BASE -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 \
    -x UCC_EC_CPU_REDUCE_NUM_THREADS=8 -x UCC_EC_CPU_PIN_THREADS=1 \
    -x UCC_LOG_LEVEL=info -np 2 $OSU -f -m 67108864:67108864 \
    > "$WORK/pin_verify_out.log" 2>&1
echo "perf exit=$?"
grep -E "^67108864" "$WORK/pin_verify_out.log"
echo "--- pinned split worker lines (per rank) ---"
grep -E "split reduce worker .* pinned to CPU" "$WORK/pin_verify_out.log" | sort -u
echo "--- routing ---"
grep -E "reduce routing" "$WORK/pin_verify_out.log" | sort -u
echo DONE
