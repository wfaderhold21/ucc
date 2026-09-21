#!/bin/bash
#SBATCH -p thor
#SBATCH -N 4
#SBATCH -t 00:40:00
#SBATCH -J split_default_verify
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
SPLIT="-x UCC_EC_CPU_USE_SPLIT_REDUCE=1"   # default per-MB + default threshold

echo "===== CORRECTNESS (default split config) ====="
for coll in allreduce reduce; do
  for ms in 262144 1048576 4194304 16777216; do
    timeout 600 mpirun $MAP $BASE $SPLIT -np 8 \
        $UCC/bin/ucc_test_mpi -c $coll -m $ms:$ms > "$WORK/dv_corr.log" 2>&1
    rc=$?
    echo "coll=$coll m=$ms exit=$rc"
    [ $rc -ne 0 ] && grep -iE "failed|error" "$WORK/dv_corr.log" | tail -2
  done
done
echo
echo "===== PERF spot-check BASE vs default-split ====="
for coll in allreduce reduce; do
  OSU=$OSUDIR/osu_$coll
  for ms in 1048576 4194304 16777216; do
    for cfg in BASE SPLIT; do
      E=""; [ "$cfg" = SPLIT ] && E=$SPLIT
      echo -n "coll=$coll m=$ms cfg=$cfg: "
      timeout 600 mpirun $MAP $BASE $E -np 8 $OSU -f -m $ms:$ms 2>/dev/null | grep -E "^${ms} "
    done
  done
done
echo DONE
