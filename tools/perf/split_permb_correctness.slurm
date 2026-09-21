#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 00:40:00
#SBATCH -J split_permb_correctness
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
MAP="--map-by socket --bind-to socket"
NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")

# Per-MB is now the default when USE_SPLIT_REDUCE=1; exercise it.
for N in 4 8; do
  printf '%s\n' "$NODES" | head -n "$N" > /tmp/chf_$N
  NP=$((N*2))
  for coll in allreduce reduce; do
    for msize in 1024 1048576 16777216 67108864; do
      timeout 600 mpirun --hostfile /tmp/chf_$N $MAP $BASE \
          -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -np $NP \
          $UCC/bin/ucc_test_mpi -c $coll -m $msize:$msize \
          > /tmp/pmb_corr.log 2>&1
      echo "N=$N np=$NP coll=$coll m=$msize exit=$? $(grep -oE 'failed:[^ ]*|OK[^ ]*' /tmp/pmb_corr.log | head -1)"
    done
  done
done
echo DONE
