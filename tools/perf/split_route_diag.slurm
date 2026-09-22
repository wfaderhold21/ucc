#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:30:00
#SBATCH -J split_route_diag
#
# Routing-engagement diagnostic for the 8-node scale grid: confirm when
# split-reduce actually engages (split vs pooled vs inline counters) for
# reduce / allreduce / reduce_scatter. 2 nodes, 2 PPN, SPLIT cfg, both
# stdout and stderr captured so the UCC ec_info finalize line is seen.
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
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH \
 -x UCC_CLS=${UCC_CLS:-basic}"
MAP="--map-by socket --bind-to socket"
echo "##DIAG-CLS## UCC_CLS=${UCC_CLS:-basic}"

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
printf '%s\n' "$NODES" | head -n 2 > /tmp/hf_2

echo "##DIAG-HEAD## nodes=$(echo "$NODES" | wc -l)"
for coll in reduce allreduce reduce_scatter; do
  for size in 4194304 67108864; do
    for cfg in BASE SPLIT; do
      E=""
      [ "$cfg" = SPLIT ] && E="-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1"
      echo "---- coll=$coll size=$size cfg=$cfg ----"
      timeout 300 mpirun --hostfile /tmp/hf_2 $MAP $BASE $E \
          -x UCC_LOG_LEVEL=info -np 4 $OSUDIR/osu_$coll -f -m $size:$size \
          > /tmp/diag_out 2> /tmp/diag_err
      grep -E "^${size} " /tmp/diag_out
      grep -h "reduce routing" /tmp/diag_out /tmp/diag_err | sort -u | head -8
    done
  done
done
echo "##DIAG-END##"