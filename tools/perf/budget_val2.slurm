#!/bin/bash
#SBATCH -p thor
#SBATCH -N 1
#SBATCH -t 00:20:00
#SBATCH -J budget_val2
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
echo "$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -1)" > /tmp/hf_1
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm -x LD_LIBRARY_PATH \
 -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 -x UCC_LOG_LEVEL=debug"
run() {
  local ppn="$1" np="$2"
  timeout 180 mpirun --hostfile /tmp/hf_1 --map-by ppr:$ppn:node --bind-to core $BASE \
    -np "$np" $OSUDIR/osu_allreduce -f -m 67108864:67108864 >/tmp/v_out 2>/tmp/v_err
  echo "== PPN=$ppn NP=$np =="
  echo "  timing: $(grep -E '^67108864 ' /tmp/v_out | tail -1 | awk "{print \$2}")"
  echo "  budget line: $(grep -h 'socket budget' /tmp/v_out /tmp/v_err | sort -u | head -1)"
  echo "  max worker idx: $(grep -hoE 'worker [0-9]+ pinned to CPU' /tmp/v_out /tmp/v_err | grep -oE '[0-9]+' | sort -n | tail -1)"
}
run 2 2
run 4 4
run 8 8
echo "DONE"