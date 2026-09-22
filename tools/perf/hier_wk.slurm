#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:20:00
#SBATCH -J hier_wk
# Working-hier engagement: routing counters + sharp + CL/TL trace for one
# 64M allreduce on 2 nodes (4 ranks, ppr:2:node), under UCC_CLS=hier.
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
MAP="--map-by ppr:2:node --bind-to core"

timeout 200 mpirun --hostfile /tmp/hf_2 $MAP $BASE \
  -x UCC_CLS=hier -x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1 \
  -x UCC_LOG_LEVEL=info -x UCC_COLL_TRACE=info \
  -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 > /tmp/w_out 2> /tmp/w_err
echo "--- timing ---"; grep -E "^67108864 " /tmp/w_out
echo "--- routing ---"; grep -h "reduce routing" /tmp/w_out /tmp/w_err | sort -u | head
echo "--- coll trace / CL / TL dispatch ---"
grep -hiE "trace|cl:.*(hier|basic)|coll.*hier|\[hier\]|tl.*(ucp|sharp)|sharp|team.*create|single node|no cl team" /tmp/w_out /tmp/w_err | sort -u | head -20
echo "--- ec cpu / split threads ---"; grep -hiE "cpu ec|split reduce enabled|worker .* pinned" /tmp/w_out /tmp/w_err | sort -u | head -8
echo "--- team create error? ---"; grep -hc "coll_ucc_module.*failed\|no CL teams" /tmp/w_err
echo "WOK-DONE"