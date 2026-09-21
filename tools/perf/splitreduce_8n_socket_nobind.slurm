#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 01:30:00
#SBATCH -J splitreduce_8n_socket_nobind
#
# Re-investigation of split-reduce on 8x Thor nodes.
# Placement: --map-by socket  --bind-to none   (2 PPN => one rank per socket, NO process bind)
# Compares reduce / allreduce end-to-end with UCC's threaded split-reduce ON vs OFF,
# at the requested 2-PPN / socket / no-bind placement.
#
# UCC build: ucc-threaded-topic/install (--enable-ec-threaded-reduce), current @ 259fe4d.
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

# 2 PPN over 8 nodes, one rank per socket, do NOT bind the process.
MAP="--map-by socket --bind-to none"
NP=16

env_for() {
  case $1 in
    BASE)   echo "" ;;
    SPLIT4) echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=4" ;;
    SPLIT8) echo "-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_REDUCE_NUM_THREADS=8" ;;
  esac
}

echo "CPU: $(grep -m1 'model name' /proc/cpuinfo)"
echo "cores: $(nproc)"
echo "nodes=$SLURM_JOB_NUM_NODES  np=$NP  map='$MAP'"
scontrol show hostnames "$SLURM_JOB_NODELIST" | tr '\n' ' '; echo
echo

echo "=============== CORRECTNESS (ucc_test_mpi, 2 PPN socket no-bind) ==============="
for coll in reduce allreduce; do
  for cfg in BASE SPLIT4 SPLIT8; do
    E=$(env_for $cfg)
    echo "--- correctness coll=$coll cfg=$cfg ---"
    timeout 600 mpirun $MAP $BASE $E -np $NP \
        $UCC/bin/ucc_test_mpi -c $coll > /tmp/corr_${coll}_${cfg}.log 2>&1
    echo "coll=$coll cfg=$cfg exit=$?"
    tail -2 /tmp/corr_${coll}_${cfg}.log
  done
done
echo

echo "=============== PERF (OSU, 8 nodes, 2 PPN, socket no-bind) ==============="
for coll in reduce allreduce; do
  OSU=$OSUDIR/osu_$coll
  for cfg in BASE SPLIT4 SPLIT8; do
    E=$(env_for $cfg)
    echo "---- coll=$coll cfg=$cfg np=$NP ----"
    timeout 900 mpirun $MAP $BASE $E -x UCC_LOG_LEVEL=info \
        -np $NP $OSU -f -m 4096:67108864 2>/tmp/err_${coll}_${cfg}.log \
        | grep -E "^(4096|16384|65536|262144|1048576|4194304|16777216|67108864) "
    grep -ho "reduce routing:.*" /tmp/err_${coll}_${cfg}.log | head -1
    grep -ho "split reduce enabled.*" /tmp/err_${coll}_${cfg}.log | head -1
    echo
  done
done
echo "DONE"
echo "End: $(date)"
