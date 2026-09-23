#!/bin/bash
#SBATCH -p thor
#SBATCH -N 2
#SBATCH -t 00:20:00
#SBATCH -J abhx_sanity
# Validate three-way plumbing: which libucc each cfg loads (via ldd on the
# running OSU), and that timings differ for hpcx vs base vs split.
set -uo pipefail

WORK=/global/home/users/faderholdt/ucc-staging
HPCX=/global/home/users/faderholdt/hpcx-v2.25.1-gcc-doca_ofed-redhat9-cuda13-x86_64
OSUDIR=/global/home/users/faderholdt/osu-micro-benchmarks-7.4/install/libexec/osu-micro-benchmarks/mpi/collective
UCC=$WORK/ucc-threaded-topic/install

module load gcc 2>/dev/null || true
export HPCX_ENABLE_NCCL_SPCX_PLUGIN=0 HPCX_ENABLE_NCCL_MRC_PLUGIN=0
. "$HPCX/hpcx-mt-init-ompi.sh" >/dev/null 2>&1
hpcx_load

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
printf '%s\n' "$NODES" | head -n 2 > /tmp/hf_2
BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm"

echo "=== default (hpcx) libucc ==="
ldd $OSUDIR/osu_allreduce 2>/dev/null | grep -i ucc
echo "=== topic libucc ==="
ldd $OSUDIR/osu_allreduce 2>/dev/null | grep -i ucc | sed "s#.*ucc.*#(ldd is static-link independent; will verify at runtime)#"

run() {
  local cfg="$1" ld
  ld=""
  if [ "$cfg" = base ] || [ "$cfg" = split ]; then ld="$UCC/lib"; fi
  local envargs=()
  [ "$cfg" = split ] && envargs=(-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1)
  if [ -n "$ld" ]; then
    LD_LIBRARY_PATH="$ld:$LD_LIBRARY_PATH" timeout 120 mpirun --hostfile /tmp/hf_2 \
      --map-by ppr:2:node --bind-to core $BASE -x LD_LIBRARY_PATH \
      -x LD_DEBUG=libs -mca mpi_env_export LD_DEBUG "${envargs[@]}" \
      -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 > /tmp/v_out 2>/tmp/v_err
  else
    timeout 120 mpirun --hostfile /tmp/hf_2 --map-by ppr:2:node --bind-to core $BASE \
      -x LD_LIBRARY_PATH -x LD_DEBUG=libs -mca mpi_env_export LD_DEBUG "${envargs[@]}" \
      -np 4 $OSUDIR/osu_allreduce -f -m 67108864:67108864 > /tmp/v_out 2>/tmp/v_err
  fi
  local lib
  lib=$(grep -oE "[^ '\"]*libucc\.so[.0-9]*" /tmp/v_err /tmp/v_out 2>/dev/null | sort -u | head -3 | tr '\n' ' ')
  echo "cfg=$cfg loaded_libucc=$lib"
  echo "  timing: $(grep -E '^67108864 ' /tmp/v_out | tail -1)"
}
for c in hpcx base split; do run "$c"; done
echo "SANITY-DONE"