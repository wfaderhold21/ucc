#!/bin/bash
#SBATCH -p thor
#SBATCH -N 8
#SBATCH -t 04:00:00
#SBATCH -J ab_hpcx_scale
#
# Three-way baseline decomposition on thor, 8 nodes:
#   hpcx  = HPC-X bundled (stock) UCC  (no simd/split feature)
#   base  = topic build, split OFF     (AVX2 on)
#   split = topic build, split ON      (AVX2 + threaded split)
# Sweep N in {1,2,4,8} x PPN in {2,8} x {reduce,allreduce} x {4M,16M,64M,128M}.
# Correct multi-node placement via --map-by ppr:PPN:node.
# hpcx - base = AVX2+threaded-kernel delta vs stock; base - split = threading.
set -uo pipefail

WORK=/global/home/users/faderholdt/ucc-staging
HPCX=/global/home/users/faderholdt/hpcx-v2.25.1-gcc-doca_ofed-redhat9-cuda13-x86_64
OSUDIR=/global/home/users/faderholdt/osu-micro-benchmarks-7.4/install/libexec/osu-micro-benchmarks/mpi/collective
UCC=$WORK/ucc-threaded-topic/install

module load gcc 2>/dev/null || true
export HPCX_ENABLE_NCCL_SPCX_PLUGIN=0 HPCX_ENABLE_NCCL_MRC_PLUGIN=0
. "$HPCX/hpcx-mt-init-ompi.sh" >/dev/null 2>&1
hpcx_load
# Keep the base HPC-X env for reference; prepend topic lib only for base/split.

BASE="--mca coll_ucc_enable 1 --mca coll_ucc_priority 100 --mca coll_hcoll_enable 0 \
 --mca pml_ucx_enable 1 --mca pml_ucx_priority 100 \
 -x UCX_NET_DEVICES=mlx5_0:1 -x UCX_TLS=rc,shm"

NODES=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
N_LIST="${N_LIST:-1 2 4 8}"
PPN_LIST="${PPN_LIST:-2 8}"
COLLS="${COLLS:-reduce allreduce}"
SIZES="${SIZES:-4194304 16777216 67108864 134217728}"

echo "##ABHX-HEAD## job=$SLURM_JOB_ID ucc(topic)=$UCC n='$N_LIST' ppn='$PPN_LIST' colls='$COLLS'"

run_one() {
  local n="$1" ppn="$2" coll="$3" size="$4" cfg="$5"
  local np=$((n * ppn)) envargs ld
  ld=""
  case "$cfg" in
    hpcx)  envargs=() ; ld="" ;;
    base)  ld=$UCC/lib ; envargs=() ;;
    split) ld=$UCC/lib ; envargs=(-x UCC_EC_CPU_USE_SPLIT_REDUCE=1 -x UCC_EC_CPU_PIN_THREADS=1) ;;
  esac
  local out err rc
  out=$(mktemp); err=$(mktemp)
  if [ -n "$ld" ]; then
    LD_LIBRARY_PATH="$ld:$LD_LIBRARY_PATH" timeout 600 \
      mpirun --hostfile /tmp/hf_$n --map-by ppr:$ppn:node --bind-to core $BASE \
      -x LD_LIBRARY_PATH "${envargs[@]}" -np "$np" $OSUDIR/osu_$coll -f -m "$size:$size" \
      > "$out" 2> "$err"
  else
    timeout 600 \
      mpirun --hostfile /tmp/hf_$n --map-by ppr:$ppn:node --bind-to core $BASE \
      -x LD_LIBRARY_PATH "${envargs[@]}" -np "$np" $OSUDIR/osu_$coll -f -m "$size:$size" \
      > "$out" 2> "$err"
  fi
  rc=$?
  local row
  row=$(grep -E "^${size} " "$out" | tail -1)
  if [ -n "$row" ]; then
    echo "##ABHX## n=$n ppn=$ppn coll=$coll size=$size cfg=$cfg np=$np rc=$rc $row"
  else
    echo "##ABHX## n=$n ppn=$ppn coll=$coll size=$size cfg=$cfg np=$np rc=$rc status=fail"
  fi
  rm -f "$out" "$err"
}

for n in $N_LIST; do
  printf '%s\n' "$NODES" | head -n "$n" > /tmp/hf_$n
  for ppn in $PPN_LIST; do
    for coll in $COLLS; do
      for size in $SIZES; do
        echo "== n=$n ppn=$ppn coll=$coll size=$size =="
        run_one "$n" "$ppn" "$coll" "$size" hpcx
        run_one "$n" "$ppn" "$coll" "$size" base
        run_one "$n" "$ppn" "$coll" "$size" split
      done
    done
  done
done
echo "##ABHX-END##"
echo "End: $(date)"