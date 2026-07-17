#!/bin/bash
#SBATCH --job-name=rs-profile
#SBATCH --partition=thor
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=1
#SBATCH --time=00:20:00
#SBATCH --output=%x-%j.log
#
# Profile the knomial reduce-scatter path at 2-4MB to decide whether CPU
# reduction (ucc_ec_cpu_reduce) is still on the critical path -> Plan #3.
# Wraps ONLY rank 0 with `perf record`; other ranks run bare. Emits a flat
# symbol report per (collective, nranks).

set -euo pipefail
module load gcc hpcx/2.25

INSTALL=${INSTALL:-$HOME/ucc/install}
OUTDIR=${OUTDIR:-$HOME/ucc/results/plan3-profile}
mkdir -p "$OUTDIR"

export PATH="$INSTALL/bin:$PATH"
export LD_LIBRARY_PATH="$INSTALL/lib:$INSTALL/lib64:${LD_LIBRARY_PATH:-}"
export UCX_TLS=sm,rc
export UCX_NET_DEVICES=mlx5_0:1
# Force SRA path + the shipped pipeline (512K frags, depth 4 at 2-4MB).
export UCC_TL_UCP_TUNE="allreduce:0-inf:@sra_knomial"
export UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX=4
export UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE="thresh=256K:fragsize=512K:nfrags=2:pdepth=4:parallel"

# perf wrapper: only rank 0 records; user+kernel cycles, dwarf call graph.
cat > "$OUTDIR/perf_wrap.sh" <<'WRAP'
#!/bin/bash
if [ "${OMPI_COMM_WORLD_RANK:-1}" = "0" ]; then
  exec perf record -o "$PERF_DATA" --call-graph dwarf -e cycles -- "$@"
else
  exec "$@"
fi
WRAP
chmod +x "$OUTDIR/perf_wrap.sh"

# collectives to profile: reduce_scatter isolates the target phase; allreduce
# is the real-world composite (RS + allgather).
for COLL in reduce_scatter allreduce; do
  for NP in 2 8; do
    TAG="${COLL}-np${NP}"
    export PERF_DATA="$OUTDIR/perf-${TAG}.data"
    echo "=== profiling $TAG (2MB,4MB float32 sum) ==="
    mpirun --np "$NP" --map-by ppr:1:node --bind-to core \
      -x PATH -x LD_LIBRARY_PATH -x UCX_TLS -x UCX_NET_DEVICES \
      -x UCC_TL_UCP_TUNE -x UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX \
      -x UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE -x PERF_DATA \
      "$OUTDIR/perf_wrap.sh" \
      ucc_perftest -c "$COLL" -m host -d float32 -o sum \
      -b 512K -e 1M -f 2 -w 50 -n 300 -F \
      > "$OUTDIR/run-${TAG}.log" 2>&1 || true
    # flat symbol report for rank 0
    perf report -i "$PERF_DATA" --stdio -g none 2>/dev/null \
      | grep -vE '^\s*#' | head -40 > "$OUTDIR/report-${TAG}.txt" || true
    echo "  -> $OUTDIR/report-${TAG}.txt"
  done
done
echo "ALL DONE -> $OUTDIR"
