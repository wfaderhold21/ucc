#!/usr/bin/env bash
#
# 4-node, max-PPN (112 ranks/node = 1 per physical core), CPU-only sweep of
# the SRA-knomial allreduce host pipelining change on the GAIA partition.
#
# Runs two arms back-to-back within the same allocation:
#   pipe — UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=auto  (host-pipeline default)
#   mono — UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=disabled (monolithic, control)
#
# Size sweep: 16K–16M elements at float32 = 64KB–64MB, factor 2 (10 points).
# This covers the pdepth-crossover range documented in 1ed3716b.
#
# Submit from the repository root on the gaia login node:
#   cd /labhome/faderholdt/ucc-sra-build/task22/src
#   sbatch contrib/slurm_allreduce_sra_gaia_4node_cpu.sh
#
# Skip rebuild with SKIP_BUILD=1:
#   sbatch --export=ALL,SKIP_BUILD=1 contrib/slurm_allreduce_sra_gaia_4node_cpu.sh

#SBATCH --job-name=ucc-sra-gaia-4node-cpu
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=112
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

# --- Paths ------------------------------------------------------------------
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
export PATH="$MPI_HOME/bin:$PATH"

SRC_DIR=${SRC_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-${JOB_ID}"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-4node-cpu-${JOB_ID}"}

SKIP_BUILD=${SKIP_BUILD:-0}

# --- Geometry ---------------------------------------------------------------
# gaia DGX nodes: 2 sockets x 56 cores x HT2 = 224 CPUs; max PPN = 112 (one
# rank per physical core).  NNODES is set by SLURM from --nodes above.
NNODES=${SLURM_NNODES:-4}
PPN=${PPN:-112}
NP=$((NNODES * PPN))

# Also run a low-rank point (8 ppn, 32 total) as supplementary context since
# the pipeline benefit is strongest at moderate rank counts.
EXTRA_NP=${EXTRA_NP:-$((NNODES * 8))}

# --- Benchmark config -------------------------------------------------------
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
# 16K–16M elements @ float32 = 64KB–64MB, factor 2 (10 size points)
MIN_COUNT=${MIN_COUNT:-16K}
MAX_COUNT=${MAX_COUNT:-16M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

# At high PPN (448 ranks across 4 nodes), RC creates O(n^2) QP pairs that
# exhaust the HCA's QP resources (syndrome 0x65b500).  DC (Dynamic Connection)
# uses a single DCT per rank for inbound traffic and avoids that explosion.
# Falls back to ud_verbs if DC is unavailable; sm handles intra-node.
UCX_TLS=${UCX_TLS:-"sm,dc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_0:1"}

mkdir -p "$RESULT_DIR"

# --- Build ------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "== building UCC (no CUDA) in $SRC_DIR =="
    cd "$SRC_DIR"
    # configure already exists in the rsync'd source; skip autogen.sh to avoid
    # automake re-generating Makefiles that reference wrong-arch objects.
    make distclean 2>/dev/null || true
    ./configure \
        --prefix="$INSTALL_DIR" \
        --with-mpi="$MPI_HOME" \
        --with-ucx="$UCX_HOME" \
        --without-cuda \
        --without-sharp
    make -j$(nproc)
    make install
    echo "== build done: $INSTALL_DIR =="
else
    echo "== SKIP_BUILD=1, reusing $INSTALL_DIR =="
fi

BIN="$INSTALL_DIR/bin/ucc_perftest"
if [[ ! -x "$BIN" ]]; then
    echo "ERROR: $BIN not found" >&2
    exit 1
fi

UCC_LIB="$INSTALL_DIR/lib:$INSTALL_DIR/lib64"
MPI_LIB="$MPI_HOME/lib:$MPI_HOME/lib64"
UCX_LIB="$UCX_HOME/lib:$UCX_HOME/lib64"
RUN_LD="${UCC_LIB}:${MPI_LIB}:${UCX_LIB}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

echo "install:   $INSTALL_DIR"
echo "results:   $RESULT_DIR"
echo "geometry:  $NNODES nodes x $PPN ppn = $NP ranks (+ extra $EXTRA_NP-rank point)"
echo "dtype/op:  $DTYPE / $OP"
echo "count:     $MIN_COUNT..$MAX_COUNT factor $FACTOR"
echo "UCX_TLS:   $UCX_TLS  UCX_NET_DEVICES: $UCX_NET_DEVICES"

# --- Run helper -------------------------------------------------------------
SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "arm,nranks,ppn,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_arm() {
    local arm="$1" pipeline="$2" nranks="$3" ppn="$4"
    local out="$RESULT_DIR/allreduce-np${nranks}-ppn${ppn}-arm-${arm}.log"

    echo ""
    echo ">> arm=$arm pipeline=$pipeline nranks=$nranks ppn=$ppn"

    "$MPI_HOME/bin/mpirun" \
        --np "$nranks" \
        --map-by "ppr:${ppn}:node" \
        --bind-to core \
        -x "PATH=$INSTALL_DIR/bin:$PATH" \
        -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x "UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline" \
        -x "UCX_TLS=$UCX_TLS" \
        -x "UCX_NET_DEVICES=$UCX_NET_DEVICES" \
        -x "OMP_NUM_THREADS=1" \
        "$BIN" \
            -c allreduce \
            -m host \
            -d "$DTYPE" \
            -o "$OP" \
            -b "$MIN_COUNT" \
            -e "$MAX_COUNT" \
            -f "$FACTOR" \
            -w "$WARMUP" \
            -n "$ITERS" \
            -F \
        2>&1 | tee "$out"

    awk -v arm="$arm" -v nranks="$nranks" -v ppn="$ppn" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print arm "," nranks "," ppn "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

# --- Sweeps -----------------------------------------------------------------
# Max-PPN (448 ranks) — primary measurement
run_arm "pipe" "auto" "$NP"       "$PPN"
run_arm "mono" "n"    "$NP"       "$PPN"

# Low-PPN (32 ranks, 8 ppn) — supplementary; pipeline benefit typically stronger
EXTRA_PPN=8
run_arm "pipe" "auto" "$EXTRA_NP" "$EXTRA_PPN"
run_arm "mono" "n"    "$EXTRA_NP" "$EXTRA_PPN"

# --- Summary ----------------------------------------------------------------
echo ""
echo "== Summary: avg bus BW (GB/s), pipeline=auto vs disabled =="
for nranks in "$NP" "$EXTRA_NP"; do
    ppn=$(( nranks / NNODES ))
    echo ""
    echo "-- nranks=$nranks (${ppn} ppn) --"
    printf "%12s %10s %10s %8s\n" "size_bytes" "pipe(GB/s)" "mono(GB/s)" "pipe/mono"
    sizes=$(awk -F, -v r="$nranks" 'NR>1 && $2==r {print $4}' "$SUMMARY_CSV" | sort -n -u)
    for sz in $sizes; do
        pipe_bw=$(awk -F, -v r="$nranks" -v s="$sz" \
            'NR>1 && $1=="pipe" && $2==r && $4==s {print $7}' "$SUMMARY_CSV" | tail -1)
        mono_bw=$(awk -F, -v r="$nranks" -v s="$sz" \
            'NR>1 && $1=="mono" && $2==r && $4==s {print $7}' "$SUMMARY_CSV" | tail -1)
        ratio=""
        if [[ -n "$pipe_bw" && -n "$mono_bw" && "$mono_bw" != "0" ]]; then
            ratio=$(awk -v p="$pipe_bw" -v m="$mono_bw" \
                'BEGIN { printf "%.2fx", p/m }')
        fi
        printf "%12s %10s %10s %8s\n" "$sz" "${pipe_bw:--}" "${mono_bw:--}" "${ratio:--}"
    done
done

echo ""
echo "== done: results in $RESULT_DIR =="
echo "done" > "$RESULT_DIR/done"
