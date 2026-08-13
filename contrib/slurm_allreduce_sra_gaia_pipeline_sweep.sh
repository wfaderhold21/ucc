#!/usr/bin/env bash
#
# GAIA 4-node SRA-knomial allreduce pipeline sweep: full Cartesian product of
# pipeline depth x fragment size, at one fixed geometry.
#
#   pipeline depth  (pdepth) : 1 2 4 8 16      (fragments in flight)
#   fragment size   (frag)   : 256 512 1024 2048 4096  -- UNITS: KiB (KB=1024B)
#
# 25 cells + one monolithic (non-pipelined) baseline arm = 26 runs. Every other
# benchmark variable (nodes, PPN, dtype, op, size sweep, warmup, iters, UCX
# transport/device, binding) is held constant across cells.
#
# This is the GAIA counterpart of contrib/slurm_allreduce_sra_pipeline_sweep.sh
# (thor). The thor script is left untouched. GAIA-specific choices vs thor:
#
#   partition     GAIA (dgx-gaia-*)             thor used --partition=thor
#   modules       none -- GAIA has no module env; MPI/UCX come from fixed
#                 system paths (/usr/mpi/gcc/openmpi-4.1.9a1, /usr) exactly as
#                 in contrib/slurm_allreduce_sra_gaia_4node_cpu.sh
#   launcher      $MPI_HOME/bin/mpirun by absolute path (no module-provided
#                 mpirun on PATH at job start)
#   paths         SRC_DIR defaults to the directory containing this script's
#                 parent (the rsync'd tree under /labhome/faderholdt/...),
#                 not ~/ucc as on thor
#   UCX_TLS       sm,dc -- RC exhausts HCA QP resources at high PPN on these
#                 nodes (syndrome 0x65b500); DC is required for the max-PPN
#                 arm and is kept here so cells are comparable to task22 data
#   placement     --map-by ppr:PPN:node --bind-to core, PPN=8 -> 32 ranks on
#                 4 nodes (one rank per NUMA-ish slice, cross-node IB path is
#                 the object of study)
#
# NOTE ON THE MULTI-NODE GUARD (commit 0b0a1f01): that guard only fires on the
# *auto* path. ucc_pipeline_params_is_auto() is checked before it, so an
# explicit UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE string is used verbatim and all
# 25 cells are genuinely exercised on a 4-node team.
#
# Submit from the rsync'd repository root on the gaia login node:
#   sbatch contrib/slurm_allreduce_sra_gaia_pipeline_sweep.sh
# Reuse an existing install (no rebuild):
#   sbatch --export=ALL,SKIP_BUILD=1,INSTALL_DIR=/path/to/install ...

#SBATCH --job-name=ucc-sra-gaia-pdepth-frag
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

# --- Paths (GAIA-specific) ---------------------------------------------------
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
export PATH="$MPI_HOME/bin:$PATH"

# SLURM copies the batch script into /var/spool/slurm/d/, so BASH_SOURCE does
# NOT point into the repository under sbatch. Prefer SLURM_SUBMIT_DIR (the
# directory the job was submitted from = the repo root); fall back to the
# script's parent only when run directly from a shell.
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-${JOB_ID}"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-pdepth-frag-${JOB_ID}"}
SKIP_BUILD=${SKIP_BUILD:-0}

# --- Geometry (held constant across all cells) -------------------------------
NNODES=${SLURM_NNODES:-4}
PPN=${PPN:-${SLURM_NTASKS_PER_NODE:-8}}
NP=${NP:-$((NNODES * PPN))}

# --- Benchmark config (held constant across all cells) -----------------------
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
# 32K..4M elements @ float32 = 128KB..16MB, factor 2 -> 8 size points.
MIN_COUNT=${MIN_COUNT:-32K}
MAX_COUNT=${MAX_COUNT:-4M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

UCX_TLS=${UCX_TLS:-"sm,dc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_0:1"}

# --- Sweep axes --------------------------------------------------------------
# FRAGS_KB is in KiB; it is emitted into the pipeline string as "<n>K".
FRAGS_KB=${FRAGS_KB:-"256 512 1024 2048 4096"}
PDEPTHS=${PDEPTHS:-"1 2 4 8 16"}
# Pipelining threshold, in bytes. Kept below MIN_COUNT*sizeof(dtype) (128KB) so
# every size point in the sweep is pipelined in every cell. The parser seeds
# threshold=SIZE_MAX, so it MUST be set explicitly or pipelining stays off.
THRESH=${THRESH:-64K}
# n_frags is the floor on fragment count; frag_size is what actually varies.
NFRAGS=${NFRAGS:-2}
# Include the monolithic (non-pipelined) control arm.
RUN_MONO=${RUN_MONO:-1}

mkdir -p "$RESULT_DIR"

# --- Build -------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "== building UCC (no CUDA) in $SRC_DIR =="
    cd "$SRC_DIR"
    make distclean 2>/dev/null || true
    ./configure \
        --prefix="$INSTALL_DIR" \
        --with-mpi="$MPI_HOME" \
        --with-ucx="$UCX_HOME" \
        --without-cuda \
        --without-sharp
    make -j"$(nproc)"
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

echo "install:      $INSTALL_DIR"
echo "results:      $RESULT_DIR"
echo "nodelist:     ${SLURM_JOB_NODELIST:-unknown}"
echo "geometry:     $NNODES nodes x $PPN ppn = $NP ranks"
echo "dtype/op:     $DTYPE / $OP"
echo "count sweep:  $MIN_COUNT..$MAX_COUNT elems, factor $FACTOR (warmup $WARMUP, iters $ITERS)"
echo "UCX_TLS:      $UCX_TLS   UCX_NET_DEVICES: $UCX_NET_DEVICES"
echo "pdepths:      $PDEPTHS"
echo "frag sizes:   $FRAGS_KB (KiB)"
echo "threshold:    $THRESH (bytes)  nfrags floor: $NFRAGS"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "config,frag_kb,pdepth,pipeline,nranks,ppn,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_one() {
    local label="$1" pipeline="$2" frag_kb="$3" pdepth="$4"
    local out="$RESULT_DIR/cell-${label}.log"

    echo ""
    echo ">> cell=$label frag_kb=$frag_kb pdepth=$pdepth pipeline=$pipeline"

    {
        echo "cell=$label frag_kb=$frag_kb pdepth=$pdepth pipeline=$pipeline"
        echo "nranks=$NP ppn=$PPN nodes=${SLURM_JOB_NODELIST:-unknown}"
    } > "$out"

    if ! "$MPI_HOME/bin/mpirun" \
        --np "$NP" \
        --map-by "ppr:${PPN}:node" \
        --bind-to core \
        -x "PATH=$INSTALL_DIR/bin:$PATH" \
        -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x "UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline" \
        -x "UCX_TLS=$UCX_TLS" \
        -x "UCX_NET_DEVICES=$UCX_NET_DEVICES" \
        -x "OMP_NUM_THREADS=1" \
        "$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" \
               -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" \
               -w "$WARMUP" -n "$ITERS" -F \
        2>&1 | tee -a "$out"
    then
        echo "!! cell $label FAILED (see $out)" | tee -a "$out"
        echo "$label" >> "$RESULT_DIR/failed_cells"
    fi

    awk -v c="$label" -v fk="$frag_kb" -v pd="$pdepth" -v p="$pipeline" \
        -v r="$NP" -v ppn="$PPN" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print c "," fk "," pd "," p "," r "," ppn "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

# --- Sweep: full Cartesian product ------------------------------------------
if [[ "$RUN_MONO" == "1" ]]; then
    run_one "mono" "n" "-" "-"
fi

for f in $FRAGS_KB; do
    for d in $PDEPTHS; do
        run_one "f${f}k_d${d}" \
                "thresh=${THRESH}:fragsize=${f}K:nfrags=${NFRAGS}:pdepth=${d}:parallel" \
                "$f" "$d"
    done
done

# --- Completeness check ------------------------------------------------------
echo ""
echo "== completeness =="
# Expected number of size points per cell, derived from the count sweep so a
# short/truncated cell is flagged as INVALID rather than silently accepted.
EXPECT_ROWS=$(awk -F, 'NR>1 {print $7}' "$SUMMARY_CSV" | sort -n -u | wc -l)
total=0; missing=0; invalid=0; ok=0
for f in $FRAGS_KB; do
    for d in $PDEPTHS; do
        total=$((total + 1))
        n=$(awk -F, -v c="f${f}k_d${d}" 'NR>1 && $1==c {n++} END {print n+0}' "$SUMMARY_CSV")
        if [[ "$n" -eq 0 ]]; then
            echo "MISSING cell frag=${f}K pdepth=${d} (no data rows)"
            missing=$((missing + 1))
        elif [[ "$n" -ne "$EXPECT_ROWS" ]]; then
            echo "INVALID cell frag=${f}K pdepth=${d} ($n rows, expected $EXPECT_ROWS)"
            invalid=$((invalid + 1))
        else
            ok=$((ok + 1))
        fi
    done
done
echo "cells complete: $ok/$total   (missing $missing, invalid $invalid, expected rows/cell $EXPECT_ROWS)"
if [[ "$missing" -ne 0 || "$invalid" -ne 0 ]]; then
    echo "WARNING: sweep is INCOMPLETE -- do not compare these cells as a full matrix"
fi
[[ -f "$RESULT_DIR/failed_cells" ]] && \
    { echo "cells whose mpirun exited nonzero:"; cat "$RESULT_DIR/failed_cells"; }

# --- Summary matrices --------------------------------------------------------
{
    echo "== bw_avg (GB/s): rows=pdepth, cols=frag_size(KiB), one block per size =="
    echo "geometry: $NNODES x $PPN = $NP ranks, $DTYPE/$OP, warmup $WARMUP iters $ITERS"
    sizes=$(awk -F, 'NR>1 {print $7}' "$SUMMARY_CSV" | sort -n -u)
    for sz in $sizes; do
        mono=$(awk -F, -v s="$sz" 'NR>1 && $1=="mono" && $7==s {print $10}' \
                "$SUMMARY_CSV" | tail -1)
        echo ""
        echo "-- size_bytes=$sz   mono=${mono:--} GB/s --"
        printf "%8s" "pdepth"
        for f in $FRAGS_KB; do printf "%10s" "${f}K"; done
        printf "\n"
        for d in $PDEPTHS; do
            printf "%8s" "$d"
            for f in $FRAGS_KB; do
                bw=$(awk -F, -v c="f${f}k_d${d}" -v s="$sz" \
                        'NR>1 && $1==c && $7==s {print $10}' "$SUMMARY_CSV" | tail -1)
                printf "%10s" "${bw:--}"
            done
            printf "\n"
        done
    done
} | tee "$RESULT_DIR/summary.txt"

echo ""
echo "== done: results in $RESULT_DIR =="
echo "done" > "$RESULT_DIR/done"
