#!/usr/bin/env bash
#
# Controlled GAIA allreduce comparison: exactly 4 nodes x 8 ranks/node = 32
# ranks.  Each SRA pipeline cell has an immediately adjacent @ring control with
# the same placement, transport, datatype/op, count sweep, warmups and iters.
#
# The 5 x 5 SRA grid is PDEPTHS x FRAGS_KB.  Ring has no SRA pipeline knobs,
# but is deliberately repeated for every grid point so every comparison has a
# separately recorded, otherwise-identical control.  Power-of-two counts and
# 32 ranks ensure count %% tsize == 0 for the ring implementation.
#
# Submit only after reviewing the local validation output:
#   bash -n contrib/slurm_allreduce_sra_gaia_32rank_sweep_ring_comparison.sh
#   VALIDATE_ONLY=1 bash contrib/slurm_allreduce_sra_gaia_32rank_sweep_ring_comparison.sh
#   sbatch contrib/slurm_allreduce_sra_gaia_32rank_sweep_ring_comparison.sh
#
# By default the submitted, isolated source archive is built in the allocation.
# To reuse a known-compatible installation, pass SKIP_BUILD=1 and INSTALL_DIR.

#SBATCH --job-name=ucc-sra-gaia-32r-ring
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
export PATH="$MPI_HOME/bin:$PATH"

# SLURM spools the script, so locate the submitted source via submit directory.
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}
SCRIPT_REL=contrib/slurm_allreduce_sra_gaia_32rank_sweep_ring_comparison.sh
SCRIPT_PATH="$SRC_DIR/$SCRIPT_REL"
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-${JOB_ID}"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-32r-sra-ring-${JOB_ID}"}
VALIDATE_ONLY=${VALIDATE_ONLY:-0}
SKIP_BUILD=${SKIP_BUILD:-0}
# A submitted source archive has no .git directory.  The submission command
# supplies these from the producing checkout; a direct checkout derives them.
SOURCE_BRANCH=${SOURCE_BRANCH:-$(git -C "$SRC_DIR" branch --show-current 2>/dev/null || printf unknown)}
SOURCE_COMMIT=${SOURCE_COMMIT:-$(git -C "$SRC_DIR" rev-parse HEAD 2>/dev/null || printf unknown)}

# These are invariants, rather than submission-time tuning knobs.  Changing a
# #SBATCH option without changing these checks causes the job to fail early.
EXPECTED_NNODES=4
EXPECTED_PPN=8
EXPECTED_NP=32
NNODES=${SLURM_NNODES:-$EXPECTED_NNODES}
PPN=${SLURM_NTASKS_PER_NODE:-$EXPECTED_PPN}
NP=$((NNODES * PPN))

DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
# float32: 32K..4M elements = 128KiB..16MiB, 8 points, all divisible by 32.
MIN_COUNT=${MIN_COUNT:-32K}
MAX_COUNT=${MAX_COUNT:-4M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}
UCX_TLS=${UCX_TLS:-sm,dc}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-mlx5_0:1}
PDEPTHS=${PDEPTHS:-"1 2 4 8 16"}
FRAGS_KB=${FRAGS_KB:-"256 512 1024 2048 4096"}
THRESH=${THRESH:-64K}
NFRAGS=${NFRAGS:-2}

die() { echo "ERROR: $*" >&2; exit 2; }

validate_matrix() {
    [[ "$NNODES" == "$EXPECTED_NNODES" ]] || die "requires exactly 4 nodes (got $NNODES)"
    [[ "$PPN" == "$EXPECTED_PPN" ]] || die "requires exactly 8 ranks/node (got $PPN)"
    [[ "$NP" == "$EXPECTED_NP" ]] || die "requires exactly 32 ranks (got $NP)"
    [[ "$DTYPE" == float32 ]] || die "fixed comparison datatype is float32 (got $DTYPE)"
    [[ "$OP" == sum ]] || die "fixed comparison operation is sum (got $OP)"
    [[ "$MIN_COUNT" == 32K && "$MAX_COUNT" == 4M && "$FACTOR" == 2 ]] ||
        die "fixed count sweep is 32K..4M, factor 2"
    [[ "$WARMUP" == 20 && "$ITERS" == 100 ]] || die "fixed warmups/iters are 20/100"
    [[ "$UCX_TLS" == sm,dc && "$UCX_NET_DEVICES" == mlx5_0:1 ]] ||
        die "fixed UCX transport/device are sm,dc / mlx5_0:1"
    [[ "$PDEPTHS" == "1 2 4 8 16" && "$FRAGS_KB" == "256 512 1024 2048 4096" ]] ||
        die "fixed SRA matrix is 5 depths x 5 fragment sizes"
    [[ "$THRESH" == 64K && "$NFRAGS" == 2 ]] || die "fixed SRA threshold/nfrags are 64K/2"
    local depths frags
    read -r -a depths <<< "$PDEPTHS"
    read -r -a frags <<< "$FRAGS_KB"
    [[ ${#depths[@]} -eq 5 && ${#frags[@]} -eq 5 ]] || die "matrix must contain 25 SRA cells"
    echo "matrix: ${#depths[@]} SRA depths x ${#frags[@]} fragment sizes = $(( ${#depths[@]} * ${#frags[@]} )) paired SRA/ring points"
}

validate_matrix
if [[ "$VALIDATE_ONLY" == 1 ]]; then
    echo "validation: exact 4 x 8 = 32 geometry; 25 SRA + 25 paired ring runs"
    echo "validation: ring divisibility holds: all power-of-two counts are divisible by 32"
    exit 0
fi

mkdir -p "$RESULT_DIR"

if [[ "$SKIP_BUILD" != 1 ]]; then
    echo "building submitted revision in $SRC_DIR"
    cd "$SRC_DIR"
    # Source archives made from git-tracked files do not contain the generated
    # configure script.  Bootstrap it explicitly instead of assuming that the
    # submitting worktree's generated file was archived.
    if [[ ! -x ./configure ]]; then
        [[ -x ./autogen.sh ]] || die "missing both ./configure and ./autogen.sh"
        ./autogen.sh
    fi
    ./configure --prefix="$INSTALL_DIR" --with-mpi="$MPI_HOME" --with-ucx="$UCX_HOME" \
        --without-cuda --without-sharp
    make -j"$(nproc)"
    make install
fi
[[ -x "$INSTALL_DIR/bin/ucc_perftest" ]] || die "missing $INSTALL_DIR/bin/ucc_perftest; set INSTALL_DIR or do not set SKIP_BUILD=1"

UCC_LIB="$INSTALL_DIR/lib:$INSTALL_DIR/lib64"
MPI_LIB="$MPI_HOME/lib:$MPI_HOME/lib64"
UCX_LIB="$UCX_HOME/lib:$UCX_HOME/lib64"
RUN_LD="$UCC_LIB:$MPI_LIB:$UCX_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
BIN="$INSTALL_DIR/bin/ucc_perftest"
MANIFEST="$RESULT_DIR/run-manifest.txt"
SUMMARY_CSV="$RESULT_DIR/summary.csv"

{
    echo "script: $SCRIPT_REL"
    sha256sum "$SCRIPT_PATH"
    echo "submitted_job_id: $JOB_ID"
    echo "output_path: ${SLURM_JOB_ID:+$SLURM_SUBMIT_DIR/}slurm-ucc-sra-gaia-32r-ring-${JOB_ID}.out"
    echo "error_path: ${SLURM_JOB_ID:+$SLURM_SUBMIT_DIR/}slurm-ucc-sra-gaia-32r-ring-${JOB_ID}.err"
    echo "result_dir: $RESULT_DIR"
    echo "branch: $SOURCE_BRANCH"
    echo "commit: $SOURCE_COMMIT"
    echo "worktree_status:"
    if git -C "$SRC_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
        git -C "$SRC_DIR" status --short
    else
        echo "source archive (no worktree metadata)"
    fi
    echo "geometry: $NNODES nodes x $PPN ranks/node = $NP ranks"
    echo "placement: --map-by ppr:${PPN}:node --bind-to core"
    echo "datatype/op: $DTYPE/$OP"
    echo "counts: $MIN_COUNT..$MAX_COUNT elements, factor $FACTOR"
    echo "warmup/iterations: $WARMUP/$ITERS"
    echo "ucx_tls/device: $UCX_TLS / $UCX_NET_DEVICES"
    echo "sra_selection: UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial"
    echo "sra_pipeline_template: thresh=${THRESH}:fragsize=<frag>K:nfrags=${NFRAGS}:pdepth=<depth>:parallel"
    echo "ring_selection: UCC_TL_UCP_TUNE=allreduce:0-inf:@ring"
    echo "ring_pipeline: unset (not applicable)"
    echo "matrix: pdepth={$PDEPTHS}; frag_kb={$FRAGS_KB}; 25 paired points"
} | tee "$MANIFEST"

echo "arm,point,frag_kb,pdepth,algorithm,pipeline,nranks,ppn,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" > "$SUMMARY_CSV"

run_arm() {
    local arm="$1" point="$2" frag="$3" depth="$4" pipeline="$5" out
    out="$RESULT_DIR/${point}-${arm}.log"
    local -a cmd=("$MPI_HOME/bin/mpirun" --np "$NP" --map-by "ppr:${PPN}:node" --bind-to core
        -x "PATH=$INSTALL_DIR/bin:$PATH" -x "LD_LIBRARY_PATH=$RUN_LD"
        -x "UCC_TL_UCP_TUNE=allreduce:0-inf:@${arm}" -x "UCX_TLS=$UCX_TLS"
        -x "UCX_NET_DEVICES=$UCX_NET_DEVICES" -x "OMP_NUM_THREADS=1")
    if [[ "$arm" == sra_knomial ]]; then
        cmd+=(-x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline")
    fi
    cmd+=("$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" -w "$WARMUP" -n "$ITERS" -F)
    {
        echo "arm=$arm point=$point frag_kb=$frag pdepth=$depth pipeline=${pipeline:--}"
        printf 'command:'; printf ' %q' "${cmd[@]}"; printf '\n'
    } | tee "$out"
    if ! "${cmd[@]}" 2>&1 | tee -a "$out"; then
        echo "$point,$arm" >> "$RESULT_DIR/failed_arms"
    fi
    awk -v a="$arm" -v pt="$point" -v f="$frag" -v d="$depth" -v p="$pipeline" -v r="$NP" -v ppn="$PPN" \
        '/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {print a "," pt "," f "," d "," a "," p "," r "," ppn "," $2 "," $1 "," $3 "," $6 "," $7 "," $8}' \
        "$out" >> "$SUMMARY_CSV"
}

for frag in $FRAGS_KB; do
    for depth in $PDEPTHS; do
        point="f${frag}k_d${depth}"
        pipeline="thresh=${THRESH}:fragsize=${frag}K:nfrags=${NFRAGS}:pdepth=${depth}:parallel"
        run_arm sra_knomial "$point" "$frag" "$depth" "$pipeline"
        run_arm ring "$point" "$frag" "$depth" "-"
    done
done

echo "completed: $RESULT_DIR" | tee -a "$MANIFEST"
