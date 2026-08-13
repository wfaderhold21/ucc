#!/usr/bin/env bash
#
# Common implementation for the Gaia and Thor four-node SRA guard jobs.
# Invoke through one of the slurm_allreduce_sra_guard_* wrappers.

set -euo pipefail

SRC_DIR=${SRC_DIR:?set SRC_DIR to the ucc-sra checkout}
JOB_ID=${SLURM_JOB_ID:-manual}
MATRIX=${MATRIX:?set MATRIX=gaia or thor}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-guard-$MATRIX-$JOB_ID"}
BUILD_DIR=${BUILD_DIR:-"$SRC_DIR/build-guard-$MATRIX-$JOB_ID"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/sra-guard-$MATRIX-$JOB_ID"}
SKIP_BUILD=${SKIP_BUILD:-0}
BUILD_JOBS=${BUILD_JOBS:-16}
MODULES=${MODULES:-}
RUN_TIMEOUT=${RUN_TIMEOUT:-20m}

MIN_COUNT=${MIN_COUNT:-16K}
MAX_COUNT=${MAX_COUNT:-16M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}

PIPE_AUTO=auto
PIPE_FORCED=${PIPE_FORCED:-"thresh=256K:fragsize=512K:nfrags=2:pdepth=4:parallel"}
PIPE_MONO=n

mkdir -p "$BUILD_DIR" "$RESULT_DIR"

if [[ -n "$MODULES" ]]; then
    set +eu
    # shellcheck disable=SC1091
    source /etc/profile >/dev/null 2>&1
    for module_name in $MODULES; do
        module load "$module_name"
    done
    set -eu
fi

MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}
UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
if [[ -z "$MPI_HOME" ]] && command -v mpicc >/dev/null 2>&1; then
    MPI_HOME=$(dirname "$(dirname "$(command -v mpicc)")")
fi
[[ -n "$MPI_HOME" && -n "$UCX_HOME" ]] ||
    { echo "MPI_HOME and UCX_HOME must resolve before build" >&2; exit 2; }
MPI_RUN=${MPI_RUN:-"$MPI_HOME/bin/mpirun"}

if [[ "$SKIP_BUILD" != "1" ]]; then
    [[ -x "$SRC_DIR/configure" ]] || (cd "$SRC_DIR" && ./autogen.sh)
    (
        cd "$BUILD_DIR"
        "$SRC_DIR/configure" --prefix="$INSTALL_DIR" \
            --with-mpi="$MPI_HOME" --with-ucx="$UCX_HOME" \
            --without-cuda --without-sharp
        make -j"$BUILD_JOBS"
        make install
    )
fi

PERFTEST="$INSTALL_DIR/bin/ucc_perftest"
[[ -x "$PERFTEST" ]] || { echo "missing $PERFTEST" >&2; exit 2; }

mapfile -t ALLOCATED_NODES < <(scontrol show hostnames "${SLURM_JOB_NODELIST}")
FIRST_NODE=${ALLOCATED_NODES[0]}
NNODES=${SLURM_NNODES:-4}
[[ "$NNODES" -eq 4 ]] ||
    { echo "SRA guard matrix requires exactly four allocated nodes" >&2; exit 2; }

RUN_LD="$INSTALL_DIR/lib:$INSTALL_DIR/lib64:$MPI_HOME/lib:$MPI_HOME/lib64:$UCX_HOME/lib:$UCX_HOME/lib64"
RUN_LD="${RUN_LD}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

{
    echo "job_id=$JOB_ID"
    echo "matrix=$MATRIX"
    echo "nodes=${SLURM_JOB_NODELIST}"
    printf "node=%s\n" "${ALLOCATED_NODES[@]}"
    echo "source=$SRC_DIR"
    git -C "$SRC_DIR" rev-parse HEAD
    echo "install=$INSTALL_DIR"
    echo "mpi=$MPI_HOME"
    echo "ucx=$UCX_HOME"
    "$MPI_RUN" --version | sed -n '1,2p'
} > "$RESULT_DIR/provenance.txt" 2>&1

SUMMARY="$RESULT_DIR/summary.csv"
echo "label,nranks,ppn,tls,devices,ib_paths,pipeline,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs,rc" > "$SUMMARY"
overall_rc=0

run_arm() {
    local label="$1" nranks="$2" ppn="$3" tls="$4" devices="$5"
    local ib_paths="$6" pipeline="$7" placement="${8:-all}"
    local out="$RESULT_DIR/${label}.log"
    local host_args=()
    if [[ "$placement" == "first" ]]; then
        host_args=(--host "${FIRST_NODE}:${ppn}")
    fi

    echo "run=$label ranks=$nranks ppn=$ppn tls=$tls devices=$devices paths=$ib_paths pipeline=$pipeline"
    set +e
    timeout -k 30s "$RUN_TIMEOUT" \
        "$MPI_RUN" -np "$nranks" "${host_args[@]}" \
        --map-by "ppr:${ppn}:node" --bind-to core \
        -x "PATH=$INSTALL_DIR/bin:$PATH" \
        -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x "OMP_NUM_THREADS=1" \
        -x "UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline" \
        -x "UCX_TLS=$tls" \
        -x "UCX_NET_DEVICES=$devices" \
        -x "UCX_IB_NUM_PATHS=$ib_paths" \
        "$PERFTEST" -c allreduce -m host -d "$DTYPE" -o "$OP" \
        -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" \
        -w "$WARMUP" -n "$ITERS" -F \
        2>&1 | tee "$out"
    local rc=${PIPESTATUS[0]}
    set -e
    (( rc == 0 )) || overall_rc=1

    awk -v label="$label" -v nranks="$nranks" -v ppn="$ppn" \
        -v tls="$tls" -v devices="$devices" -v paths="$ib_paths" \
        -v pipeline="$pipeline" -v rc="$rc" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print label "," nranks "," ppn "," tls "," devices "," paths "," \
                  pipeline "," $2 "," $1 "," $3 "," $6 "," $7 "," $8 "," rc
        }' "$out" >> "$SUMMARY"
}

run_triplet() {
    local prefix="$1" nranks="$2" ppn="$3" tls="$4" devices="$5"
    local paths="$6" placement="${7:-all}"
    run_arm "${prefix}-auto" "$nranks" "$ppn" "$tls" "$devices" "$paths" "$PIPE_AUTO" "$placement"
    run_arm "${prefix}-forced" "$nranks" "$ppn" "$tls" "$devices" "$paths" "$PIPE_FORCED" "$placement"
    run_arm "${prefix}-mono" "$nranks" "$ppn" "$tls" "$devices" "$paths" "$PIPE_MONO" "$placement"
}

case "$MATRIX" in
    gaia)
        # One-node control: automatic pipelining should remain enabled.
        run_triplet single-8r 8 8 "self,sm" "mlx5_0:1" 1 first

        # Low-PPN cross-node controls: DC, dual-port DC, and RC.
        run_triplet dc1-32r 32 8 "sm,dc" "mlx5_0:1" 1
        run_triplet dc2-32r 32 8 "sm,dc" "mlx5_0:1,mlx5_1:1" 2
        run_triplet rc1-32r 32 8 "sm,rc" "mlx5_0:1" 1

        # Max-PPN geometry. RC is intentionally excluded due to QP exhaustion.
        run_triplet dc1-448r 448 112 "sm,dc" "mlx5_0:1" 1
        run_triplet dc2-448r 448 112 "sm,dc" "mlx5_0:1,mlx5_1:1" 2
        ;;
    thor)
        run_triplet single-8r 8 8 "self,sm" "mlx5_0:1" 1 first
        run_triplet rc1-32r 32 8 "sm,rc" "mlx5_0:1" 1
        run_triplet rc1-128r 128 32 "sm,rc" "mlx5_0:1" 1
        ;;
    *)
        echo "unknown MATRIX=$MATRIX" >&2
        exit 2
        ;;
esac

if (( overall_rc == 0 )); then
    echo done > "$RESULT_DIR/done"
else
    echo "one or more SRA matrix arms failed" >&2
fi
exit "$overall_rc"
