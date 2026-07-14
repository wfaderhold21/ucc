#!/usr/bin/env bash
#
# Benchmark the onesided alltoall peer-ordering change: odd local ranks start
# remote-first, even local ranks start local-first, halving peak NIC pressure
# at t=0 (vs all ranks hitting the NIC simultaneously).
#
# Submit from the repository root:
#   sbatch contrib/slurm_alltoall_ordering_perftest.sh
#
# Common overrides:
#   sbatch --export=ALL,ORDERS="seq ilv full",PPN=16 \
#     contrib/slurm_alltoall_ordering_perftest.sh
#
# What is being measured
# ----------------------
# Three peer-ordering modes are compared head-to-head with GET (forced via
# UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=get):
#
#   seq   Sequential peer order (grank+1, grank+2, ...). All ranks start
#         remote simultaneously. This is the unmodified baseline.
#
#   ilv   Local/remote split with parity stagger, no coprime stride.
#         Odd local ranks: remote-first. Even local ranks: local-first.
#
#   full  Parity stagger + coprime stride + per-rank remote rotation.
#         Odd local ranks: remote-first (rotated so each odd rank hits a
#         different remote endpoint). Even local ranks: local-first.
#
# Fragmentation is fixed at NFRAGS=1 (disabled) so the ordering signal is
# not confounded by the fragmentation path. A single FRAG_SIZE=0 arm is used.
#
# Notes
# -----
# - ucc_perftest -b/-e are per-peer element counts; with -d uint8 the count
#   equals per-peer bytes.
# - -M global sets UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS, required for the
#   onesided path.
# - GET is forced explicitly so the result does not depend on the AUTO
#   threshold (CONGESTION_THRESHOLD=8), even though 32 PPN would pick it
#   anyway.

#SBATCH --job-name=ucc-a2a-order
#SBATCH --partition=thor
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=32
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

SRC_DIR=${HOME}/ucc-a2a-v3
JOB_ID=${SLURM_JOB_ID:-manual}
BUILD_DIR=${SRC_DIR}
INSTALL_DIR="$BUILD_DIR/install"
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/alltoall-ordering-$JOB_ID"}

NNODES=${SLURM_NNODES:-4}
NTASKS_PER_NODE=${SLURM_NTASKS_PER_NODE:-32}
NTASKS=${SLURM_NTASKS:-$((NNODES * NTASKS_PER_NODE))}

MIN_BYTES=${MIN_BYTES:-2K}
MAX_BYTES=${MAX_BYTES:-4M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

# Orders to compare. seq is the unchanged baseline; ilv and full exercise the
# new parity stagger. Override with e.g. ORDERS="seq full" to narrow the sweep.
ORDERS=${ORDERS:-"seq ilv full"}

# Single arm: fragmentation disabled. Keeps the ordering comparison clean.
ARMS=${ARMS:-"nofrag:1:0"}

PERFTEST_EXTRA_ARGS=${PERFTEST_EXTRA_ARGS:-}
MPIRUN_ARGS=${MPIRUN_ARGS:-}
SKIP_BUILD=1
PERFTEST_BIN=${PERFTEST_BIN:-"$INSTALL_DIR/bin/ucc_perftest"}

UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_3:1"}

MODULES=${MODULES:-"gcc hpcx"}

mkdir -p "$RESULT_DIR"

if [[ -n "${MODULES:-}" ]]; then
    set +e
    # shellcheck disable=SC1091
    source /etc/profile >/dev/null 2>&1
    for module_name in $MODULES; do
        module load "$module_name"
    done
    set -e
fi

UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}
if [[ -z "$MPI_HOME" ]] && command -v mpicc >/dev/null 2>&1; then
    MPI_HOME=$(dirname "$(dirname "$(command -v mpicc)")")
fi

echo "source:        $SRC_DIR"
echo "install:       $INSTALL_DIR"
echo "results:       $RESULT_DIR"
echo "nodes/tasks:   $NNODES nodes, $NTASKS_PER_NODE tasks/node, $NTASKS tasks"
echo "orders:        $ORDERS"
echo "size sweep:    $MIN_BYTES .. $MAX_BYTES bytes per peer, factor $FACTOR"

if [[ ! -x "$PERFTEST_BIN" ]]; then
    echo "ucc_perftest not found or not executable: $PERFTEST_BIN" >&2
    exit 1
fi

export PATH="$INSTALL_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$INSTALL_DIR/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if [[ -n "${UCX_HOME:-}" ]]; then
    export LD_LIBRARY_PATH="$UCX_HOME/lib:$UCX_HOME/lib64:$LD_LIBRARY_PATH"
fi

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export UCC_TL_UCP_TUNE=${UCC_TL_UCP_TUNE:-"alltoall:0-inf:@onesided"}
# Force GET so the ordering effect is visible regardless of AUTO threshold.
export UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=${UCC_TL_UCP_ALLTOALL_ONESIDED_ALG:-"get"}
export UCX_TLS
export UCX_NET_DEVICES

{
    echo "== environment =="
    echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=$UCC_TL_UCP_ALLTOALL_ONESIDED_ALG"
    env | grep -E '^(UCX_|UCC_)' | sort
} | tee "$RESULT_DIR/env.log"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "order,arm,nfrags,frag_size,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_perftest() {
    local order="$1" label="$2" nfrags="$3" frag_size="$4" out="$5"

    export UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER="$order"
    export UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS="$nfrags"
    export UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE="$frag_size"

    local cmd=(mpirun
        --np "$NTASKS"
        --map-by "ppr:${NTASKS_PER_NODE}:node"
        --bind-to core
    )
    cmd+=(
        -x PATH
        -x LD_LIBRARY_PATH
        -x OMP_NUM_THREADS
        -x UCC_TL_UCP_TUNE
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_ALG
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    if [[ -n "$MPIRUN_ARGS" ]]; then
        read -r -a extra_mpirun_args <<< "$MPIRUN_ARGS"
        cmd+=("${extra_mpirun_args[@]}")
    fi
    cmd+=(
        "$PERFTEST_BIN"
        -c alltoall
        -m host
        -d uint8
        -b "$MIN_BYTES"
        -e "$MAX_BYTES"
        -f "$FACTOR"
        -w "$WARMUP"
        -n "$ITERS"
        -M global
        -F
    )
    if [[ -n "$PERFTEST_EXTRA_ARGS" ]]; then
        read -r -a extra_perftest_args <<< "$PERFTEST_EXTRA_ARGS"
        cmd+=("${extra_perftest_args[@]}")
    fi

    {
        echo "order=$order arm=$label nfrags=$nfrags frag_size=$frag_size"
        echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=$UCC_TL_UCP_ALLTOALL_ONESIDED_ALG"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER=$order"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS=$nfrags"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE=$frag_size"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"

    awk -v order="$order" -v arm="$label" -v nfrags="$nfrags" \
        -v frag_size="$frag_size" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print order "," arm "," nfrags "," frag_size "," $2 "," $1 "," \
                  $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

for order in $ORDERS; do
    for arm in $ARMS; do
        label="${arm%%:*}"
        rest="${arm#*:}"
        nfrags="${rest%%:*}"
        frag_size="${rest#*:}"
        out="$RESULT_DIR/alltoall-order-${order}-arm-${label}.log"
        echo ">> run: order=$order arm=$label (nfrags=$nfrags frag_size=$frag_size)"
        run_perftest "$order" "$label" "$nfrags" "$frag_size" "$out"
    done
done

echo
echo "== summary (avg bus bandwidth, GB/s) =="
echo "wrote $SUMMARY_CSV"
{
    printf "%12s" "size"
    for order in $ORDERS; do printf "%13s" "$order"; done
    printf "\n"
    sizes=$(awk -F, 'NR>1 {print $5}' "$SUMMARY_CSV" | sort -n -u)
    for sz in $sizes; do
        printf "%12s" "$sz"
        for order in $ORDERS; do
            bw=$(awk -F, -v o="$order" -v s="$sz" \
                    'NR>1 && $1==o && $5==s {print $8}' \
                    "$SUMMARY_CSV" | tail -1)
            printf "%13s" "${bw:--}"
        done
        printf "\n"
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
