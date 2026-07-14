#!/usr/bin/env bash
#
# Build UCC and benchmark the onesided alltoall message-fragmentation path
# (ALLTOALL_ONESIDED_NFRAGS) and, secondarily, the peer-issue ordering
# (ALLTOALL_ONESIDED_ORDER) with ucc_perftest.
#
# Submit from the repository root (default target: thor, 4 nodes x 32 PPN):
#   sbatch contrib/slurm_alltoall_order_perftest.sh
#
# Common overrides:
#   sbatch --export=ALL,UCX_HOME=/path/to/ucx,NFRAGS="0 1 2 4 8",ORDERS="seq" \
#     contrib/slurm_alltoall_order_perftest.sh
#
# What is being measured
# ----------------------
# The onesided alltoall paces outstanding RMA with a "tokens" inflight window
# derived from a UCX perf probe. When that window drops to <=1 whole per-peer
# message, a single message no longer fits the budget, so the per-peer block is
# split into `nfrags` fragments and a fragment becomes the inflight unit. This
# only engages when per-peer bytes > 8 KiB (ALLTOALL_ONESIDED_MIN_FRAG_SZ) and
# is capped at 64 fragments.
#
# On master this count is chosen automatically with no way to disable it, so a
# clean frag-vs-no-frag comparison is impossible. This branch adds the config
# knob ALLTOALL_ONESIDED_NFRAGS:
#     0  -> automatic (default; count falls out of the pacing model)
#     1  -> fragmentation disabled  (baseline / master-equivalent behavior)
#    >1  -> force that many fragments (still clamped by the 8 KiB floor and the
#           64-fragment ceiling)
# so this script can A/B the feature: NFRAGS=1 is the control, NFRAGS=0 is what
# ships, and NFRAGS=2,4,8,... traces the sensitivity curve.
#
# Notes
# -----
# - ucc_perftest -b/-e are per-peer element counts for alltoall; with -d uint8
#   the count equals per-peer bytes, which is exactly what gates fragmentation.
# - -M global exports/imports global mem handles and sets
#   UCC_COLL_ARGS_FLAG_MEM_MAPPED_BUFFERS, required for the onesided path.
# - Memory: per-rank src+dst buffers are 2 * per_peer_bytes * ntasks. At 128
#   ranks and MAX_BYTES=4M that is ~1 GiB/rank (~32 GiB/node). Raise MAX_BYTES
#   only if the node has the RAM for it.

#SBATCH --job-name=ucc-a2a-nfrags
#SBATCH --partition=thor
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=32
#SBATCH --time=03:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

SRC_DIR=${HOME}/ucc-staging/ucc-a2a-v3/
JOB_ID=${SLURM_JOB_ID:-manual}
BUILD_ROOT=$SRC_DIR
BUILD_DIR=${BUILD_ROOT}
INSTALL_DIR="$BUILD_DIR/install"
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/alltoall-nfrags-$JOB_ID"}

NNODES=${SLURM_NNODES:-4}
NTASKS_PER_NODE=${SLURM_NTASKS_PER_NODE:-32}
NTASKS=${SLURM_NTASKS:-$((NNODES * NTASKS_PER_NODE))}
BUILD_JOBS=${BUILD_JOBS:-${SLURM_CPUS_ON_NODE:-8}}

# Size sweep is centered on the fragmentation regime. Fragmentation only
# engages above 8 KiB per peer, so the sweep spans below the floor (control /
# no-harm check) up through the large sizes where it should pay off.
MIN_BYTES=${MIN_BYTES:-2K}
MAX_BYTES=${MAX_BYTES:-4M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

# Primary dimension: fragmentation "arms", each "label:NFRAGS:FRAG_SIZE".
#   off          NFRAGS=1            -> fragmentation disabled (baseline)
#   auto-legacy  NFRAGS=0 FRAG_SIZE=0-> shipped pacing-token heuristic (arm A)
#   fs64k        NFRAGS=0 FRAG_SIZE=64K -> new fixed-fragment-size heuristic (B)
#   fs32k/fs128k               -> bracket 64K to confirm the ~64K sweet spot
# This A/Bs the new ceil(per_peer_bytes/FRAG_SIZE) selection against both the
# current auto path and the no-frag baseline in one binary.
ARMS=${ARMS:-"off:1:0 auto-legacy:0:0 fs32k:0:32768 fs64k:0:65536 fs128k:0:131072"}
# Secondary dimension: peer issue order. Held at the sequential baseline by
# default so the arm comparison is not contaminated by ordering. Set to
# e.g. "seq full" to cross fragmentation with the interleaved ordering.
ORDERS=${ORDERS:-"seq"}

PERFTEST_EXTRA_ARGS=${PERFTEST_EXTRA_ARGS:-}
CONFIGURE_ARGS=${CONFIGURE_ARGS:-}
MPIRUN_ARGS=${MPIRUN_ARGS:-}
SKIP_BUILD=1
PERFTEST_BIN=${PERFTEST_BIN:-"$INSTALL_DIR/bin/ucc_perftest"}

# UCX transport selection, forwarded into every mpirun via -x. sm,rc pins the
# intra-node (shared memory) and inter-node (RC over IB) transports; the net
# device pins the HCA port so the onesided path can't silently drift onto a
# different NIC between runs.
UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_3:1"}

# On thor the toolchain, UCX and MPI all come from `module load gcc hpcx`;
# hpcx only populates the environment on the compute nodes, not the login node.
# Override MODULES="" to skip, or set it to your cluster's module names.
MODULES=${MODULES:-"gcc hpcx"}

mkdir -p "$RESULT_DIR"

if [[ -n "${MODULES:-}" ]]; then
    # /etc/profile and `module` can run commands that return nonzero; under
    # `set -e` that would kill the script mid-source before anything prints.
    # Disable errexit just for the environment bring-up, then restore it.
    set +e
    # shellcheck disable=SC1091
    source /etc/profile >/dev/null 2>&1
    for module_name in $MODULES; do
        module load "$module_name"
    done
    set -e
fi

# Resolve UCX / MPI locations after modules are loaded. hpcx exports
# HPCX_UCX_DIR / HPCX_MPI_DIR; fall back to explicit *_HOME overrides or to
# whatever mpicc is on PATH. ucc_perftest is MPI-bootstrapped, so --with-mpi is
# required or the perftest binary is silently not built.
UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}
if [[ -z "$MPI_HOME" ]] && command -v mpicc >/dev/null 2>&1; then
    MPI_HOME=$(dirname "$(dirname "$(command -v mpicc)")")
fi

echo "source:        $SRC_DIR"
echo "build:         $BUILD_DIR"
echo "install:       $INSTALL_DIR"
echo "results:       $RESULT_DIR"
echo "nodes/tasks:   $NNODES nodes, $NTASKS_PER_NODE tasks/node, $NTASKS tasks"
echo "nfrags:        $NFRAGS"
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
export UCX_TLS
export UCX_NET_DEVICES

# Log the UCX/UCC transport environment once so a policy change can never be
# confused with an accidental transport shift between runs.
{
    echo "== environment =="
    echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
    env | grep -E '^(UCX_|UCC_)' | sort
} | tee "$RESULT_DIR/env.log"

# summary.csv accumulates one row per (order, arm, size) with avg bus BW.
SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "order,arm,nfrags,frag_size,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

# Ordered list of arm labels for the summary columns (off first as the control).
ARM_LABELS=""
for arm in $ARMS; do
    ARM_LABELS+="${arm%%:*} "
done

run_perftest() {
    # $1=order $2=arm_label $3=nfrags $4=frag_size $5=output log.
    # NFRAGS/FRAG_SIZE are exported here so mpirun -x forwards them by name.
    local order="$1" label="$2" nfrags="$3" frag_size="$4" out="$5"

    export UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER="$order"
    export UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS="$nfrags"
    export UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE="$frag_size"

    local cmd=(mpirun
        --np "$NTASKS"
        --map-by "ppr:${NTASKS_PER_NODE}:node"
        --bind-to core
    )
    # Forward the environment each rank needs. OpenMPI does not export the
    # launcher's environment by default, so every UCC/UCX var is named with -x.
    # Vars set by name (-x FOO) forward FOO's current value; UCX transport is
    # pinned with the explicit -x FOO=VAL form.
    cmd+=(
        -x PATH
        -x LD_LIBRARY_PATH
        -x OMP_NUM_THREADS
        -x UCC_TL_UCP_TUNE
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
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER=$order"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS=$nfrags"
        echo "UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE=$frag_size"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"

    # Extract the data table rows (count size time_avg time_min time_max
    # bw_avg bw_max bw_min) into the combined CSV. Data rows begin with a
    # numeric count; header/prose lines are skipped.
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
# Pretty-print a size x arm table per order. Column order follows ARM_LABELS
# (off first as the control); sizes are pre-sorted numerically with sort(1) so
# no gawk-only extensions are needed. Field 8 of the CSV is bw_avg_gbs.
{
    for order in $ORDERS; do
        printf "\norder=%s\n" "$order"
        printf "%12s" "size"
        for a in $ARM_LABELS; do printf "%13s" "$a"; done
        printf "\n"
        # unique, numerically-sorted list of sizes seen for this order
        sizes=$(awk -F, -v o="$order" 'NR>1 && $1==o {print $5}' "$SUMMARY_CSV" \
                | sort -n -u)
        for sz in $sizes; do
            printf "%12s" "$sz"
            for a in $ARM_LABELS; do
                bw=$(awk -F, -v o="$order" -v s="$sz" -v a="$a" \
                        'NR>1 && $1==o && $5==s && $2==a {print $8}' \
                        "$SUMMARY_CSV" | tail -1)
                printf "%13s" "${bw:--}"
            done
            printf "\n"
        done
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
