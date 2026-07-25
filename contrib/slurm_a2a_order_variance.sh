#!/usr/bin/env bash
#
# Onesided alltoall peer-ORDER variance repeat (A1).
#
# One SBATCH job == ONE repeat of the seq/stride/ilv/full comparison at
# 8 nodes x 32 ppn = 256 ranks, using the same blocking benchmark
# (ucc_bench_a2a_mpi) and the same UCC install as jobs 10313 (CNP) and
# 10345 (ordering bandwidth). The point is variance: submit this script
# several times with a DIFFERENT $ORDERS permutation each time so that
# time-drift on the fabric cannot be mistaken for a mode effect.
#
# Example (4-run Latin square, each mode visits each time slot once):
#   sbatch --export=ALL,MODULES='gcc hpcx/2.25',RUN_TAG=r1,ORDERS='seq stride ilv full' contrib/slurm_a2a_order_variance.sh
#   sbatch --export=ALL,MODULES='gcc hpcx/2.25',RUN_TAG=r2,ORDERS='stride full seq ilv' contrib/slurm_a2a_order_variance.sh
#   sbatch --export=ALL,MODULES='gcc hpcx/2.25',RUN_TAG=r3,ORDERS='ilv seq full stride' contrib/slurm_a2a_order_variance.sh
#   sbatch --export=ALL,MODULES='gcc hpcx/2.25',RUN_TAG=r4,ORDERS='full ilv stride seq' contrib/slurm_a2a_order_variance.sh
#
# Output: $SRC_DIR/results_<jobid>/
#   summary.csv          one row per (order, size) with aggregate bandwidth and
#                        CNP/ECN counters BOTH raw and normalized per GiB moved
#   summary.txt          seq-vs-ilv focused table for this run
#   a2a-order-<mode>.log raw benchmark output per mode
#   env.log              transport/knob environment
#   DONE                 terminal marker, written only after the row-count check
#
# Normalization
# -------------
# The benchmark's CNP/ECN columns are hardware counters read from
# /sys/class/infiniband/<hca>/ports/1/hw_counters/ and MPI_SUM-reduced over all
# ranks. Every rank on a node reads the SAME node-level HCA counter, so the
# absolute totals are inflated ~ppn x. That inflation is a constant factor,
# identical for every mode and size, so the per-GiB normalization below is a
# valid RELATIVE metric (mode vs mode at the same size) and must not be read as
# an absolute CNP rate.
#
#   cluster bytes moved during the timed window
#     = total_bytes(col 2) * NTASKS * ITERS
#   because col 2 is the per-rank total (k * 8 * npes) for ONE collective, the
#   benchmark times $ITERS iterations after SKIP=10 warmup iterations, and the
#   counters accumulate over exactly those timed iterations.
#
# Column layout of the benchmark with -c <hca> (13 columns):
#   1 Size(B)/peer  2 Total(B)/rank  3 BW  4 Agg BW (MB/s)  5 Max BW
#   6 Avg Lat  7 Min Lat  8 Max Lat  9 Var  10 CNP Sent  11 CNP Handled
#   12 CNP Ignored  13 ECN Marked
# The float fields are fixed-width printf and CAN fuse when a value overflows
# its field (e.g. Var touching Max Lat). The parser below therefore reads the
# four counters from the END of the line (they are %15lu/%20lu integers with
# room to spare at these magnitudes) and only trusts $1/$2/$4 from the front.

#SBATCH --job-name=ucc-a2a-var
#SBATCH --partition=thor
#SBATCH --nodes=8
#SBATCH --nodelist=thor[001-008]
#SBATCH --ntasks-per-node=32
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -eo pipefail

SRC_DIR=${SRC_DIR:-${HOME}/ucc-a2a-v3}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install"}
BENCH_SRC=${BENCH_SRC:-"$HOME/benchmarks/ucc/a2a/ucc_bench_a2a_mpi.c"}
JOB_ID=${SLURM_JOB_ID:-manual}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results_$JOB_ID"}
RUN_TAG=${RUN_TAG:-"$JOB_ID"}

NNODES=${SLURM_NNODES:-8}
NTASKS_PER_NODE=${SLURM_NTASKS_PER_NODE:-32}
NTASKS=${SLURM_NTASKS:-$((NNODES * NTASKS_PER_NODE))}

# Mode order is deliberately a submit-time parameter: it is the anti-drift knob.
ORDERS=${ORDERS:-"seq stride ilv full"}

SMAX=${SMAX:-524288}          # 524288 int64 elements = 4 MiB per peer
ITERS=${ITERS:-30}            # timed iterations per size (SKIP=10 extra warmup)
EXPECTED_ROWS=${EXPECTED_ROWS:-20}   # sizes 8 B .. 4 MiB per peer, factor 2

ALG=${ALG:-get}
NFRAGS=${NFRAGS:-1}
FRAG_SIZE=${FRAG_SIZE:-0}
CNP_HCA=${CNP_HCA:-mlx5_3}
UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_3:1"}
MODULES=${MODULES:-"gcc hpcx"}
MPIRUN_ARGS=${MPIRUN_ARGS:-}

# Per-launch wall-clock cap so one hung mode cannot burn the whole allocation
# and starve the remaining modes. timeout exits 124 on expiry; -k SIGKILLs 30s
# after the initial SIGTERM. Set RUN_TIMEOUT= (empty) to disable.
RUN_TIMEOUT=${RUN_TIMEOUT:-15m}
TIMEOUT_CMD=()
if [[ -n "$RUN_TIMEOUT" ]]; then
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD=(timeout -k 30s "$RUN_TIMEOUT")
    else
        echo "WARN: 'timeout' not found; per-launch cap disabled" >&2
    fi
fi

mkdir -p "$RESULT_DIR"

if [[ -n "${MODULES:-}" ]]; then
    # shellcheck disable=SC1091
    source /etc/profile >/dev/null 2>&1 || true
    for module_name in $MODULES; do
        module load "$module_name"
    done
fi

command -v mpicc >/dev/null 2>&1 ||
    { echo "mpicc not found (load HPC-X)" >&2; exit 1; }
command -v mpirun >/dev/null 2>&1 ||
    { echo "mpirun not found (load HPC-X)" >&2; exit 1; }
[[ -f "$BENCH_SRC" ]] ||
    { echo "blocking benchmark source not found: $BENCH_SRC" >&2; exit 1; }
[[ -r "$INSTALL_DIR/include/ucc/api/ucc.h" ]] ||
    { echo "UCC headers missing under $INSTALL_DIR" >&2; exit 1; }

echo "run tag:       $RUN_TAG"
echo "source:        $SRC_DIR"
echo "install:       $INSTALL_DIR"
echo "benchmark src: $BENCH_SRC"
echo "results:       $RESULT_DIR"
echo "nodes/tasks:   $NNODES nodes, $NTASKS_PER_NODE tasks/node, $NTASKS tasks"
echo "nodelist:      ${SLURM_JOB_NODELIST:-manual}"
echo "orders:        $ORDERS   (this run's anti-drift permutation)"
echo "device:        $UCX_NET_DEVICES   cnp hca: $CNP_HCA"
echo "size sweep:    8 .. $((SMAX * 8)) bytes per peer, factor 2"
echo "iterations:    $ITERS timed iterations/size plus SKIP=10 warmup"

# ---- build a patched copy of the benchmark ---------------------------------
# 1. -s is wired to `count` (upstream leaves it dead) so the per-peer sweep can
#    be capped at SMAX.
# 2. The unsupported onesided in-place flag is dropped (the ucp onesided
#    alltoall rejects UCC_COLL_ARGS_FLAG_IN_PLACE).
# 3. Timed statistics divide by `iter`, not `iter - SKIP`: the loop already runs
#    iter + SKIP times and samples only i >= SKIP, so `iter` is the sample count.
BUILD_DIR="$RESULT_DIR/build"
PATCHED_BENCH_SRC="$BUILD_DIR/ucc_bench_a2a_mpi.c"
BENCH_BIN="$BUILD_DIR/ucc_bench_a2a_mpi"
mkdir -p "$BUILD_DIR"
sed -e 's/size = atoi(optarg);/count = atoi(optarg);/' \
    -e '/UCC_COLL_ARGS_FLAG_IN_PLACE/d' \
    -e 's/(iter - SKIP)/(iter)/g' \
    "$BENCH_SRC" > "$PATCHED_BENCH_SRC"

grep -q 'count = atoi(optarg);' "$PATCHED_BENCH_SRC" ||
    { echo "failed to wire benchmark -s option to count" >&2; exit 1; }
if grep -q 'UCC_COLL_ARGS_FLAG_IN_PLACE' "$PATCHED_BENCH_SRC"; then
    echo "failed to remove unsupported in-place flag" >&2
    exit 1
fi
if grep -q 'iter - SKIP' "$PATCHED_BENCH_SRC"; then
    echo "failed to correct benchmark timed-iteration denominator" >&2
    exit 1
fi

mpicc -O3 -g -I"$INSTALL_DIR/include" \
    "$PATCHED_BENCH_SRC" -o "$BENCH_BIN" \
    -L"$INSTALL_DIR/lib" -Wl,-rpath,"$INSTALL_DIR/lib" -lucc
echo "built:         $BENCH_BIN"

export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$INSTALL_DIR/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export UCC_TL_UCP_TUNE=${UCC_TL_UCP_TUNE:-"alltoall:0-inf:@onesided"}
export UCC_TL_UCP_ALLTOALL_ONESIDED_ALG="$ALG"
export UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS="$NFRAGS"
export UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE="$FRAG_SIZE"
export UCX_TLS UCX_NET_DEVICES

{
    echo "== environment =="
    echo "run_tag=$RUN_TAG job=$JOB_ID"
    echo "orders=$ORDERS"
    echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=$ALG"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS=$NFRAGS"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE=$FRAG_SIZE"
    echo "UCX_TLS=$UCX_TLS"
    echo "UCX_NET_DEVICES=$UCX_NET_DEVICES"
    echo "CNP_HCA=$CNP_HCA"
    echo "NTASKS=$NTASKS ITERS=$ITERS SMAX=$SMAX"
} | tee "$RESULT_DIR/env.log"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "run_tag,job_id,order,slot,size_bytes,total_bytes_per_rank,agg_bw_mib_s,agg_bw_gib_s,gib_moved,cnp_sent,cnp_handled,cnp_ignored,ecn_marked,cnp_sent_per_gib,ecn_marked_per_gib" \
    > "$SUMMARY_CSV"

run_one() {
    local order="$1" slot="$2" out="$3"
    local cmd

    export UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER="$order"

    cmd=("${TIMEOUT_CMD[@]}" mpirun
        --np "$NTASKS"
        --map-by "ppr:${NTASKS_PER_NODE}:node"
        --bind-to core
        -x LD_LIBRARY_PATH
        -x UCC_TL_UCP_TUNE
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_ALG
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE
        -x UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    if [[ -n "$MPIRUN_ARGS" ]]; then
        local extra_mpirun_args
        read -r -a extra_mpirun_args <<< "$MPIRUN_ARGS"
        cmd+=("${extra_mpirun_args[@]}")
    fi
    cmd+=("$BENCH_BIN" -c "$CNP_HCA" -p "$NTASKS_PER_NODE" -i "$ITERS" -s "$SMAX")

    {
        echo ">> run: order=$order slot=$slot of this run's permutation"
        echo "started: $(date -Is)"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    # Keep the permutation moving if one launch fails or times out; a partial
    # run is detected later by the row-count check rather than by aborting here.
    set +e +o pipefail
    "${cmd[@]}" 2>&1 | tee -a "$out"
    local rc=${PIPESTATUS[0]}
    set -eo pipefail

    if [[ $rc -eq 124 ]]; then
        echo "!! order=$order TIMED OUT after $RUN_TIMEOUT" | tee -a "$out"
    elif [[ $rc -ne 0 ]]; then
        echo "!! order=$order FAILED rc=$rc" | tee -a "$out"
    fi

    awk -v tag="$RUN_TAG" -v job="$JOB_ID" -v order="$order" -v slot="$slot" \
        -v ntasks="$NTASKS" -v iters="$ITERS" '
        # Data rows: two leading integers, and four trailing integer counters.
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            # 13 fields when nothing fused; 12 when the variance field
            # overflowed and merged with max latency (seen at the largest
            # sizes in job 10313). Both keep the four counters at the tail.
            if (NF < 12) { next }
            cnp_sent    = $(NF-3)
            cnp_handled = $(NF-2)
            cnp_ignored = $(NF-1)
            ecn_marked  = $(NF)
            # Defensive: if the variance field overflowed its printf width it
            # can fuse with CNP Sent. Both floats carry two fractional digits,
            # so split after the first decimal point plus two digits.
            if ((dot = index(cnp_sent, ".")) > 0) {
                cnp_sent = substr(cnp_sent, dot + 3)
            }
            if (cnp_sent !~ /^[0-9]+$/ || ecn_marked !~ /^[0-9]+$/) { next }

            size  = $1
            total = $2
            agg_mib = $4
            # Cluster bytes moved over the timed window: the per-rank total for
            # one collective, times every rank, times the timed iterations.
            gib = (total * ntasks * iters) / 1073741824.0
            printf "%s,%s,%s,%s,%s,%s,%s,%.4f,%.4f,%s,%s,%s,%s,%.4f,%.4f\n", \
                   tag, job, order, slot, size, total, agg_mib, \
                   agg_mib / 1024.0, gib, \
                   cnp_sent, cnp_handled, cnp_ignored, ecn_marked, \
                   (gib > 0 ? cnp_sent / gib : 0), \
                   (gib > 0 ? ecn_marked / gib : 0)
        }' "$out" >> "$SUMMARY_CSV"
}

slot=0
for order in $ORDERS; do
    slot=$((slot + 1))
    run_one "$order" "$slot" "$RESULT_DIR/a2a-order-${order}.log"
done

# ---- per-run summary, seq vs ilv focused ------------------------------------
{
    echo "run_tag=$RUN_TAG job=$JOB_ID permutation='$ORDERS'"
    echo "ranks=$NTASKS nodelist=${SLURM_JOB_NODELIST:-manual} device=$UCX_NET_DEVICES"
    echo
    echo "== aggregate bandwidth (GiB/s) by mode =="
    printf "%12s" "size_B"
    for order in $ORDERS; do printf "%12s" "$order"; done
    printf "%12s\n" "ilv/seq"
    sizes=$(awk -F, 'NR>1 {print $5}' "$SUMMARY_CSV" | sort -n -u)
    for size in $sizes; do
        printf "%12s" "$size"
        for order in $ORDERS; do
            v=$(awk -F, -v o="$order" -v s="$size" \
                    'NR>1 && $3==o && $5==s {print $8}' "$SUMMARY_CSV" | tail -1)
            printf "%12s" "${v:--}"
        done
        sq=$(awk -F, -v s="$size" 'NR>1 && $3=="seq" && $5==s {print $8}' "$SUMMARY_CSV" | tail -1)
        il=$(awk -F, -v s="$size" 'NR>1 && $3=="ilv" && $5==s {print $8}' "$SUMMARY_CSV" | tail -1)
        if [[ -n "$sq" && -n "$il" ]]; then
            printf "%12s\n" "$(awk -v a="$sq" -v b="$il" \
                'BEGIN {if (a+0 == 0) print "-"; else printf "%+.1f%%", 100*(b/a - 1)}')"
        else
            printf "%12s\n" "-"
        fi
    done

    for metric in cnp_sent_per_gib ecn_marked_per_gib; do
        case "$metric" in
            cnp_sent_per_gib)   col=14 ;;
            ecn_marked_per_gib) col=15 ;;
        esac
        echo
        echo "== $metric (normalized by cluster GiB moved; RELATIVE metric) =="
        printf "%12s" "size_B"
        for order in $ORDERS; do printf "%16s" "$order"; done
        printf "%12s\n" "ilv/seq"
        for size in $sizes; do
            printf "%12s" "$size"
            for order in $ORDERS; do
                v=$(awk -F, -v o="$order" -v s="$size" -v c="$col" \
                        'NR>1 && $3==o && $5==s {print $c}' "$SUMMARY_CSV" | tail -1)
                printf "%16s" "${v:--}"
            done
            sq=$(awk -F, -v s="$size" -v c="$col" 'NR>1 && $3=="seq" && $5==s {print $c}' "$SUMMARY_CSV" | tail -1)
            il=$(awk -F, -v s="$size" -v c="$col" 'NR>1 && $3=="ilv" && $5==s {print $c}' "$SUMMARY_CSV" | tail -1)
            if [[ -n "$sq" && -n "$il" ]]; then
                printf "%12s\n" "$(awk -v a="$sq" -v b="$il" \
                    'BEGIN {if (a+0 == 0) print "-"; else printf "%+.1f%%", 100*(b/a - 1)}')"
            else
                printf "%12s\n" "-"
            fi
        done
    done
} | tee "$RESULT_DIR/summary.txt"

# ---- completion gate --------------------------------------------------------
# sacct cannot reach the accounting DB on this cluster, so job state is not a
# usable completion signal. The DONE marker is written only when every mode
# produced the full size sweep; a partial run leaves no marker.
status=ok
for order in $ORDERS; do
    n=$(awk -F, -v o="$order" 'NR>1 && $3==o' "$SUMMARY_CSV" | wc -l)
    echo "rows: order=$order n=$n (expected $EXPECTED_ROWS)"
    if [[ "$n" -ne "$EXPECTED_ROWS" ]]; then
        status=incomplete
    fi
done

if [[ "$status" == "ok" ]]; then
    {
        echo "run_tag=$RUN_TAG"
        echo "job_id=$JOB_ID"
        echo "orders=$ORDERS"
        echo "rows_per_order=$EXPECTED_ROWS"
        echo "finished=$(date -Is)"
    } > "$RESULT_DIR/DONE"
    echo "done: $RESULT_DIR"
else
    echo "INCOMPLETE: $RESULT_DIR (no DONE marker written)" >&2
    exit 1
fi
