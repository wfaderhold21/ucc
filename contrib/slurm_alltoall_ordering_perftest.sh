#!/usr/bin/env bash
#
# Compare sequential and full onesided alltoall peer ordering with the same
# blocking UCC benchmark used for CNP identification. Unlike ucc_perftest's
# per-rank bus-bandwidth output, this benchmark reports cluster aggregate
# bandwidth, which is the comparison made here.
#
# Each mpirun is restricted to exactly one HCA with UCX_NET_DEVICES:
#   roce  mlx5_3:1
#   ib    mlx5_0:1
#
# Submit from the repository root on hpcac-internal:
#   sbatch --export=ALL,MODULES='gcc hpcx/2.25' \
#     contrib/slurm_alltoall_ordering_perftest.sh
#
# The benchmark source is copied into the result directory and patched before
# it is built:
#   1. -s controls the maximum per-peer int64 count.
#   2. The unsupported onesided in-place flag is removed if present.
#   3. Timed statistics divide by ITERS, not ITERS-SKIP. SKIP is additional
#      warmup in this benchmark and is already excluded from the samples.

#SBATCH --job-name=ucc-a2a-order
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
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/alltoall-ordering-$JOB_ID"}

NNODES=${SLURM_NNODES:-8}
NTASKS_PER_NODE=${SLURM_NTASKS_PER_NODE:-32}
NTASKS=${SLURM_NTASKS:-$((NNODES * NTASKS_PER_NODE))}

ORDERS=${ORDERS:-"seq full"}
FABRICS=${FABRICS:-"roce ib"}
ROCE_UCX_NET_DEVICES=${ROCE_UCX_NET_DEVICES:-"mlx5_3:1"}
IB_UCX_NET_DEVICES=${IB_UCX_NET_DEVICES:-"mlx5_0:1"}

# ucc_bench_a2a_mpi uses int64 elements and sweeps powers of two from one
# element through SMAX. 524288 elements = 4 MiB per peer.
SMAX=${SMAX:-524288}
ITERS=${ITERS:-30}

ALG=${ALG:-get}
NFRAGS=${NFRAGS:-1}
FRAG_SIZE=${FRAG_SIZE:-0}
UCX_TLS=${UCX_TLS:-"sm,rc"}
MODULES=${MODULES:-"gcc hpcx"}
MPIRUN_ARGS=${MPIRUN_ARGS:-}

# Cap each device/order launch independently. Set RUN_TIMEOUT= to disable.
RUN_TIMEOUT=${RUN_TIMEOUT:-15m}
TIMEOUT_CMD=()
if [[ -n "$RUN_TIMEOUT" ]]; then
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD=(timeout -k 30s "$RUN_TIMEOUT")
    else
        echo "WARN: 'timeout' not found; per-run cap disabled" >&2
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

echo "source:        $SRC_DIR"
echo "install:       $INSTALL_DIR"
echo "benchmark src: $BENCH_SRC"
echo "results:       $RESULT_DIR"
echo "nodes/tasks:   $NNODES nodes, $NTASKS_PER_NODE tasks/node, $NTASKS tasks"
echo "nodelist:      ${SLURM_JOB_NODELIST:-manual}"
echo "orders:        $ORDERS"
echo "devices:       roce=$ROCE_UCX_NET_DEVICES ib=$IB_UCX_NET_DEVICES"
echo "size sweep:    8 .. $((SMAX * 8)) bytes per peer, factor 2"
echo "iterations:    $ITERS timed iterations/size plus benchmark warmup"

# Build the CNP-identification benchmark locally in the result directory.
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
export UCX_TLS

{
    echo "== environment =="
    echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=$ALG"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS=$NFRAGS"
    echo "UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE=$FRAG_SIZE"
    echo "UCX_TLS=$UCX_TLS"
    echo "ROCE_UCX_NET_DEVICES=$ROCE_UCX_NET_DEVICES"
    echo "IB_UCX_NET_DEVICES=$IB_UCX_NET_DEVICES"
} | tee "$RESULT_DIR/env.log"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "fabric,ucx_net_device,order,size_bytes,total_bytes,bw_mib_s,aggregate_bw_mib_s,max_bw_mib_s,avg_latency_us,min_latency_us,max_latency_us,variance_us2" \
    > "$SUMMARY_CSV"

run_one() {
    local fabric="$1" device="$2" order="$3" out="$4"
    local cmd

    # Every launch gets one device, never a comma-separated or fallback list.
    export UCX_NET_DEVICES="$device"
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
    cmd+=("$BENCH_BIN" -p "$NTASKS_PER_NODE" -i "$ITERS" -s "$SMAX")

    {
        echo ">> run: fabric=$fabric order=$order"
        echo "UCX_NET_DEVICES=$UCX_NET_DEVICES"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    # Keep the matrix moving if one launch fails or times out.
    set +e +o pipefail
    "${cmd[@]}" 2>&1 | tee -a "$out"
    local rc=${PIPESTATUS[0]}
    set -eo pipefail

    if [[ $rc -eq 124 ]]; then
        echo "!! fabric=$fabric device=$device order=$order TIMED OUT after $RUN_TIMEOUT" |
            tee -a "$out"
    elif [[ $rc -ne 0 ]]; then
        echo "!! fabric=$fabric device=$device order=$order FAILED rc=$rc" |
            tee -a "$out"
    fi

    awk -v fabric="$fabric" -v device="$device" -v order="$order" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            max_latency = $8
            variance = $9
            # The benchmark uses adjacent fixed-width printf fields. If a
            # variance fills its field, it can touch max latency and arrive
            # as one awk token (for example, 3260097.1920446488234.19).
            # Both values have two fractional digits, so split after the
            # first decimal point plus two digits.
            if (variance == "" && (dot = index(max_latency, ".")) > 0) {
                variance = substr(max_latency, dot + 3)
                max_latency = substr(max_latency, 1, dot + 2)
            }
            print fabric "," device "," order "," $1 "," $2 "," $3 "," \
                  $4 "," $5 "," $6 "," $7 "," max_latency "," variance
        }' "$out" >> "$SUMMARY_CSV"
}

for fabric in $FABRICS; do
    case "$fabric" in
        roce) device="$ROCE_UCX_NET_DEVICES" ;;
        ib)   device="$IB_UCX_NET_DEVICES" ;;
        *)
            echo "unknown fabric '$fabric' (expected: roce or ib)" >&2
            exit 1
            ;;
    esac
    for order in $ORDERS; do
        out="$RESULT_DIR/alltoall-${fabric}-order-${order}.log"
        run_one "$fabric" "$device" "$order" "$out"
    done
done

echo
echo "== cluster aggregate bandwidth (GiB/s) =="
echo "wrote $SUMMARY_CSV"
{
    for fabric in $FABRICS; do
        case "$fabric" in
            roce) device="$ROCE_UCX_NET_DEVICES" ;;
            ib)   device="$IB_UCX_NET_DEVICES" ;;
        esac
        echo
        echo "fabric=$fabric UCX_NET_DEVICES=$device"
        printf "%12s %16s %16s %13s\n" \
               "size_B" "seq_agg_GiB/s" "full_agg_GiB/s" "full_vs_seq"
        sizes=$(awk -F, -v f="$fabric" \
                    'NR>1 && $1==f {print $4}' "$SUMMARY_CSV" | sort -n -u)
        for size in $sizes; do
            seq_mib=$(awk -F, -v f="$fabric" -v s="$size" \
                          'NR>1 && $1==f && $3=="seq" && $4==s {print $7}' \
                          "$SUMMARY_CSV" | tail -1)
            full_mib=$(awk -F, -v f="$fabric" -v s="$size" \
                           'NR>1 && $1==f && $3=="full" && $4==s {print $7}' \
                           "$SUMMARY_CSV" | tail -1)
            seq_gib="-"
            full_gib="-"
            delta="-"
            if [[ -n "$seq_mib" ]]; then
                seq_gib=$(awk -v value="$seq_mib" 'BEGIN {printf "%.2f", value / 1024}')
            fi
            if [[ -n "$full_mib" ]]; then
                full_gib=$(awk -v value="$full_mib" 'BEGIN {printf "%.2f", value / 1024}')
            fi
            if [[ -n "$seq_mib" && -n "$full_mib" ]]; then
                delta=$(awk -v seq="$seq_mib" -v full="$full_mib" \
                            'BEGIN {
                                if (seq == 0) {
                                    print "-"
                                } else {
                                    printf "%+.1f%%", 100 * (full / seq - 1)
                                }
                            }')
            fi
            printf "%12s %16s %16s %13s\n" \
                   "$size" "$seq_gib" "$full_gib" "$delta"
        done
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
