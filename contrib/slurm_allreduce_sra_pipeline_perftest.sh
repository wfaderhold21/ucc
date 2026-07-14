#!/usr/bin/env bash
#
# Benchmark the host-side SRA allreduce pipelining change: fragment the
# reduce-scatter -> allgather pipeline so fragment i's allgather (pure network)
# overlaps fragment i+1's reduce-scatter (network + CPU reduction). See
# docs/allreduce_perf/plan-1-host-sra-pipelining.md.
#
# Submit from the repository root:
#   sbatch contrib/slurm_allreduce_sra_pipeline_perftest.sh
#
# Common overrides:
#   sbatch --export=ALL,SKIP_BUILD=1,RANKS="2 4 8" \
#     contrib/slurm_allreduce_sra_pipeline_perftest.sh
#
# What is being measured
# ----------------------
# A single dense run of the patched build: 8 nodes x 32 PPN = 256 ranks, the
# SRA-knomial allreduce forced (UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial),
# with the host-path pipelining enabled (PIPELINE=auto -> on the patched build
# this is the new host default: thresh 256KB, ~256KB frags, pdepth 2).
#
# Override PIPELINE=n to reproduce the old monolithic path in the same build, or
# set ARMS="new hpcx" (+ HPCX_UCC_DIR) to A/B against stock HPC-X UCC.
#
# Notes
# -----
# - ucc_perftest -b/-e are element counts; byte size = count * sizeof(dtype).
#   With -d float32 the default 32K..1M count sweep is 128KB..4MB; the low end
#   sits one step below the 256KB threshold (below it the pipeline is inactive).
# - Reduction cost is the thing being hidden, so float32 SUM is the default;
#   try DTYPE=float64 or OP=avg to grow the reduction cost and the expected win.
# - 32 ranks/node all share the one NIC (mlx5_0:1): a NIC-contended regime with
#   heavy on-node reduction, where hiding the CPU reduction cost matters most.

#SBATCH --job-name=ucc-ar-sra-pipe
#SBATCH --partition=thor
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
#SBATCH --time=02:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -e

SRC_DIR=${SRC_DIR:-${HOME}/ucc}
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/allreduce-sra-pipe-$JOB_ID"}

# Set SKIP_BUILD=1 to reuse an existing $INSTALL_DIR.
SKIP_BUILD=${SKIP_BUILD:-0}

# Job geometry: NNODES nodes x PPN ranks/node = NP total ranks. This run is a
# single dense point (8 nodes x 32 PPN = 256 ranks), all ranks on a node sharing
# the one NIC (mlx5_0:1).
NNODES=${SLURM_NNODES:-8}
PPN=${PPN:-${SLURM_NTASKS_PER_NODE:-32}}
NP=${NP:-$((NNODES * PPN))}

# Rank-count sweep (each placed PPN per node). Default: the single NP point.
# Low-rank A/B: PPN=1 RANKS="2 4 6"; dense: PPN=32 RANKS=256.
RANKS=${RANKS:-"$NP"}

# Element-count sweep (bytes = count * sizeof dtype). With float32:
#   32K->128KB (below thresh), 64K->256KB, ... 1M->4MB.
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
MIN_COUNT=${MIN_COUNT:-32K}
MAX_COUNT=${MAX_COUNT:-1M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

# Single run: the patched build only.
ARMS=${ARMS:-"new"}

# Pipeline setting. On the patched build, auto activates the new host-path
# pipelining defaults (thresh 256KB, ~256KB frags, pdepth 2).
PIPELINE=${PIPELINE:-auto}

PERFTEST_EXTRA_ARGS=${PERFTEST_EXTRA_ARGS:-}
MPIRUN_ARGS=${MPIRUN_ARGS:-}

UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_0:1"}

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

# --- Build ------------------------------------------------------------------
# 1) module load gcc hpcx   (handled by MODULES above)
# 2) ./autogen.sh
# 3) ./configure --prefix=`pwd`/install --with-ucx --with-mpi --without-sharp
# 4) make -j4 && make install
if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "== building UCC in $SRC_DIR =="
    cd "$SRC_DIR"
    ./autogen.sh
    ./configure --prefix="$(pwd)/install" --with-ucx --with-mpi --without-sharp
    make -j4
    make install
    echo "== build complete =="
else
    echo "== SKIP_BUILD=1, reusing $INSTALL_DIR =="
fi

UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}
if [[ -z "$MPI_HOME" ]] && command -v mpicc >/dev/null 2>&1; then
    MPI_HOME=$(dirname "$(dirname "$(command -v mpicc)")")
fi

# Baseline install: stock UCC shipped with HPC-X.
HPCX_UCC_DIR=${HPCX_UCC_DIR:-}

echo "source:        $SRC_DIR"
echo "new install:   $INSTALL_DIR"
echo "hpcx install:  ${HPCX_UCC_DIR:-<unset>}"
echo "results:       $RESULT_DIR"
echo "geometry:      $NNODES nodes x $PPN ppn = $NP ranks"
echo "arms:          $ARMS"
echo "pipeline:      $PIPELINE"
echo "dtype/op:      $DTYPE / $OP"
echo "count sweep:   $MIN_COUNT .. $MAX_COUNT elems, factor $FACTOR"
echo "net devices:   $UCX_NET_DEVICES"

# Resolve an arm label to its UCC install prefix.
arm_install() {
    case "$1" in
        new)  echo "$INSTALL_DIR" ;;
        hpcx) echo "${HPCX_UCC_DIR:?HPCX_UCC_DIR not set -- load the hpcx module or pass HPCX_UCC_DIR=...}" ;;
        /*)   echo "$1" ;;   # allow passing a raw install path as an arm
        *)    echo "$1" ;;
    esac
}

# Base library path shared by both arms: UCX + MPI, but NOT any UCC. Each arm
# prepends its own UCC install so the two never mix.
BASE_LD_LIBRARY_PATH=""
if [[ -n "${UCX_HOME:-}" ]]; then
    BASE_LD_LIBRARY_PATH="$UCX_HOME/lib:$UCX_HOME/lib64"
fi
if [[ -n "${MPI_HOME:-}" ]]; then
    BASE_LD_LIBRARY_PATH="$BASE_LD_LIBRARY_PATH:$MPI_HOME/lib:$MPI_HOME/lib64"
fi
BASE_LD_LIBRARY_PATH="${BASE_LD_LIBRARY_PATH}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# Sanity-check each arm's binary before running anything.
for arm in $ARMS; do
    bin="$(arm_install "$arm")/bin/ucc_perftest"
    if [[ ! -x "$bin" ]]; then
        echo "arm '$arm': ucc_perftest not found or not executable: $bin" >&2
        exit 1
    fi
done

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
# Force the SRA-knomial (BW-optimized) allreduce so the result does not depend
# on the AUTO algorithm-selection threshold, in both installs.
export UCC_TL_UCP_TUNE=${UCC_TL_UCP_TUNE:-"allreduce:0-inf:@sra_knomial"}
export UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE="$PIPELINE"
export UCX_TLS
export UCX_NET_DEVICES

{
    echo "== environment =="
    echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
    env | grep -E '^(UCX_|UCC_|HPCX_)' | sort
} | tee "$RESULT_DIR/env.log"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "arm,nranks,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_perftest() {
    local arm="$1" nranks="$2" out="$3"
    local install bin arm_ld
    install=$(arm_install "$arm")
    bin="$install/bin/ucc_perftest"
    arm_ld="$install/lib:$install/lib64:$BASE_LD_LIBRARY_PATH"

    local cmd=(mpirun
        --np "$nranks"
        --map-by "ppr:${PPN}:node"
        --bind-to core
    )
    cmd+=(
        -x "PATH=$install/bin:$PATH"
        -x "LD_LIBRARY_PATH=$arm_ld"
        -x OMP_NUM_THREADS
        -x UCC_TL_UCP_TUNE
        -x UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    if [[ -n "$MPIRUN_ARGS" ]]; then
        read -r -a extra_mpirun_args <<< "$MPIRUN_ARGS"
        cmd+=("${extra_mpirun_args[@]}")
    fi
    cmd+=(
        "$bin"
        -c allreduce
        -m host
        -d "$DTYPE"
        -o "$OP"
        -b "$MIN_COUNT"
        -e "$MAX_COUNT"
        -f "$FACTOR"
        -w "$WARMUP"
        -n "$ITERS"
        -F
    )
    if [[ -n "$PERFTEST_EXTRA_ARGS" ]]; then
        read -r -a extra_perftest_args <<< "$PERFTEST_EXTRA_ARGS"
        cmd+=("${extra_perftest_args[@]}")
    fi

    {
        echo "arm=$arm nranks=$nranks install=$install"
        echo "UCC_TL_UCP_TUNE=$UCC_TL_UCP_TUNE"
        echo "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$PIPELINE"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"

    awk -v arm="$arm" -v nranks="$nranks" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print arm "," nranks "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

for nranks in $RANKS; do
    for arm in $ARMS; do
        out="$RESULT_DIR/allreduce-np${nranks}-ppn${PPN}-arm-${arm}.log"
        echo ">> run: np=$nranks ($PPN/node) arm=$arm pipeline=$PIPELINE (install=$(arm_install "$arm"))"
        run_perftest "$arm" "$nranks" "$out"
    done
done

echo
echo "== summary (avg bus BW, GB/s) : pipeline=$PIPELINE, per rank count =="
echo "wrote $SUMMARY_CSV"
{
    for nranks in $RANKS; do
        echo "-- nranks=$nranks --"
        printf "%12s" "size"
        for arm in $ARMS; do printf "%13s" "$arm"; done
        printf "%13s\n" "new/hpcx"
        sizes=$(awk -F, -v r="$nranks" 'NR>1 && $2==r {print $3}' "$SUMMARY_CSV" \
                | sort -n -u)
        for sz in $sizes; do
            printf "%12s" "$sz"
            new_bw="" ; hpcx_bw=""
            for arm in $ARMS; do
                bw=$(awk -F, -v a="$arm" -v r="$nranks" -v s="$sz" \
                        'NR>1 && $1==a && $2==r && $3==s {print $6}' \
                        "$SUMMARY_CSV" | tail -1)
                printf "%13s" "${bw:--}"
                [[ "$arm" == "new"  ]] && new_bw="$bw"
                [[ "$arm" == "hpcx" ]] && hpcx_bw="$bw"
            done
            if [[ -n "$new_bw" && -n "$hpcx_bw" && "$hpcx_bw" != "0" ]]; then
                awk -v n="$new_bw" -v h="$hpcx_bw" 'BEGIN { printf "%12.2fx\n", n/h }'
            else
                printf "%13s\n" "-"
            fi
        done
        echo
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
