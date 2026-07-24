#!/usr/bin/env bash
#
# Head-to-head onesided alltoall benchmark: the onesided-strided branch vs the
# UCC that ships in HPC-X. Runs the same ucc_perftest alltoall sweep three ways
# on the same node set / transport and prints an ours-vs-hpcx bandwidth table.
#
# Configs measured (one ucc_perftest launch per config per size):
#   hpcx       HPC-X's stock ucc_perftest + stock UCC. Onesided path (-M global),
#              ORDER is seq-only there (no stagger / mixed / frag knobs exist).
#              This is the external baseline.
#   ours-seq   our build, ALLTOALL_ONESIDED_ORDER=seq. Same algorithm as stock
#              onesided -> should tie hpcx. This is the no-regression check that
#              isolates "our build" from "our new ordering".
#   ours-full  our build, ORDER=full, ALG=auto (auto-selects mixed GET/PUT on
#              multi-node), fragmentation auto. All three new optimizations on --
#              parity stagger + mixed GET/PUT + fragmentation. This is the arm
#              that the branch is trying to win with.
#
# Submit from the repo root on the thor login node:
#   sbatch contrib/slurm_alltoall_vs_hpcx.sh
#
# Common overrides:
#   sbatch --export=ALL,SKIP_BUILD=1 contrib/slurm_alltoall_vs_hpcx.sh
#   sbatch --export=ALL,MIN_BYTES=64K,MAX_BYTES=8M contrib/slurm_alltoall_vs_hpcx.sh
#   sbatch --export=ALL,CONFIGS="ours-seq ours-full" contrib/slurm_alltoall_vs_hpcx.sh
#
# The device is pinned to mlx5_3:1 for every launch so a config change can never
# be confused with a NIC change.

#SBATCH --job-name=ucc-a2a-vs-hpcx
#SBATCH --partition=thor
#SBATCH --nodes=8
#SBATCH --ntasks-per-node=32
# thor mixes x86 (thor[001-016]) and BlueField-3 ARM (thorbf3a[001-016]) nodes.
# Build + run must stay on one ISA; pin to the x86 nodes.
#SBATCH --nodelist=thor[001-008]
#SBATCH --exclude=thorbf3a[001-016]
#SBATCH --time=03:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

# Default to the directory sbatch was launched from (the repo root), so the
# script measures whichever checkout you submit it in. Override with SRC_DIR=...
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(pwd)}}
JOB_ID=${SLURM_JOB_ID:-manual}
BUILD_DIR=${SRC_DIR}
INSTALL_DIR="$BUILD_DIR/install"
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/alltoall-vs-hpcx-$JOB_ID"}

NNODES=${SLURM_NNODES:-8}
NTASKS_PER_NODE=${SLURM_NTASKS_PER_NODE:-32}
NTASKS=${SLURM_NTASKS:-$((NNODES * NTASKS_PER_NODE))}
BUILD_JOBS=${BUILD_JOBS:-${SLURM_CPUS_ON_NODE:-16}}

# Which configs to run and in what order. hpcx first as the reference column.
CONFIGS=${CONFIGS:-"hpcx ours-seq ours-full"}

# Per-peer size sweep. The onesided changes are documented to pay off at
# 1-4 MB/peer, so the sweep spans small (no-harm) up through that regime.
MIN_BYTES=${MIN_BYTES:-1K}
MAX_BYTES=${MAX_BYTES:-4M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-20}
ITERS=${ITERS:-100}

# Build the branch by default so a fresh checkout is measured; set SKIP_BUILD=1
# to reuse install/ as-is.
SKIP_BUILD=${SKIP_BUILD:-0}
CONFIGURE_ARGS=${CONFIGURE_ARGS:-}
MPIRUN_ARGS=${MPIRUN_ARGS:-}
PERFTEST_EXTRA_ARGS=${PERFTEST_EXTRA_ARGS:-}

UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_3:1"}
MODULES=${MODULES:-"gcc hpcx"}

# Per-run wall-clock cap. A single hung launch (e.g. an onesided deadlock or the
# malformed-handle abort this branch has hit) would otherwise block until the
# whole SBATCH --time allocation expires, starving every remaining config. Wrap
# each mpirun in `timeout` so one bad run is killed and the sweep continues.
# timeout exits 124 on expiry; -k sends SIGKILL 30s after the initial SIGTERM if
# mpirun ignores it. Set RUN_TIMEOUT= (empty) to disable.
RUN_TIMEOUT=${RUN_TIMEOUT:-10m}
TIMEOUT_CMD=()
if [[ -n "$RUN_TIMEOUT" ]]; then
    if command -v timeout >/dev/null 2>&1; then
        TIMEOUT_CMD=(timeout -k 30s "$RUN_TIMEOUT")
    else
        echo "WARN: 'timeout' not found; per-run cap disabled" >&2
    fi
fi

mkdir -p "$RESULT_DIR"

# ---------------------------------------------------------------------------
# Environment bring-up. module/profile can return nonzero; keep errexit off
# around them so the job does not die mid-source before anything prints.
# ---------------------------------------------------------------------------
module load gcc hpcx

UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}
HPCX_UCC=${HPCX_UCC_DIR:-}
if [[ -z "$MPI_HOME" ]] && command -v mpicc >/dev/null 2>&1; then
    MPI_HOME=$(dirname "$(dirname "$(command -v mpicc)")")
fi

OURS_PERFTEST="$INSTALL_DIR/bin/ucc_perftest"
HPCX_PERFTEST="${HPCX_UCC:+$HPCX_UCC/bin/ucc_perftest}"

echo "source:        $SRC_DIR"
echo "install:       $INSTALL_DIR"
echo "results:       $RESULT_DIR"
echo "nodes/tasks:   $NNODES nodes, $NTASKS_PER_NODE tasks/node, $NTASKS tasks"
echo "configs:       $CONFIGS"
echo "size sweep:    $MIN_BYTES .. $MAX_BYTES per peer, factor $FACTOR"
echo "UCX_HOME:      $UCX_HOME"
echo "MPI_HOME:      $MPI_HOME"
echo "HPCX_UCC_DIR:  $HPCX_UCC"
echo "device:        $UCX_NET_DEVICES  tls=$UCX_TLS"

# ---------------------------------------------------------------------------
# Build (our branch only; hpcx is prebuilt in the module).
# ---------------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "== build =="
    if [[ -z "$UCX_HOME" || -z "$MPI_HOME" ]]; then
        echo "UCX_HOME/MPI_HOME not resolved after 'module load $MODULES'; cannot build" >&2
        exit 1
    fi
    cd "$SRC_DIR"
    if [[ ! -x ./configure ]]; then ./autogen.sh; fi
    ./configure --prefix="$INSTALL_DIR" \
        --with-ucx="$UCX_HOME" --with-mpi="$MPI_HOME" $CONFIGURE_ARGS
    make -j"$BUILD_JOBS"
    make install
fi

if [[ ! -x "$OURS_PERFTEST" ]]; then
    echo "our ucc_perftest not found: $OURS_PERFTEST" >&2
    exit 1
fi

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}

# Export our install into the batch environment so our ucc_perftest and our
# libucc load as a matched pair. Without this the onesided global mem handle is
# built by one and read by the other -> "malformed mem map handle". The ours-*
# configs forward LD_LIBRARY_PATH by name (-x LD_LIBRARY_PATH) and inherit this;
# the hpcx config overrides it with an explicit, install-free path so the stock
# HPC-X perftest cannot pick up our libucc.
export PATH="$INSTALL_DIR/bin:$PATH"
export LD_LIBRARY_PATH="$INSTALL_DIR/lib:$INSTALL_DIR/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
if [[ -n "${UCX_HOME:-}" ]]; then
    export LD_LIBRARY_PATH="$UCX_HOME/lib:$UCX_HOME/lib64:$LD_LIBRARY_PATH"
fi

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "config,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" > "$SUMMARY_CSV"

# run_config <config-label>. Selects the perftest binary, the library path, and
# the UCC onesided knobs for that config, then sweeps sizes in one launch.
run_config() {
    local cfg="$1"
    # ld_forward=1 -> inherit the exported batch LD_LIBRARY_PATH (install first)
    # ld_forward=0 -> pin an explicit, install-free path (hpcx only)
    local perftest lib_path tune order alg nfrags frag_size ld_forward

    case "$cfg" in
        hpcx)
            if [[ -z "$HPCX_PERFTEST" || ! -x "$HPCX_PERFTEST" ]]; then
                echo ">> skip $cfg: HPC-X ucc_perftest not found ($HPCX_PERFTEST)" >&2
                return 0
            fi
            perftest="$HPCX_PERFTEST"
            lib_path="$UCX_HOME/lib:$UCX_HOME/lib64:${HPCX_UCC}/lib:${HPCX_UCC}/lib64"
            ld_forward=0
            tune="alltoall:0-inf:@onesided"
            order="seq"; alg="auto"; nfrags="0"; frag_size="0"
            ;;
        ours-seq)
            perftest="$OURS_PERFTEST"
            # Pin our install libucc first, EXPLICITLY (not by-name forwarding).
            # Forwarding -x LD_LIBRARY_PATH by name let HPC-X's libucc win on the
            # remote ranks -> our perftest read a memh built by a different libucc
            # -> "malformed mem map handle". An explicit path per rank guarantees
            # the matched (our perftest + our libucc) pair everywhere.
            lib_path="$INSTALL_DIR/lib:$INSTALL_DIR/lib64:$UCX_HOME/lib:$UCX_HOME/lib64"
            ld_forward=0
            tune="alltoall:0-inf:@onesided"
            order="seq"; alg="auto"; nfrags="0"; frag_size="0"
            ;;
        ours-full)
            perftest="$OURS_PERFTEST"
            lib_path="$INSTALL_DIR/lib:$INSTALL_DIR/lib64:$UCX_HOME/lib:$UCX_HOME/lib64"
            ld_forward=0
            tune="alltoall:0-inf:@onesided"
            order="full"; alg="auto"; nfrags="0"; frag_size="0"
            ;;
        *)
            echo ">> skip unknown config: $cfg" >&2
            return 0
            ;;
    esac

    # LD_LIBRARY_PATH forwarding: by name for ours (matched install pair),
    # explicit override for hpcx (no install on the path).
    local ld_arg
    if [[ "$ld_forward" == "1" ]]; then
        ld_arg="LD_LIBRARY_PATH"
    else
        ld_arg="LD_LIBRARY_PATH=${lib_path}"
    fi

    local out="$RESULT_DIR/alltoall-${cfg}.log"
    local cmd=("${TIMEOUT_CMD[@]}" mpirun
        --np "$NTASKS"
        --map-by "ppr:${NTASKS_PER_NODE}:node"
        --bind-to core
        -x "PATH"
        -x "$ld_arg"
        -x "OMP_NUM_THREADS"
        -x "UCC_TL_UCP_TUNE=${tune}"
        -x "UCC_TL_UCP_ALLTOALL_ONESIDED_ORDER=${order}"
        -x "UCC_TL_UCP_ALLTOALL_ONESIDED_ALG=${alg}"
        -x "UCC_TL_UCP_ALLTOALL_ONESIDED_NFRAGS=${nfrags}"
        -x "UCC_TL_UCP_ALLTOALL_ONESIDED_FRAG_SIZE=${frag_size}"
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    if [[ -n "$MPIRUN_ARGS" ]]; then
        read -r -a extra <<< "$MPIRUN_ARGS"; cmd+=("${extra[@]}")
    fi
    cmd+=("$perftest"
        -c alltoall -m host -d uint8
        -b "$MIN_BYTES" -e "$MAX_BYTES" -f "$FACTOR"
        -w "$WARMUP" -n "$ITERS"
        -M global -F
    )
    if [[ -n "$PERFTEST_EXTRA_ARGS" ]]; then
        read -r -a extra <<< "$PERFTEST_EXTRA_ARGS"; cmd+=("${extra[@]}")
    fi

    # Preflight: resolve libucc under this config's exact lib path so the log
    # proves which libucc will load (matched-pair check for the ours-* arms).
    local resolved_ucc
    resolved_ucc=$(LD_LIBRARY_PATH="${lib_path:-$LD_LIBRARY_PATH}" ldd "$perftest" 2>/dev/null \
                   | awk '/libucc\.so/{print $3; exit}')

    {
        echo "== config=$cfg =="
        echo "perftest: $perftest"
        echo "libucc:   ${resolved_ucc:-<unresolved>}"
        echo "tune=$tune order=$order alg=$alg nfrags=$nfrags frag_size=$frag_size"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"
    local rc=${PIPESTATUS[0]}
    if [[ $rc -eq 124 ]]; then
        echo "!! config=$cfg TIMED OUT after $RUN_TIMEOUT (killed); continuing" \
            | tee -a "$out"
    fi

    # ucc_perftest data rows: count size time_avg time_min time_max bw_avg bw_max bw_min
    awk -v cfg="$cfg" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print cfg "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

{
    echo "== environment =="
    echo "UCX_NET_DEVICES=$UCX_NET_DEVICES"
    echo "UCX_TLS=$UCX_TLS"
    env | grep -E '^(UCX_|UCC_|HPCX_)' | sort
} | tee "$RESULT_DIR/env.log"

for cfg in $CONFIGS; do
    echo ">> run config: $cfg"
    run_config "$cfg"
done

# ---------------------------------------------------------------------------
# Comparison table: size x config (avg bus BW, GB/s) + speedup vs hpcx.
# ---------------------------------------------------------------------------
{
    echo
    echo "== avg bus bandwidth (GB/s), 8x32=$NTASKS ranks, device $UCX_NET_DEVICES =="
    printf "%12s" "size"
    for c in $CONFIGS; do printf "%13s" "$c"; done
    if echo "$CONFIGS" | grep -qw hpcx; then
        for c in $CONFIGS; do
            [[ "$c" == "hpcx" ]] && continue
            printf "%14s" "$c/hpcx"
        done
    fi
    printf "\n"

    sizes=$(awk -F, 'NR>1 {print $2}' "$SUMMARY_CSV" | sort -n -u)
    for sz in $sizes; do
        printf "%12s" "$sz"
        for c in $CONFIGS; do
            bw=$(awk -F, -v c="$c" -v s="$sz" 'NR>1 && $1==c && $2==s {print $5}' "$SUMMARY_CSV" | tail -1)
            printf "%13s" "${bw:--}"
        done
        if echo "$CONFIGS" | grep -qw hpcx; then
            base=$(awk -F, -v s="$sz" 'NR>1 && $1=="hpcx" && $2==s {print $5}' "$SUMMARY_CSV" | tail -1)
            for c in $CONFIGS; do
                [[ "$c" == "hpcx" ]] && continue
                bw=$(awk -F, -v c="$c" -v s="$sz" 'NR>1 && $1==c && $2==s {print $5}' "$SUMMARY_CSV" | tail -1)
                if [[ -n "$base" && -n "$bw" ]]; then
                    printf "%14s" "$(awk -v a="$bw" -v b="$base" 'BEGIN{ if (b>0) printf "%.3f", a/b; else printf "-" }')"
                else
                    printf "%14s" "-"
                fi
            done
        fi
        printf "\n"
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "wrote $SUMMARY_CSV"
echo "done: $RESULT_DIR"
