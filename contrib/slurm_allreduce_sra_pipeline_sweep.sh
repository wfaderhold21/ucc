#!/usr/bin/env bash
#
# Sweep the host-side SRA allreduce pipeline parameters (frag_size x pdepth) on
# the patched build, at a single dense geometry (8 nodes x 32 PPN = 256 ranks).
# This is Plan #2 territory: find the frag_size/pdepth that wins across the whole
# 512KB-4MB range at high rank counts, where the shipped 256KB/pdepth2 default
# is too fine mid-range (2MB dip) yet wins big at 4MB.
#
# Pure RUNTIME sweep -- no rebuild. A non-auto UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE
# is used verbatim by get_pipeline_params(), so each config is just an env value.
#
# Submit from the repository root:
#   sbatch contrib/slurm_allreduce_sra_pipeline_sweep.sh
#
# Each config is "label=<pipeline string>". IMPORTANT: the parser seeds defaults
# with threshold=SIZE_MAX, so every string MUST set thresh= or pipelining stays
# off. Order must be given explicitly (parser default is sequential); we want
# parallel. 'mono' is the old monolithic path (n) as the in-build baseline.

#SBATCH --job-name=ucc-ar-sra-sweep
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
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/allreduce-sra-sweep-$JOB_ID"}

NNODES=${SLURM_NNODES:-8}
PPN=${PPN:-${SLURM_NTASKS_PER_NODE:-32}}
NP=${NP:-$((NNODES * PPN))}

# Rank-count sweep. Each entry is a total rank count, placed PPN per node
# (--map-by ppr:PPN:node). Default: the single NP point above. For a low-rank
# sweep set PPN=1 and RANKS="2 4 6" (needs #SBATCH --nodes>=6).
RANKS=${RANKS:-"$NP"}

# Size sweep (float32: count*4 = bytes). 32K..1M -> 128KB..4MB.
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
MIN_COUNT=${MIN_COUNT:-32K}
MAX_COUNT=${MAX_COUNT:-1M}
FACTOR=${FACTOR:-2}
# 256-rank runs are noisy (~15% jitter); use more iters for stable medians.
WARMUP=${WARMUP:-50}
ITERS=${ITERS:-200}

# Pipeline configs to sweep: "label=<UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE value>".
# f256_d2 reproduces the shipped auto default. Override CONFIGS to narrow.
CONFIGS=${CONFIGS:-"
mono=n
f256_d2=thresh=256K:fragsize=256K:nfrags=2:pdepth=2:parallel
f256_d4=thresh=256K:fragsize=256K:nfrags=2:pdepth=4:parallel
f512_d2=thresh=256K:fragsize=512K:nfrags=2:pdepth=2:parallel
f512_d4=thresh=256K:fragsize=512K:nfrags=2:pdepth=4:parallel
f1m_d2=thresh=256K:fragsize=1M:nfrags=2:pdepth=2:parallel
f1m_d4=thresh=256K:fragsize=1M:nfrags=2:pdepth=4:parallel
"}

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

UCX_HOME=${UCX_HOME:-${HPCX_UCX_DIR:-}}
MPI_HOME=${MPI_HOME:-${HPCX_MPI_DIR:-${OMPI_HOME:-}}}

BIN="$INSTALL_DIR/bin/ucc_perftest"
if [[ ! -x "$BIN" ]]; then
    echo "patched ucc_perftest not found: $BIN (build first, or set INSTALL_DIR)" >&2
    exit 1
fi

LD="$INSTALL_DIR/lib:$INSTALL_DIR/lib64"
[[ -n "${UCX_HOME:-}" ]] && LD="$LD:$UCX_HOME/lib:$UCX_HOME/lib64"
[[ -n "${MPI_HOME:-}" ]] && LD="$LD:$MPI_HOME/lib:$MPI_HOME/lib64"
LD="${LD}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export UCC_TL_UCP_TUNE=${UCC_TL_UCP_TUNE:-"allreduce:0-inf:@sra_knomial"}
export UCX_TLS UCX_NET_DEVICES

echo "install:       $INSTALL_DIR"
echo "results:       $RESULT_DIR"
echo "geometry:      $NNODES nodes x $PPN ppn = $NP ranks"
echo "dtype/op:      $DTYPE / $OP"
echo "count sweep:   $MIN_COUNT .. $MAX_COUNT elems, factor $FACTOR (warmup $WARMUP, iters $ITERS)"
echo "net devices:   $UCX_NET_DEVICES"
echo "configs:"
echo "$CONFIGS" | sed '/^[[:space:]]*$/d;s/^/  /'

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "config,pipeline,nranks,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_one() {
    local label="$1" pipeline="$2" nranks="$3" out="$4"

    export UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE="$pipeline"

    local cmd=(mpirun
        --np "$nranks"
        --map-by "ppr:${PPN}:node"
        --bind-to core
        -x "PATH=$INSTALL_DIR/bin:$PATH"
        -x "LD_LIBRARY_PATH=$LD"
        -x OMP_NUM_THREADS
        -x UCC_TL_UCP_TUNE
        -x UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    if [[ -n "$MPIRUN_ARGS" ]]; then
        read -r -a extra <<< "$MPIRUN_ARGS"; cmd+=("${extra[@]}")
    fi
    cmd+=("$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP"
          -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" -w "$WARMUP" -n "$ITERS" -F)

    {
        echo "config=$label pipeline=$pipeline nranks=$nranks"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"

    awk -v c="$label" -v p="$pipeline" -v r="$nranks" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print c "," p "," r "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

for nranks in $RANKS; do
    for line in $CONFIGS; do
        label="${line%%=*}"
        pipeline="${line#*=}"
        out="$RESULT_DIR/sweep-np${nranks}-${label}.log"
        echo ">> run: nranks=$nranks config=$label pipeline=$pipeline"
        run_one "$label" "$pipeline" "$nranks" "$out"
    done
done

# One bw_avg (GB/s) matrix per rank count: rows = size, cols = config, plus the
# per-row best config.
echo
echo "== bw_avg (GB/s) matrices : rows=size, cols=config, per rank count =="
echo "wrote $SUMMARY_CSV"
{
    labels=$(for line in $CONFIGS; do echo "${line%%=*}"; done)
    for nranks in $RANKS; do
        echo "-- nranks=$nranks --"
        printf "%12s" "size"
        for l in $labels; do printf "%10s" "$l"; done
        printf "%12s\n" "best"
        sizes=$(awk -F, -v r="$nranks" 'NR>1 && $3==r {print $4}' "$SUMMARY_CSV" \
                | sort -n -u)
        for sz in $sizes; do
            printf "%12s" "$sz"
            best_bw=0; best_l="-"
            for l in $labels; do
                bw=$(awk -F, -v l="$l" -v r="$nranks" -v s="$sz" \
                        'NR>1 && $1==l && $3==r && $4==s {print $7}' \
                        "$SUMMARY_CSV" | tail -1)
                printf "%10s" "${bw:--}"
                if [[ -n "$bw" ]] && awk -v a="$bw" -v b="$best_bw" 'BEGIN{exit !(a>b)}'; then
                    best_bw="$bw"; best_l="$l"
                fi
            done
            printf "%12s\n" "$best_l"
        done
        echo
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
