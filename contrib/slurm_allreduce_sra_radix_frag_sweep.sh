#!/usr/bin/env bash
#
# Plan #2 sweep: turn Plan #1's hard-coded pipeline constants and the fixed
# REDUCE_SCATTER_KN_RADIX=4 into measured optima, and check whether `ring` owns a
# contiguous sub-window of 256KB-4MB.
#
# Cross-product swept (all via runtime env -- NO rebuild needed once a build with
# the Plan #1 defaults is installed):
#   {radix in RADIXES} x {frag_size in FRAGS} x {pdepth in PDEPTHS}  (alg=sra)
#   + `mono`  (monolithic host path, the pre-Plan-#1 behavior)
#   + `ring`  (allreduce ring; radix/pipeline N/A)
# over MIN_COUNT..MAX_COUNT x {RANKS}.
#
# Knobs used per config:
#   UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX  -> radix (RS knomial radix; tl_ucp.c default 4)
#   UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE-> frag/pdepth pipeline string (or `n` = mono)
#   UCC_TL_UCP_TUNE                     -> @sra_knomial vs @ring for the alg dimension
#
# ring requires count % tsize == 0 (allreduce_ring.c:108). All RANKS here are
# powers of two and the count sweep is power-of-two, so ring is always valid.
#
# Submit from the repository root on the SLURM login node:
#   sbatch contrib/slurm_allreduce_sra_radix_frag_sweep.sh
# Narrow the grid via env, e.g.:
#   RADIXES="4 8" FRAGS="256K 512K" PDEPTHS="2" sbatch contrib/slurm_...

#SBATCH --job-name=ucc-ar-radix-sweep
#SBATCH --partition=thor
#SBATCH --nodes=16
#SBATCH --ntasks-per-node=32
#SBATCH --time=03:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -e

SRC_DIR=${SRC_DIR:-${HOME}/ucc}
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/allreduce-radix-sweep-$JOB_ID"}

# Low-rank sweep by default: one rank per node isolates the inter-node network
# path (this is the regime Plan #1 tuned at 2/4/6 ranks). Override for a dense
# high-rank point, e.g. RANKS="256" PPN=32.
RANKS=${RANKS:-"2 4 8 16"}
PPN=${PPN:-1}

# Geometry list of "nranks:ppn". Default: each RANKS entry at fixed PPN.
# PPN-sweep mode: set PPN_LIST (+ NODES) to scale processes-per-node on a fixed
# node count -- e.g. NODES=8 PPN_LIST="1 2 4 8 16 32" gives 8..256 ranks on 8
# nodes, isolating the effect of intra-node density at each total rank count.
GEOMETRIES=()
if [[ -n "${PPN_LIST:-}" ]]; then
    NODES=${NODES:-1}
    for ppn in $PPN_LIST; do
        GEOMETRIES+=("$(( NODES * ppn )):${ppn}")
    done
else
    for nranks in $RANKS; do
        GEOMETRIES+=("${nranks}:${PPN}")
    done
fi
RANK_LIST=$(for g in "${GEOMETRIES[@]}"; do echo "${g%%:*}"; done)

# Size sweep (float32: count*4 = bytes). 64K..1M count -> 256KB..4MB.
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
MIN_COUNT=${MIN_COUNT:-64K}
MAX_COUNT=${MAX_COUNT:-1M}
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-50}
ITERS=${ITERS:-200}

# Grid dimensions (Plan #2: radix {2,4,8} x frag {128K,256K,512K} x pdepth {2,3}).
RADIXES=${RADIXES:-"2 4 8"}
FRAGS=${FRAGS:-"128K 256K 512K"}
PDEPTHS=${PDEPTHS:-"2 3"}
THRESH=${THRESH:-256K}
# Extra non-grid configs to include. mono = pre-Plan-#1 monolithic host path.
INCLUDE_MONO=${INCLUDE_MONO:-1}
INCLUDE_RING=${INCLUDE_RING:-1}

MPIRUN_ARGS=${MPIRUN_ARGS:-}
UCX_TLS=${UCX_TLS:-"sm,rc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_0:1"}
MODULES=${MODULES:-"gcc hpcx"}

mkdir -p "$RESULT_DIR"

# Build the config list: each entry is "label|radix|alg|pipeline".
#   radix    = RS knomial radix, or '-' to leave at build default
#   alg      = sra_knomial | ring
#   pipeline = SRA pipeline string, 'n' for monolithic, or '-' to leave default
CONFIGS=()
for radix in $RADIXES; do
    for frag in $FRAGS; do
        for pd in $PDEPTHS; do
            label="r${radix}_f${frag}_d${pd}"
            pl="thresh=${THRESH}:fragsize=${frag}:nfrags=2:pdepth=${pd}:parallel"
            CONFIGS+=("${label}|${radix}|sra_knomial|${pl}")
        done
    done
done
[[ "$INCLUDE_MONO" == "1" ]] && CONFIGS+=("mono|4|sra_knomial|n")
[[ "$INCLUDE_RING" == "1" ]] && CONFIGS+=("ring|-|ring|-")

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
    echo "ucc_perftest not found: $BIN (build first, or set INSTALL_DIR)" >&2
    exit 1
fi

LD="$INSTALL_DIR/lib:$INSTALL_DIR/lib64"
[[ -n "${UCX_HOME:-}" ]] && LD="$LD:$UCX_HOME/lib:$UCX_HOME/lib64"
[[ -n "${MPI_HOME:-}" ]] && LD="$LD:$MPI_HOME/lib:$MPI_HOME/lib64"
LD="${LD}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

export OMP_NUM_THREADS=${OMP_NUM_THREADS:-1}
export UCX_TLS UCX_NET_DEVICES

echo "install:       $INSTALL_DIR"
echo "results:       $RESULT_DIR"
echo "geometry:      ${GEOMETRIES[*]} (nranks:ppn)"
echo "dtype/op:      $DTYPE / $OP"
echo "count sweep:   $MIN_COUNT .. $MAX_COUNT elems, factor $FACTOR (warmup $WARMUP, iters $ITERS)"
echo "grid:          radix={$RADIXES} frag={$FRAGS} pdepth={$PDEPTHS} thresh=$THRESH"
echo "net devices:   $UCX_NET_DEVICES"
echo "configs (${#CONFIGS[@]}):"
printf '  %s\n' "${CONFIGS[@]}"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "config,radix,alg,pipeline,nranks,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

run_one() {
    local label="$1" radix="$2" alg="$3" pipeline="$4" nranks="$5" ppn="$6" out="$7"

    unset UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX
    unset UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE
    [[ "$radix"    != "-" ]] && export UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX="$radix"
    [[ "$pipeline" != "-" ]] && export UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE="$pipeline"
    export UCC_TL_UCP_TUNE="allreduce:0-inf:@${alg}"

    local cmd=(mpirun
        --np "$nranks"
        --map-by "ppr:${ppn}:node"
        --bind-to core
        -x "PATH=$INSTALL_DIR/bin:$PATH"
        -x "LD_LIBRARY_PATH=$LD"
        -x OMP_NUM_THREADS
        -x UCC_TL_UCP_TUNE
        -x "UCX_TLS=${UCX_TLS}"
        -x "UCX_NET_DEVICES=${UCX_NET_DEVICES}"
    )
    [[ "$radix"    != "-" ]] && cmd+=(-x UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX)
    [[ "$pipeline" != "-" ]] && cmd+=(-x UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE)
    if [[ -n "$MPIRUN_ARGS" ]]; then
        read -r -a extra <<< "$MPIRUN_ARGS"; cmd+=("${extra[@]}")
    fi
    cmd+=("$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP"
          -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" -w "$WARMUP" -n "$ITERS" -F)

    {
        echo "config=$label radix=$radix alg=$alg pipeline=$pipeline nranks=$nranks"
        echo "command: ${cmd[*]}"
    } | tee "$out"

    "${cmd[@]}" 2>&1 | tee -a "$out"

    awk -v c="$label" -v rx="$radix" -v a="$alg" -v p="$pipeline" -v r="$nranks" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print c "," rx "," a "," p "," r "," $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

for geom in "${GEOMETRIES[@]}"; do
    IFS=':' read -r nranks ppn <<< "$geom"
    for entry in "${CONFIGS[@]}"; do
        IFS='|' read -r label radix alg pipeline <<< "$entry"
        out="$RESULT_DIR/sweep-np${nranks}-${label}.log"
        echo ">> run: nranks=$nranks ppn=$ppn config=$label radix=$radix alg=$alg pipeline=$pipeline"
        run_one "$label" "$radix" "$alg" "$pipeline" "$nranks" "$ppn" "$out"
    done
done

# bw_avg (GB/s) matrix per rank count: rows = size, cols = config, + per-row best.
echo
echo "== bw_avg (GB/s) matrices : rows=size, cols=config, per rank count =="
echo "wrote $SUMMARY_CSV"
{
    labels=$(for entry in "${CONFIGS[@]}"; do echo "${entry%%|*}"; done)
    for nranks in $RANK_LIST; do
        echo "-- nranks=$nranks --"
        printf "%12s" "size"
        for l in $labels; do printf "%12s" "$l"; done
        printf "%14s\n" "best"
        sizes=$(awk -F, -v r="$nranks" 'NR>1 && $5==r {print $6}' "$SUMMARY_CSV" \
                | sort -n -u)
        for sz in $sizes; do
            printf "%12s" "$sz"
            best_bw=0; best_l="-"
            for l in $labels; do
                bw=$(awk -F, -v l="$l" -v r="$nranks" -v s="$sz" \
                        'NR>1 && $1==l && $5==r && $6==s {print $9}' \
                        "$SUMMARY_CSV" | tail -1)
                printf "%12s" "${bw:--}"
                if [[ -n "$bw" ]] && awk -v a="$bw" -v b="$best_bw" 'BEGIN{exit !(a>b)}'; then
                    best_bw="$bw"; best_l="$l"
                fi
            done
            printf "%14s\n" "$best_l"
        done
        echo
    done
} | tee "$RESULT_DIR/summary.txt"

echo
echo "done: $RESULT_DIR"
