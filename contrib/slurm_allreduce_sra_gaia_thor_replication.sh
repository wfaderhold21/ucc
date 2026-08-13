#!/usr/bin/env bash
# Task 242: reproduce the Thor SRA fragment/depth regime on four Gaia nodes.
#
# All five arms run in one exclusive four-node allocation. Each host is declared
# with its actual slots for the arm (host:8 or host:32). Replicate is the outer
# loop. The twelve pipeline cells use a fixed seeded permutation and a balanced
# rotation, while monolithic controls are run before the first cell and after
# every four pipeline cells.

#SBATCH --job-name=ucc-sra-t242-thor-repl
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=32
#SBATCH --exclusive
#SBATCH --time=08:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -u

JOB_ID=${SLURM_JOB_ID:-manual}
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$PWD}}
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
INSTALL_DIR=${INSTALL_DIR:-/labhome/faderholdt/ucc-sra-build/task8-4-t60/src/install-t60}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-thor-replication-${JOB_ID}"}

DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
MIN_COUNT=${MIN_COUNT:-256K} # float32: 1 MiB
MAX_COUNT=${MAX_COUNT:-4M}   # float32: 16 MiB
FACTOR=${FACTOR:-2}
WARMUP=${WARMUP:-50}
ITERS=${ITERS:-200}
REPLICATES=${REPLICATES:-3}
FRAGS_KB=${FRAGS_KB:-"128 256 512 1024 2048 4096"}
PDEPTHS=${PDEPTHS:-"2 4"}
THRESH=${THRESH:-256K}
NFRAGS=${NFRAGS:-2}
BUILD_CAP=${BUILD_CAP:-16}
RADIX=${RADIX:-4}
ORDER_SEED=${ORDER_SEED:-2270806}
CELL_TIMEOUT=${CELL_TIMEOUT:-20m}
ARMS="gaia_base transport_rc dense_dc400 thor_like_rc400 dense_dc200"
# Seeded base permutation (seed 2270806).
BASE_ORDER=("512:4" "128:2" "2048:2" "256:4" "4096:4" "1024:2" \
            "512:2" "128:4" "2048:4" "256:2" "4096:2" "1024:4")

if [[ ${1:-} == "--validate-matrix" ]]; then
    declare -A expected seen
    for f in $FRAGS_KB; do for d in $PDEPTHS; do expected["$f:$d"]=1; done; done
    [[ ${#expected[@]} -eq 12 && ${#BASE_ORDER[@]} -eq 12 ]] || {
        echo "INVALID: expected 12 unique fragment/depth cells" >&2; exit 2; }
    for cell in "${BASE_ORDER[@]}"; do
        [[ -n ${expected[$cell]+x} && -z ${seen[$cell]+x} ]] || {
            echo "INVALID: missing or duplicate cell $cell" >&2; exit 2; }
        seen[$cell]=1
    done
    arms=$(wc -w <<< "$ARMS")
    pipeline_cells=$((arms * REPLICATES * 12))
    controls=$((arms * REPLICATES * 4))
    launches=$((pipeline_cells + controls))
    rows=$((launches * 5))
    echo "VALID arms=$arms canaries=$arms replicates=$REPLICATES pipeline_cells=$pipeline_cells controls=$controls full_launches=$launches expected_rows_if_all_viable=$rows seed=$ORDER_SEED"
    exit 0
fi

BIN="$INSTALL_DIR/bin/ucc_perftest"
if [[ ! -x "$BIN" ]]; then
    echo "FATAL: missing executable $BIN" >&2
    exit 1
fi

mkdir -p "$RESULT_DIR"
RUN_LD="$INSTALL_DIR/lib:$INSTALL_DIR/lib64:$UCX_HOME/lib:$UCX_HOME/lib64:$MPI_HOME/lib:$MPI_HOME/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
mapfile -t ALL_HOSTS < <(scontrol show hostnames "${SLURM_JOB_NODELIST:?}")
if [[ ${#ALL_HOSTS[@]} -ne 4 ]]; then
    echo "FATAL: expected 4 allocated hosts, got ${#ALL_HOSTS[@]}" >&2
    exit 2
fi
HOSTS4=$(IFS=,; echo "${ALL_HOSTS[*]:0:4}")
HOSTS4_SPEC8=""
HOSTS4_SPEC32=""
for host in "${ALL_HOSTS[@]:0:4}"; do
    HOSTS4_SPEC8+="${HOSTS4_SPEC8:+,}${host}:8"
    HOSTS4_SPEC32+="${HOSTS4_SPEC32:+,}${host}:32"
done

CSV="$RESULT_DIR/summary.csv"
STATUS="$RESULT_DIR/cell_status.csv"
ORDER="$RESULT_DIR/execution_order.csv"
CANARY="$RESULT_DIR/canary_status.csv"
echo "arm,nnodes,ppn,tls,device,replicate,control_index,config,requested_frag_kb,requested_pdepth,pipeline,nranks,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" > "$CSV"
echo "ordinal,arm,replicate,control_index,config,rc,timed_out,rows,log" > "$STATUS"
echo "ordinal,arm,replicate,position,control_index,config,requested_frag_kb,requested_pdepth" > "$ORDER"
echo "arm,nnodes,ppn,tls,device,rc,timed_out,rows,placement_ok,device_traffic_delta,supported,reason" > "$CANARY"

{
    echo "job_id: $JOB_ID"
    echo "date: $(date -Is)"
    echo "submit_dir: $SRC_DIR"
    echo "script: $SRC_DIR/contrib/slurm_allreduce_sra_gaia_thor_replication.sh"
    sha256sum "$SRC_DIR/contrib/slurm_allreduce_sra_gaia_thor_replication.sh"
    echo "allocation_hosts: $HOSTS4"
    echo "geometry: allocation=4x32; low-density=4x8; dense=4x32"
    echo "dtype/op/buffer/layout: $DTYPE/$OP/host/out-of-place"
    echo "counts: $MIN_COUNT..$MAX_COUNT factor=$FACTOR; warmup=$WARMUP iterations=$ITERS"
    echo "algorithm/radix: sra_knomial/$RADIX"
    echo "fragments_kib: $FRAGS_KB; depths: $PDEPTHS; threshold=$THRESH; nfrags_floor=$NFRAGS; build_cap=$BUILD_CAP"
    echo "replicates: $REPLICATES; order_seed=$ORDER_SEED; cell_timeout=$CELL_TIMEOUT"
    echo "install_dir: $INSTALL_DIR"
    echo "source_snapshot: $SRC_DIR/task242-source-snapshot.txt"
    echo "--- versions ---"
    "$MPI_HOME/bin/mpirun" --version 2>&1 | head -5
    "$MPI_HOME/bin/ompi_info" --version 2>&1 | head -5
    "$UCX_HOME/bin/ucx_info" -v 2>&1 | head -12
    "$INSTALL_DIR/bin/ucc_info" -v 2>&1 | head -12
    echo "--- environment ---"
    env | sort
} > "$RESULT_DIR/provenance.txt" 2>&1

# One record per node: CPU/NUMA, HCA link state/rate, PCI BDF and locality.
srun --nodes=4 --ntasks=4 --ntasks-per-node=1 bash -c '
    echo "### host=$(hostname)"
    lscpu | grep -E "^(CPU\(s\)|Model name|Socket|Core|NUMA)"
    command -v numactl >/dev/null && numactl -H
    for d in mlx5_0 mlx5_1; do
        p=/sys/class/infiniband/$d
        [[ -e $p ]] || continue
        bdf=$(basename "$(readlink -f "$p/device")")
        echo "hca=$d port=1 rate=$(cat "$p/ports/1/rate") state=$(tr -d " " < "$p/ports/1/state") pci=$bdf numa=$(cat "$p/device/numa_node")"
        lspci -s "$bdf" -vv 2>/dev/null | grep -m1 -E "LnkSta:" || true
    done
' > "$RESULT_DIR/topology.txt" 2>&1

arm_params() {
    case "$1" in
        gaia_base)       ARM_NODES=4; ARM_PPN=8;  ARM_TLS="sm,dc"; ARM_DEV="mlx5_0:1"; ARM_HOSTS=$HOSTS4_SPEC8 ;;
        transport_rc)    ARM_NODES=4; ARM_PPN=8;  ARM_TLS="sm,rc"; ARM_DEV="mlx5_0:1"; ARM_HOSTS=$HOSTS4_SPEC8 ;;
        dense_dc400)     ARM_NODES=4; ARM_PPN=32; ARM_TLS="sm,dc"; ARM_DEV="mlx5_0:1"; ARM_HOSTS=$HOSTS4_SPEC32 ;;
        thor_like_rc400) ARM_NODES=4; ARM_PPN=32; ARM_TLS="sm,rc"; ARM_DEV="mlx5_0:1"; ARM_HOSTS=$HOSTS4_SPEC32 ;;
        dense_dc200)     ARM_NODES=4; ARM_PPN=32; ARM_TLS="sm,dc"; ARM_DEV="mlx5_1:1"; ARM_HOSTS=$HOSTS4_SPEC32 ;;
        *) echo "FATAL: unknown arm $1" >&2; return 2 ;;
    esac
    ARM_NP=$((ARM_NODES * ARM_PPN))
}

counter_total() {
    local dev=${1%%:*}
    srun --nodes=4 --ntasks=4 --ntasks-per-node=1 bash -c \
        'cat /sys/class/infiniband/'"$dev"'/ports/1/counters/port_xmit_data 2>/dev/null || echo 0' \
        | awk '{s+=$1} END{printf "%.0f\n",s}'
}

mpi_base() {
    "$MPI_HOME/bin/mpirun" --host "$ARM_HOSTS" --np "$ARM_NP" \
        --map-by "ppr:${ARM_PPN}:node" --bind-to core "$@"
}

declare -A SUPPORTED

# Canary every arm before any full-matrix cell. UCX is constrained to one
# inter-node transport and one HCA; positive traffic on that HCA plus correct
# hostname multiplicity demonstrates the requested path and rank placement.
for arm in $ARMS; do
    arm_params "$arm" || exit 2
    log="$RESULT_DIR/canary-${arm}.log"
    placement="$RESULT_DIR/canary-${arm}-placement.txt"
    pre=$(counter_total "$ARM_DEV")
    mpi_base hostname > "$placement" 2>&1
    place_rc=$?
    placement_ok=0
    if [[ $place_rc -eq 0 ]]; then
        placement_ok=$(awk -v ppn="$ARM_PPN" -v nodes="$ARM_NODES" '
            {n[$1]++} END {ok=(length(n)==nodes); for (h in n) if (n[h]!=ppn) ok=0; print ok+0}' "$placement")
    fi
    timeout -k 30s 10m "$MPI_HOME/bin/mpirun" --host "$ARM_HOSTS" --np "$ARM_NP" \
        --map-by "ppr:${ARM_PPN}:node" --bind-to core \
        -x "PATH=$INSTALL_DIR/bin:$PATH" -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial \
        -x "UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX=$RADIX" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=thresh=${THRESH}:fragsize=512K:nfrags=2:pdepth=2:parallel" \
        -x "UCX_TLS=$ARM_TLS" -x "UCX_NET_DEVICES=$ARM_DEV" -x OMP_NUM_THREADS=1 \
        "$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" -b 256K -e 256K -w 10 -n 20 -F \
        > "$log" 2>&1
    rc=$?
    timed_out=0; [[ $rc -eq 124 || $rc -eq 137 ]] && timed_out=1
    rows=$(awk '/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {n++} END{print n+0}' "$log")
    post=$(counter_total "$ARM_DEV")
    delta=$((post - pre))
    supported=1; reason=ok
    if [[ $rc -ne 0 || $rows -ne 1 || $placement_ok -ne 1 || $delta -le 0 ]]; then
        supported=0
        reason="rc${rc}_rows${rows}_placement${placement_ok}_traffic${delta}"
    fi
    SUPPORTED[$arm]=$supported
    echo "$arm,$ARM_NODES,$ARM_PPN,${ARM_TLS//,/|},$ARM_DEV,$rc,$timed_out,$rows,$placement_ok,$delta,$supported,$reason" >> "$CANARY"
done

ordinal=0
run_cell() {
    local arm=$1 rep=$2 position=$3 control_index=$4 label=$5 frag=$6 depth=$7 pipeline=$8
    arm_params "$arm" || return 2
    ordinal=$((ordinal + 1))
    local log="$RESULT_DIR/cell-${arm}-r${rep}-o${ordinal}-${label}.log"
    echo "$ordinal,$arm,$rep,$position,$control_index,$label,$frag,$depth" >> "$ORDER"
    timeout -k 30s "$CELL_TIMEOUT" "$MPI_HOME/bin/mpirun" --host "$ARM_HOSTS" --np "$ARM_NP" \
        --map-by "ppr:${ARM_PPN}:node" --bind-to core \
        -x "PATH=$INSTALL_DIR/bin:$PATH" -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial \
        -x "UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX=$RADIX" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline" \
        -x "UCX_TLS=$ARM_TLS" -x "UCX_NET_DEVICES=$ARM_DEV" -x OMP_NUM_THREADS=1 \
        "$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" \
        -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" -w "$WARMUP" -n "$ITERS" -F \
        > "$log" 2>&1
    local rc=$? timed_out=0 rows
    [[ $rc -eq 124 || $rc -eq 137 ]] && timed_out=1
    rows=$(awk '/^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {n++} END{print n+0}' "$log")
    echo "$ordinal,$arm,$rep,$control_index,$label,$rc,$timed_out,$rows,${log#$RESULT_DIR/}" >> "$STATUS"
    if [[ $rc -eq 0 && $rows -eq 5 ]]; then
        awk -v arm="$arm" -v nn="$ARM_NODES" -v ppn="$ARM_PPN" \
            -v tls="${ARM_TLS//,/|}" -v dev="$ARM_DEV" -v rep="$rep" \
            -v ci="$control_index" -v cfg="$label" -v fk="$frag" -v pd="$depth" \
            -v pipe="$pipeline" -v np="$ARM_NP" '
            /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
                print arm","nn","ppn","tls","dev","rep","ci","cfg","fk","pd","pipe","np","$2","$1","$3","$6","$7","$8
            }' "$log" >> "$CSV"
    else
        echo "FAILED arm=$arm rep=$rep config=$label rc=$rc rows=$rows" | tee -a "$RESULT_DIR/failed_cells"
    fi
}

arm_index=0
for arm in $ARMS; do
    arm_index=$((arm_index + 1))
    if [[ ${SUPPORTED[$arm]} -ne 1 ]]; then
        echo "$arm" >> "$RESULT_DIR/unsupported_arms"
        continue
    fi
    for rep in $(seq 1 "$REPLICATES"); do
        control=1
        run_cell "$arm" "$rep" 0 "$control" "mono-c${control}" NA NA n
        offset=$(( (ORDER_SEED + arm_index * 5 + rep * 7) % 12 ))
        for position in $(seq 1 12); do
            idx=$(( (offset + position - 1) % 12 ))
            IFS=: read -r frag depth <<< "${BASE_ORDER[$idx]}"
            run_cell "$arm" "$rep" "$position" 0 "f${frag}k-d${depth}" "$frag" "$depth" \
                "thresh=${THRESH}:fragsize=${frag}K:nfrags=${NFRAGS}:pdepth=${depth}:parallel"
            if (( position % 4 == 0 )); then
                control=$((control + 1))
                run_cell "$arm" "$rep" "$position" "$control" "mono-c${control}" NA NA n
            fi
        done
    done
done

{
    echo "supported arms:"
    for arm in $ARMS; do echo "  $arm=${SUPPORTED[$arm]}"; done
    echo "valid data rows: $(( $(wc -l < "$CSV") - 1 ))"
    echo "cell status counts:"
    awk -F, 'NR>1 {k="rc=" $6 ",timeout=" $7 ",rows=" $8; n[k]++} END{for(k in n) print "  " k ": " n[k]}' "$STATUS" | sort
} > "$RESULT_DIR/completeness.txt"

if [[ -f "$RESULT_DIR/failed_cells" ]]; then
    echo "Task 242 completed with invalid cells; see failed_cells" >&2
    touch "$RESULT_DIR/done-with-failures"
    exit 1
fi
touch "$RESULT_DIR/done"
echo "RESULTS: $RESULT_DIR"
