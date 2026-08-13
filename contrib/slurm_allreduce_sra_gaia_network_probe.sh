#!/bin/bash
# Task 60 -- SRA-knomial: how do useful fragment size and effective pipeline
# depth scale with the network?
#
# Companion to the task-59 cache probe
# (contrib/slurm_allreduce_sra_gaia_frag_cache_probe.sh). Task 59 held the
# network fixed and moved the host/cache conditions; this script does the
# reverse. Every arm below keeps the host side pinned at the task-59 "base"
# configuration -- 8 PPN, --map-by ppr:8:node --bind-to core, which packs
# ranks onto socket 0 -- so that cache/NUMA conditions are byte-for-byte the
# ones task 59 already characterised, and only network conditions move.
#
# Arms (ARM= env var), all 4 nodes / 8 PPN unless noted:
#
#   base400  UCX_NET_DEVICES=mlx5_0:1        400 Gb/s, reference arm
#   bw200    UCX_NET_DEVICES=mlx5_1:1        200 Gb/s -- the clean bandwidth
#                                            lever: measured 198.26 vs 394.95
#                                            Gb/s (1.99x) with 8B latency
#                                            1.88 vs 2.15 us, i.e. bandwidth
#                                            halves while latency is held (in
#                                            fact marginally better). Both HCAs
#                                            are on NUMA 0, same socket as the
#                                            ranks, so PCIe/NUMA locality is
#                                            unchanged.
#   dev2     mlx5_0:1,mlx5_3:1               2x400 Gb/s, both NUMA 0 --
#                                            injection width without changing
#                                            per-link latency.
#   n2       2 nodes                         node count down
#   n8       8 nodes                         node count up
#   tcp      UCX_TLS=sm,tcp on ibp24s0       high-latency / low-bandwidth
#                                            extreme. CONFOUNDED: the TCP path
#                                            also adds host CPU cost, which
#                                            task 59 showed is the dominant
#                                            per-fragment term. Supporting
#                                            evidence only -- do not use it as
#                                            a primary bandwidth lever.
#   insu     4 nodes constrained to one switch unit
#   xsu      4 nodes spanning switch units   topology at fixed node count
#
# Grid, per arm: fragsize x pdepth, 25 cells + a mono control, REPLICATES deep.
#
# Two artefacts from prior tasks are designed out of the grid rather than
# re-measured:
#   * the n_frags>=2 floor (task 59): once requested fragsize >= msgsize/2 the
#     message is still cut into exactly 2 pieces, so all larger requested
#     fragments are the identical run. Analysis must use EFFECTIVE fragment
#     size msgsize/n_frags, never the requested value. At 1 MiB the 1024K and
#     2048K columns are floor duplicates and are dropped in analysis.
#   * n_frags == 3 (task 59 spin-off) reproduces a 16-69% regression. No cell
#     in this grid lands on n_frags == 3 at any of the three message sizes.
#
# Effective depth is min(n_frags, pdepth, UCC_SCHEDULE_PIPELINED_MAX_FRAGS).
# This script is intended to run against the MAX_FRAGS=16 build so that
# bytes-in-flight has enough dynamic range to test a BDP model; provenance.txt
# records the compiled-in value. Group results by effective depth.
#
# Submit (build once, then reuse the install for every arm):
#   sbatch --export=ALL,ARM=base400 contrib/slurm_allreduce_sra_gaia_network_probe.sh
#   sbatch --export=ALL,ARM=bw200,SKIP_BUILD=1,INSTALL_DIR=<install> ...
#   sbatch --export=ALL,ARM=dev2,SKIP_BUILD=1,INSTALL_DIR=<install> ...
#   sbatch --export=ALL,ARM=n2,SKIP_BUILD=1,INSTALL_DIR=<install> --nodes=2 ...
#   sbatch --export=ALL,ARM=n8,SKIP_BUILD=1,INSTALL_DIR=<install> --nodes=8 ...
#   sbatch --export=ALL,ARM=tcp,SKIP_BUILD=1,INSTALL_DIR=<install> ...
#   sbatch --export=ALL,ARM=insu,SKIP_BUILD=1,INSTALL_DIR=<install> --constraint=su3 ...
#
#SBATCH --job-name=ucc-sra-t60-net
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --exclusive
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -u

ARM=${ARM:-base400}
JOB_ID=${SLURM_JOB_ID:-manual}

# SLURM stages the batch script into /var/spool/slurm/d, so $0 is useless for
# locating the tree. Prefer the submit dir (fixed in task 1).
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$PWD}}
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-$JOB_ID"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-t60-net-${ARM}-${JOB_ID}"}
SKIP_BUILD=${SKIP_BUILD:-0}

NP=${SLURM_NTASKS:-32}
PPN=${SLURM_NTASKS_PER_NODE:-8}
NNODES=${SLURM_JOB_NUM_NODES:-4}

# ---- constants held across every arm -------------------------------------
# Counts are float32 elements: 256K -> 1 MiB, 1M -> 4 MiB, 4M -> 16 MiB.
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
# One perftest invocation PER SIZE, rather than one sweep over a -b..-e range.
# This costs an extra mpirun launch per point but is what makes the IB counter
# deltas attributable to a single message size -- with a range sweep the
# counters aggregate over every size in the cell and cannot be correlated with
# bytes-in-flight, which is the whole point of this probe.
# Counts are float32 elements: 256K -> 1 MiB ... 4M -> 16 MiB.
SIZES=${SIZES:-"256K 512K 1M 2M 4M"}
WARMUP=${WARMUP:-50}
ITERS=${ITERS:-200}
THRESH=${THRESH:-64K}     # MUST be explicit: the parser seeds threshold to
                          # SIZE_MAX, so omitting it silently disables
                          # pipelining altogether.
NFRAGS=${NFRAGS:-2}
REPLICATES=${REPLICATES:-3}
PLACEMENT=${PLACEMENT:-"--map-by ppr:${PPN}:node --bind-to core"}

FRAGS_KB=${FRAGS_KB:-"128 256 512 1024 2048"}
PDEPTHS=${PDEPTHS:-"1 2 4 8 16"}

# ---- per-arm network conditions ------------------------------------------
UCX_TLS=${UCX_TLS:-"sm,dc"}
case "$ARM" in
    base400)      NETDEV="mlx5_0:1" ;;
    bw200)        NETDEV="mlx5_1:1" ;;
    dev2)         NETDEV="mlx5_0:1,mlx5_3:1" ;;
    n2|n8)        NETDEV="mlx5_0:1" ;;
    insu|xsu)     NETDEV="mlx5_0:1" ;;
    tcp)          NETDEV="ibp24s0"; UCX_TLS="sm,tcp" ;;
    *) echo "FATAL: unknown ARM='$ARM'" >&2; exit 2 ;;
esac
UCX_NET_DEVICES=${UCX_NET_DEVICES:-$NETDEV}

# Guard the geometry arms so a forgotten --nodes cannot silently mislabel data.
case "$ARM" in
    n2) [ "$NNODES" -eq 2 ] || { echo "FATAL: ARM=n2 needs --nodes=2, got $NNODES" >&2; exit 2; } ;;
    n8) [ "$NNODES" -eq 8 ] || { echo "FATAL: ARM=n8 needs --nodes=8, got $NNODES" >&2; exit 2; } ;;
    *)  [ "$NNODES" -eq 4 ] || { echo "FATAL: ARM=$ARM needs --nodes=4, got $NNODES" >&2; exit 2; } ;;
esac

mkdir -p "$RESULT_DIR"

# IB devices whose counters we snapshot. Superset of what any arm uses, so the
# unused ones double as a negative control: traffic must NOT appear on them.
COUNTER_DEVS=${COUNTER_DEVS:-"mlx5_0 mlx5_1 mlx5_3"}
CNT_PORT=${CNT_PORT:-1}
# port_xmit_wait is the congestion signal; the hw_counters are the retransmit
# and receiver-not-ready path. perfquery is unavailable (no UMAD port, same
# permission wall that blocked perf in task 59) but these sysfs files are
# world-readable, so unlike task 59 this probe does get real counters.
CNT_FILES="counters/port_xmit_data counters/port_rcv_data counters/port_xmit_packets counters/port_rcv_packets counters/port_xmit_wait counters/port_xmit_discards hw_counters/packet_seq_err hw_counters/out_of_sequence hw_counters/req_transport_retries_exceeded hw_counters/rnr_nak_retry_err hw_counters/out_of_buffer hw_counters/local_ack_timeout_err"

# ---- build ----------------------------------------------------------------
if [ "$SKIP_BUILD" != "1" ]; then
    cd "$SRC_DIR" || exit 1
    make distclean >/dev/null 2>&1
    ./configure --prefix="$INSTALL_DIR" --with-mpi="$MPI_HOME" \
                --with-ucx="$UCX_HOME" --without-cuda --without-sharp \
        > "$RESULT_DIR/configure.log" 2>&1 || { echo "configure failed"; exit 1; }
    make -j"$(nproc)" > "$RESULT_DIR/make.log" 2>&1 || { echo "make failed"; exit 1; }
    make install >> "$RESULT_DIR/make.log" 2>&1 || { echo "install failed"; exit 1; }
fi

BIN="$INSTALL_DIR/bin/ucc_perftest"
[ -x "$BIN" ] || { echo "FATAL: $BIN missing (SKIP_BUILD=$SKIP_BUILD)" >&2; exit 1; }
RUN_LD="$INSTALL_DIR/lib:$UCX_HOME/lib:$MPI_HOME/lib:${LD_LIBRARY_PATH:-}"

# ---- provenance -----------------------------------------------------------
{
    echo "job_id:        $JOB_ID"
    echo "arm:           $ARM"
    echo "date:          $(date -Is)"
    echo "nodelist:      ${SLURM_JOB_NODELIST:-?}"
    echo "geometry:      nodes=$NNODES ppn=$PPN np=$NP"
    echo "placement:     $PLACEMENT"
    echo "UCX_TLS:       $UCX_TLS"
    echo "UCX_NET_DEVICES: $UCX_NET_DEVICES"
    echo "sizes:         counts [$SIZES] dtype $DTYPE op $OP (one run per size)"
    echo "iters:         warmup=$WARMUP iters=$ITERS replicates=$REPLICATES"
    echo "grid frags_kb: $FRAGS_KB"
    echo "grid pdepths:  $PDEPTHS"
    echo "thresh:        $THRESH  nfrags_floor: $NFRAGS"
    echo "install_dir:   $INSTALL_DIR  skip_build=$SKIP_BUILD"
    echo "--- git ---"
    ( cd "$SRC_DIR" && git rev-parse HEAD 2>/dev/null; git status --porcelain 2>/dev/null )
    echo "--- MAX_FRAGS (governs effective depth ceiling) ---"
    grep -n "define UCC_SCHEDULE_PIPELINED_MAX_FRAGS" \
        "$SRC_DIR/src/schedule/ucc_schedule_pipelined.h" 2>/dev/null
    echo "--- counter access ---"
    echo "perf_event_paranoid: $(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)"
    echo "perfquery: $(perfquery 2>&1 | head -1)"
} > "$RESULT_DIR/provenance.txt" 2>&1

# ---- nominal link properties, recorded separately from application throughput
# NOTE: --ntasks MUST be given explicitly. Inside the allocation SLURM_NTASKS
# is the full rank count (e.g. 32), which srun inherits, so --ntasks-per-node=1
# alone still launches one task per RANK and every counter line is emitted
# PPN times.
srun --ntasks="$NNODES" --ntasks-per-node=1 --nodes="$NNODES" bash -c '
    echo "### $(hostname)"
    for d in '"$COUNTER_DEVS"' mlx5_9; do
        [ -e /sys/class/infiniband/$d ] || continue
        printf "%-8s rate=%-22s state=%-10s netdev=%-12s numa=%s\n" "$d" \
            "$(cat /sys/class/infiniband/$d/ports/1/rate 2>/dev/null)" \
            "$(cat /sys/class/infiniband/$d/ports/1/state 2>/dev/null | tr -d " ")" \
            "$(ls /sys/class/infiniband/$d/device/net/ 2>/dev/null | tr "\n" ",")" \
            "$(cat /sys/class/infiniband/$d/device/numa_node 2>/dev/null)"
    done
    echo "--- topology ---"; lscpu | grep -E "^(NUMA|Model name|Socket|Core|CPU\(s\))"
' > "$RESULT_DIR/link_and_topology.txt" 2>&1

# ---- counter snapshot helper ---------------------------------------------
# Writes "<host> <dev> <counter> <value>" lines for every node.
snapshot_counters() {
    local out="$1"
    srun --ntasks="$NNODES" --ntasks-per-node=1 --nodes="$NNODES" bash -c '
        h=$(hostname)
        for d in '"$COUNTER_DEVS"'; do
            for f in '"$CNT_FILES"'; do
                v=$(cat /sys/class/infiniband/$d/ports/'"$CNT_PORT"'/$f 2>/dev/null)
                [ -n "$v" ] && echo "$h $d $(basename $f) $v"
            done
        done
    ' > "$out" 2>/dev/null
}

# UCX_TLS ("sm,dc") and UCX_NET_DEVICES ("mlx5_0:1,mlx5_3:1") both contain
# commas, which would silently split into extra CSV columns and shift every
# field to the right. Emit them with commas mapped to '|'.
TLS_CSV=${UCX_TLS//,/|}
DEV_CSV=${UCX_NET_DEVICES//,/|}

CSV="$RESULT_DIR/summary.csv"
echo "arm,nnodes,ppn,tls,devices,replicate,config,frag_kb,pdepth,pipeline,nranks,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" > "$CSV"
CNTCSV="$RESULT_DIR/counters.csv"
echo "arm,nnodes,tls,devices,replicate,config,frag_kb,pdepth,size_bytes,host,dev,counter,delta,wall_s" > "$CNTCSV"

# ---- cell runner ----------------------------------------------------------
run_cell() {
    local label="$1" pipeline="$2" frag_kb="$3" pdepth="$4" rep="$5" cnt="$6"
    local log="$RESULT_DIR/cell-r${rep}-${label}-c${cnt}.log"
    local pre="$RESULT_DIR/.cnt_pre" post="$RESULT_DIR/.cnt_post"

    echo ">> arm=$ARM rep=$rep cell=$label count=$cnt pdepth=$pdepth pipeline=$pipeline"

    snapshot_counters "$pre"
    local t0=$SECONDS

    timeout -k 30s 20m \
    "$MPI_HOME/bin/mpirun" --np "$NP" $PLACEMENT \
        -x PATH="$INSTALL_DIR/bin:$PATH" \
        -x LD_LIBRARY_PATH="$RUN_LD" \
        -x UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial \
        -x UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE="$pipeline" \
        -x UCX_TLS="$UCX_TLS" \
        -x UCX_NET_DEVICES="$UCX_NET_DEVICES" \
        -x OMP_NUM_THREADS=1 \
        "$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" \
               -b "$cnt" -e "$cnt" \
               -w "$WARMUP" -n "$ITERS" -F \
        > "$log" 2>&1
    local rc=$?
    local wall=$(( SECONDS - t0 ))

    snapshot_counters "$post"

    if [ $rc -ne 0 ]; then
        echo "FAILED rc=$rc arm=$ARM rep=$rep cell=$label count=$cnt" \
            | tee -a "$RESULT_DIR/failed_cells"
    fi

    # perftest table -> CSV (fields: count size ... time_avg bw_avg bw_max bw_min)
    awk -v arm="$ARM" -v nn="$NNODES" -v ppn="$PPN" -v tls="$TLS_CSV" \
        -v dev="$DEV_CSV" -v rep="$rep" -v c="$label" -v fk="$frag_kb" \
        -v pd="$pdepth" -v p="$pipeline" -v np="$NP" \
        '/^[ \t]*[0-9]+[ \t]+[0-9]+[ \t]/ {
            print arm","nn","ppn","tls","dev","rep","c","fk","pd","p","np","$2","$1","$3","$6","$7","$8
         }' "$log" >> "$CSV"

    # Size actually measured, taken from the perftest output rather than assumed
    # from the count, so the counter rows key on the same value as summary.csv.
    local sz
    sz=$(awk '/^[ \t]*[0-9]+[ \t]+[0-9]+[ \t]/ {print $2; exit}' "$log")
    sz=${sz:-NA}

    # counter deltas (post - pre), joined on host+dev+counter
    awk -v arm="$ARM" -v nn="$NNODES" -v tls="$TLS_CSV" -v dev="$DEV_CSV" \
        -v rep="$rep" -v c="$label" -v fk="$frag_kb" -v pd="$pdepth" \
        -v sz="$sz" -v w="$wall" '
        NR==FNR { pre[$1" "$2" "$3]=$4; next }
        { k=$1" "$2" "$3;
          if (k in pre) { d=$4-pre[k]; if (d<0) d="NA";
                          print arm","nn","tls","dev","rep","c","fk","pd","sz","$1","$2","$3","d","w } }
    ' "$pre" "$post" >> "$CNTCSV"
}

# ---- sweep ----------------------------------------------------------------
# Replicate is the OUTER loop so that slow drift shows up as replicate spread
# rather than being aliased onto one corner of the grid (task 59 convention).
for rep in $(seq 1 "$REPLICATES"); do
    for f in $FRAGS_KB; do
        for d in $PDEPTHS; do
            for cnt in $SIZES; do
                run_cell "f${f}k_d${d}" \
                    "thresh=${THRESH}:fragsize=${f}K:nfrags=${NFRAGS}:pdepth=${d}:parallel" \
                    "$f" "$d" "$rep" "$cnt"
            done
        done
    done
    # monolithic (non-pipelined) control, once per size per replicate
    for cnt in $SIZES; do
        run_cell "mono" "n" "NA" "NA" "$rep" "$cnt"
    done
done

# ---- clamp detection ------------------------------------------------------
# A cell whose requested depth was clamped is NOT the depth it claims.
grep -l "exceeds max limit" "$RESULT_DIR"/cell-*.log 2>/dev/null \
    > "$RESULT_DIR/clamp_warnings.txt"

# ---- completeness ---------------------------------------------------------
# One perftest row per (cell, size) now, so the expected count per cell is
# simply the number of sizes. Fields: replicate=6, config=7 (commas in tls and
# devices are pre-escaped to '|', so these indices are stable).
EXPECT_ROWS=$(set -- $SIZES; echo $#)
{
    echo "== completeness (expect $EXPECT_ROWS rows/cell, one per size) =="
    for rep in $(seq 1 "$REPLICATES"); do
        for f in $FRAGS_KB; do
            for d in $PDEPTHS; do
                n=$(awk -F, -v r="$rep" -v c="f${f}k_d${d}" \
                    '$6==r && $7==c {n++} END{print n+0}' "$CSV")
                [ "$n" -eq 0 ] && echo "MISSING rep=$rep frag=${f}K pdepth=${d}"
                [ "$n" -ne 0 ] && [ "$n" -ne "$EXPECT_ROWS" ] && \
                    echo "INVALID rep=$rep frag=${f}K pdepth=${d} ($n rows, expected $EXPECT_ROWS)"
            done
        done
    done
    echo "== counter sanity: devices carrying traffic (unused HCAs are a negative control) =="
    awk -F, 'NR>1 && $12=="port_xmit_data" && $13!="NA" {s[$11]+=$13}
             END{for (d in s) printf "  %-8s port_xmit_data_delta_total=%d\n", d, s[d]}' "$CNTCSV"
    echo "== clamped cells (effective depth < requested) =="
    cat "$RESULT_DIR/clamp_warnings.txt" 2>/dev/null
} > "$RESULT_DIR/summary.txt"

rm -f "$RESULT_DIR/.cnt_pre" "$RESULT_DIR/.cnt_post"
touch "$RESULT_DIR/done"
echo "RESULTS: $RESULT_DIR"
