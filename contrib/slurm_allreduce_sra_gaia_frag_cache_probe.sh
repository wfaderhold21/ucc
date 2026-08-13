#!/usr/bin/env bash
#
# Task 59 -- GAIA SRA-knomial: does CPU cache capacity explain the fragment-size
# optimum found in task 17?
#
# Task 17 located a performance ridge at fragment sizes 2048-4096 KiB on a
# 4-node x 8-PPN GAIA allreduce, but measured no mechanism. This harness runs
# the controlled sweeps that can distinguish a cache-capacity explanation from
# mere correlation with message size.
#
# WHY THIS DESIGN (no hardware counters available)
# ------------------------------------------------
# perf_event_paranoid is 4 on every GAIA compute node and jobs run with
# CapEff=0, so LLC-miss / memory-bandwidth / cache-reference counters cannot be
# read at all (verified on dgx-gaia-45/46/47). The mechanism therefore has to be
# attacked by *perturbation* rather than by instrumentation: change the cache
# capacity available to a rank without changing anything else, and see whether
# the optimum moves the way a capacity model demands.
#
# Node topology (Intel Xeon Platinum 8480C, Sapphire Rapids):
#   L2  2 MiB  PRIVATE per core                  -> per-rank share fixed by PPN
#   L3  105 MiB SHARED per socket (56 cores)     -> per-rank share ~ 1/ranks_per_socket
# That difference is the whole experiment: varying PPN moves the L3 share per
# rank by ~8x while leaving the L2 share per rank at exactly 2 MiB.
#
# MEASURED PLACEMENT (job 61794, dgx-gaia-17) -- do not assume, this was checked:
#   `--map-by ppr:N:node --bind-to core` packs ranks onto CONSECUTIVE cores
#   starting at core 0, so for every N <= 56 ALL ranks land on socket 0 and
#   share ONE 105 MiB L3. Task 17's 8-PPN runs were therefore 8 ranks on one
#   L3 = 13.1 MiB per rank, NOT 4 ranks per socket as a naive reading of
#   "8 PPN on a 2-socket node" would suggest. Every per-rank L3 figure below
#   follows from this measurement.
#   `--map-by ppr:4:socket --bind-to core` genuinely splits 4+4 across sockets
#   (ranks 0-3 on cores 0-3/node 0, ranks 4-7 on cores 56-59/node 1) -- this is
#   what makes the `numa` arm a clean 2x change in L3 share. `--cpu-set` was
#   rejected as it conflicts with --map-by ppr (RANK_FILE policy clash).
#
# Working set per rank per pipeline slot, derived from the source:
#   a fragment of F bytes is NOT split F/N across ranks -- every rank streams
#   essentially the whole F-byte dst window (reduce-scatter reduction inputs are
#   ~F*R/(R-1) of reads) plus an F-byte reduce-scatter scratch buffer, and each
#   of the `depth` pipeline slots owns a DISTINCT scratch (ucc_schedule_pipelined
#   .c:239-241 calls frag_init per slot; reduce_scatter_knomial.c:534-545 does a
#   per-frag ucc_mc_alloc). So:
#
#       resident working set per rank  ~=  2 * depth * F
#
#   At the task-17 geometry (depth 4, 8 ranks packed on one socket -> 13.1 MiB
#   of L3 per rank): F=1024 KiB -> 8 MiB, F=2048 KiB -> 16 MiB, F=4096 KiB ->
#   32 MiB. The observed 2048-4096 KiB ridge is thus a working set of 1.2x-2.4x
#   the per-rank L3 share -- adjacent to the L3 crossing but NOT simply "fits in
#   L3". The naive containment version of the hypothesis is already strained;
#   what the arms below test is the weaker and more defensible claim that the
#   optimum TRACKS the per-rank L3 share when that share is moved.
#
# THE ARMS AND WHAT EACH ONE FALSIFIES
# ------------------------------------
#   base    Fine fragment-size sweep at fixed depth 4, PPN 8. Locates the ridge
#           at higher resolution than task 17's factor-2 grid. Run 3x for the
#           noise floor. Purely descriptive -- establishes the thing to explain.
#
#   depth   Depth x fragment at fixed PPN. A capacity model constrains the
#           PRODUCT depth*F, not F alone, so it predicts the optimal F halves
#           each time depth doubles. A "fragment size has an intrinsic sweet
#           spot" model predicts the optimal F does not move at all. This is
#           also a direct test of task 17's weakest claim (#3, that depth and
#           fragment size are substitutes) -- substitution is exactly what a
#           capacity ceiling produces.
#
#   ppn4 / ppn16 / ppn32
#           The discriminator. All of PPN 4/8/16/32 pack onto socket 0 (measured,
#           see above), so ranks-per-L3 == PPN and the L3 share per rank moves
#           8x while the private L2 share stays at exactly 2 MiB per rank.
#           Predictions at depth 4, F_opt being where 2*4*F ~ the L3 share:
#             PPN  ranks/L3   L3 per rank   F_opt if L3-capacity-bound
#              4      4         26.2 MiB       ~4096 KiB   (ridge moves UP)
#              8      8         13.1 MiB       ~2048 KiB   (task-17 baseline)
#             16     16          6.55 MiB      ~1024 KiB   (ridge moves DOWN)
#             32     32          3.28 MiB      ~512 KiB    (ridge moves DOWN)
#           If the ridge instead stays at 2048-4096 KiB across all four, L3
#           capacity is FALSIFIED; a private-L2 effect (invariant to PPN) or a
#           non-cache effect is implicated. Note the L2 model predicts exactly
#           that invariance, so these two arms separate L2 from L3 cleanly.
#           CONFOUNDER, stated up front: changing PPN also changes total rank
#           count (16/32/64/128), per-node NIC contention, and the knomial tree
#           shape. This arm therefore cannot be read alone -- it is interpreted
#           against the `numa` arm, which moves cache sharing at CONSTANT rank
#           count and constant NIC load.
#
#   numa    The confounder-free version of the PPN test, and the single most
#           important arm. 8 ranks per node in both cells, but packed onto one
#           socket (8 ranks sharing one 105 MiB L3 = 13.1 MiB each) versus split
#           4+4 across both sockets (4 ranks per L3 = 26.2 MiB each). Identical
#           rank count, identical message sizes, identical NIC load; only the L3
#           share per rank changes, by exactly 2x. A capacity model predicts the
#           packed cell's optimum sits ~1 octave LOWER in F than the split
#           cell's. Anything else falsifies L3 capacity without the PPN arm's
#           confounders. (Caveat: splitting also adds cross-socket UPI traffic
#           for shared-memory rank pairs, which is a real but opposite-signed
#           effect -- it penalises the split cell uniformly in F rather than
#           moving its argmax, so the argmax comparison survives it.)
#
# Everything else is held fixed across every cell in every arm: dtype, op,
# out-of-place, message-size grid, warmup/iteration counts, UCX transport and
# device, node count, and the explicit pipeline string.
#
# NOTE ON THE MULTI-NODE GUARD (commit 0b0a1f01): the guard only fires on the
# *auto* path. ucc_pipeline_params_is_auto() is tested first, so an explicit
# UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE string is honoured verbatim and every
# cell genuinely pipelines on a 4-node team.
#
# NOTE ON DEPTH > 4: requires the working-tree UCC_SCHEDULE_PIPELINED_MAX_FRAGS
# = 16 change. With the committed value of 4, depth 8/16 cells silently run at
# depth 4. The script asserts on this below rather than trusting it.
#
# Submit from the rsync'd repository root on the gaia login node:
#   sbatch --export=ALL,ARM=base  contrib/slurm_allreduce_sra_gaia_frag_cache_probe.sh
#   sbatch --export=ALL,ARM=depth contrib/slurm_allreduce_sra_gaia_frag_cache_probe.sh
#   sbatch --export=ALL,ARM=numa  contrib/slurm_allreduce_sra_gaia_frag_cache_probe.sh
#   sbatch --export=ALL,ARM=ppn4  --ntasks-per-node=4  contrib/...
#   sbatch --export=ALL,ARM=ppn16 --ntasks-per-node=16 contrib/...
#   sbatch --export=ALL,ARM=ppn32 --ntasks-per-node=32 contrib/...
# Reuse an existing install (strongly preferred -- the build must be IDENTICAL
# across arms or the comparison is worthless):
#   sbatch --export=ALL,ARM=depth,SKIP_BUILD=1,INSTALL_DIR=/path/to/install ...

#SBATCH --job-name=ucc-sra-t59-cache
#SBATCH --partition=GAIA
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=8
#SBATCH --time=04:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

# --- Paths (GAIA-specific; no module environment on this cluster) ------------
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
export PATH="$MPI_HOME/bin:$PATH"

# SLURM copies the batch script into /var/spool, so BASH_SOURCE does not point
# into the repository under sbatch. SLURM_SUBMIT_DIR is the repo root.
SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}
JOB_ID=${SLURM_JOB_ID:-manual}
ARM=${ARM:-base}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-${JOB_ID}"}
RESULT_DIR=${RESULT_DIR:-"$SRC_DIR/results/gaia-t59-cache-${ARM}-${JOB_ID}"}
SKIP_BUILD=${SKIP_BUILD:-0}

# --- Geometry ----------------------------------------------------------------
NNODES=${SLURM_NNODES:-4}
PPN=${PPN:-${SLURM_NTASKS_PER_NODE:-8}}
NP=${NP:-$((NNODES * PPN))}

# --- Benchmark config (held constant across every cell of every arm) ---------
DTYPE=${DTYPE:-float32}
OP=${OP:-sum}
# 256K..4M elements @ float32 = 1 MB..16 MB, factor 2 -> 5 size points.
# Two message sizes below (1, 2 MB) and two above (8, 16 MB) the 4 MB centre
# where task 17 saw the ridge, as the task requires.
MIN_COUNT=${MIN_COUNT:-256K}
MAX_COUNT=${MAX_COUNT:-4M}
FACTOR=${FACTOR:-2}
# 50/300 matches jobs 61786/61787, the pair that produced the tightest measured
# noise floor (0.8% median, 7.3% max). Do not lower these.
WARMUP=${WARMUP:-50}
ITERS=${ITERS:-300}

UCX_TLS=${UCX_TLS:-"sm,dc"}
UCX_NET_DEVICES=${UCX_NET_DEVICES:-"mlx5_0:1"}

# Pipelining threshold in bytes, kept below the smallest message (1 MB) so every
# size point pipelines in every cell. The parser seeds threshold=SIZE_MAX, so
# omitting this silently disables pipelining everywhere.
THRESH=${THRESH:-64K}
NFRAGS=${NFRAGS:-2}
REPLICATES=${REPLICATES:-1}

# --- Arm definitions ---------------------------------------------------------
# CELLS entries are "frag_kb:pdepth". PLACEMENTS entries are extra mpirun
# placement flags, tagged "<name>|<flags>"; the default is one plain placement.
PLACEMENTS=("default|--map-by ppr:${PPN}:node --bind-to core")

case "$ARM" in
  base)
    # Fine fragment grid at the task-17 depth. Non-power-of-2 points (1536,
    # 3072, 6144) are what let the ridge be located between task 17's octaves.
    CELLS=(); for f in 128 256 512 768 1024 1536 2048 3072 4096 6144 8192; do
        CELLS+=("${f}:4"); done
    REPLICATES=3
    ;;
  depth)
    # Same fragment grid at depths 1, 2, 8, 16. Depth 4 comes from the `base`
    # arm (identical config), so it is not repeated here.
    CELLS=(); for d in 1 2 8 16; do for f in 512 1024 2048 4096 8192; do
        CELLS+=("${f}:${d}"); done; done
    ;;
  numa)
    # Constant rank count, 2x change in L3 share per rank. This is the arm that
    # is free of the PPN arm's rank-count/NIC confounders.
    CELLS=(); for f in 512 1024 2048 3072 4096 6144; do CELLS+=("${f}:4"); done
    # Both verified on job 61794: ppr:8:node -> cores 0-7 (socket 0 only);
    # ppr:4:socket -> cores 0-3 (node 0) + 56-59 (node 1).
    PLACEMENTS=(
      "packed1sock|--map-by ppr:8:node --bind-to core"
      "split2sock|--map-by ppr:4:socket --bind-to core"
    )
    REPLICATES=2
    ;;
  ppn4|ppn16|ppn32)
    # Deliberately the SAME wide grid as `base`, spanning 128..8192 KiB. The
    # predicted optima range from ~512 KiB (PPN 32) to ~4096 KiB (PPN 4); a
    # narrower per-arm grid risks the argmax landing on a grid edge, which is
    # indistinguishable from "the optimum moved off the end" and would make the
    # arm uninterpretable in exactly the case the hypothesis predicts.
    CELLS=(); for f in 128 256 512 768 1024 1536 2048 3072 4096 6144 8192; do
        CELLS+=("${f}:4"); done
    REPLICATES=2
    ;;
  *)
    echo "ERROR: unknown ARM='$ARM' (base|depth|numa|ppn4|ppn16|ppn32)" >&2
    exit 2
    ;;
esac

# Guard the PPN arms against a geometry/arm mismatch, which would silently
# produce a mislabelled dataset that looks perfectly valid.
case "$ARM" in
  ppn4)  [[ "$PPN" == "4"  ]] || { echo "ERROR: ARM=ppn4 needs --ntasks-per-node=4 (got $PPN)"  >&2; exit 2; };;
  ppn16) [[ "$PPN" == "16" ]] || { echo "ERROR: ARM=ppn16 needs --ntasks-per-node=16 (got $PPN)" >&2; exit 2; };;
  ppn32) [[ "$PPN" == "32" ]] || { echo "ERROR: ARM=ppn32 needs --ntasks-per-node=32 (got $PPN)" >&2; exit 2; };;
  *)     [[ "$PPN" == "8"  ]] || { echo "ERROR: ARM=$ARM assumes 8 PPN (got $PPN)" >&2; exit 2; };;
esac

mkdir -p "$RESULT_DIR"

# --- Provenance --------------------------------------------------------------
# Recorded per job, because the cache argument is only as good as the guarantee
# that every arm ran the same binary on the same kind of node.
{
    echo "== task 59 provenance =="
    echo "job:        ${JOB_ID}   arm: ${ARM}"
    echo "nodelist:   ${SLURM_JOB_NODELIST:-unknown}"
    echo "geometry:   ${NNODES} nodes x ${PPN} ppn = ${NP} ranks"
    echo "src_dir:    ${SRC_DIR}"
    echo "install:    ${INSTALL_DIR}"
    echo "-- git --"
    ( cd "$SRC_DIR" && git rev-parse HEAD 2>/dev/null && git status --porcelain 2>/dev/null ) || echo "(not a git tree)"
    echo "-- MAX_FRAGS (depth>4 arms are invalid unless this is >= the max depth swept) --"
    grep -n "define UCC_SCHEDULE_PIPELINED_MAX_FRAGS" "$SRC_DIR/src/schedule/ucc_schedule_pipelined.h" || true
    echo "-- perf availability --"
    echo "perf_event_paranoid=$(cat /proc/sys/kernel/perf_event_paranoid 2>/dev/null)"
    grep CapEff /proc/self/status || true
    perf stat -e cycles true 2>&1 | tail -3 || true
} > "$RESULT_DIR/provenance.txt"

# Topology, memory placement and binding, captured from the actual nodes rather
# than assumed -- the L2/L3 arithmetic in the analysis depends on these.
srun --ntasks-per-node=1 bash -c '
    echo "### $(hostname)"
    lscpu | egrep "^Model name|^Socket|^Core|^Thread|^CPU\(s\)|cache"
    lscpu -C
    numactl -H | egrep "^available|^node [01] (cpus|size|free)|^node distances"
    echo "governor: $(cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor 2>/dev/null || echo n/a)"
    echo "thp: $(cat /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || echo n/a)"
' > "$RESULT_DIR/topology.txt" 2>&1 || true

# --- Build -------------------------------------------------------------------
if [[ "$SKIP_BUILD" != "1" ]]; then
    echo "== building UCC (no CUDA) in $SRC_DIR =="
    cd "$SRC_DIR"
    make distclean 2>/dev/null || true
    ./configure --prefix="$INSTALL_DIR" --with-mpi="$MPI_HOME" \
        --with-ucx="$UCX_HOME" --without-cuda --without-sharp
    make -j"$(nproc)"
    make install
else
    echo "== SKIP_BUILD=1, reusing $INSTALL_DIR =="
fi

BIN="$INSTALL_DIR/bin/ucc_perftest"
[[ -x "$BIN" ]] || { echo "ERROR: $BIN not found" >&2; exit 1; }

RUN_LD="$INSTALL_DIR/lib:$INSTALL_DIR/lib64:$MPI_HOME/lib:$MPI_HOME/lib64:$UCX_HOME/lib:$UCX_HOME/lib64${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

echo "arm:          $ARM"
echo "results:      $RESULT_DIR"
echo "geometry:     $NNODES x $PPN = $NP ranks   nodes=${SLURM_JOB_NODELIST:-unknown}"
echo "sizes:        $MIN_COUNT..$MAX_COUNT elems x$FACTOR ($DTYPE/$OP, w$WARMUP n$ITERS)"
echo "cells:        ${#CELLS[@]}  placements: ${#PLACEMENTS[@]}  replicates: $REPLICATES"

SUMMARY_CSV="$RESULT_DIR/summary.csv"
echo "arm,placement,replicate,config,frag_kb,pdepth,pipeline,nranks,ppn,size_bytes,count,time_avg_us,bw_avg_gbs,bw_max_gbs,bw_min_gbs" \
    > "$SUMMARY_CSV"

# Collect clamp warnings separately. A depth-8 cell that silently ran at depth 4
# is the single most dangerous failure mode here -- it would look like clean
# evidence that depth does not matter.
CLAMP_LOG="$RESULT_DIR/clamp_warnings.txt"
: > "$CLAMP_LOG"

run_one() {
    local place_name="$1" place_flags="$2" rep="$3" label="$4" pipeline="$5" \
          frag_kb="$6" pdepth="$7"
    local out="$RESULT_DIR/cell-${place_name}-r${rep}-${label}.log"

    echo ""
    echo ">> arm=$ARM place=$place_name rep=$rep cell=$label pipeline=$pipeline"
    {
        echo "arm=$ARM placement=$place_name flags=$place_flags rep=$rep"
        echo "cell=$label frag_kb=$frag_kb pdepth=$pdepth pipeline=$pipeline"
        echo "nranks=$NP ppn=$PPN nodes=${SLURM_JOB_NODELIST:-unknown}"
    } > "$out"

    if ! "$MPI_HOME/bin/mpirun" --np "$NP" $place_flags \
        -x "PATH=$INSTALL_DIR/bin:$PATH" \
        -x "LD_LIBRARY_PATH=$RUN_LD" \
        -x "UCC_TL_UCP_TUNE=allreduce:0-inf:@sra_knomial" \
        -x "UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE=$pipeline" \
        -x "UCX_TLS=$UCX_TLS" \
        -x "UCX_NET_DEVICES=$UCX_NET_DEVICES" \
        -x "OMP_NUM_THREADS=1" \
        "$BIN" -c allreduce -m host -d "$DTYPE" -o "$OP" \
               -b "$MIN_COUNT" -e "$MAX_COUNT" -f "$FACTOR" \
               -w "$WARMUP" -n "$ITERS" -F 2>&1 | tee -a "$out"
    then
        echo "!! cell $label FAILED (see $out)" | tee -a "$out"
        echo "$place_name r$rep $label" >> "$RESULT_DIR/failed_cells"
    fi

    if grep -q "exceeds max limit" "$out"; then
        echo "CLAMPED: place=$place_name rep=$rep cell=$label -- requested depth $pdepth NOT achieved" \
            >> "$CLAMP_LOG"
    fi

    awk -v a="$ARM" -v pl="$place_name" -v rp="$rep" -v c="$label" -v fk="$frag_kb" \
        -v pd="$pdepth" -v p="$pipeline" -v r="$NP" -v ppn="$PPN" '
        /^[[:space:]]*[0-9]+[[:space:]]+[0-9]+[[:space:]]/ {
            print a "," pl "," rp "," c "," fk "," pd "," p "," r "," ppn "," \
                  $2 "," $1 "," $3 "," $6 "," $7 "," $8
        }' "$out" >> "$SUMMARY_CSV"
}

# --- Sweep -------------------------------------------------------------------
# Replicate is the OUTER loop on purpose: replicates separated in time capture
# drift (thermal, neighbour noise) that back-to-back repeats would hide, so the
# noise floor derived from them is not optimistically biased.
for rep in $(seq 1 "$REPLICATES"); do
  for pspec in "${PLACEMENTS[@]}"; do
    pname="${pspec%%|*}"; pflags="${pspec#*|}"
    # Monolithic control per placement per replicate: anchors every arm to the
    # non-pipelined baseline so arms remain comparable if a node differs.
    run_one "$pname" "$pflags" "$rep" "mono" "n" "-" "-"
    for cell in "${CELLS[@]}"; do
        f="${cell%%:*}"; d="${cell##*:}"
        run_one "$pname" "$pflags" "$rep" "f${f}k_d${d}" \
            "thresh=${THRESH}:fragsize=${f}K:nfrags=${NFRAGS}:pdepth=${d}:parallel" \
            "$f" "$d"
    done
  done
done

# --- Completeness ------------------------------------------------------------
echo ""
echo "== completeness =="
EXPECT_ROWS=$(awk -F, 'NR>1 {print $10}' "$SUMMARY_CSV" | sort -n -u | wc -l)
total=0; bad=0
for rep in $(seq 1 "$REPLICATES"); do
  for pspec in "${PLACEMENTS[@]}"; do
    pname="${pspec%%|*}"
    for cell in "${CELLS[@]}"; do
        f="${cell%%:*}"; d="${cell##*:}"; total=$((total+1))
        n=$(awk -F, -v pl="$pname" -v rp="$rep" -v c="f${f}k_d${d}" \
            'NR>1 && $2==pl && $3==rp && $4==c {n++} END {print n+0}' "$SUMMARY_CSV")
        if [[ "$n" -ne "$EXPECT_ROWS" ]]; then
            echo "INCOMPLETE $pname r$rep frag=${f}K d=${d}: $n rows, expected $EXPECT_ROWS"
            bad=$((bad+1))
        fi
    done
  done
done
echo "cells complete: $((total-bad))/$total (expected rows/cell $EXPECT_ROWS)"
[[ "$bad" -ne 0 ]] && echo "WARNING: dataset INCOMPLETE -- do not read it as a full matrix"

if [[ -s "$CLAMP_LOG" ]]; then
    echo ""
    echo "WARNING: pipeline depth was CLAMPED in these cells -- their nominal"
    echo "depth is NOT what executed. Do not treat them as depth evidence:"
    cat "$CLAMP_LOG"
fi
[[ -f "$RESULT_DIR/failed_cells" ]] && { echo "failed cells:"; cat "$RESULT_DIR/failed_cells"; }

# --- Ridge location per size -------------------------------------------------
# The analysis question is "where is the best fragment size", so report the
# argmax directly rather than making a human scan a matrix for it.
{
    echo "== arm=$ARM  bw_avg GB/s, rows=frag_kb, cols=size_bytes (mean over replicates) =="
    echo "geometry: $NNODES x $PPN = $NP ranks, $DTYPE/$OP, w$WARMUP n$ITERS"
    sizes=$(awk -F, 'NR>1 {print $10}' "$SUMMARY_CSV" | sort -n -u)
    for pspec in "${PLACEMENTS[@]}"; do
      pname="${pspec%%|*}"
      depths=$(awk -F, -v pl="$pname" 'NR>1 && $2==pl && $6!="-" {print $6}' "$SUMMARY_CSV" | sort -n -u)
      for d in $depths; do
        echo ""
        echo "---- placement=$pname pdepth=$d ----"
        printf "%10s" "frag_kb"; for s in $sizes; do printf "%12s" "$s"; done; printf "%14s\n" "wset_MiB(d*2F)"
        frags=$(awk -F, -v pl="$pname" -v d="$d" 'NR>1 && $2==pl && $6==d {print $5}' "$SUMMARY_CSV" | sort -n -u)
        for f in $frags; do
            printf "%10s" "$f"
            for s in $sizes; do
                printf "%12s" "$(awk -F, -v pl="$pname" -v d="$d" -v f="$f" -v s="$s" \
                    'NR>1 && $2==pl && $6==d && $5==f && $10==s {t+=$13; n++}
                     END {if (n) printf "%.2f", t/n; else printf "-"}' "$SUMMARY_CSV")"
            done
            printf "%14s\n" "$(awk -v d="$d" -v f="$f" 'BEGIN{printf "%.1f", 2*d*f/1024}')"
        done
        echo "  argmax frag_kb per size:"
        for s in $sizes; do
            printf "    size=%-10s best_frag=%s\n" "$s" \
              "$(awk -F, -v pl="$pname" -v d="$d" -v s="$s" \
                 'NR>1 && $2==pl && $6==d && $10==s {t[$5]+=$13; n[$5]++}
                  END {b=""; for (f in t) {m=t[f]/n[f]; if (b=="" || m>bv) {b=f; bv=m}} print b " (" bv " GB/s)"}' "$SUMMARY_CSV")"
        done
      done
    done
} | tee "$RESULT_DIR/summary.txt"

echo ""
echo "== done: $RESULT_DIR =="
echo "done" > "$RESULT_DIR/done"
