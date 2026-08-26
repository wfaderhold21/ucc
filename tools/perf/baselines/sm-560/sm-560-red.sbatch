#!/bin/bash
#SBATCH -p GAIA
#SBATCH -N 1
#SBATCH -J sm560p2
#SBATCH -t 00:15:00
#SBATCH --output=/labhome/faderholdt/slurm-%j-red.out
export CUDA_VISIBLE_DEVICES=0
export OMP_NUM_THREADS=1
cd /labhome/faderholdt
R=/labhome/faderholdt/results-560
PT=/labhome/faderholdt/ucc-baseline-cuda/tools/perf/ucc_perftest

echo "=== node: $(hostname) $(date -u) ==="
echo "=== /opt layout ==="
ls -d /opt/* 2>&1 | head -25
echo "=== /opt/nvidia ==="
ls /opt/nvidia 2>&1
echo "=== find nsys candidates ==="
NSYS=$(find /opt /usr/local /usr/share -maxdepth 6 -name nsys -type f 2>/dev/null | head -5)
echo "candidates:"
echo "$NSYS"
# prefer a 2023.4 build if present
NSYS=$(echo "$NSYS" | grep -m1 '2023.4' || true)
NSYS=$(echo "$NSYS" | head -1)
echo "using NSYS=$NSYS"
if [ -z "$NSYS" ] || [ ! -x "$NSYS" ]; then
    echo "NO NSYS FOUND - aborting profile"
    exit 1
fi
"$NSYS" --version

run_profile () {
    local tag=$1; shift
    echo "=== profile: $tag ==="
    "$NSYS" profile -s none -t cuda -o "$R/prof_560_$tag" --force-overwrite true \
        mpiexec -n 1 "$PT" "$@" > "$R/perftest_$tag.stdout" 2> "$R/nsys_$tag.stderr"
    local rc=$?
    echo "$rc" > "$R/perftest_$tag.rc"
    "$NSYS" stats "$R/prof_560_$tag.nsys-rep" --report cuda_gpu_kern_sum --force-overwrite true > "$R/kern_sum_$tag.txt" 2>&1
    "$NSYS" stats "$R/prof_560_$tag.nsys-rep" --report cuda_gpu_trace  --force-overwrite true > "$R/kern_trace_$tag.txt" 2>&1
    sqlite3 "$R/prof_560_$tag.sqlite" "SELECT COUNT(*) FROM CUPTI_ACTIVITY_KIND_KERNEL;" > "$R/kern_count_$tag.txt" 2>&1
    echo "--- kern_sum_$tag ---"
    cat "$R/kern_sum_$tag.txt"
    echo "--- perftest_$tag.stdout (tail) ---"
    tail -8 "$R/perftest_$tag.stdout"
    echo "--- rc: $rc ---"
}

# clean stale failed-run artifacts
rm -f "$R"/prof_560_red*.nsys-rep "$R"/prof_560_redT* "$R"/kern_*_red*.txt "$R"/perftest_red*.stdout "$R"/perftest_red*.rc "$R"/nsys_red*.stderr "$R"/kern_count_red*.txt

run_profile red  -c reducedt -m cuda -d float32 -o sum -b 16M -e 16M -F
run_profile redT -c reducedt -m cuda -d float32 -o sum -b 16M -e 16M -F -T

echo "=== DONE 560 RED ==="
ls -l "$R"
