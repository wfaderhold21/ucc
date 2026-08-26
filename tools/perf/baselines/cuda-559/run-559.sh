#!/bin/bash
# Task 559: CUDA collective baseline for HSA-770 (threaded / offloaded reduce).
# Builds UCC with CUDA (nvcc 12.4 at /usr/local/cuda-12.4) and runs the full
# single-rank perftest sweep: 2 colls x 2 mtypes x 4 dts x 4 ops x 17 counts.
#
# Submit with: sbatch -p GAIA -N 1 -t 1:30:00 run-559.sh
#SBATCH -p GAIA
#SBATCH -N 1
#SBATCH -n 1
#SBATCH --cpus-per-task=224
#SBATCH --exclusive
#SBATCH -J ucc-baseline-559
#SBATCH -t 1:30:00
#SBATCH --error=slurm-%j.err
set -u
set -o pipefail

export PATH=/usr/local/cuda-12.4/bin:$PATH
export CUDA_VISIBLE_DEVICES=0
export OMP_NUM_THREADS=1

BUILD=$HOME/ucc-baseline-cuda
INSTALL=$HOME/ucc-install-559
RES=$HOME/results-559
rm -rf "$RES"
mkdir -p "$RES"

# ---------------------------------------------------------------------------
# Machine specification (top of results)
# ---------------------------------------------------------------------------
{
    echo "=== hostname ==="
    hostname
    echo
    echo "=== lscpu ==="
    lscpu
    echo
    echo "=== free -g ==="
    free -g
    echo
    echo "=== nvidia-smi -L ==="
    nvidia-smi -L
    echo
    echo "=== driver version ==="
    nvidia-smi --query-gpu=driver_version --format=csv,noheader
    echo
    echo "=== nvidia-smi ==="
    nvidia-smi
    echo
    echo "=== MPI / CUDA toolchain ==="
    /usr/local/bin/mpiexec --version 2>&1 | head -2
    nvcc --version
    echo
    echo "=== slurm ==="
    scontrol show job $SLURM_JOB_ID | grep -E '^(JobId|JobName|NodeList|Elapsed|Partition|CPUS|GRES)'
} > "$RES/machine-spec.log" 2>&1
cat "$RES/machine-spec.log"

# ---------------------------------------------------------------------------
# Build UCC with CUDA
# ---------------------------------------------------------------------------
mkdir -p "$BUILD"
cd "$BUILD" || exit 1
echo ">>> autogen"
./autogen.sh > "$RES/autogen.log" 2>&1 || { echo "autogen FAILED"; exit 1; }
echo ">>> configure"
./configure --with-cuda=/usr/local/cuda-12.4 --with-mpi --prefix="$INSTALL" > "$RES/configure.log" 2>&1
cfg_rc=$?
if [ $cfg_rc -ne 0 ]; then
    echo ">>> configure FAILED (rc=$cfg_rc); see $RES/configure.log"
    tail -50 "$RES/configure.log"
    exit 1
fi
grep -E 'CUDA support|UCX support|NVCC gencodes|CUDA version|TL modules|MC modules|NVLS|NVML' "$RES/configure.log"
grep -E 'CUDA support:' "$RES/configure.log" | grep -q 'yes' || { echo "CUDA NOT ENABLED in build"; exit 1; }
echo ">>> make -j$(nproc)"
make -j"$(nproc)" > "$RES/make.log" 2>&1
mk_rc=$?
if [ $mk_rc -ne 0 ]; then
    echo ">>> make FAILED (rc=$mk_rc); see $RES/make.log"
    tail -50 "$RES/make.log"
    exit 1
fi
ls -l "$BUILD/tools/perf/ucc_perftest" || exit 1

# Smoke test: build must have CUDA support and run a small allreduce.
echo ">>> smoke test (allreduce, cuda, float32, sum, 1M)"
if ! mpiexec -n 1 "$BUILD/tools/perf/ucc_perftest" -c allreduce -m cuda -d float32 -o sum -b 1M -e 1M -F > "$RES/smoke.log" 2>&1; then
    echo ">>> smoke test FAILED"
    cat "$RES/smoke.log"
    exit 1
fi
cat "$RES/smoke.log"
head -5 "$RES/smoke.log" | grep -q 'Collective:' || exit 1

# ---------------------------------------------------------------------------
# Full sweep: 2 colls x 2 mtypes x 4 dts x 4 ops
#   -b 1K -e 64M, default -f 2 -> 17 counts (1024 .. 67108864), -F full print
# ---------------------------------------------------------------------------
FAIL=0
: > "$RES/commands.log"
for coll in reduce allreduce; do
    for mtype in cuda cuda-mng; do
        for dt in float32 float64 int32 int64; do
            for op in sum prod min max; do
                log="$RES/${coll}_${mtype}_${dt}_${op}.log"
                cmd="mpiexec -n 1 $BUILD/tools/perf/ucc_perftest -c $coll -m $mtype -d $dt -o $op -b 1K -e 64M -F"
                echo "$cmd" >> "$RES/commands.log"
                if ! $cmd > "$log" 2>&1; then
                    echo ">>> FAILED: $cmd"
                    tail -20 "$log"
                    FAIL=$((FAIL + 1))
                fi
            done
        done
    done
done

echo ">>> sweep done, failures: $FAIL"
echo "$FAIL" > "$RES/fail-count"
ls "$RES" | wc -l
exit $FAIL
