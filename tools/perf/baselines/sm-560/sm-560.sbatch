#!/bin/bash
#SBATCH -p GAIA
#SBATCH -N 1
#SBATCH -J sm-560
#SBATCH -t 00:30:00
#SBATCH --export=ALL

set -u
R=/labhome/faderholdt/results-560
rm -rf "$R"
mkdir -p "$R"

export CUDA_VISIBLE_DEVICES=0
export OMP_NUM_THREADS=1

{
  echo "=== machine-spec: $(date -u +%Y-%m-%dT%H:%M:%SZ) slurm job ${SLURM_JOBID:-unset} node $(hostname) ==="
  uname -a
  echo "--- os-release"
  head -3 /etc/os-release
  echo "--- nvidia-smi summary"
  nvidia-smi
  echo "--- nvidia-smi -q (GPU 0)"
  nvidia-smi -q -i 0
  echo "--- nvidia-smi csv"
  nvidia-smi --query-gpu=index,name,driver_version,memory.total,compute_cap --format=csv
  echo "--- nsys version"
  nsys --version
  echo "--- nvcc version"
  nvcc --version || true
  echo "--- gcc"
  gcc --version | head -1
  echo "--- cpu"
  lscpu | grep -E 'Model name|^CPU\(s\)|Thread\(s\)|Core\(s\)|Socket\(s\)|NUMA'
  echo "--- memory"
  free -g
  echo "--- perftest binary"
  ls -l /labhome/faderholdt/ucc-baseline-cuda/tools/perf/ucc_perftest
  echo "--- nsys environment"
  nsys status --environment || true
} > "$R/machine-spec.log" 2>&1

# NOTE: no `--` separator before the app: this nsys build (2023.4.4.54)
# mis-parses a bare `--` ("option is ambiguous"); nsys stops parsing its own
# options at the first non-option arg, so the app + its args pass through.
nsys profile -s none -t cuda -o "$R/prof_560" --force-overwrite true \
  mpiexec -n 1 /labhome/faderholdt/ucc-baseline-cuda/tools/perf/ucc_perftest \
  -c reduce -m cuda -d float32 -o sum -b 16M -e 16M -F \
  > "$R/perftest.stdout" 2> "$R/nsys.stderr"
status=$?
echo "perftest exit status: $status" > "$R/perftest.exit"
cat "$R/perftest.exit"

nsys stats "$R/prof_560.nsys-rep" --report cuda_gpu_kern_sum > "$R/kern_sum.txt" 2>&1
nsys stats "$R/prof_560.nsys-rep" --report cuda_gpu_trace  > "$R/kern_trace.txt" 2>&1
ls -l "$R"
echo "DONE-560"
