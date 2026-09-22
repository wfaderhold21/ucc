#!/bin/bash
# ROADMAP A4 breadth-first discovery run on thor (host-only tl/ucp).
set -euo pipefail
set +u
HPCX=/global/software/rocky-9.x86_64/modules/gcc/11/hpcx/2.25.1
cd "$HPCX" && . ./hpcx-init-ompi.sh && hpcx_load
set -u
export UCX_TLS=rc,sm,self
R=/global/home/users/faderholdt/build-staging/ucc-tuning
export LD_LIBRARY_PATH=$R/install/lib:$LD_LIBRARY_PATH
export PATH=$R/install/bin:$PATH
A=${UCC_TUNE_ARTIFACTS:-/global/home/users/faderholdt/thor-a4-2026-09-21}
mkdir -p "$A"
cd $R/tools/tune
echo "=== provenance ==="
echo "JOB=$SLURM_JOB_ID NODES=${SLURM_JOB_NODELIST:-?} TEAM_SIZES=8,16,32,64"
echo "START=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
ucc_info -v 2>&1 | head -1
echo "=== dry-run cost model ==="
python3 ucc_offline_tune.py --dry-run \
  --collective allreduce,allgather,reduce_scatter,alltoall \
  --component tl/ucp --mem-type host --team-sizes 8,16,32,64 \
  --min-bytes 8 --max-bytes 1048576 --factor 2 \
  --datatype float32 --op sum --n-reps 3 \
  --launcher "mpirun -np {team_size}" \
  --perftest $R/install/bin/ucc_perftest --ucc-info $R/install/bin/ucc_info \
  --ucx-info /usr/bin/ucx_info 2>&1 | tee "$A/a4_dryrun.txt" | tail -4
echo "=== sweep (screening + readback, no validation) ==="
python3 ucc_offline_tune.py \
  --collective allreduce,allgather,reduce_scatter,alltoall \
  --component tl/ucp --mem-type host --team-sizes 8,16,32,64 \
  --min-bytes 8 --max-bytes 1048576 --factor 2 \
  --datatype float32 --op sum \
  --n-reps 3 --n-iter 200 --n-warmup 20 \
  --readback-level debug \
  --launcher "mpirun -np {team_size}" \
  --perftest $R/install/bin/ucc_perftest --ucc-info $R/install/bin/ucc_info \
  --ucx-info /usr/bin/ucx_info \
  --output-dir "$A" \
  --no-validate \
  2>&1 | tee "$A/a4_tuning.log"
echo "=== findings ==="
cat "$A/findings.md" 2>/dev/null || echo "(no findings)"
echo "END=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "=== A4 DONE ==="
