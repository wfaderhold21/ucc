#!/usr/bin/env bash
#
# Task 417 — execute one geometry's replicates from the frozen protocol matrix.
#
# Submit with:
#   sbatch --nodes=4 --ntasks-per-node=8 \
#           --export=ALL,MATRIX_CSV=/path/matrix-v1.csv \
#           contrib/slurm_task417_geometry.sh G1
#
# Required positional argument:
#   $1        — G1, G2, G3, or G4
# Required environment at submission:
#   MATRIX_CSV  — path to the machine-readable matrix
#   SKIP_BUILD  — set to 1 if UCC already built
#   INSTALL_DIR — path to UCC install (optional, auto-computed if not set)
#
# Protocol parameters are FROZEN — do not modify sweep axes, timeouts, or guards.
#

#SBATCH --job-name=ucc-sra-t417-%x
#SBATCH --partition=GAIA
#SBATCH --time=10:00:00
#SBATCH --output=slurm-%x-%j.out
#SBATCH --error=slurm-%x-%j.err

set -euo pipefail

GEOMETRY=${1:?GEOMETRY not set (G1/G2/G3/G4)}
MATRIX_CSV=${MATRIX_CSV:?MATRIX_CSV not set}

# ─── Paths (GAIA-specific) ──────────────────────────────────────────────────
MPI_HOME=${MPI_HOME:-/usr/mpi/gcc/openmpi-4.1.9a1}
UCX_HOME=${UCX_HOME:-/usr}
export PATH="$MPI_HOME/bin:$PATH"

SRC_DIR=${SRC_DIR:-${SLURM_SUBMIT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}}
JOB_ID=${SLURM_JOB_ID:-manual}
INSTALL_DIR=${INSTALL_DIR:-"$SRC_DIR/install-${JOB_ID}"}
RESULT_DIR="$SRC_DIR/results/task417-geometry-${GEOMETRY}"
SKIP_BUILD=${SKIP_BUILD:-0}

SOURCE_BRANCH=${SOURCE_BRANCH:-$(git -C "$SRC_DIR" branch --show-current 2>/dev/null || printf unknown)}
SOURCE_COMMIT=${SOURCE_COMMIT:-$(git -C "$SRC_DIR" rev-parse HEAD 2>/dev/null || printf unknown)}

# ─── Protocol constants (frozen) ─────────────────────────────────────────────
BUILD_CAP=16
TIMEOUT_PER_CELL=300  # 5 minutes
WARMUP=20
ITERS=100
DTYPE="float32"
OP="sum"
UCX_TLS="sm,dc"
UCX_NET_DEVICES="mlx5_0:1"
OMP_NUM_THREADS=1
MAX_REPLICATES=10
CANARY_DIV_PCT=15

die() { echo "FATAL: $*" >&2; exit 2; }

# ─── Geometry spec ──────────────────────────────────────────────────────────
case "$GEOMETRY" in
  G1) EXPECTED_NODES=4;  EXPECTED_PPN=8;  EXPECTED_RANKS=32  ;;
  G2) EXPECTED_NODES=2;  EXPECTED_PPN=16; EXPECTED_RANKS=32  ;;
  G3) EXPECTED_NODES=16; EXPECTED_PPN=8;  EXPECTED_RANKS=128 ;;
  G4) EXPECTED_NODES=8;  EXPECTED_PPN=16; EXPECTED_RANKS=128 ;;
  *) die "Unknown geometry $GEOMETRY" ;;
esac

# ─── Pre-flight validation ─────────────────────────────────────────────────
[[ -f "$MATRIX_CSV" ]] || die "Matrix CSV not found: $MATRIX_CSV"
[[ -d "$RESULT_DIR" && -f "$RESULT_DIR/done_flag" ]] && die "Results already exist at $RESULT_DIR"

mkdir -p "$RESULT_DIR"

# ─── Build (if needed) ──────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" != "1" ]]; then
  echo "== building UCC =="
  (
    cd "$SRC_DIR"
    export LD_LIBRARY_PATH="$MPI_HOME/lib:$MPI_HOME/lib64:$UCX_HOME/lib:$UCX_HOME/lib64"
    ./configure --prefix="$INSTALL_DIR" --without-cuda \
      --with-mpi="$MPI_HOME" --with-ucx="$UCX_HOME" \
      UCC_SCHEDULE_PIPELINED_MAX_FRAGS=$BUILD_CAP
    make -j"$(nproc)" install
  )
fi

BIN="$INSTALL_DIR/bin/ucc_perftest"
[[ -x "$BIN" ]] || die "missing $BIN; set INSTALL_DIR or do not set SKIP_BUILD=1"

# ─── Library path ───────────────────────────────────────────────────────────
UCC_LIB="$INSTALL_DIR/lib:$INSTALL_DIR/lib64"
MPI_LIB="$MPI_HOME/lib:$MPI_HOME/lib64"
UCX_LIB="$UCX_HOME/lib:$UCX_HOME/lib64"
RUN_LD="$UCC_LIB:$MPI_LIB:$UCX_LIB${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

# ─── Manifest ───────────────────────────────────────────────────────────────
MANIFEST="$RESULT_DIR/manifest.txt"
{
  echo "task: 417"
  echo "geometry: $GEOMETRY"
  echo "job_id: $JOB_ID"
  echo "source_branch: $SOURCE_BRANCH"
  echo "source_commit: $SOURCE_COMMIT"
  echo "harness_sha256: 55d676a9674c109ddea37ec28c9cfea622a00e197080e50e07ca915f157635a0"
  echo "build_cap: $BUILD_CAP"
  echo "install_dir: $INSTALL_DIR"
  echo "result_dir: $RESULT_DIR"
  echo "nranks: $EXPECTED_RANKS"
  echo "ppn: $EXPECTED_PPN"
  echo "nodes: $EXPECTED_NODES"
  echo "ucx_tls: $UCX_TLS"
  echo "ucx_net_devices: $UCX_NET_DEVICES"
  echo "warmup: $WARMUP"
  echo "iters: $ITERS"
  echo "timeout_per_cell: $TIMEOUT_PER_CELL"
  echo "nodelist: ${SLURM_JOB_NODELIST:-unknown}"
  echo "submit_time: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
} | tee "$MANIFEST"

BIN_SHA=$(sha256sum "$BIN" | cut -d' ' -f1)
echo "binary_sha256: $BIN_SHA" | tee -a "$MANIFEST"

# ─── Cell execution via Python driver ───────────────────────────────────────
# Python script reads matrix, randomizes per replicate, runs cells, records results.

RUNNER_SCRIPT="$RESULT_DIR/run_cells.py"
cat > "$RUNNER_SCRIPT" <<'PYEOF'
#!/usr/bin/env python3
"""Task 417 cell runner — executes cells from the frozen matrix for one geometry."""
import csv
import json
import math
import os
import random
import shutil
import subprocess
import sys
import hashlib
import time

TIMEOUT = 300  # 5 minutes per cell
WARMUP = 20
ITERS = 100
DTYPE = "float32"
OP = "sum"
UCX_TLS = "sm,dc"
UCX_NET_DEVICES = "mlx5_0:1"
OMP_NUM_THREADS = "1"
CANARY_DIV_PCT = 15
MAX_REPLICATES = 10

geometry = os.environ["GEOMETRY"]
matrix_csv = os.environ["MATRIX_CSV"]
src_dir = os.environ["SRC_DIR"]
install_dir = os.environ["INSTALL_DIR"]
mpi_home = os.environ.get("MPI_HOME", "/usr/mpi/gcc/openmpi-4.1.9a1")
ucx_home = os.environ.get("UCX_HOME", "/usr")

result_dir = os.path.join(src_dir, "results", f"task417-geometry-{geometry}")
summary_csv = os.path.join(result_dir, "summary.csv")
accounting_file = os.path.join(result_dir, "accounting.json")
manifest_file = os.path.join(result_dir, "manifest.txt")

# Geometry specs
GEOMETRY_SPECS = {
    "G1": {"nranks": 32, "ppn": 8, "nodes": 4},
    "G2": {"nranks": 32, "ppn": 16, "nodes": 2},
    "G3": {"nranks": 128, "ppn": 8, "nodes": 16},
    "G4": {"nranks": 128, "ppn": 16, "nodes": 8},
}
spec = GEOMETRY_SPECS[geometry]
nranks = spec["nranks"]
ppn = spec["ppn"]

bin_path = os.path.join(install_dir, "bin", "ucc_perftest")

# Library path
ucc_lib = f"{install_dir}/lib:{install_dir}/lib64"
mpi_lib = f"{mpi_home}/lib:{mpi_home}/lib64"
ucx_lib = f"{ucx_home}/lib:{ucx_home}/lib64"
run_ld = f"{ucc_lib}:{mpi_lib}:{ucx_lib}"

print(f"Running {geometry}: {nranks} ranks, PPN={ppn}, {spec['nodes']} nodes")

# Load matrix for this geometry
with open(matrix_csv, 'r') as f:
    reader = csv.DictReader(f)
    all_rows = [r for r in reader if r['geometry'] == geometry]

# Group by replicate
by_rep = {}
for r in all_rows:
    rep = r['replicate']
    by_rep.setdefault(rep, []).append(r)

sorted_reps = sorted(by_rep.keys(), key=int)

# Initialize outputs
summary_writer = csv.writer(open(summary_csv, 'w', newline=''))
summary_writer.writerow([
    "geometry", "replicate", "cell_index", "arm", "message_bytes",
    "n_frags_eff", "f_eff_kib", "depth_eff", "count", "time_us",
    "bw_gbs", "exit_code"
])

accounting = {"cells": [], "replicates": {}}
cell_idx = 0
total_cells = 0
failed_cells = 0

def run_benchmark(cell, rep):
    global cell_idx, total_cells, failed_cells
    cell_idx += 1
    total_cells += 1

    arm = cell.get('arm', 'pipeline')
    msg_bytes_str = cell.get('msg_bytes', '')
    n_frags = cell.get('actual_n_frags', '')
    f_eff_kib = cell.get('f_eff_kib', '')
    depth = cell.get('actual_depth', '')
    f_req_kib = cell.get('f_req_kib', '0')
    pdepth = cell.get('pdepth', '0')

    # Build pipeline string
    if arm == "mono":
        pipeline_str = "n"
    else:
        pipeline_str = f"thresh=64K:fragsize={f_req_kib}K:nfrags=2:pdepth={pdepth}:parallel"

    # Compute count
    if msg_bytes_str and msg_bytes_str != '':
        try:
            count = int(msg_bytes_str) // 4
        except ValueError:
            count = "4M"
    else:
        count = "4M"

    cell_dir = os.path.join(result_dir, f"replicate-{rep}")
    os.makedirs(cell_dir, exist_ok=True)
    cell_file = os.path.join(cell_dir, f"cell-{cell_idx:04d}.csv")

    print(f"  [{cell_idx}] R{rep} {arm} msg={msg_bytes_str or 'sweep'} nf={n_frags} d={depth} ...", flush=True)

    env = os.environ.copy()
    env["OMP_NUM_THREADS"] = OMP_NUM_THREADS
    env["LD_LIBRARY_PATH"] = run_ld
    env["UCX_TLS"] = UCX_TLS
    env["UCX_NET_DEVICES"] = UCX_NET_DEVICES
    env["UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE"] = pipeline_str

    start_time = time.time()
    try:
        if arm == "mono":
            # Sweep all sizes
            cmd = [
                "srun", "-n", str(nranks),
                bin_path, "-c", "allreduce", "-b", "32K", "-e", "4M",
                "-f", "2", "-n", str(ITERS), "-w", str(WARMUP),
                "-t", DTYPE, "-o", OP
            ]
        else:
            # Specific size
            cmd = [
                "srun", "-n", str(nranks),
                bin_path, "-c", "allreduce", "-b", str(count), "-e", str(count),
                "-f", "1", "-n", str(ITERS), "-w", str(WARMUP),
                "-t", DTYPE, "-o", OP
            ]
        with open(cell_file, 'w') as outf:
            result = subprocess.run(
                cmd, env=env, stdout=outf, stderr=outf,
                timeout=TIMEOUT
            )
            exit_code = result.returncode
    except subprocess.TimeoutExpired:
        exit_code = 124
        # Write timeout marker
        with open(cell_file, 'w') as outf:
            outf.write(f"TIMEOUT after {TIMEOUT}s\n")
    except Exception as e:
        exit_code = 2
        with open(cell_file, 'w') as outf:
            outf.write(f"ERROR: {e}\n")

    duration = time.time() - start_time

    if exit_code != 0:
        failed_cells += 1

    # Parse output
    bw_gbs = "N/A"
    time_us = "N/A"
    if os.path.exists(cell_file) and os.path.getsize(cell_file) > 0:
        try:
            with open(cell_file) as f:
                lines = f.readlines()
            for line in reversed(lines):
                parts = line.split()
                if parts and parts[0].replace('.', '').isdigit():
                    bw_gbs = parts[-1]
                    time_us = parts[-2]
                    break
        except Exception:
            pass

    # Write summary row
    summary_writer.writerow([
        geometry, rep, cell_idx, arm, msg_bytes_str or '',
        n_frags, f_eff_kib, depth, count, time_us, bw_gbs, exit_code
    ])

    # Update accounting
    accounting["cells"].append({
        "idx": cell_idx, "rep": rep, "arm": arm,
        "exit_code": exit_code, "duration": round(duration, 2),
        "bw_gbs": bw_gbs
    })
    if rep not in accounting["replicates"]:
        accounting["replicates"][rep] = {"total": 0, "failed": 0}
    accounting["replicates"][rep]["total"] += 1
    if exit_code != 0:
        accounting["replicates"][rep]["failed"] += 1

    return exit_code

# ─── Execute ─────────────────────────────────────────────────────────────────
print(f"== running cells for {geometry} ==")

for rep in sorted_reps:
    reps = by_rep[rep]
    seed = int(reps[0]['seed'])
    rng = random.Random(seed)

    # Deduplicate pipeline cells
    seen = set()
    pipeline = []
    for r in reps:
        if r['arm'] != 'pipeline':
            continue
        key = (r['msg_bytes'], r['f_eff_bytes'], r['actual_depth'])
        if key in seen:
            continue
        seen.add(key)
        pipeline.append(r)

    # Randomize
    rng.shuffle(pipeline)

    # Canary start
    canary = {
        'arm': 'canary', 'msg_bytes': '4194304',
        'f_req_kib': '1024', 'pdepth': '4',
        'actual_n_frags': '4', 'f_eff_bytes': '1048576',
        'actual_depth': '2', 'f_eff_kib': '1024.0',
    }
    run_benchmark(canary, rep)

    # Pipeline cells
    for cell in pipeline:
        run_benchmark(cell, rep)

    # Mono
    mono = [r for r in reps if r['arm'] == 'mono']
    if mono:
        run_benchmark(mono[0], rep)

    # Canary end
    run_benchmark(canary, rep)

# ─── Canary validation ──────────────────────────────────────────────────────
print("== canary check ==")
canary_ok = True
canary_cells_by_rep = {}
for entry in accounting["cells"]:
    if entry.get("arm") == "canary":
        rep = entry["rep"]
        canary_cells_by_rep.setdefault(rep, []).append(entry)

for rep in sorted_reps:
    entries = canary_cells_by_rep.get(rep, [])
    if not entries:
        print(f"  FAIL: R{rep} no canary cells found")
        canary_ok = False
        continue
    for entry in entries:
        if entry["exit_code"] != 0:
            print(f"  FAIL: R{rep} canary cell {entry['idx']} exit_code={entry['exit_code']}")
            canary_ok = False
        elif entry["bw_gbs"] == "N/A" or entry["bw_gbs"] == "0" or entry["bw_gbs"] is None:
            print(f"  FAIL: R{rep} canary cell {entry['idx']} produced no bandwidth (bw={entry['bw_gbs']})")
            canary_ok = False
        else:
            try:
                bw = float(entry["bw_gbs"])
                if bw <= 0:
                    print(f"  FAIL: R{rep} canary cell {entry['idx']} bw={bw} <= 0")
                    canary_ok = False
            except (ValueError, TypeError):
                print(f"  FAIL: R{rep} canary cell {entry['idx']} bw unparseable: {entry['bw_gbs']}")
                canary_ok = False

if canary_ok:
    print("  All canary checks passed")
else:
    print("  CANARY FAILED — aborting (no failure rate filtering or downstream analysis)")
    sys.exit(3)

# ─── Failure rate check ─────────────────────────────────────────────────────
print("== failure rate check ==")
excluded = []
for rep, stats in accounting.get("replicates", {}).items():
    total = stats.get("total", 0)
    failed = stats.get("failed", 0)
    if total > 0:
        rate = failed / total
        if rate > 0.10:
            print(f"  EXCLUDED: R{rep} failure rate {rate*100:.1f}% > 10%")
            excluded.append(rep)
        else:
            print(f"  R{rep}: {failed}/{total} failed ({rate*100:.1f}%) — OK")

# ─── Write accounting ───────────────────────────────────────────────────────
with open(accounting_file, 'w') as f:
    json.dump(accounting, f, indent=2)

# ─── Checksums ──────────────────────────────────────────────────────────────
print("== checksums ==")
sha_file = os.path.join(result_dir, "SHA256SUMS")
sha_list = []
for rep in sorted_reps:
    rep_dir = os.path.join(result_dir, f"replicate-{rep}")
    for fname in sorted(os.listdir(rep_dir)):
        fpath = os.path.join(rep_dir, fname)
        if os.path.isfile(fpath):
            h = hashlib.sha256()
            with open(fpath, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    h.update(chunk)
            sha_list.append(f"{h.hexdigest()}  {rep_dir}/{fname}")

with open(sha_file, 'w') as f:
    f.write('\n'.join(sha_list) + '\n')
print(f"SHA256SUMS written ({len(sha_list)} files)")

# ─── Summary ────────────────────────────────────────────────────────────────
print(f"")
print(f"Total cells: {total_cells}")
print(f"Failed cells: {failed_cells}")
print(f"Results: {result_dir}")

# Completion flag
with open(os.path.join(result_dir, "done_flag"), 'w') as f:
    f.write("done\n")
PYEOF

# Run the Python cell runner
export GEOMETRY MATRIX_CSV SRC_DIR INSTALL_DIR
python3 "$RUNNER_SCRIPT"

# Append completion to manifest
echo "completion_time: $(date -u '+%Y-%m-%dT%H:%M:%SZ')" | tee -a "$MANIFEST"
echo ""
echo "== done: $GEOMETRY results in $RESULT_DIR =="
