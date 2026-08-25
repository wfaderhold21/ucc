#!/usr/bin/env bash
#
# Task 418 v2 — execute one geometry's replicates from the redesigned matrix.
#
# Submit with:
#   sbatch --nodes=<N> --ntasks-per-node=<PPN> \
#           --export=ALL,MATRIX_CSV=/path/matrix-v2-r20.csv \
#           contrib/slurm_task418_v2_geometry.sh G1
#
# Required positional argument:
#   $1        — G1, G2, G3, or G4
# Required environment at submission:
#   MATRIX_CSV  — path to the machine-readable matrix (v2, 20-rep start)
#   SKIP_BUILD  — set to 1 if UCC already built
#   INSTALL_DIR — path to UCC install (optional, auto-computed if not set)
#
# Protocol parameters are FROZEN — do not modify sweep axes, timeouts, or guards.

#SBATCH --job-name=ucc-sra-t418v2-%x
#SBATCH --partition=GAIA
#SBATCH --time=04:00:00
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
RESULT_DIR="$SRC_DIR/results/task418-v2-geometry-${GEOMETRY}"
SKIP_BUILD=${SKIP_BUILD:-0}

SOURCE_BRANCH=${SOURCE_BRANCH:-$(git -C "$SRC_DIR" branch --show-current 2>/dev/null || printf unknown)}
SOURCE_COMMIT=${SOURCE_COMMIT:-$(git -C "$SRC_DIR" rev-parse HEAD 2>/dev/null || printf unknown)}

# ─── Protocol constants (frozen — v2 redesign 2026-08-19) ────────────────────
BUILD_CAP=16
TIMEOUT_PER_CELL=300  # 5 minutes
WARMUP=20
ITERS=100
DTYPE="float32"
OP="sum"
UCX_TLS="sm,dc"
UCX_NET_DEVICES="mlx5_0:1"
OMP_NUM_THREADS=1
CANARY_DIV_PCT=15
UCC_TL_UCP_TUNE="allreduce:0-inf:@sra_knomial"  # task-530: force SRA algorithm

die() { echo "FATAL: $*" >&2; exit 2; }

# ─── Geometry spec ──────────────────────────────────────────────────────────
case "$GEOMETRY" in
    G1) NRANKS=32; PPN=8;  NODES=4 ;;
    G2) NRANKS=32; PPN=16; NODES=2 ;;
    G3) NRANKS=128; PPN=8; NODES=16 ;;
    G4) NRANKS=128; PPN=16; NODES=8 ;;
    *) die "unknown geometry: $GEOMETRY" ;;
esac

# ─── Pre-flight validation ─────────────────────────────────────────────────
[[ -f "$MATRIX_CSV" ]] || die "Matrix CSV not found: $MATRIX_CSV"
[[ -d "$RESULT_DIR" && -f "$RESULT_DIR/done_flag" ]] && die "Results already exist at $RESULT_DIR"

mkdir -p "$RESULT_DIR"

# ─── Build (if needed) ──────────────────────────────────────────────────────
if [[ "$SKIP_BUILD" != "1" ]]; then
    BUILD_DIR="$SRC_DIR/build-${JOB_ID}"
    mkdir -p "$BUILD_DIR"
    cmake -S "$SRC_DIR" -B "$BUILD_DIR" \
        -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
        -DCMAKE_BUILD_TYPE=Release \
        -DUCC_SCHEDULE_PIPELINED_MAX_FRAGS=$BUILD_CAP \
        -DCMAKE_C_COMPILER=mpicc \
        -DCMAKE_CXX_COMPILER=mpicxx
    cmake --build "$BUILD_DIR" --parallel
    cmake --install "$BUILD_DIR"
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
    echo "protocol: task418-v2"
    echo "geometry: $GEOMETRY"
    echo "nranks: $NRANKS"
    echo "ppn: $PPN"
    echo "nodes: $NODES"
    echo "job_id: $JOB_ID"
    echo "submit_time: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
    echo "nodelist: ${SLURM_NODELIST:-unknown}"
    echo "build_cap: $BUILD_CAP"
    echo "warmup: $WARMUP"
    echo "iters: $ITERS"
    echo "dtype: $DTYPE"
    echo "op: $OP"
    echo "ucx_tls: $UCX_TLS"
    echo "ucx_net_devices: $UCX_NET_DEVICES"
    echo "omp_num_threads: $OMP_NUM_THREADS"
    echo "ucc_tl_ucp_tune: $UCC_TL_UCP_TUNE"
    echo "source_branch: $SOURCE_BRANCH"
    echo "source_commit: $SOURCE_COMMIT"
    echo "matrix_csv: $MATRIX_CSV"
    echo "install_dir: $INSTALL_DIR"
    echo "mpi_home: $MPI_HOME"
    echo "ucx_home: $UCX_HOME"
} | tee "$MANIFEST"

BIN_SHA=$(sha256sum "$BIN" | cut -d' ' -f1)
echo "binary_sha256: $BIN_SHA" | tee -a "$MANIFEST"

HARNESS_SHA=$(sha256sum "$SRC_DIR/results/task418/harness_task418_v2.py" | cut -d' ' -f1)
echo "harness_sha256: $HARNESS_SHA" | tee -a "$MANIFEST"

# ─── Cell execution via Python driver ───────────────────────────────────────
RUNNER_SCRIPT="$RESULT_DIR/run_cells.py"
cat > "$RUNNER_SCRIPT" <<'PYEOF'
#!/usr/bin/env python3
"""Task 418 v2 cell runner — executes cells from the redesigned matrix for one geometry."""
import csv
import hashlib
import json
import math
import os
import subprocess
import sys
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
UCC_TL_UCP_TUNE = "allreduce:0-inf:@sra_knomial"

geometry = os.environ["GEOMETRY"]
matrix_csv = os.environ["MATRIX_CSV"]
src_dir = os.environ["SRC_DIR"]
install_dir = os.environ["INSTALL_DIR"]
mpi_home = os.environ.get("MPI_HOME", "/usr/mpi/gcc/openmpi-4.1.9a1")
ucx_home = os.environ.get("UCX_HOME", "/usr")

result_dir = os.path.join(src_dir, "results", f"task418-v2-geometry-{geometry}")
summary_csv = os.path.join(result_dir, "summary.csv")
accounting_file = os.path.join(result_dir, "accounting.json")
manifest_file = os.path.join(result_dir, "manifest.txt")

# Geometry specs (frozen)
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
print(f"Binary: {bin_path}")
print(f"UCC_TL_UCP_TUNE={UCC_TL_UCP_TUNE}")

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
print(f"Replicates: {len(sorted_reps)} ({sorted_reps[0]}..{sorted_reps[-1]})")

# Initialize outputs
summary_writer = csv.writer(open(summary_csv, 'w', newline=''))
SUMMARY_HDR = [
    "geometry", "replicate", "cell_index", "arm",
    "message_bytes", "n_frags_eff", "f_eff_kib", "depth_eff",
    "count", "time_us", "bw_gbs", "exit_code",
]
summary_writer.writerow(SUMMARY_HDR)

accounting = {"cells": [], "replicates": {}}
cell_idx = 0
total_cells = 0
failed_cells = 0

def run_benchmark(cell, rep):
    global cell_idx, total_cells, failed_cells
    total_cells += 1

    arm = cell.get("arm", "pipeline")
    msg_bytes = cell.get("msg_bytes", "")
    f_req_kib = cell.get("f_req_kib", "")
    pdepth = cell.get("pdepth", "")

    if arm in ("pipeline", "canary"):
        pipeline_str = f"thresh=64K:fragsize={f_req_kib}K:nfrags=2:pdepth={pdepth}:parallel"
    elif arm == "mono":
        pipeline_str = "n"
    else:
        print(f"  unknown arm: {arm}")
        return 1

    cell_dir = os.path.join(result_dir, f"replicate-{rep}")
    os.makedirs(cell_dir, exist_ok=True)
    idx = cell.get("cell_index", "0000")
    log_file = os.path.join(cell_dir, f"cell-{int(idx):04d}.csv")

    env = os.environ.copy()
    env["LD_LIBRARY_PATH"] = run_ld
    env["OMP_NUM_THREADS"] = OMP_NUM_THREADS
    env["UCX_TLS"] = UCX_TLS
    env["UCX_NET_DEVICES"] = UCX_NET_DEVICES
    env["UCC_TL_UCP_TUNE"] = UCC_TL_UCP_TUNE
    env["UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE"] = pipeline_str

    if arm == "mono":
        # Sweep from 32K to 16M elements (128KB to 64MB), covering 4/8/16 MiB
        cmd = [
            "srun", "-n", str(nranks),
            bin_path, "-c", "allreduce",
            "-b", "32K", "-e", "16M", "-f", "2",
            "-n", str(ITERS), "-w", str(WARMUP),
            "-d", DTYPE, "-o", OP, "-F",
        ]
    else:
        # Specific element count: msg_bytes / 4 for float32
        count = int(msg_bytes) // 4
        cmd = [
            "srun", "-n", str(nranks),
            bin_path, "-c", "allreduce",
            "-b", str(count), "-e", str(count), "-f", "1",
            "-n", str(ITERS), "-w", str(WARMUP),
            "-d", DTYPE, "-o", OP, "-F",
        ]

    print(f"  [{geometry} R{rep} cell{idx}] {arm} msg={msg_bytes or 'sweep'} "
          f"pipe={pipeline_str}")

    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd, env=env, capture_output=True, text=True,
            timeout=TIMEOUT,
        )
        elapsed = time.monotonic() - t0
        exit_code = proc.returncode
        stdout = proc.stdout
        stderr = proc.stderr
    except subprocess.TimeoutExpired:
        elapsed = TIMEOUT
        exit_code = -1
        stdout = ""
        stderr = "TIMEOUT"

    # Write raw output
    with open(log_file, 'w') as f:
        f.write(f"# geometry={geometry} replicate={rep} cell_index={idx} arm={arm}\n")
        f.write(f"# message_bytes={msg_bytes} pipeline={pipeline_str}\n")
        f.write(f"# exit_code={exit_code} elapsed_s={elapsed:.1f}\n")
        f.write(stdout)
        if stderr:
            f.write(f"\n# STDERR:\n{stderr}")

    # Parse output: collect all numeric benchmark lines
    parsed_rows = []
    for line in stdout.splitlines():
        parts = line.split()
        if parts and parts[0].replace(".", "").isdigit() and len(parts) >= 8:
            # -F format: count iters t_us bw_min bw_max bw_avg ...
            t_us = float(parts[2])
            bw = float(parts[-3])  # bw_avg with -F
            count_str = parts[0]
            # Compute message bytes from count and datatype
            try:
                count_int = int(count_str)
                row_bytes = count_int * 4  # float32 = 4 bytes
            except ValueError:
                row_bytes = 0
            parsed_rows.append((row_bytes, t_us, bw, count_str))

    # Derive effective parameters for pipeline arms
    if arm == "pipeline" and msg_bytes:
        n_frags_eff = cell.get("actual_n_frags", "")
        depth_eff = cell.get("actual_depth", "")
        f_eff_kib = cell.get("f_eff_kib", "")
    else:
        n_frags_eff = ""
        depth_eff = ""
        f_eff_kib = ""

    if not parsed_rows:
        # No valid output: write one zero-row
        time_us = 0.0
        bw_gbs = 0.0
        count_str = ""
        summary_writer.writerow([
            geometry, rep, idx, arm, msg_bytes,
            n_frags_eff, f_eff_kib, depth_eff,
            count_str, time_us, bw_gbs, exit_code,
        ])
        status = f"NO_OUTPUT(exit={exit_code})"
        failed_cells += 1
        print(f"    -> {status} ({elapsed:.1f}s wall)")
        accounting["cells"].append({
            "geometry": geometry, "rep": rep, "cell_index": idx,
            "arm": arm, "message_bytes": msg_bytes,
            "time_us": time_us, "bw_gbs": bw_gbs,
            "exit_code": exit_code, "elapsed_s": round(elapsed, 1),
        })
        return exit_code

    # Write summary rows: one per parsed line for mono, last line for pipeline/canary
    if arm == "mono":
        rows_to_write = parsed_rows  # all sizes
    else:
        rows_to_write = [parsed_rows[-1]]  # only the target size

    for (row_bytes, t_us, bw, cnt_str) in rows_to_write:
        row_msg_bytes = str(row_bytes) if row_bytes > 0 else msg_bytes
        summary_writer.writerow([
            geometry, rep, idx, arm, row_msg_bytes,
            n_frags_eff, f_eff_kib, depth_eff,
            cnt_str, t_us, bw, exit_code,
        ])

    # Use last row for status display and accounting
    time_us = parsed_rows[-1][1]
    bw_gbs = parsed_rows[-1][2]

    status = "OK" if exit_code == 0 else f"FAIL(exit={exit_code})"
    if exit_code != 0:
        failed_cells += 1
    print(f"    -> {status} t={time_us:.1f}us bw={bw_gbs:.2f}GB/s "
          f"({elapsed:.1f}s wall)")

    accounting["cells"].append({
        "geometry": geometry, "rep": rep, "cell_index": idx,
        "arm": arm, "message_bytes": msg_bytes,
        "time_us": time_us, "bw_gbs": bw_gbs,
        "exit_code": exit_code, "elapsed_s": round(elapsed, 1),
    })
    return exit_code

# ─── Execute ─────────────────────────────────────────────────────────────────
print(f"== running cells for {geometry} ==")

for rep in sorted_reps:
    reps = by_rep[rep]
    # Separate by arm type
    pipeline_cells = [c for c in reps if c.get("arm") == "pipeline"]
    mono_cells = [c for c in reps if c.get("arm") == "mono"]
    canary_cells = sorted(
        [c for c in reps if c.get("arm") == "canary"],
        key=lambda c: c.get("canary", "")
    )
    # Order: canary-start, mono, pipeline (shuffled in matrix), canary-end
    rep_cells = canary_cells[:1] + mono_cells + pipeline_cells + canary_cells[1:]
    for c in rep_cells:
        run_benchmark(c, rep)

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
    if len(entries) != 2:
        print(f"  R{rep}: expected 2 canary cells, got {len(entries)} — FAIL")
        canary_ok = False
        continue
    t1, t2 = entries[0]["time_us"], entries[1]["time_us"]
    if t1 <= 0 or t2 <= 0:
        print(f"  R{rep}: canary time_us <= 0 — FAIL")
        canary_ok = False
        continue
    div = abs(t1 - t2) / ((t1 + t2) / 2) * 100
    if div > CANARY_DIV_PCT:
        print(f"  R{rep}: canary divergence {div:.1f}% > {CANARY_DIV_PCT}% — FLAGGED")
    else:
        print(f"  R{rep}: canary divergence {div:.1f}% — OK")

if canary_ok:
    print("  All canary checks passed")
else:
    print("  CANARY FAILED — aborting")
    sys.exit(3)

# ─── Failure rate check ─────────────────────────────────────────────────────
print("== failure rate check ==")
for rep in sorted_reps:
    rep_cells = [e for e in accounting["cells"] if e["rep"] == rep]
    total = len(rep_cells)
    failed = sum(1 for e in rep_cells if e["exit_code"] != 0)
    rate = failed / total if total > 0 else 0
    accounting["replicates"][rep] = {"total": total, "failed": failed}
    if rate > 0.1:
        print(f"  R{rep}: {failed}/{total} failed ({rate*100:.1f}%) — EXCLUDED")
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
        if fname.startswith("cell-") and fname.endswith(".csv"):
            h = hashlib.sha256(open(os.path.join(rep_dir, fname), "rb").read())
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