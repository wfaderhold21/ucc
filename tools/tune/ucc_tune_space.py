#!/usr/bin/env python3
"""
ucc_tune_space.py — search space enumeration for the UCC offline tuner.

Responsibilities:
- Parse `ucc_info -A` output into a structured algorithm map.
- Define secondary knobs (radix, pipeline, num_posts, etc.) per algorithm.
- Provide helpers to derive TUNE env var names and CL/TL competition-control
  env vars for isolated single-component measurement.
- Generate msg-size grids and datatype-size lookups used when building RunSpecs.
"""

from __future__ import annotations

import dataclasses
import logging
import os
import re
import subprocess
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Algorithm info (mirrors ucc_base_coll_alg_info_t)
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class AlgInfo:
    id: int
    name: str
    desc: str


# Parsed output of `ucc_info -A`.
# Outer key: component string, e.g. "tl/ucp", "cl/hier".
# Inner key: collective name, e.g. "allreduce", "bcast".
# Value: ordered list of AlgInfo (by id).
ComponentAlgs = dict  # dict[str, dict[str, list[AlgInfo]]]


# ---------------------------------------------------------------------------
# ucc_info -A output parser
# ---------------------------------------------------------------------------

# Matches section header:  "tl/ucp algorithms:"
_SECTION_RE = re.compile(r"^(\w+/\w+)\s+algorithms:\s*$")

# Matches collective name line (2-space indent, no leading digits):
_COLL_RE = re.compile(r"^  (\w+)\s*$")

# Matches algorithm entry:  "     0 :          knomial : description text"
_ALG_RE = re.compile(r"^\s+(\d+)\s*:\s*(\S+)\s*:\s*(.*?)\s*$")


def parse_ucc_info_algs(output: str) -> ComponentAlgs:
    """
    Parse the stdout of `ucc_info -A` into a ComponentAlgs dict.

    Returns an empty dict if the output is empty or contains no algorithm
    sections; does not raise on partial/corrupt output.
    """
    result: ComponentAlgs = {}
    current_component: Optional[str] = None
    current_coll: Optional[str] = None

    for line in output.splitlines():
        m = _SECTION_RE.match(line)
        if m:
            current_component = m.group(1)
            current_coll = None
            result.setdefault(current_component, {})
            continue

        if current_component is None:
            continue

        m = _COLL_RE.match(line)
        if m:
            # ucc_info -A prints capitalised collective names: ucc_info.c:62
            # uses ucc_coll_type_str(), whose table (src/utils/ucc_log.h:26)
            # yields "Allreduce", "Bcast", "Reduce_scatter", ... Every consumer
            # here looks the key up with the lowercase perftest/TUNE spelling,
            # so normalise at the parse boundary. Each of the 16 names in
            # ucc_coll_type_str() lowercases exactly onto its ucc_pt_op_map
            # spelling in tools/perf/ucc_pt_config.cc.
            current_coll = m.group(1).lower()
            result[current_component].setdefault(current_coll, [])
            continue

        if current_coll is None:
            continue

        m = _ALG_RE.match(line)
        if m:
            alg = AlgInfo(
                id=int(m.group(1)),
                name=m.group(2),
                desc=m.group(3),
            )
            result[current_component][current_coll].append(alg)

    return result


def run_ucc_info_raw(
    ucc_info_path: str = "ucc_info",
    extra_env: Optional[dict] = None,
    timeout_s: int = 30,
) -> str:
    """
    Run `ucc_info -A` and return raw stdout.

    Raises RuntimeError on non-zero exit or timeout.
    """
    env = os.environ.copy()
    if extra_env:
        env.update(extra_env)

    try:
        proc = subprocess.run(
            [ucc_info_path, "-A"],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout_s,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"Could not find {ucc_info_path!r}: {exc}") from exc
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"ucc_info -A timed out after {timeout_s}s")

    if proc.returncode != 0:
        raise RuntimeError(
            f"ucc_info -A exited {proc.returncode}:\n{proc.stderr[:500]}"
        )

    return proc.stdout


def run_ucc_info_algs(
    ucc_info_path: str = "ucc_info",
    extra_env: Optional[dict] = None,
    timeout_s: int = 30,
) -> ComponentAlgs:
    """
    Run `ucc_info -A` and return parsed ComponentAlgs.

    Raises RuntimeError on non-zero exit or timeout.
    """
    return parse_ucc_info_algs(run_ucc_info_raw(ucc_info_path, extra_env, timeout_s))


# ---------------------------------------------------------------------------
# TUNE env var and CL/TL competition control
# ---------------------------------------------------------------------------

def tune_env_var(component: str) -> str:
    """
    Return the TUNE env var name for a component.

    Examples:
      "tl/ucp"  → "UCC_TL_UCP_TUNE"
      "cl/hier" → "UCC_CL_HIER_TUNE"
    """
    kind, name = component.split("/", 1)
    return f"UCC_{kind.upper()}_{name.upper()}_TUNE"


def competition_env(component: str) -> dict:
    """
    Return env vars that restrict UCC to measuring only the given component.

    Forcing a TUNE string alone is not enough — competing components
    (NCCL, CUDA, HIER) may still score higher and win.  These vars isolate
    the target component for unambiguous measurement.

    For TL components: sets UCC_TLS=<name> and UCC_CLS=basic.
    For CL components: sets UCC_CLS=<name> (TLs needed by that CL remain).
    """
    kind, name = component.split("/", 1)
    if kind == "tl":
        return {"UCC_TLS": name, "UCC_CLS": "basic"}
    elif kind == "cl":
        return {"UCC_CLS": name}
    else:
        logger.warning("Unknown component kind %r in %r", kind, component)
        return {}


# ---------------------------------------------------------------------------
# Secondary knobs
# ---------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True)
class Knob:
    """One tuneable secondary parameter for an algorithm."""
    env_var: str
    description: str
    default: str                 # UCC default value (string form)
    candidates: tuple            # tuple of string values to sweep


# Keys are (component, collective, alg_name).  A missing key means the
# algorithm has no secondary knobs worth sweeping.
#
# Candidate values are conservative representative points, not exhaustive.
# The sweep module iterates them one dimension at a time (coordinate descent).
#
# All values are strings matching the env var's expected format exactly, as
# documented in the config table in tl_ucp.c / tl_cuda.c.

_KNOBS: dict[tuple[str, str, str], list[Knob]] = {
    # -----------------------------------------------------------------------
    # tl/ucp — allreduce
    # -----------------------------------------------------------------------
    ("tl/ucp", "allreduce", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_ALLREDUCE_KN_RADIX",
            description="Radix of recursive-knomial allreduce",
            default="auto",
            candidates=("2", "4", "8"),
        ),
    ],
    ("tl/ucp", "allreduce", "sra_knomial"): [
        Knob(
            env_var="UCC_TL_UCP_ALLREDUCE_SRA_KN_RADIX",
            description="Radix of scatter-reduce-allgather knomial allreduce",
            default="auto",
            candidates=("2", "4", "8"),
        ),
        Knob(
            env_var="UCC_TL_UCP_ALLREDUCE_SRA_KN_PIPELINE",
            description="Pipeline settings for SRA knomial allreduce",
            default="auto",
            # Keyed format: thresh=<memunit>:fragsize=<memunit>:nfrags=<uint>:pdepth=<uint>
            candidates=(
                "thresh=0:fragsize=64k:nfrags=4:pdepth=2",
                "thresh=0:fragsize=128k:nfrags=4:pdepth=2",
                "thresh=0:fragsize=256k:nfrags=4:pdepth=2",
                "thresh=0:fragsize=128k:nfrags=8:pdepth=2",
                "thresh=0:fragsize=128k:nfrags=4:pdepth=4",
            ),
        ),
    ],
    ("tl/ucp", "allreduce", "sliding_window"): [
        Knob(
            env_var="UCC_TL_UCP_ALLREDUCE_SLIDING_WIN_BUF_SIZE",
            description="Buffer size for sliding window allreduce",
            default="65536",
            candidates=("32k", "64k", "128k", "256k"),
        ),
        Knob(
            env_var="UCC_TL_UCP_ALLREDUCE_SLIDING_WIN_PUT_WINDOW_SIZE",
            description="Max concurrent puts (0 = team size)",
            default="0",
            candidates=("0", "2", "4", "8"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — bcast
    # -----------------------------------------------------------------------
    ("tl/ucp", "bcast", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_BCAST_KN_RADIX",
            description="Radix of recursive-knomial bcast",
            default="4",
            candidates=("2", "4", "8"),
        ),
    ],
    ("tl/ucp", "bcast", "sag_knomial"): [
        Knob(
            env_var="UCC_TL_UCP_BCAST_SAG_KN_RADIX",
            description="Radix of scatter-allgather knomial bcast",
            default="auto",
            candidates=("2", "4", "8"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — allgather
    # -----------------------------------------------------------------------
    ("tl/ucp", "allgather", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_ALLGATHER_KN_RADIX",
            description="Radix of knomial allgather",
            default="auto",
            candidates=("2", "4", "8"),
        ),
    ],
    ("tl/ucp", "allgather", "batched"): [
        Knob(
            env_var="UCC_TL_UCP_ALLGATHER_BATCHED_NUM_POSTS",
            description="Max outstanding send/recv in batched allgather",
            default="auto",
            candidates=("1", "2", "4", "8", "16"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — reduce
    # -----------------------------------------------------------------------
    ("tl/ucp", "reduce", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_REDUCE_KN_RADIX",
            description="Radix of knomial reduce",
            default="4",
            candidates=("2", "4", "8"),
        ),
    ],
    ("tl/ucp", "reduce", "srg_knomial"): [
        Knob(
            env_var="UCC_TL_UCP_REDUCE_SRG_KN_RADIX",
            description="Radix of scatter-reduce-gather knomial reduce",
            default="auto",
            candidates=("2", "4", "8"),
        ),
        Knob(
            env_var="UCC_TL_UCP_REDUCE_SRG_KN_PIPELINE",
            description="Pipeline settings for SRG knomial reduce",
            default="auto",
            candidates=(
                "thresh=0:fragsize=64k:nfrags=4:pdepth=2",
                "thresh=0:fragsize=128k:nfrags=4:pdepth=2",
                "thresh=0:fragsize=256k:nfrags=4:pdepth=2",
            ),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — reduce_scatter
    # -----------------------------------------------------------------------
    ("tl/ucp", "reduce_scatter", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_REDUCE_SCATTER_KN_RADIX",
            description="Radix of knomial reduce_scatter",
            default="4",
            candidates=("2", "4", "8"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — barrier
    # -----------------------------------------------------------------------
    ("tl/ucp", "barrier", "knomial"): [
        Knob(
            env_var="UCC_TL_UCP_BARRIER_KN_RADIX",
            description="Radix of recursive-knomial barrier",
            default="8",
            candidates=("2", "4", "8", "16"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/ucp — alltoall / alltoallv
    # -----------------------------------------------------------------------
    ("tl/ucp", "alltoall", "pairwise"): [
        Knob(
            env_var="UCC_TL_UCP_ALLTOALL_PAIRWISE_NUM_POSTS",
            description="Max outstanding messages in pairwise alltoall",
            default="auto",
            candidates=("1", "2", "4", "8", "16"),
        ),
    ],
    ("tl/ucp", "alltoallv", "pairwise"): [
        Knob(
            env_var="UCC_TL_UCP_ALLTOALLV_PAIRWISE_NUM_POSTS",
            description="Max outstanding messages in pairwise alltoallv",
            default="auto",
            candidates=("1", "2", "4", "8"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/cuda — allgather / allgatherv ring
    # -----------------------------------------------------------------------
    ("tl/cuda", "allgather", "ring"): [
        Knob(
            env_var="UCC_TL_CUDA_ALLGATHER_RING_MAX_RINGS",
            description="Max rings used in allgather ring algorithm",
            default="auto",
            candidates=("1", "2", "4"),
        ),
        Knob(
            env_var="UCC_TL_CUDA_ALLGATHER_RING_NUM_CHUNKS",
            description="Chunks each ring message is split into",
            default="4",
            candidates=("2", "4", "8"),
        ),
    ],
    ("tl/cuda", "allgatherv", "ring"): [
        Knob(
            env_var="UCC_TL_CUDA_ALLGATHER_RING_MAX_RINGS",
            description="Max rings used in allgatherv ring algorithm",
            default="auto",
            candidates=("1", "2", "4"),
        ),
        Knob(
            env_var="UCC_TL_CUDA_ALLGATHER_RING_NUM_CHUNKS",
            description="Chunks each ring message is split into",
            default="4",
            candidates=("2", "4", "8"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/cuda — reduce_scatter / reduce_scatterv ring
    # -----------------------------------------------------------------------
    ("tl/cuda", "reduce_scatter", "ring"): [
        Knob(
            env_var="UCC_TL_CUDA_REDUCE_SCATTER_RING_MAX_RINGS",
            description="Max rings used in reduce_scatter ring algorithm",
            default="auto",
            candidates=("1", "2", "4"),
        ),
    ],
    ("tl/cuda", "reduce_scatterv", "ring"): [
        Knob(
            env_var="UCC_TL_CUDA_REDUCE_SCATTER_RING_MAX_RINGS",
            description="Max rings used in reduce_scatterv ring algorithm",
            default="auto",
            candidates=("1", "2", "4"),
        ),
    ],
    # -----------------------------------------------------------------------
    # tl/cuda — allreduce nvls
    # -----------------------------------------------------------------------
    ("tl/cuda", "allreduce", "nvls"): [
        Knob(
            env_var="UCC_TL_CUDA_NVLS_SM_COUNT",
            description="Number of SMs to use for NVLS allreduce",
            default="4",
            candidates=("2", "4", "8", "16"),
        ),
        Knob(
            env_var="UCC_TL_CUDA_NVLS_THREADS",
            description="Threads per block for NVLS allreduce",
            default="1024",
            candidates=("256", "512", "1024"),
        ),
    ],
}


def knobs_for(component: str, collective: str, alg_name: str) -> list[Knob]:
    """Return secondary knobs for a (component, collective, alg_name) triple."""
    return list(_KNOBS.get((component, collective, alg_name), []))


# ---------------------------------------------------------------------------
# Msg-size grid
# ---------------------------------------------------------------------------

def msg_size_grid(
    min_bytes: int = 8,
    max_bytes: int = 1 << 30,   # 1 GiB
    factor: int = 2,
) -> list[int]:
    """
    Return a geometric grid of byte sizes from min_bytes to max_bytes.

    The factor must be >= 2.  Each step multiplies by factor.  max_bytes is
    always included as the last point if the grid does not land on it exactly.
    """
    if factor < 2:
        raise ValueError(f"factor must be >= 2, got {factor}")
    sizes: list[int] = []
    s = min_bytes
    while s <= max_bytes:
        sizes.append(s)
        s *= factor
    if not sizes or sizes[-1] < max_bytes:
        sizes.append(max_bytes)
    return sizes


# ---------------------------------------------------------------------------
# Datatype sizes (matches ucc_dt_size() for the types perftest supports)
# ---------------------------------------------------------------------------

_DT_SIZE: dict[str, int] = {
    "int8": 1, "uint8": 1,
    "int16": 2, "uint16": 2, "float16": 2, "bfloat16": 2,
    "int32": 4, "uint32": 4, "float32": 4,
    "int64": 8, "uint64": 8, "float64": 8,
    "int128": 16, "uint128": 16,
    # float128 maps to float64 in UCC
    "float128": 8,
    # complex types
    "float32_complex": 8, "float64_complex": 16, "float128_complex": 16,
}


def dtype_size(datatype: str) -> int:
    """Return the byte size of one element for the given perftest datatype string."""
    sz = _DT_SIZE.get(datatype)
    if sz is None:
        raise ValueError(f"Unknown datatype: {datatype!r}")
    return sz


def bytes_to_count(size_bytes: int, datatype: str) -> int:
    """
    Convert a byte size to an element count for the given datatype.

    Returns at least 1.  Rounds down; the caller should note that the actual
    measured size will be size_bytes rounded down to the nearest element.
    """
    sz = dtype_size(datatype)
    return max(1, size_bytes // sz)


# ---------------------------------------------------------------------------
# CLI entry point — inspect the search space
# ---------------------------------------------------------------------------

def _cli_main() -> None:
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Inspect UCC tuning search space.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    sub = parser.add_subparsers(dest="cmd")

    p_algs = sub.add_parser("algs", help="Run ucc_info -A and show parsed algorithms")
    p_algs.add_argument("--ucc-info", default="ucc_info")

    p_tune = sub.add_parser("tune-var", help="Show TUNE env var for a component")
    p_tune.add_argument("component", help="e.g. tl/ucp")

    p_comp = sub.add_parser("competition", help="Show competition-control env vars")
    p_comp.add_argument("component", help="e.g. tl/ucp")

    p_knobs = sub.add_parser("knobs", help="Show knobs for a component/coll/alg")
    p_knobs.add_argument("component")
    p_knobs.add_argument("collective")
    p_knobs.add_argument("alg_name")

    p_grid = sub.add_parser("grid", help="Print msg-size grid")
    p_grid.add_argument("--min", type=int, default=8)
    p_grid.add_argument("--max", type=int, default=1 << 30)
    p_grid.add_argument("--factor", type=int, default=2)

    args = parser.parse_args()

    if args.cmd == "algs":
        algs = run_ucc_info_algs(args.ucc_info)
        for comp, colls in sorted(algs.items()):
            print(f"{comp} algorithms:")
            for coll, alg_list in sorted(colls.items()):
                print(f"  {coll}")
                for a in alg_list:
                    print(f"    {a.id:3d} : {a.name:20s} : {a.desc}")
            print()

    elif args.cmd == "tune-var":
        print(tune_env_var(args.component))

    elif args.cmd == "competition":
        env = competition_env(args.component)
        for k, v in env.items():
            print(f"{k}={v}")

    elif args.cmd == "knobs":
        ks = knobs_for(args.component, args.collective, args.alg_name)
        if not ks:
            print("(no knobs)")
        for k in ks:
            print(f"{k.env_var}  [default={k.default!r}]")
            print(f"  {k.description}")
            print(f"  candidates: {', '.join(k.candidates)}")

    elif args.cmd == "grid":
        grid = msg_size_grid(args.min, args.max, args.factor)
        for s in grid:
            print(s)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    _cli_main()
