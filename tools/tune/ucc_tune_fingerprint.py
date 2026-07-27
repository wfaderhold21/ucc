#!/usr/bin/env python3
"""
ucc_tune_fingerprint.py — platform fingerprint collection for the offline tuner.

The fingerprint tags generated configs so they can be matched back to the
hardware they were measured on.  It is also the key for a multi-platform DB:
the external launcher selects the right UCC_CONFIG_FILE by comparing the
fingerprint of the live allocation against fingerprints stored with each conf.

Fields collected:
  ucc_version   — from `ucc_info -v`  ("X.Y.Z revision <hash>")
  ucx_version   — from `ucx_info -v`  (first version line)
  cpu_model     — from /proc/cpuinfo (Linux) or sysctl (macOS)
  gpu_model     — from nvidia-smi (or "none")
  gpu_driver    — from nvidia-smi (or "none")
  cuda_version  — from nvidia-smi (or "none")
  hostname      — socket.gethostname() (informational only, not in hash)
  timestamp     — ISO 8601 UTC (informational only, not in hash)
  hash          — SHA-256 of the stable key fields (for DB lookup / file naming)
"""

from __future__ import annotations

import dataclasses
import hashlib
import logging
import os
import platform
import re
import socket
import subprocess
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

_UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# Fingerprint dataclass
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Fingerprint:
    ucc_version: str
    ucx_version: str
    cpu_model: str
    gpu_model: str
    gpu_driver: str
    cuda_version: str
    hostname: str
    timestamp: str
    hash: str

    def summary(self) -> str:
        lines = [
            f"UCC     : {self.ucc_version}",
            f"UCX     : {self.ucx_version}",
            f"CPU     : {self.cpu_model}",
            f"GPU     : {self.gpu_model}",
            f"Driver  : {self.gpu_driver}",
            f"CUDA    : {self.cuda_version}",
            f"Host    : {self.hostname}",
            f"Time    : {self.timestamp}",
            f"Hash    : {self.hash[:12]}…",
        ]
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-tool collectors (each returns a string, never raises)
# ---------------------------------------------------------------------------

def _run(cmd: list, timeout_s: int = 10) -> str:
    """Run a command and return stdout, or empty string on any failure."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        return r.stdout if r.returncode == 0 else ""
    except Exception:
        return ""


def _ucc_version(ucc_info_path: str) -> str:
    """Parse 'UCC version=X.Y.Z revision ...' from `ucc_info -v`."""
    out = _run([ucc_info_path, "-v"])
    m = re.search(r"UCC version=(\S+)", out)
    return m.group(1) if m else _UNKNOWN


def _ucx_version(ucx_info_path: str) -> str:
    """Parse UCX version line from `ucx_info -v`."""
    out = _run([ucx_info_path, "-v"])
    # UCX prints: "# UCX version=X.Y.Z ..."
    m = re.search(r"UCX version[=:\s]+(\S+)", out, re.IGNORECASE)
    return m.group(1) if m else _UNKNOWN


def _cpu_model() -> str:
    """Best-effort CPU model string — Linux /proc/cpuinfo or macOS sysctl."""
    if platform.system() == "Linux":
        try:
            with open("/proc/cpuinfo") as f:
                for line in f:
                    if line.startswith("model name"):
                        return line.split(":", 1)[1].strip()
        except OSError:
            pass
    out = _run(["sysctl", "-n", "hw.model"])
    if out.strip():
        return out.strip()
    return platform.processor() or _UNKNOWN


def _gpu_info() -> tuple[str, str, str]:
    """
    Query GPU model, driver version, and CUDA version via nvidia-smi.

    Returns (gpu_model, driver_version, cuda_version).
    All fields default to "none" when nvidia-smi is unavailable.
    """
    out = _run(
        ["nvidia-smi",
         "--query-gpu=name,driver_version",
         "--format=csv,noheader,nounits"],
    )
    gpu_model = _UNKNOWN
    driver = _UNKNOWN
    if out.strip():
        parts = [p.strip() for p in out.splitlines()[0].split(",")]
        if len(parts) >= 2:
            gpu_model = parts[0] or _UNKNOWN
            driver = parts[1] or _UNKNOWN

    # CUDA version from nvidia-smi header line
    cuda = _UNKNOWN
    header = _run(["nvidia-smi"])
    m = re.search(r"CUDA Version:\s*(\S+)", header)
    if m:
        cuda = m.group(1)

    if gpu_model == _UNKNOWN:
        gpu_model = "none"
        driver = "none"
        cuda = "none"

    return gpu_model, driver, cuda


def _stable_hash(fp: "Fingerprint") -> str:
    """SHA-256 of the fields that identify a hardware/software configuration."""
    canonical = "|".join([
        fp.ucc_version,
        fp.ucx_version,
        fp.cpu_model,
        fp.gpu_model,
        fp.gpu_driver,
        fp.cuda_version,
    ])
    return hashlib.sha256(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def collect(
    ucc_info_path: str = "ucc_info",
    ucx_info_path: str = "ucx_info",
) -> Fingerprint:
    """
    Collect a platform fingerprint.  Never raises — missing tools produce
    "unknown" or "none" for the relevant fields.
    """
    ucc_ver = _ucc_version(ucc_info_path)
    ucx_ver = _ucx_version(ucx_info_path)
    cpu = _cpu_model()
    gpu, driver, cuda = _gpu_info()

    fp = Fingerprint(
        ucc_version=ucc_ver,
        ucx_version=ucx_ver,
        cpu_model=cpu,
        gpu_model=gpu,
        gpu_driver=driver,
        cuda_version=cuda,
        hostname=socket.gethostname(),
        timestamp=datetime.now(tz=timezone.utc).isoformat(),
        hash="",  # filled below
    )
    fp = dataclasses.replace(fp, hash=_stable_hash(fp))
    return fp


def from_dict(d: dict) -> Fingerprint:
    """Reconstruct a Fingerprint from a dict (e.g. loaded from JSON)."""
    return Fingerprint(**d)
