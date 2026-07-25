#!/usr/bin/env python3
"""Combine per-run summary.csv files from slurm_a2a_order_variance.sh.

Each input is one repeat of the seq/stride/ilv/full comparison (one SBATCH job,
one mode permutation). This aggregates them into mean +/- spread per (mode,size)
and, separately, into the per-run ilv/seq ratio -- which is the statistic that
actually answers "does ilv's edge survive noise", because the ratio is formed
within a run and so is immune to run-to-run fabric drift.

Usage:
    a2a_order_variance_combine.py results_*/summary.csv
"""

import csv
import statistics
import sys
from collections import defaultdict

MODES = ["seq", "stride", "ilv", "full"]
METRICS = [
    ("agg_bw_gib_s", "aggregate GiB/s", "%.2f"),
    ("cnp_sent_per_gib", "CNP Sent per GiB moved", "%.0f"),
    ("ecn_marked_per_gib", "ECN Marked per GiB moved", "%.0f"),
]


def load(paths):
    # data[metric][size][mode][run_tag] = value
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    runs = []
    for path in paths:
        with open(path, newline="") as handle:
            for row in csv.DictReader(handle):
                tag = "%s(%s)" % (row["run_tag"], row["job_id"])
                if tag not in runs:
                    runs.append(tag)
                size = int(row["size_bytes"])
                for metric, _, _ in METRICS:
                    data[metric][size][row["order"]][tag] = float(row[metric])
    return data, runs


def spread(values):
    """mean, half-range, n. Half-range is the honest spread for n in 3..4."""
    if not values:
        return None, None, 0
    mean = statistics.fmean(values)
    return mean, (max(values) - min(values)) / 2.0, len(values)


def main(paths):
    data, runs = load(paths)
    print("runs combined (%d): %s" % (len(runs), ", ".join(runs)))

    for metric, title, fmt in METRICS:
        print()
        print("== %s: mean +/- half-range over %d runs ==" % (title, len(runs)))
        header = "%12s" % "size_B"
        for mode in MODES:
            header += "%22s" % mode
        header += "%18s" % "ilv/seq mean"
        header += "%14s" % "ilv/seq range"
        print(header)
        for size in sorted(data[metric]):
            line = "%12d" % size
            per_mode = data[metric][size]
            for mode in MODES:
                vals = list(per_mode.get(mode, {}).values())
                mean, half, n = spread(vals)
                if mean is None:
                    line += "%22s" % "-"
                else:
                    cell = (fmt % mean) + " +/- " + (fmt % half)
                    line += "%22s" % cell
            # Within-run ratio: only runs that measured both modes.
            ratios = []
            for tag in runs:
                s = per_mode.get("seq", {}).get(tag)
                i = per_mode.get("ilv", {}).get(tag)
                if s is not None and i is not None and s != 0:
                    ratios.append(i / s)
            if ratios:
                line += "%17.1f%%" % (100 * (statistics.fmean(ratios) - 1))
                line += "%13.1f%%" % (100 * (max(ratios) - min(ratios)))
            else:
                line += "%18s%14s" % ("-", "-")
            print(line)

        # Per-run ilv/seq ratios restricted to the large-size regime, where the
        # fabric is actually saturated and the congestion claim lives.
        print("  per-run ilv/seq ratio, sizes >= 32768 B/peer:")
        for tag in runs:
            ratios = []
            for size in sorted(data[metric]):
                if size < 32768:
                    continue
                s = data[metric][size].get("seq", {}).get(tag)
                i = data[metric][size].get("ilv", {}).get(tag)
                if s is not None and i is not None and s != 0:
                    ratios.append(i / s)
            if ratios:
                print("    %-14s n=%2d  mean %+6.1f%%  min %+6.1f%%  max %+6.1f%%"
                      % (tag, len(ratios), 100 * (statistics.fmean(ratios) - 1),
                         100 * (min(ratios) - 1), 100 * (max(ratios) - 1)))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    main(sys.argv[1:])
