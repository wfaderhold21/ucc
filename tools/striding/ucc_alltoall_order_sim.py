#!/usr/bin/env python3
#
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# See file LICENSE for terms.
#

"""Alltoall peer-order occupancy simulator.

This standalone tool models the issue order used by the onesided alltoall
prototype and emits per-node, per-timestep local/remote occupancy tables.
"""

from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple, Union


POLICIES = ("seq", "stride", "ilv", "ilv+stride", "ilv+rot", "full", "rank2")
MAPPINGS = ("block", "node")


@dataclass(frozen=True)
class RankLayout:
    rank_to_node: List[int]
    rank_to_local: List[int]
    node_to_ranks: List[List[int]]


def coprime_stride(size: int) -> int:
    if size <= 2:
        return 1

    for stride in range(size // 2 + 1, size):
        if math.gcd(stride, size) == 1:
            return stride

    return 1


def build_layout(nnodes: int, ppn: int, mapping: str) -> RankLayout:
    size = nnodes * ppn
    rank_to_node = [0] * size
    rank_to_local = [0] * size
    node_to_ranks = [[] for _ in range(nnodes)]

    for rank in range(size):
        if mapping == "block":
            node = rank // ppn
            local = rank % ppn
        elif mapping == "node":
            node = rank % nnodes
            local = rank // nnodes
        else:
            raise ValueError(f"unsupported mapping: {mapping}")

        rank_to_node[rank] = node
        rank_to_local[rank] = local
        node_to_ranks[node].append(rank)

    return RankLayout(rank_to_node, rank_to_local, node_to_ranks)


def base_order(rank: int, size: int, stride: int) -> List[int]:
    return [(rank + 1 + i * stride) % size for i in range(size)]


def split_local_remote(
    rank: int, order: Sequence[int], layout: RankLayout
) -> Tuple[List[int], List[int]]:
    node = layout.rank_to_node[rank]
    local: List[int] = []
    remote: List[int] = []

    for peer in order:
        if layout.rank_to_node[peer] == node:
            local.append(peer)
        else:
            remote.append(peer)

    return local, remote


def interleave(remote: Sequence[int], local: Sequence[int]) -> List[int]:
    order: List[int] = []
    for i in range(max(len(remote), len(local))):
        if i < len(remote):
            order.append(remote[i])
        if i < len(local):
            order.append(local[i])
    return order


def rotate(values: Sequence[int], shift: int) -> List[int]:
    if not values:
        return []

    shift %= len(values)
    return list(values[shift:]) + list(values[:shift])


def rotated_remote(rank: int, remote: Sequence[int], layout: RankLayout) -> List[int]:
    if not remote:
        return []

    local_rank = layout.rank_to_local[rank]
    ppn = len(layout.node_to_ranks[layout.rank_to_node[rank]])
    if ppn > 1 and len(remote) % ppn == 0:
        shift = local_rank * ppn
    else:
        shift = local_rank

    return rotate(remote, shift)


def peer_order(rank: int, size: int, layout: RankLayout, policy: str) -> List[int]:
    policy = policy.lower()
    stride = coprime_stride(size)

    if policy == "seq":
        return base_order(rank, size, 1)

    if policy == "stride":
        return base_order(rank, size, stride)

    if policy == "rank2":
        local, remote = split_local_remote(rank, base_order(rank, size, 1), layout)
        return local + remote if rank % 2 == 0 else remote + local

    use_stride = policy in ("ilv+stride", "full")
    rotate_remote = policy in ("ilv+rot", "full")
    if policy not in ("ilv", "ilv+stride", "ilv+rot", "full"):
        raise ValueError(f"unsupported policy: {policy}")

    local, remote = split_local_remote(
        rank, base_order(rank, size, stride if use_stride else 1), layout
    )
    if rotate_remote:
        remote = rotated_remote(rank, remote, layout)
    return interleave(remote, local)


CsvValue = Union[int, str]


def occupancy(nnodes: int, ppn: int, mapping: str, policy: str) -> List[Dict[str, CsvValue]]:
    layout = build_layout(nnodes, ppn, mapping)
    size = nnodes * ppn
    rank_orders = [peer_order(rank, size, layout, policy) for rank in range(size)]
    rows: List[Dict[str, CsvValue]] = []

    for timestep in range(size):
        counts = [[0, 0] for _ in range(nnodes)]
        for rank, order in enumerate(rank_orders):
            node = layout.rank_to_node[rank]
            peer = order[timestep]
            is_local = layout.rank_to_node[peer] == node
            counts[node][0 if is_local else 1] += 1

        for node in range(nnodes):
            rows.append(
                {
                    "policy": policy,
                    "mapping": mapping,
                    "nnodes": nnodes,
                    "ppn": ppn,
                    "timestep": timestep,
                    "node": node,
                    "intra_count": counts[node][0],
                    "inter_count": counts[node][1],
                }
            )

    return rows


def write_csv(path: Path, rows: Sequence[Dict[str, CsvValue]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = (
        "policy",
        "mapping",
        "nnodes",
        "ppn",
        "timestep",
        "node",
        "intra_count",
        "inter_count",
    )
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def heat_color(count: int, ppn: int, channel: str) -> str:
    if ppn <= 0:
        return "#ffffff"

    intensity = count / ppn
    if channel == "intra":
        r = int(235 - 175 * intensity)
        g = int(245 - 105 * intensity)
        b = int(255 - 15 * intensity)
    else:
        r = int(255 - 35 * intensity)
        g = int(240 - 150 * intensity)
        b = int(230 - 170 * intensity)
    return f"#{r:02x}{g:02x}{b:02x}"


def write_svg(path: Path, rows: Sequence[Dict[str, CsvValue]], nnodes: int, ppn: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    size = max(int(row["timestep"]) for row in rows) + 1
    cell_w = max(3, min(12, 960 // max(1, size)))
    cell_h = 12
    left = 86
    top = 28
    width = left + size * cell_w + 20
    height = top + nnodes * 2 * cell_h + 44
    by_key = {
        (int(row["node"]), int(row["timestep"])): row
        for row in rows
    }

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
        f'viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        '<style>text{font-family:Arial,sans-serif;font-size:10px;fill:#222}</style>',
        '<text x="8" y="16">alltoall peer-order occupancy</text>',
    ]

    for node in range(nnodes):
        for channel_idx, channel in enumerate(("intra", "inter")):
            y = top + (node * 2 + channel_idx) * cell_h
            parts.append(f'<text x="8" y="{y + 9}">n{node} {channel}</text>')
            for timestep in range(size):
                row = by_key[(node, timestep)]
                count = int(row[f"{channel}_count"])
                x = left + timestep * cell_w
                color = heat_color(count, ppn, channel)
                parts.append(
                    f'<rect x="{x}" y="{y}" width="{cell_w}" height="{cell_h}" '
                    f'fill="{color}"><title>t={timestep} node={node} '
                    f'{channel}={count}</title></rect>'
                )

    legend_y = height - 22
    parts.extend(
        [
            f'<text x="{left}" y="{legend_y}">blue: intra-node ops</text>',
            f'<text x="{left + 150}" y="{legend_y}">orange: inter-node ops</text>',
            "</svg>",
        ]
    )
    path.write_text("\n".join(parts), encoding="utf-8")


def parse_csv_list(value: str, valid: Iterable[str], label: str) -> List[str]:
    valid_set = set(valid)
    if value == "all":
        return list(valid)

    out = [item.strip().lower() for item in value.split(",") if item.strip()]
    bad = [item for item in out if item not in valid_set]
    if bad:
        raise argparse.ArgumentTypeError(
            f"unsupported {label}: {', '.join(bad)}; valid values: {', '.join(valid)}"
        )
    return out


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate alltoall peer-order occupancy CSV and SVG heatmaps."
    )
    parser.add_argument("--nnodes", type=int, default=4)
    parser.add_argument("--ppn", type=int, default=32)
    parser.add_argument(
        "--mappings",
        type=lambda value: parse_csv_list(value, MAPPINGS, "mapping"),
        default=list(MAPPINGS),
        help="Comma-separated mappings or 'all'. Valid: block,node.",
    )
    parser.add_argument(
        "--policies",
        type=lambda value: parse_csv_list(value, POLICIES, "policy"),
        default=list(POLICIES),
        help="Comma-separated policies or 'all'.",
    )
    parser.add_argument("--out-dir", type=Path, default=Path("striding_occupancy"))
    parser.add_argument("--no-heatmap", action="store_true")
    args = parser.parse_args()

    if args.nnodes <= 0 or args.ppn <= 0:
        parser.error("--nnodes and --ppn must be positive")

    generated: List[Path] = []
    for mapping in args.mappings:
        for policy in args.policies:
            rows = occupancy(args.nnodes, args.ppn, mapping, policy)
            stem = f"{mapping}_{policy.replace('+', '_')}"
            csv_path = args.out_dir / f"{stem}_occupancy.csv"
            write_csv(csv_path, rows)
            generated.append(csv_path)

            if not args.no_heatmap:
                svg_path = args.out_dir / f"{stem}_heatmap.svg"
                write_svg(svg_path, rows, args.nnodes, args.ppn)
                generated.append(svg_path)

    for path in generated:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
