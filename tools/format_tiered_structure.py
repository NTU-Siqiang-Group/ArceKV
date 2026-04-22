#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path


def split_top_level(text: str, delimiter: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(text):
        if ch == "[":
            depth += 1
        elif ch == "]":
            depth -= 1
        elif ch == delimiter and depth == 0:
            parts.append(text[start:i])
            start = i + 1
    parts.append(text[start:])
    return [part for part in parts if part]


def parse_levels(tree: str) -> list[tuple[str, str]]:
    levels: list[tuple[str, str]] = []
    i = 0
    while i < len(tree):
        if tree[i] != "L":
            raise ValueError(f"expected level at offset {i}")
        level_start = i
        i += 1
        while i < len(tree) and tree[i].isdigit():
            i += 1
        level_name = tree[level_start:i]
        if i >= len(tree) or tree[i] != ":":
            raise ValueError(f"expected ':' after {level_name}")
        i += 1
        if i >= len(tree) or tree[i] != "[":
            raise ValueError(f"expected '[' after {level_name}:")
        bracket_start = i
        depth = 0
        while i < len(tree):
            if tree[i] == "[":
                depth += 1
            elif tree[i] == "]":
                depth -= 1
                if depth == 0:
                    i += 1
                    break
            i += 1
        levels.append((level_name, tree[bracket_start:i]))
    return levels


def parse_runs(level_body: str) -> list[tuple[str, list[str]]]:
    if not (level_body.startswith("[") and level_body.endswith("]")):
        raise ValueError("invalid level body")
    inner = level_body[1:-1]
    if not inner:
      return []
    if inner.startswith("["):
        files = [entry.strip("[]") for entry in split_top_level(inner, ",")]
        return [("L0", files)]

    runs: list[tuple[str, list[str]]] = []
    for entry in split_top_level(inner, ","):
        run_id, files_blob = entry.split(":[", 1)
        files = [file_name.strip() for file_name in files_blob[:-1].split(",")
                 if file_name.strip()]
        runs.append((run_id, files))
    return runs


def format_structure(content: str) -> str:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    metadata: dict[str, str] = {}
    tree = ""
    for line in lines:
        if line.startswith("tree="):
            tree = line[len("tree="):]
        else:
            key, value = line.split("=", 1)
            metadata[key] = value

    output: list[str] = []
    output.append("Tiered Structure Summary")
    output.append(f"db_path: {metadata.get('db_path', '<unknown>')}")
    output.append(f"live_sst_files: {metadata.get('live_sst_files', '<unknown>')}")
    output.append(
        f"total_sorted_runs: {metadata.get('total_sorted_runs', '<unknown>')}")
    output.append("")

    for key in sorted(metadata):
        if key.startswith("L") and ": runs" in metadata[key]:
            continue

    for line in lines:
        if line.startswith("L") and ": runs=" in line:
            output.append(line)

    if tree:
        output.append("")
        output.append("Tree")
        for level_name, level_body in parse_levels(tree):
            runs = parse_runs(level_body)
            if runs and runs[0][0] == "L0":
                output.append(f"{level_name}:")
                for file_name in runs[0][1]:
                    output.append(f"  [{file_name}]")
                continue

            output.append(f"{level_name}:")
            for run_id, files in runs:
                output.append(f"  {run_id}:")
                for file_name in files:
                    output.append(f"    {file_name}")

    return "\n".join(output) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pretty-print a tiered compaction structure report")
    parser.add_argument("input", type=Path, help="Path to db_structure.txt")
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional output path. Defaults to stdout.",
    )
    args = parser.parse_args()

    formatted = format_structure(args.input.read_text())
    if args.output is None:
        sys.stdout.write(formatted)
    else:
        args.output.write_text(formatted)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
