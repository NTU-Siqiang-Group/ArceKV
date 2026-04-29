#!/usr/bin/env python3

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


def read_rows(path):
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    out = []
    for row in rows:
        if row["phase"] != "correctness":
            continue
        point_ops = int(row["point_ops"])
        range_ops = int(row["range_ops"])
        insert_ops = int(row["insert_ops"])
        total_ops = point_ops + range_ops + insert_ops
        weighted_avg = 0.0
        if total_ops:
            weighted_avg = (
                float(row["point_avg_micros"]) * point_ops
                + float(row["range_avg_micros"]) * range_ops
                + float(row["insert_avg_micros"]) * insert_ops
            ) / total_ops
        out.append(
            {
                "t": float(row["elapsed_seconds"]),
                "logical_runs": float(row["logical_runs"]),
                "live_sst_files": float(row["live_sst_files"]),
                "point": float(row["point_avg_micros"]),
                "range": float(row["range_avg_micros"]),
                "insert": float(row["insert_avg_micros"]),
                "weighted": weighted_avg,
                "M": float(row["M"]),
                "c": float(row["c"]),
            }
        )
    if out and out[0]["t"] != 0.0:
        t0 = out[0]["t"]
        for row in out:
            row["t"] -= t0
    return out


def plot_two_series(arce_rows, level_rows, key, ylabel, title, out_path, logy=False):
    plt.figure(figsize=(10, 4.8))
    plt.plot([r["t"] for r in arce_rows], [r[key] for r in arce_rows], label="Arce Dynamic", linewidth=1.6)
    plt.plot([r["t"] for r in level_rows], [r[key] for r in level_rows], label="Leveled", linewidth=1.6)
    if logy:
        plt.yscale("log")
    plt.xlabel("Correctness Phase Time (s)")
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()


def plot_latency_panel(arce_rows, level_rows, out_path):
    fig, axes = plt.subplots(4, 1, figsize=(10, 11.5), sharex=True)
    series = [
        ("weighted", "All Ops Avg (us)"),
        ("point", "Point Avg (us)"),
        ("range", "Range Avg (us)"),
        ("insert", "Insert Avg (us)"),
    ]
    for ax, (key, label) in zip(axes, series):
        ax.plot([r["t"] for r in arce_rows], [r[key] for r in arce_rows], label="Arce Dynamic", linewidth=1.4)
        ax.plot([r["t"] for r in level_rows], [r[key] for r in level_rows], label="Leveled", linewidth=1.4)
        ax.set_ylabel(label)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper right")
    axes[-1].set_xlabel("Correctness Phase Time (s)")
    fig.suptitle("Evaluation-Phase Latency")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def plot_arce_mc(arce_rows, out_path):
    fig, axes = plt.subplots(2, 1, figsize=(10, 5.8), sharex=True)
    axes[0].plot([r["t"] for r in arce_rows], [r["M"] for r in arce_rows], color="tab:blue", linewidth=1.5)
    axes[0].set_ylabel("M")
    axes[0].grid(True, alpha=0.3)
    axes[1].plot([r["t"] for r in arce_rows], [r["c"] for r in arce_rows], color="tab:orange", linewidth=1.5)
    axes[1].set_ylabel("c")
    axes[1].set_xlabel("Correctness Phase Time (s)")
    axes[1].grid(True, alpha=0.3)
    fig.suptitle("Arce Dynamic Controller During Evaluation")
    fig.tight_layout()
    fig.savefig(out_path)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--arce_csv", required=True)
    parser.add_argument("--level_csv", required=True)
    parser.add_argument("--out_dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    arce_rows = read_rows(args.arce_csv)
    level_rows = read_rows(args.level_csv)
    if not arce_rows or not level_rows:
        raise SystemExit("missing correctness-phase rows in one of the CSV files")

    plot_two_series(
        arce_rows,
        level_rows,
        "logical_runs",
        "Logical Sorted Runs",
        "Evaluation-Phase Logical Sorted Runs",
        out_dir / "arce_vs_level_eval_logical_runs.svg",
    )
    plot_two_series(
        arce_rows,
        level_rows,
        "live_sst_files",
        "Live SST Files",
        "Evaluation-Phase Live SST Files",
        out_dir / "arce_vs_level_eval_live_files.svg",
    )
    plot_two_series(
        arce_rows,
        level_rows,
        "weighted",
        "Average Latency (us)",
        "Evaluation-Phase Average Latency Across All Operations",
        out_dir / "arce_vs_level_eval_all_ops_latency.svg",
    )
    plot_latency_panel(
        arce_rows,
        level_rows,
        out_dir / "arce_vs_level_eval_latency_panel.svg",
    )
    plot_arce_mc(arce_rows, out_dir / "arce_eval_mc.svg")


if __name__ == "__main__":
    main()
