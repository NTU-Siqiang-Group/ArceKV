## RocksDB: A Persistent Key-Value Store for Flash and RAM Storage

[![CircleCI Status](https://circleci.com/gh/facebook/rocksdb.svg?style=svg)](https://circleci.com/gh/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on [LevelDB](https://github.com/google/leveldb) by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key-value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it especially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/main/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Questions and discussions are welcome on the [RocksDB Developers Public](https://www.facebook.com/groups/rocksdb.dev/) Facebook group and [email list](https://groups.google.com/g/rocksdb) on Google Groups.

## Project-Specific Notes

This repository includes an extended `Arce` compaction mode and a new
DynamicCompaction-style policy on top of it.

The important distinction is:

- `kCompactionStyleArce` keeps the Tiering-like layout, where a logical sorted run may
  contain multiple SST files in one level.
- the new Arce dynamic policy ports the DynamicCompaction decision logic onto
  that Arce layout, including:
  - dynamic picking
  - parameter selection for `(M, c)`
  - Adaptive slowdown / stop control
  - background refresh of the dynamic parameters

Unlike the `prod` branch DynamicCompaction implementation, this version does
not require `one SST == one run`. It preserves the existing Arce multi-SST
sorted-run structure and applies the dynamic policy at the logical-run level.

The tree layout looks like this:

```text
L0: [sst_A] [sst_B] [sst_C]

L1: run_17 = [sst_D sst_E sst_F]
    run_16 = [sst_G sst_H]

L2: run_12 = [sst_I sst_J sst_K sst_L]
```

Interpretation:

- each bracket in `L0` is a standalone overlapping run
- in `L1+`, one logical run may contain multiple SSTs
- the dynamic picker reasons about logical runs, not individual SST files
- compaction iterators rebuild the run boundary from `sorted_run_id`

## Dependencies

- `cmake`
- `make`
- `gcc` / `g++`
- `gflags`
- OpenMP is optional but recommended for faster `(M, c)` grid search

## Build

```sh
mkdir -p build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j
```

The `make` and CMake build paths both try to enable OpenMP automatically when
the toolchain supports it.

## Enable Arce Dynamic Compaction

The public knob is still `kCompactionStyleArce`. The dynamic policy is enabled
through `arce_compaction_controller`.

```c++
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/arce_dynamic_compaction.h"

rocksdb::Options GetArceDynamicOptions() {
  rocksdb::Options opt;
  opt.create_if_missing = true;
  opt.compaction_style = rocksdb::kCompactionStyleArce;
  opt.num_levels = 4;

  auto controller =
      std::make_shared<rocksdb::ArceDynamicCompaction::ArceCompactionController>();
  controller->buffer_size = 64LL << 20;   // memtable / write buffer size
  controller->SetEntrySize(24 + 1000);    // key bytes + value bytes
  controller->SetWorkloadProportions(
      0.33, 0.33, 0.33);                  // range, update, point proportions

  opt.arce_compaction_controller = controller;
  return opt;
}
```

The most recommended usage is to keep a background
parameter refresher running. The integration harness shows this pattern:

```c++
auto controller =
    std::make_shared<rocksdb::ArceDynamicCompaction::ArceCompactionController>();
controller->SetBufferSize(write_buffer_size);
controller->SetEntrySize(key_size + value_size);
controller->SetWorkloadProportions(load_r, load_u, load_p);

rocksdb::Options opt;
opt.compaction_style = rocksdb::kCompactionStyleArce;
opt.arce_compaction_controller = controller;

// Open the DB first, then periodically refresh (M, c) from the live tree.
std::atomic<bool> stop_refresh{false};
std::thread refresh_thread([&] {
  while (!stop_refresh.load()) {
    // Rebuild the logical run tree from live files and recompute (M, c).
    RefreshMcFromState(db, controller.get());
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
});
```

Notes:

- `SetWorkloadProportions(r, u, p)` takes proportions, not already-normalized
  absolute weights.
- the controller internally normalizes the workload using
  `u = buffer_size / entry_size`, which matches the DynamicCompaction model.
- the background refresher is important if the workload mix changes over time;
  otherwise `(M, c)` remains fixed after initialization.
- if `arce_compaction_controller == nullptr`, RocksDB will create a default Arce
  controller automatically.
- the current implementation is intended for a single active compaction
  decision stream; using high background compaction parallelism with this policy
  is not the primary target.

For a runnable end-to-end example, see:

- [examples/arce_dynamic_compaction_integration.cc](/home/junfeng/agentkv/examples/arce_dynamic_compaction_integration.cc)

## Benchmark / Evaluation Harness

The repository includes a long-run integration and evaluation harness for both
Arce Dynamic and the default leveled baseline:

```sh
make -C examples arce_dynamic_compaction_integration
```

Example:

```sh
./examples/arce_dynamic_compaction_integration \
  --engine=arce \
  --db_path=/tmp/rocksdb_arce_dynamic \
  --total_bytes=$((40 * 1024 * 1024 * 1024)) \
  --correctness_ops=40000000 \
  --sample_interval_ms=1000 \
  --timeseries_output_path=report_assets/arce_timeseries.csv \
  --raw_stats_output_path=report_assets/arce_raw.stats \
  --structure_output_path=report_assets/arce_structure.txt
```

Use `--engine=level` to run the same harness against the default leveled
baseline.

The harness records:

- per-second time-series samples
- logical run count
- live SST count
- sampled point / range / insert latency
- raw `rocksdb.stats` / `rocksdb.cfstats`
- final tree / structure dump


## Improvement against Leveling

On the corrected `40 GiB` preload + `40,000,000` mixed-operation evaluation,
Arce Dynamic and default leveled compaction behaved differently depending on
what is measured.

Evaluation-phase latency:

| Metric | Arce | Leveled |
| --- | ---: | ---: |
| All-op average latency (us) | `15.96` | `17.56` |
| Point average latency (us) | `6.66` | `6.82` |
| Range average latency (us) | `38.77` | `43.71` |
| Insert average latency (us) | `2.45` | `2.15` |

Whole-run cost:

| Metric | Arce Dynamic | Leveled |
| --- | ---: | ---: |
| Total runtime (s) | `929.0` | `1310.5` |
| Compaction write (GB) | `426.09` | `275.90` |
| Compaction read (GB) | `373.00` | `222.81` |
| Write-stall delays | `2` | `1061` |


## License

RocksDB is dual-licensed under both the GPLv2 (found in the COPYING file in the root directory) and Apache 2.0 License (found in the LICENSE.Apache file in the root directory).  You may select, at your option, one of the above-listed licenses.
