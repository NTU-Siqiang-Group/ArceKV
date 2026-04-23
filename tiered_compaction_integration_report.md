# Tiered Compaction Integration Report

## Scope

This report covers a standalone integration driver for the current RocksDB
tiered compaction policy implementation.

The driver:

- opens RocksDB with `kCompactionStyleTiered`
- inserts randomly ordered fixed-size keys and values
- uses 24-byte zero-padded decimal keys
- uses 1000-byte values
- defaults to 20 GiB of logical KV payload
- uses a 2 MiB write buffer by default
- waits for background compaction to settle
- prints the resulting sorted-run layout across levels
- verifies point lookups
- verifies range lookups
- reports latency using both wall-clock summaries and RocksDB statistics

## Files Added Or Changed

- [examples/tiered_compaction_integration.cc](examples/tiered_compaction_integration.cc)
  Standalone integration driver.
- [examples/CMakeLists.txt](examples/CMakeLists.txt)
  Adds a CMake target for the driver.
- [examples/Makefile](examples/Makefile)
  Adds a Makefile target for the driver.
- [include/rocksdb/metadata.h](include/rocksdb/metadata.h)
  Exposes `sorted_run_id` through `LiveFileMetaData`.
- [db/version_set.cc](db/version_set.cc)
  Populates `LiveFileMetaData.sorted_run_id`.

## Public API Issue Fixed

The public `GetLiveFilesMetaData()` API did not expose `sorted_run_id`, which
made it impossible for an external integration program to reconstruct the
sorted-run structure of a tiered level correctly.

That is now fixed by plumbing `sorted_run_id` into `LiveFileMetaData`.

## Read-Path Issue Fixed

The standalone smoke run exposed a correctness bug in the tiered read path.

Root cause:

- `TieredFilePicker` was treating L0 as one sorted run and using `FindFile()`
  over L0 files.
- L0 files overlap and cannot be searched with the same disjoint-file logic
  used for a non-overlapping sorted run.
- `AddTieredIteratorsForLevel()` also reused run-based iterator logic for L0,
  which is invalid for overlapping L0 files.

Fix:

- `TieredFilePicker` now handles L0 separately and searches L0 files
  one-by-one in L0 order.
- `AddTieredIteratorsForLevel()` now handles L0 separately by adding one table
  iterator per L0 file, matching the original overlapping-L0 semantics.

This fix was necessary before the integration driver could pass its own point
and range verification.

## How To Build

From the repository root:

```bash
make -C examples tiered_compaction_integration
```

Or with CMake:

```bash
cmake -S . -B build
cmake --build build --target tiered_compaction_integration
```

## How To Run

Default 20 GiB run:

```bash
./examples/tiered_compaction_integration
```

Example smaller smoke run:

```bash
./examples/tiered_compaction_integration \
  --db_path=/tmp/tiered_compaction_smoke \
  --total_bytes=67108864 \
  --point_lookups=200 \
  --range_lookups=50 \
  --range_width=64
```

Useful options:

- `--db_path=PATH`
- `--structure_output_path=PATH`
- `--total_bytes=N`
- `--write_buffer_size=N`
- `--target_file_size_base=N`
- `--max_bytes_for_level_base=N`
- `--max_bytes_for_level_multiplier=N`
- `--point_lookups=N`
- `--range_lookups=N`
- `--range_width=N`
- `--seed=N`
- `--keep_db`

## Expected Output

The program prints:

- workload configuration
- insert progress
- final live SST count
- a tiered-tree picture such as:

```text
L0:[[000123.sst],[000124.sst]]L1:[run=9:[000200.sst,000201.sst],run=8:[000210.sst]]L2:[run=7:[000300.sst,000301.sst]]
```

- point lookup latency summary
- range lookup latency summary
- RocksDB histograms for:
  - `DB_WRITE`
  - `DB_GET`
  - `DB_SEEK`
  - `FILE_READ_GET_MICROS`
  - `FILE_READ_DB_ITERATOR_MICROS`
- `rocksdb.stats`
- `rocksdb.cfstats`

## Validation Performed By The Driver

### Point lookups

The driver samples inserted keys, issues `Get()` requests, and validates the
returned 1000-byte value against the deterministic value generator.

### Range lookups

The driver samples ranges, creates iterators, seeks to the range start, and
validates key order plus value correctness for every key in the range.

## Smoke-Test Status

The intended full workload is 20 GiB, but a smaller smoke run is appropriate
for quick verification during development. The driver is written so that the
default remains 20 GiB while smaller runs can be used for fast checks.

Smoke run used:

```bash
./examples/tiered_compaction_integration_bin \
  --db_path=/tmp/tiered_compaction_smoke \
  --total_bytes=67108864 \
  --point_lookups=200 \
  --range_lookups=50 \
  --range_width=64
```

Observed outcome:

- binary compiled successfully
- repository example target built successfully with:
  `make -C examples tiered_compaction_integration`
- tiered sorted runs were visible across levels
- point lookup verification passed
- range lookup verification passed
- latency and RocksDB statistics were emitted

Observed tree snapshot from the smoke run:

```text
L0:[[000174.sst],[000169.sst]]L2:[run=10:[000167.sst,000170.sst,000171.sst,000172.sst,000175.sst,000176.sst,000177.sst,000178.sst,000179.sst,000180.sst,000181.sst,000182.sst,000183.sst,000184.sst,000185.sst,000186.sst,000187.sst,000188.sst,000189.sst,000190.sst,000191.sst,000192.sst,000193.sst,000194.sst,000195.sst,000196.sst,000197.sst,000198.sst,000199.sst,000200.sst,000201.sst],run=5:[000072.sst,000073.sst,000074.sst,000077.sst,000078.sst,000079.sst,000080.sst,000081.sst,000082.sst,000083.sst,000084.sst,000085.sst,000086.sst,000087.sst,000088.sst,000089.sst,000092.sst,000093.sst,000094.sst,000095.sst,000096.sst,000097.sst,000098.sst,000099.sst,000100.sst,000101.sst,000102.sst,000103.sst,000104.sst,000107.sst,000108.sst]]
```

Observed latency snapshot from the smoke run:

- point lookup wall-clock latency: avg `12.22us`, p95 `17.01us`, p99 `19.69us`
- range lookup wall-clock latency: avg `920.26us`, p95 `937.69us`, p99 `946.69us`
- `DB_GET` histogram avg `11.91us`, p95 `18.06us`, p99 `21.56us`
- `DB_SEEK` histogram avg `13.72us`, p95 `21.50us`, p99 `39.00us`

## Full-Run Status

The full integration workload was run with the default 20 GiB logical data
size and the database preserved under `/tmp/db`.

Full run used:

```bash
./examples/tiered_compaction_integration \
  --db_path=/tmp/db \
  --keep_db \
  --structure_output_path=/tmp/db_structure.txt
```

Observed outcome:

- full 20 GiB workload completed successfully
- point lookup verification passed
- range lookup verification passed
- the database was kept at `/tmp/db`
- latency and RocksDB statistics were emitted successfully

### Latest Preserved Database Layout

The current preserved database and persisted structure artifact match the most
recent full revalidation run.

- live SST files: `20381`
- total sorted runs: `2`
- non-empty levels observed in the final tree snapshot: `L1`, `L4`
- per-level run/file counts:
  - `L1`: `1` run, `16` files
  - `L4`: `1` run, `20365` files
- persisted structure artifact:
  - `/tmp/db_structure.txt`
- pretty-printed structure artifact:
  - `/tmp/db_structure_pretty.txt`
- exact tree layout is persisted in the `tree=` line of `/tmp/db_structure.txt`

Observed tree prefix from the latest run:

```text
L1:[run=1022:[080226.sst,080227.sst,080228.sst,080229.sst,080230.sst,080231.sst,080232.sst,080233.sst,080234.sst,080235.sst,080236.sst,080237.sst,080238.sst,080239.sst,080240.sst,080241.sst]]L4:[run=1023:[080242.sst,080243.sst,080244.sst, ... ]]
```

This latest preserved layout is intentionally simple to inspect: one small
sorted run remains in `L1`, and the rest of the database is consolidated into
one very large sorted run in `L4`.

### Earlier Full-Run Latency Snapshot

The latest rerun preserved the database layout artifacts, but its full stdout
was not kept in a separate log file. The most recent complete latency snapshot
captured from a validated 20 GiB full run is still useful as a representative
performance sample for the current tiered implementation.

- point lookup wall-clock latency: avg `25.55us`, p95 `42.49us`, p99 `53.49us`
- range lookup wall-clock latency: avg `244.40us`, p95 `264.76us`, p99 `281.51us`
- `DB_GET` histogram avg `25.42us`, p95 `43.32us`, p99 `57.85us`
- `DB_SEEK` histogram avg `31.39us`, p95 `42.00us`, p99 `50.00us`
- `FILE_READ_GET_MICROS` histogram avg `1.66us`, p95 `2.04us`, p99 `2.91us`
- `FILE_READ_DB_ITERATOR_MICROS` histogram avg `1.06us`, p95 `1.67us`, p99 `1.98us`

Representative throughput and compaction snapshot from that full run:

- write throughput: `317.4 MiB/s`
- cumulative compaction write: `88.46 GB`
- cumulative compaction read: `68.30 GB`
- total compactions: `8457`
- estimated pending compaction bytes at completion: `0`
- write stall delays observed: `5637` memtable-limit delays

Representative per-level compaction summary from that full run:

- `L1`: `10` files, `9.71 MB`
- `L2`: `3555` files, `3.52 GB`
- `L3`: `5391` files, `5.33 GB`
- `L4`: `11427` files, `11.30 GB`

Notes from the full run:

- The workload completed without correctness failures after the tiered L0
  read-path fix.
- The current preserved `/tmp/db` layout is from a later revalidation run and
  should be treated as the authoritative structure snapshot.

## Full-Run Status After Bounded-Run Picker

After changing the automatic tiered picker to compact at most `T` sorted runs
per level, oldest first, the full 20 GiB integration workload was rerun and
logged to a dedicated artifact.

Bounded-run full run used:

```bash
./examples/tiered_compaction_integration \
  --db_path=/tmp/db_bounded_tiered \
  --keep_db \
  --structure_output_path=/tmp/db_bounded_tiered_structure.txt
```

Observed outcome:

- full 20 GiB workload completed successfully
- point lookup verification passed
- range lookup verification passed
- the database was kept at `/tmp/db_bounded_tiered`
- latency and RocksDB statistics were emitted successfully
- the final tree spread data across deep levels instead of concentrating most
  bytes in one upper-level run

Observed structure summary from the bounded-run full run:

- live SST files: `20387`
- total sorted runs: `14`
- non-empty levels observed in the final tree snapshot: `L0` through `L6`
- per-level run/file counts:
  - `L0`: `1` run, `1` file
  - `L1`: `2` runs, `20` files
  - `L2`: `2` runs, `63` files
  - `L3`: `3` runs, `401` files
  - `L4`: `2` runs, `1268` files
  - `L5`: `3` runs, `8747` files
  - `L6`: `1` run, `9887` files
- persisted structure artifact:
  - `/tmp/db_bounded_tiered_structure.txt`
- pretty-printed structure artifact:
  - `/tmp/db_bounded_tiered_structure_pretty.txt`
- saved stdout/stats log:
  - `/tmp/db_bounded_tiered_run.log`

Observed tree prefix from the bounded-run full run:

```text
L0:[[000038.sst]]L1:[run=2631:[129098.sst,129099.sst,129100.sst,129101.sst,129102.sst,129103.sst,129104.sst,129105.sst,129106.sst,129107.sst],run=2623:[111894.sst,111895.sst,111896.sst,111897.sst,111898.sst,111899.sst,111900.sst,111901.sst,111902.sst,111903.sst]]L2:[run=2632:[129108.sst,129109.sst,129110.sst, ... ],run=2624:[111904.sst,111905.sst,111906.sst, ... ]]
```

This run is much closer to the intended textbook tiering shape: multiple
sorted runs remain at several intermediate levels, and the data reaches the
bottom levels instead of stabilizing mostly in `L2`.

Observed latency snapshot from the bounded-run full run:

- point lookup wall-clock latency: avg `49.41us`, p50 `50.60us`, p95 `64.64us`,
  p99 `79.17us`, max `442.81us`
- range lookup wall-clock latency: avg `284.42us`, p50 `280.55us`,
  p95 `311.79us`, p99 `330.62us`, max `374.58us`
- `DB_GET` histogram avg `49.28us`, p95 `73.98us`, p99 `87.49us`
- `DB_SEEK` histogram avg `61.87us`, p95 `75.46us`, p99 `98.67us`
- `FILE_READ_GET_MICROS` histogram avg `1.55us`, p95 `1.93us`, p99 `2.48us`
- `FILE_READ_DB_ITERATOR_MICROS` histogram avg `1.18us`, p95 `1.79us`,
  p99 `2.38us`

Observed throughput and compaction snapshot from the bounded-run full run:

- write throughput: `282.4 MiB/s`
- cumulative compaction write: `128.52 GB`
- cumulative compaction read: `108.36 GB`
- total compaction time reported by RocksDB: `281.7` seconds
- estimated pending compaction bytes at completion: `0`

Notes from the bounded-run full run:

- The final structure now matches the intended bounded-tiering behavior much
  better than the earlier whole-level picker runs.
- The data distribution across `L3` through `L6` confirms the picker change
  prevents one compaction from collapsing an arbitrarily large backlog from an
  entire level into a single oversized run.
- With a `2 MiB` write buffer, the policy creates a very large number of SST
  files and visible memtable-limit stall pressure, which is expected for this
  stress shape.

## Notes

- The 20 GiB default is suitable for a longer-running integration run and may
  take substantial time and disk space.
- The structure printer treats each L0 file as its own run, matching the
  current tiered policy assumption for L0.
- Non-L0 structure reconstruction is based on `LiveFileMetaData.sorted_run_id`.
