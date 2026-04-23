# Tiered Compaction Implementation Report

## Scope

This document describes the current implementation of the academic tiering
policy added to this RocksDB tree. It focuses on the logical data structure,
read-path correctness, compaction behavior, and recovery after reopen.

The current policy has these defining rules:

- each non-L0 level may contain multiple sorted runs
- each sorted run still preserves the leveled invariant internally:
  files in one run are non-overlapping and sorted by key
- different runs in the same level may overlap in key range
- a compaction is triggered when the number of sorted runs in a level reaches
  or exceeds `T`
- `T` is taken from `max_bytes_for_level_multiplier`
- automatic tiered compaction now compacts at most `T` runs from a level,
  oldest first, to better match textbook tiering
- the selected runs are merged into one new sorted run in the next level

The main implementation points are in:

- [db/version_set.h](db/version_set.h)
- [db/version_set.cc](db/version_set.cc)
- [db/compaction/compaction_picker_tiered.h](db/compaction/compaction_picker_tiered.h)
- [db/compaction/compaction_picker_tiered.cc](db/compaction/compaction_picker_tiered.cc)
- [db/compaction/compaction.cc](db/compaction/compaction.cc)
- [db/compaction/compaction_job.cc](db/compaction/compaction_job.cc)
- [db/db_impl/db_impl_compaction_flush.cc](db/db_impl/db_impl_compaction_flush.cc)
- [db/version_edit.h](db/version_edit.h)
- [db/version_edit.cc](db/version_edit.cc)

## 1. Tree Structure

### 1.1 High-level layout

Under the original leveled model, a non-L0 level is one sorted run. Under this
tiered model, a non-L0 level is a sequence of sorted runs:

```text
L0: [f9] [f8] [f7] ...
L1: [run=31: f1 f2 f3] [run=27: g1 g2]
L2: [run=40: h1 h2 h3 h4] [run=35: i1 i2]
```

Properties:

- `L0` remains special and naturally tiered: each file is effectively its own
  run
- in `L1+`, each run contains multiple SSTs that are key-sorted and
  non-overlapping within that run
- two runs in the same level may overlap each other in key range
- runs are ordered by recency, newest first

That last property is important because it lets the read path search newer runs
before older runs and preserve MVCC visibility rules without changing RocksDB's
fundamental sequence-number ordering semantics.

### 1.2 How the structure is materialized

The base file metadata is still stored per file in `VersionStorageInfo`. The
tiered structure is materialized as a derived view over the files in a level.

The main data types are:

- `LevelSortedRunBrief` in [db/version_set.h](db/version_set.h)
- `LevelSortedRunsBrief` in [db/version_set.h](db/version_set.h)
- `sorted_run_id` added to `FileMetaData` and persisted in version edits

The derived run layout is built in
[db/version_set.cc](db/version_set.cc:3653) by
`VersionStorageInfo::GenerateLevelSortedRunsBrief()`.

That function:

- groups files in a level by `sorted_run_id`
- sorts files inside each run by smallest key
- sorts runs by `sorted_run_id` descending

Descending `sorted_run_id` means newer runs come first. That is the mechanism
used to make run order represent recency.

### 1.3 What identifies a run

The identity of a run is `sorted_run_id`.

This value is:

- stored per file in `FileMetaData`
- written into manifest edits through `VersionEdit`
- exposed through `LiveFileMetaData` so external tools can reconstruct the tree

Relevant code:

- manifest field definition:
  [db/version_edit.h](db/version_edit.h)
- manifest encode/decode:
  [db/version_edit.cc](db/version_edit.cc:347)
- public metadata exposure:
  [include/rocksdb/metadata.h](include/rocksdb/metadata.h:171)
- live metadata population:
  [db/version_set.cc](db/version_set.cc:8349)

## 2. Consistent Reads

The critical read-path requirement is:

- if a key appears in multiple runs in the same level, the newest visible
  version must win

This must hold for both point lookups and iterators.

### 2.1 Read-path design

The tiered implementation is intentionally isolated from the leveled read path
using explicit branching rather than rewriting the shared path globally.

The main branch points are in
[db/version_set.cc](db/version_set.cc):

- point lookup selection via `TieredFilePicker`
- iterator construction via `AddTieredIteratorsForLevel()`

This keeps the original leveled logic intact when
`compaction_style != kCompactionStyleTiered`.

### 2.2 Point lookup path

For point lookups, the tiered path is implemented by `TieredFilePicker` in
[db/version_set.cc](db/version_set.cc:355).

The logic is:

1. walk levels from newer to older, same as RocksDB normally does
2. inside each non-L0 level, walk the runs in recency order
3. for each run, use the existing non-overlapping-file logic to locate the
   candidate file inside that run
4. if the key is found in a newer run, older runs and lower levels no longer
   matter for that key

This is the important distinction from leveled lookup:

- leveled lookup assumes one globally non-overlapping run for a level
- tiered lookup cannot binary-search the whole level because different runs may
  overlap
- instead, it searches one run at a time

Inside a run, the invariant still holds, so binary search over files remains
valid.

L0 is handled separately. `L0` cannot reuse the run search logic because files
overlap and are ordered by recency rather than disjoint key ranges. The L0 path
therefore checks L0 files one by one in L0 order.

### 2.3 Why timestamp and sequence-number order are preserved

The tiered read path does not change RocksDB's core MVCC semantics. It changes
only how candidate files are discovered.

Correctness comes from combining:

- RocksDB's normal level ordering
- run recency ordering within a level
- internal-key ordering and sequence-number visibility inside SST readers

When overlapping runs exist in one level:

- a newer run is searched before an older run
- if both contain the same user key, the newer run's entry is found first
- sequence/timestamp resolution inside the file still uses the normal
  comparator logic

The tiered tests that validate this include:

- [db/version_set_test.cc](db/version_set_test.cc:1858)
- [db/version_set_test.cc](db/version_set_test.cc:1963)

### 2.4 Iterator design

Range lookup and full iteration cannot assume one run per level either.

The tiered iterator path is:

- `Version::AddTieredIteratorsForLevel()` for user iteration:
  [db/version_set.cc](db/version_set.cc:2558)
- `VersionSet::MakeInputIterator()` for compaction input iteration:
  [db/version_set.cc](db/version_set.cc:8160)

For user iterators:

- `L0` adds one table iterator per file, matching standard overlapping-L0
  semantics
- each non-L0 sorted run contributes one run iterator
- a run iterator is implemented by reusing `LevelIterator` over that run's
  `LevelFilesBrief`
- the outer merge iterator merges all run iterators together

This works because:

- files inside one run are still disjoint, so `LevelIterator` is valid there
- runs are merged the same way different levels or overlapping sources are
  merged elsewhere in RocksDB
- duplicate user keys across runs are resolved by normal internal-key ordering

For compaction iterators:

- the same isolation principle is used
- when tiered compaction selects only a subset of runs, the compaction input
  iterator groups selected files by `sorted_run_id`
- each selected run is iterated independently
- the merge iterator then produces one globally sorted stream for compaction

This was an important follow-up fix after switching the picker from
whole-level compaction to bounded `T`-run compaction. Without this change,
compaction would still have read the entire source level even if the picker
selected only some runs.

Tiered iterator tests include:

- [db/version_set_test.cc](db/version_set_test.cc:1893)
- [db/version_set_test.cc](db/version_set_test.cc:1918)

## 3. Compaction

### 3.1 Trigger condition and scoring

Tiered compaction scoring is implemented in
[db/version_set.cc](db/version_set.cc:4073) inside
`VersionStorageInfo::ComputeCompactionScore()`.

For tiered style, score is based on run count, not bytes:

- `run_limit = max_bytes_for_level_multiplier`
- `num_runs = NumTieredRunsForCompaction(level)`
- if `num_runs >= run_limit`, score is `num_runs / run_limit`
- otherwise score is `0`

That means:

- `T` is interpreted as maximum allowed sorted runs per level
- compaction is triggered as soon as run count reaches `T`
- levels with higher run-count pressure rank higher

`NumTieredRunsForCompaction()` is in
[db/version_set.cc](db/version_set.cc:3692).

Behavior:

- for `L0`, file count is used as run count
- for `L1+`, number of distinct runs is used
- if a file in that level is already in compaction, the level is treated as
  unavailable for new tiered picking

### 3.2 Picking inputs

Automatic tiered picking is implemented in
[db/compaction/compaction_picker_tiered.cc](db/compaction/compaction_picker_tiered.cc:171).

Current automatic policy:

- choose the highest-score eligible level
- select at most `T` runs from that level
- choose the oldest runs first
- compact those selected runs into one new run at `level + 1`

This bounded-run selection is done by
`SelectTieredCompactionInputs()` in the same file.

Selection details:

- for `L0`, select at most `T` oldest files
- for `L1+`, `GenerateLevelSortedRunsBrief()` keeps runs in newest-first order,
  so the picker takes runs from the back to get oldest-first behavior
- all files belonging to those selected runs become the compaction input

This is deliberately closer to textbook tiering than the earlier whole-level
merge behavior, which could collapse an arbitrarily large backlog from a level
into one oversized run.

Manual compaction is separate:

- `PickCompactionForCompactRange()` in
  [db/compaction/compaction_picker_tiered.cc](db/compaction/compaction_picker_tiered.cc:108)
- for tiered manual compaction, if the requested range overlaps a level, the
  implementation picks the whole level

That manual path is intentionally more conservative and simpler than the
automatic bounded-run policy.

### 3.3 Building the new run

Once a compaction is picked:

- the selected input files are wrapped into `CompactionInputFiles`
- the compaction output level is `input_level + 1`
- the merge iterator reads the selected runs and produces one sorted output
  stream
- all output SSTs generated by that compaction are tagged with one fresh
  `output_sorted_run_id`

The run ID assignment is created in
[db/compaction/compaction.cc](db/compaction/compaction.cc:68)
inside `Compaction::FinalizeInputInfo()`.

For tiered compaction:

- if this is an inter-level tiered compaction
- `output_sorted_run_id_ = version_set()->NewSortedRunId()`

Then, during output file installation, every file produced by that compaction
inherits that same run ID in
[db/compaction/compaction_job.cc](db/compaction/compaction_job.cc:2500).

That is how multiple output SSTs from one compaction become one new sorted run.

### 3.4 Why the new run is recognized as one run

Recognition is entirely metadata-based:

- every output file from the compaction gets the same `sorted_run_id`
- on the next version build, files in a level are regrouped by `sorted_run_id`
- those files are sorted by smallest key inside the run
- the regrouped result becomes one `LevelSortedRunBrief`

So "being a run" is not a separate persistent object. It is reconstructed from
per-file run IDs.

### 3.5 Boundary handling

One subtle issue is that shared compaction helpers in RocksDB often assume a
non-L0 level is one globally ordered file chain. That is not true for tiered
levels with multiple runs.

To avoid corrupt assumptions, boundary helpers in
[db/compaction/compaction.cc](db/compaction/compaction.cc:81)
were updated so that for tiered compactions they scan every input file when
computing smallest/largest boundaries, rather than relying on just the first
and last file of the level.

## 4. Recovery After Reopen

### 4.1 What is persisted

The persistent source of truth is the MANIFEST.

For each new SST file, the manifest entry now includes:

- file metadata
- level placement
- `sorted_run_id`

This is handled by:

- encode in [db/version_edit.cc](db/version_edit.cc:347)
- decode in [db/version_edit.cc](db/version_edit.cc:504)

### 4.2 How reopen reconstructs runs

On reopen:

1. RocksDB replays manifest edits into file metadata
2. each file recovers its `sorted_run_id`
3. `VersionStorageInfo::GenerateLevelSortedRunsBrief()` groups files by run ID
4. each level's run structure is rebuilt in memory

No separate run catalog is needed.

This is one of the main reasons the manifest change is central to the design:

- without persisting `sorted_run_id`, reopen would only know per-file level
  placement
- the system would lose the run partitioning inside a tiered level

### 4.3 Recovery of future run ID allocation

The system must also avoid reusing run IDs after reopen.

That is handled in
[db/version_set.cc](db/version_set.cc:7158).

During recovery:

- scan all recovered files
- find `max_sorted_run_id`
- set `next_sorted_run_id_ = max_sorted_run_id + 1`

Fresh run IDs are then allocated by:

- `VersionSet::NewSortedRunId()` in
  [db/version_set.h](db/version_set.h:1459)

This guarantees that every future compaction-generated run gets a distinct
monotonically increasing ID.

### 4.4 External observability after reopen

The public metadata surface was also updated so that tools can reconstruct the
same run layout after reopen using `GetLiveFilesMetaData()`.

That is why `LiveFileMetaData` now exposes `sorted_run_id` in
[include/rocksdb/metadata.h](include/rocksdb/metadata.h:171).

The standalone integration tool uses exactly that API to reconstruct and print
the tiered tree.

## Summary

The current tiered implementation is built around one persistent concept:
`sorted_run_id`.

Everything else follows from that:

- tree structure:
  files in a level are grouped by run ID
- consistent reads:
  search newer runs before older runs, while preserving file-level and
  internal-key ordering
- iterators:
  one iterator per run, merged at the outer layer
- compaction:
  trigger on run count, compact selected runs into one new run with one fresh
  output run ID
- recovery:
  persist run IDs in the manifest, then rebuild runs from them on reopen

With the bounded automatic picker now compacting at most `T` oldest runs, the
runtime behavior is much closer to textbook tiering than the earlier
whole-level merge approach.
