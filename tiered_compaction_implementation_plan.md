# Tiered Compaction Implementation Plan

## Feasible Plan

I would implement this as a new compaction style, `kCompactionStyleTiered`, not by mutating existing leveled compaction behavior in place. That keeps current `kCompactionStyleLevel` semantics intact and gives us a clean place to encode "multiple sorted runs per level" end to end.

Two design constraints should guide the implementation:

1. Tiered-specific logic should be isolated behind explicit `if (compaction_style_ == kCompactionStyleTiered)` or equivalent dispatch, so the existing leveled read and compaction paths stay unchanged.
2. Sorted-run identity must be manifest-persisted in a way that survives restart and version replay without changing the meaning of old manifests.

1. Add a first-class tiered compaction mode and persist sorted-run identity.
   - Extend `CompactionStyle` and option parsing/printing in `include/rocksdb/advanced_options.h`, `options/cf_options.cc`, `options/options_helper.cc`, and validation in `db/column_family.cc`.
   - Add manifest-persisted sorted-run metadata in `db/version_edit.h`, `db/version_edit.cc`, `db/version_builder.cc`, and related edit handlers.
   - Critical design choice: every added SST must carry a `sorted_run_id`. Flush output files share one run id; one compaction job's output files share one run id. Without this, overlaps across runs cannot be reconstructed after reopen.
   - Reasonable manifest solution:
     - Extend the per-file metadata carried by `VersionEdit::AddFile` to include an optional `sorted_run_id`.
     - Default old manifests to `sorted_run_id = 0`, which means "legacy single-run behavior".
     - For tiered CFs, allocate monotonically increasing run ids from a `VersionSet` counter, similar to file numbers.
     - Persist the run id only on file-add records. File-delete records remain file-number based, so no extra delete-side manifest format is needed.
     - Store the next run-id allocator state in manifest durable state, or recover it conservatively as `max(sorted_run_id seen) + 1` during replay.
   - This keeps backward compatibility manageable: old manifests replay as one-run-per-level, while new manifests can represent multiple runs per level precisely.

2. Extend version metadata from "files per level" to "runs per level".
   - Keep `files_[level]` as the base storage to minimize churn, but add derived per-level run grouping in `VersionStorageInfo` in `db/version_set.h` and `db/version_set.cc`.
   - Generate something like `LevelSortedRunBrief` from `files_[level]`, grouping contiguous files by `sorted_run_id`.
   - Preserve run ordering by recency within each level. Reads must search newer runs before older runs.
   - Keep the invariant: files inside a run are non-overlapping and sorted; runs within a level may overlap.
   - Isolation rule: do not replace existing `LevelFilesBrief` semantics for leveled compaction. Add a new tiered-only view, e.g. `LevelSortedRunsBrief`, and build/use it only when the CF is tiered.

3. Change overlap/search helpers to become run-aware.
   - Today `FindFile`, `SomeFileOverlapsRange`, `GetOverlappingInputs*`, `OverlapInLevel`, `ApproximateSize`, and `RangeMightExistAfterSortedRun` assume level `> 0` is one disjoint run in `db/version_set.cc`.
   - Do not rewrite the existing helpers in-place unless the helper is already naturally style-agnostic.
   - Prefer this structure:
     - keep current leveled helper unchanged
     - add a tiered-specific helper beside it
     - dispatch at the call site based on compaction style
   - Tiered helper behavior:
     - iterate runs in a level in newest-to-oldest order
     - binary search within each run
     - union matching files across all runs in that level
   - This is the core correctness change for both point and range reads, and the explicit branch reduces regression risk for existing leveled users.

4. Update point lookup and MultiGet read paths.
   - Refactor `FilePicker` and `FilePickerMultiGet` in `db/version_set.cc`.
   - Current behavior for levels `>= 1` is "one binary search, then maybe scan neighbors." Tiered behavior must be "for each run in this level: binary search that run, check overlaps, then continue to next run."
   - Keep L0 logic as-is conceptually.
   - Isolation rule:
     - leave the current `FilePicker` and `FilePickerMultiGet` path untouched for `kCompactionStyleLevel`
     - add parallel tiered-aware picker implementations, or at minimum tiered-specific branches inside these classes that bypass the original level `> 0` logic entirely
   - The tiered path should not depend on `FileIndexer` assumptions that only hold for one sorted run per level.

5. Update iterator/range-scan construction.
   - This area needs explicit treatment because iterators are used both by foreground scans and by compaction execution.
   - Current assumption:
     - L0 uses many file iterators merged together
     - each level `>= 1` uses one concatenating iterator because files are non-overlapping
   - Tiered read iterator plan in `db/version_set.cc`:
     - keep the current leveled path unchanged
     - for a tiered level, build one concatenating iterator per sorted run
     - merge the run iterators for that level in recency order
     - then merge that level's iterator with other levels as usual
   - Tiered compaction input iterator plan in `db/compaction/compaction.cc`:
     - when compaction inputs come from a tiered level, do not treat the whole level as one concatenating iterator unless all selected files are from one run
     - instead, group compaction inputs by `sorted_run_id`, build one concatenating iterator per input run, then feed those iterators into the existing merging layer
   - This preserves the efficient "concat within a run, merge across runs" structure for both compaction and user range scans.

6. Implement a dedicated `TieredCompactionPicker`.
   - Add `db/compaction/compaction_picker_tiered.h/.cc`, wired from the existing picker factory path.
   - Trigger policy:
     - define per-level sorted-run count
     - compute score as `num_sorted_runs(level) / max_bytes_for_level_multiplier`
     - compact when score `>= 1`
   - Picking policy:
     - choose a level whose run count exceeds the threshold
     - compact all runs in that level together
     - output a new single sorted run to the next level
   - L0 follows the same rule, which matches your requirement.

7. Keep compaction execution mostly unchanged, but set output run ids correctly.
   - Compaction merge logic can largely stay the same because merging many input SSTs into sorted output SSTs is already supported.
   - The required execution change is to stamp all output files from one tiered compaction with the same new `sorted_run_id`.
   - Disable or bypass leveled-only optimizations that rely on one-run-per-level assumptions, especially trivial move paths and partial-file picking in `db/compaction/compaction_picker_level.cc` and `db/compaction/compaction.cc`.
   - Isolation rule: tiered compaction should have its own picker and its own pick-to-execute flow. Avoid making `LevelCompactionPicker` understand tiered semantics.

8. Audit non-read callers that assume non-overlap for levels `> 0`.
   - Important hotspots are `db/version_set.cc`, `db/db_impl/db_impl_compaction_flush.cc`, and `db/db_impl/compacted_db_impl.cc`.
   - Some can stay conservative in tiered mode by falling back from binary search to per-run search.
   - Bottommost-file detection and "key does not exist beyond output level" helpers also need review, since they currently rely on per-level non-overlap.
   - Where practical, prefer a tiered-only slow-but-safe path over trying to generalize an optimized leveled-only helper.

9. Define the first implementation boundary clearly.
   - Include: automatic compaction, reads, reopen/recovery, iterators, MultiGet, and manifest persistence.
   - Defer if needed: advanced manual-compaction heuristics, specialized compaction priorities, and tiered-specific performance tuning.
   - If we defer anything, it should degrade conservatively, not return wrong results.

## Files Likely Touched

`include/rocksdb/advanced_options.h`, `options/cf_options.cc`, `options/options_helper.cc`, `db/column_family.cc`, `db/version_edit.h`, `db/version_edit.cc`, `db/version_builder.cc`, `db/version_set.h`, `db/version_set.cc`, `db/compaction/compaction_picker.h`, new `db/compaction/compaction_picker_tiered.*`, and likely small changes in `db/compaction/compaction.cc` and `db/db_impl/db_impl_compaction_flush.cc`.

## Main Risks

- Manifest persistence is the highest-risk part. If sorted-run identity is not encoded durably, correctness breaks after restart.
- Read-path assumptions are spread across more places than just `Get()`. Iterators, MultiGet, approximate size, and overlap helpers all need audit.
- Some leveled optimizations should be disabled for tiered mode first, then reintroduced only if proven safe.

## Risk Mitigations

- Manifest change mitigation:
  - keep the change additive by extending file-add metadata rather than inventing a separate manifest record type for sorted runs
  - default missing `sorted_run_id` to legacy single-run semantics
  - add reopen/recovery tests as part of the first validation wave
- Read-path isolation mitigation:
  - preserve current leveled helper code paths exactly
  - route tiered CFs through separate helper branches or separate classes
  - prefer correctness-first tiered code even if it is initially less optimized
- Iterator mitigation:
  - do not flatten a tiered level into one concatenating iterator
  - model the tiered level as "merge of runs", where each run is still "concat of files"
  - apply the same structure consistently in both foreground scans and compaction input iterators

## Next Step

If this plan matches your intent, the next step is to turn it into an implementation task list with exact code changes, then design unit tests and integration tests around those invariants.

## Steps And Milestones

The safest way to land this feature is to split it into dependency-ordered milestones. Each milestone should leave the tree in a buildable state and should have a narrow validation target.

### Milestone 1: Add Tiered Style Skeleton And Persistent Run Metadata

Goal: make RocksDB understand a new tiered compaction style and persist sorted-run identity, without changing read behavior yet.

Steps:

1. Add `kCompactionStyleTiered` to the compaction style enum and option string conversion logic.
2. Update option sanitization and validation so the new style is accepted.
3. Extend per-file metadata in version edits to carry `sorted_run_id`.
4. Update manifest encode/decode and version replay so `sorted_run_id` survives reopen.
5. Add allocator plumbing for new run ids in flush/compaction output creation, even if initially unused by picking logic.
6. Default old manifests or non-tiered files to legacy single-run semantics.

Exit criteria:

- DB opens with `kCompactionStyleTiered`.
- Manifest replay preserves `sorted_run_id`.
- Existing non-tiered behavior remains unchanged.

### Milestone 2: Build Tiered Version Metadata View

Goal: add a tiered-only in-memory representation of sorted runs per level while preserving the existing leveled structures.

Steps:

1. Add a tiered-only structure such as `LevelSortedRunsBrief` to `VersionStorageInfo`.
2. Group files by `sorted_run_id` per level.
3. Preserve run ordering by recency.
4. Generate this view only for tiered CFs.
5. Keep `LevelFilesBrief` and current leveled metadata generation untouched.

Exit criteria:

- A tiered CF can expose run counts per level.
- Files within one run are recognized as sorted/non-overlapping.
- Existing leveled metadata code path is unchanged.

### Milestone 3: Add Tiered-Specific Lookup Helpers

Goal: introduce tiered-only overlap and file-search helpers without modifying the leveled fast path.

Steps:

1. Add tiered-specific helpers for:
   - point file search within a level
   - overlapping-input discovery within a level
   - overlap checks for a key range within a level
2. Make these helpers iterate runs in newest-to-oldest order and binary search within each run.
3. Dispatch to them only when the CF is tiered.
4. Keep existing helpers intact for leveled CFs.

Exit criteria:

- Tiered helper logic is isolated and callable.
- Existing leveled helper behavior and signatures remain stable where possible.

### Milestone 4: Add Tiered Point Read Path

Goal: make `Get()` and `MultiGet()` correct for multiple sorted runs per non-L0 level.

Steps:

1. Add tiered-specific logic in `FilePicker`.
2. Add tiered-specific logic in `FilePickerMultiGet`.
3. Bypass `FileIndexer`-based assumptions for tiered non-L0 levels.
4. Ensure search order is:
   - all relevant runs in current level, newest to oldest
   - then next level
5. Keep existing leveled picker path unchanged.

Exit criteria:

- Point lookup is correct with overlapping runs in non-L0 levels.
- MultiGet is correct with overlapping runs in non-L0 levels.

### Milestone 5: Add Tiered Iterator Path For Range Reads

Goal: make range scan and iterator construction correct for tiered levels.

Steps:

1. Identify iterator-construction call sites used by foreground reads.
2. For tiered levels, build one concatenating iterator per sorted run.
3. Merge those per-run iterators for the level.
4. Merge the resulting level iterator with iterators from other levels.
5. Keep the current leveled “one concat iterator per level” path unchanged.

Exit criteria:

- Forward iteration across tiered levels returns correct key order and visibility.
- Tiered range lookup no longer assumes one sorted run per level.

### Milestone 6: Add Tiered Compaction Picker

Goal: implement the academic tiering trigger and pick policy.

Steps:

1. Add `TieredCompactionPicker`.
2. Wire picker creation from compaction style.
3. Define per-level score as `num_sorted_runs(level) / max_bytes_for_level_multiplier`.
4. Trigger compaction when score `>= 1`.
5. Pick all runs in the chosen level.
6. Output one new run in the next level.
7. Apply the same rule to L0.

Exit criteria:

- Automatic compaction is triggered by sorted-run count.
- Picked compactions match the intended tiering semantics.

### Milestone 7: Add Tiered Compaction Input Iterator Path

Goal: make compaction execution correct when the input level contains multiple runs.

Steps:

1. Update compaction input iterator construction in `db/compaction/compaction.cc`.
2. Group selected input files by `sorted_run_id`.
3. Build one concatenating iterator per input run.
4. Merge run iterators through the existing compaction merging layer.
5. Stamp all output files from one compaction with a new shared `sorted_run_id`.

Exit criteria:

- Tiered compaction execution merges multiple runs correctly.
- Output files form one new sorted run in the next level.

### Milestone 8: Audit And Isolate Remaining Shared Helpers

Goal: clean up remaining places where non-L0 overlap assumptions still leak through.

Steps:

1. Audit helpers such as:
   - `OverlapInLevel`
   - `GetOverlappingInputs`
   - `ApproximateSize`
   - bottommost-file detection
   - key-existence-beyond-output-level checks
2. For each helper, choose one of:
   - keep leveled path unchanged and add tiered branch
   - make the helper safely style-aware if risk is low
3. Prefer conservative correctness over optimization.

Exit criteria:

- No known correctness path still assumes one sorted run per non-L0 level in tiered mode.

### Milestone 9: Manual Compaction And Recovery Hardening

Goal: make the feature operationally safe beyond the automatic path.

Steps:

1. Review manual compaction behavior under tiered style.
2. Confirm reopen/recovery behavior with multiple runs per level.
3. Disable unsafe optimizations such as trivial move if assumptions do not hold.
4. Verify manifest replay, flush, and compaction all preserve run identity.

Exit criteria:

- Restart/recovery is correct.
- Manual compaction does not violate tiered invariants.

## Recommended Implementation Order

1. Milestone 1
2. Milestone 2
3. Milestone 3
4. Milestone 4
5. Milestone 5
6. Milestone 6
7. Milestone 7
8. Milestone 8
9. Milestone 9

## Suggested Checkpoints Between Milestones

- After Milestone 2: review data model and manifest design before touching reads.
- After Milestone 5: review all foreground read-path behavior before enabling automatic compaction.
- After Milestone 7: review compaction execution and output-run semantics.
- After Milestone 9: move to full test plan and broader validation.
