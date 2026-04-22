//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_tiered.h"

#include <utility>
#include <vector>

#include "logging/log_buffer.h"

namespace ROCKSDB_NAMESPACE {
namespace {

uint32_t GetTieredOutputPathId(const ImmutableCFOptions& ioptions) {
  return ioptions.cf_paths.empty() ? 0 : 0;
}

std::vector<FileMetaData*> SelectTieredCompactionInputs(
    const MutableCFOptions& mutable_cf_options, VersionStorageInfo* vstorage,
    int level) {
  std::vector<FileMetaData*> selected_files;
  const size_t run_limit = static_cast<size_t>(
      std::max(1.0, mutable_cf_options.max_bytes_for_level_multiplier));

  if (level == 0) {
    const auto& level_files = vstorage->LevelFiles(level);
    const size_t files_to_take = std::min(run_limit, level_files.size());
    selected_files.reserve(files_to_take);
    for (size_t i = 0; i < files_to_take; ++i) {
      selected_files.push_back(level_files[i]);
    }
    return selected_files;
  }

  const auto& level_runs = vstorage->LevelSortedRuns(level);
  const size_t runs_to_take = std::min(run_limit, level_runs.num_runs());
  size_t runs_selected = 0;
  for (auto it = level_runs.runs.rbegin();
       it != level_runs.runs.rend() && runs_selected < runs_to_take; ++it) {
    for (size_t i = 0; i < it->files.num_files; ++i) {
      selected_files.push_back(it->files.files[i].file_metadata);
    }
    ++runs_selected;
  }
  return selected_files;
}

}  // namespace

bool TieredCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  for (int i = 0; i <= vstorage->MaxInputLevel(); ++i) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

Compaction* TieredCompactionPicker::PickCompactionForCompactRange(
    const std::string& /*cf_name*/,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    int input_level, int output_level,
    const CompactRangeOptions& compact_range_options,
    const InternalKey* begin, const InternalKey* end,
    InternalKey** compaction_end, bool* manual_conflict,
    uint64_t /*max_file_num_to_ignore*/, const std::string& trim_ts,
    const std::string& full_history_ts_low) {
  assert(vstorage->IsTiered());
  assert(compaction_end != nullptr);
  assert(manual_conflict != nullptr);

  *manual_conflict = false;
  *compaction_end = nullptr;

  if (input_level == ColumnFamilyData::kCompactAllLevels) {
    for (int level = 0; level <= vstorage->MaxInputLevel(); ++level) {
      if (vstorage->NumLevelFiles(level) == 0) {
        continue;
      }
      input_level = level;
      output_level = level + 1;
      break;
    }
  }

  if (input_level < 0 || input_level >= NumberLevels() - 1) {
    return nullptr;
  }

  if (output_level == ColumnFamilyData::kCompactToBaseLevel) {
    output_level = input_level + 1;
  }
  if (output_level != input_level + 1) {
    return nullptr;
  }

  Slice begin_user_key;
  Slice end_user_key;
  const Slice* begin_ptr = nullptr;
  const Slice* end_ptr = nullptr;
  if (begin != nullptr) {
    begin_user_key = begin->user_key();
    begin_ptr = &begin_user_key;
  }
  if (end != nullptr) {
    end_user_key = end->user_key();
    end_ptr = &end_user_key;
  }

  if ((begin_ptr != nullptr || end_ptr != nullptr) &&
      !vstorage->OverlapInLevel(input_level, begin_ptr, end_ptr)) {
    return nullptr;
  }

  const std::vector<FileMetaData*>& level_files = vstorage->LevelFiles(input_level);
  if (level_files.empty()) {
    return nullptr;
  }
  if (AreFilesInCompaction(level_files)) {
    *manual_conflict = true;
    return nullptr;
  }

  CompactionInputFiles inputs;
  inputs.level = input_level;
  inputs.files = level_files;

  std::vector<CompactionInputFiles> compaction_inputs({inputs});
  if (FilesRangeOverlapWithCompaction(
          compaction_inputs, output_level,
          Compaction::EvaluateProximalLevel(vstorage, mutable_cf_options,
                                            ioptions_, input_level,
                                            output_level))) {
    *manual_conflict = true;
    return nullptr;
  }

  auto* compaction = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(compaction_inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          ioptions_.compaction_style, vstorage->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes,
      compact_range_options.target_path_id,
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      Temperature::kUnknown, compact_range_options.max_subcompactions,
      /*grandparents=*/{},
      /*earliest_snapshot=*/std::nullopt,
      /*snapshot_checker=*/nullptr, CompactionReason::kManualCompaction,
      trim_ts, /*score=*/-1, /*l0_files_might_overlap=*/true,
      compact_range_options.blob_garbage_collection_policy,
      compact_range_options.blob_garbage_collection_age_cutoff);

  RegisterCompaction(compaction);
  vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options,
                                   full_history_ts_low);
  return compaction;
}

Compaction* TieredCompactionPicker::PickCompaction(
    const std::string& /*cf_name*/,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots*/,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/, const std::string& full_history_ts_low,
    bool /*require_max_output_level*/) {
  for (int i = 0; i < NumberLevels() - 1; ++i) {
    const double score = vstorage->CompactionScore(i);
    const int start_level = vstorage->CompactionScoreLevel(i);
    if (score < 1) {
      break;
    }

    std::vector<FileMetaData*> start_level_files = SelectTieredCompactionInputs(
        mutable_cf_options, vstorage, start_level);
    if (start_level_files.empty()) {
      continue;
    }

    CompactionInputFiles start_level_inputs;
    start_level_inputs.level = start_level;
    start_level_inputs.files = std::move(start_level_files);

    auto* c = new Compaction(
        vstorage, ioptions_, mutable_cf_options, mutable_db_options,
        {std::move(start_level_inputs)}, start_level + 1,
        MaxFileSizeForLevel(mutable_cf_options, start_level + 1,
                            ioptions_.compaction_style, vstorage->base_level(),
                            ioptions_.level_compaction_dynamic_level_bytes),
        mutable_cf_options.max_compaction_bytes,
        GetTieredOutputPathId(ioptions_), GetCompressionType(
                                                  vstorage, mutable_cf_options,
                                                  start_level + 1,
                                                  vstorage->base_level()),
        GetCompressionOptions(mutable_cf_options, vstorage, start_level + 1),
        Temperature::kUnknown,
        /*max_subcompactions=*/0,
        /*grandparents=*/{},
        /*earliest_snapshot=*/std::nullopt,
        /*snapshot_checker=*/nullptr,
        start_level == 0 ? CompactionReason::kLevelL0FilesNum
                         : CompactionReason::kLevelMaxLevelSize,
        /*trim_ts=*/"", score,
        /*l0_files_might_overlap=*/true);

    RegisterCompaction(c);
    vstorage->ComputeCompactionScore(ioptions_, mutable_cf_options,
                                     full_history_ts_low);
    return c;
  }
  return nullptr;
}

}  // namespace ROCKSDB_NAMESPACE
