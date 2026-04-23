//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_udp.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "logging/log_buffer.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {
namespace {

uint32_t GetUDPOutputPathId(const ImmutableCFOptions& ioptions) {
  return ioptions.cf_paths.empty() ? 0 : 0;
}

uint32_t NewRandomSeed() {
  return Random::GetTLSInstance()->Next();
}

bool LevelAvailable(const VersionStorageInfo* vstorage, int level) {
  for (const auto* file : vstorage->LevelFiles(level)) {
    if (file->being_compacted) {
      return false;
    }
  }
  return true;
}

void AppendRunFiles(const SortedRunBrief& run,
                    std::vector<FileMetaData*>* selected_files) {
  for (size_t i = 0; i < run.files.num_files; ++i) {
    selected_files->push_back(run.files.files[i].file_metadata);
  }
}

std::vector<FileMetaData*> SelectUDPRandomInputRuns(
    VersionStorageInfo* vstorage, int level, Random* rnd) {
  std::vector<FileMetaData*> selected_files;
  if (level == 0) {
    const auto& level_files = vstorage->LevelFiles(level);
    if (level_files.empty()) {
      return selected_files;
    }
    const size_t files_to_take = rnd->Uniform(
        static_cast<int>(level_files.size())) + 1;
    std::vector<size_t> indexes;
    indexes.reserve(level_files.size());
    for (size_t i = 0; i < level_files.size(); ++i) {
      indexes.push_back(i);
    }
    RandomShuffle(indexes.begin(), indexes.end(), rnd->Next());
    for (size_t i = 0; i < files_to_take; ++i) {
      selected_files.push_back(level_files[indexes[i]]);
    }
    return selected_files;
  }

  const auto& level_runs = vstorage->LevelSortedRuns(level);
  if (level_runs.num_runs() == 0) {
    return selected_files;
  }
  const size_t runs_to_take =
      rnd->Uniform(static_cast<int>(level_runs.num_runs())) + 1;
  std::vector<size_t> indexes;
  indexes.reserve(level_runs.num_runs());
  for (size_t i = 0; i < level_runs.num_runs(); ++i) {
    indexes.push_back(i);
  }
  RandomShuffle(indexes.begin(), indexes.end(), rnd->Next());
  for (size_t i = 0; i < runs_to_take; ++i) {
    AppendRunFiles(level_runs.runs[indexes[i]], &selected_files);
  }
  return selected_files;
}

std::vector<FileMetaData*> SelectUDPRandomOutputRun(
    VersionStorageInfo* vstorage, int level, Random* rnd,
    uint64_t* selected_sorted_run_id) {
  std::vector<FileMetaData*> selected_files;
  if (level <= 0 || level >= vstorage->num_levels() ||
      !LevelAvailable(vstorage, level)) {
    return selected_files;
  }
  const auto& level_runs = vstorage->LevelSortedRuns(level);
  if (level_runs.num_runs() == 0) {
    return selected_files;
  }
  const size_t run_index = rnd->Uniform(static_cast<int>(level_runs.num_runs()));
  *selected_sorted_run_id = level_runs.runs[run_index].sorted_run_id;
  AppendRunFiles(level_runs.runs[run_index], &selected_files);
  return selected_files;
}

}  // namespace

bool UDPCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* /*vstorage*/) const {
  return true;
}

Compaction* UDPCompactionPicker::PickCompaction(
    const std::string& /*cf_name*/,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots*/,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/, const std::string& /*full_history_ts_low*/,
    bool /*require_max_output_level*/) {
  assert(vstorage->IsUDP());

  Random rnd(NewRandomSeed());
  std::vector<int> eligible_levels;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    if (vstorage->NumLevelFiles(level) == 0 || !LevelAvailable(vstorage, level)) {
      continue;
    }
    eligible_levels.push_back(level);
  }
  if (eligible_levels.empty()) {
    return nullptr;
  }

  const int start_level =
      eligible_levels[rnd.Uniform(static_cast<int>(eligible_levels.size()))];
  const int output_level =
      start_level == vstorage->num_levels() - 1 ? start_level : start_level + 1;

  std::vector<FileMetaData*> start_level_files =
      SelectUDPRandomInputRuns(vstorage, start_level, &rnd);
  if (start_level_files.empty()) {
    return nullptr;
  }

  CompactionInputFiles start_level_inputs;
  start_level_inputs.level = start_level;
  start_level_inputs.files = std::move(start_level_files);

  std::vector<CompactionInputFiles> compaction_inputs;
  compaction_inputs.push_back(std::move(start_level_inputs));

  uint64_t selected_output_sorted_run_id = 0;
  if (output_level != start_level) {
    std::vector<FileMetaData*> output_run_files =
        SelectUDPRandomOutputRun(vstorage, output_level, &rnd,
                                 &selected_output_sorted_run_id);
    if (!output_run_files.empty()) {
      CompactionInputFiles output_level_inputs;
      output_level_inputs.level = output_level;
      output_level_inputs.files = std::move(output_run_files);
      compaction_inputs.push_back(std::move(output_level_inputs));
    }
  }

  auto* c = new Compaction(
      vstorage, ioptions_, mutable_cf_options, mutable_db_options,
      std::move(compaction_inputs), output_level,
      MaxFileSizeForLevel(mutable_cf_options, output_level,
                          ioptions_.compaction_style, vstorage->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes, GetUDPOutputPathId(ioptions_),
      GetCompressionType(vstorage, mutable_cf_options, output_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, output_level),
      Temperature::kUnknown,
      /*max_subcompactions=*/0,
      /*grandparents=*/{},
      /*earliest_snapshot=*/std::nullopt,
      /*snapshot_checker=*/nullptr, CompactionReason::kLevelMaxLevelSize,
      /*trim_ts=*/"", /*score=*/0,
      /*l0_files_might_overlap=*/true);

  if (selected_output_sorted_run_id != 0) {
    c->set_preselected_output_sorted_run_id(selected_output_sorted_run_id);
  }

  RegisterCompaction(c);
  return c;
}

}  // namespace ROCKSDB_NAMESPACE
