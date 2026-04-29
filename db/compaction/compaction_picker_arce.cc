//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/compaction/compaction_picker_arce.h"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <memory>
#include <utility>
#include <vector>

#include "logging/log_buffer.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE {
namespace {

using ArceDynamicCompaction::DynAction;
using ArceDynamicCompaction::TreeState;
using ArceDynamicCompaction::ArceCompactionController;

uint32_t GetArceOutputPathId(const ImmutableCFOptions& /*ioptions*/) {
  return 0;
}

size_t CountArceSortedRuns(const VersionStorageInfo* vstorage) {
  size_t total_runs = 0;
  for (int level = 0; level < vstorage->num_levels(); ++level) {
    if (vstorage->NumLevelFiles(level) == 0) {
      continue;
    }
    if (level == 0) {
      total_runs += vstorage->NumLevelFiles(level);
    } else {
      std::unordered_set<uint64_t> run_ids;
      for (const auto* file : vstorage->LevelFiles(level)) {
        run_ids.insert(file->sorted_run_id);
      }
      total_runs += run_ids.size();
    }
    if (total_runs > 1) {
      return total_runs;
    }
  }
  return total_runs;
}

uint64_t SumFileSizes(const std::vector<FileMetaData*>& files) {
  uint64_t total_size = 0;
  for (const auto* file : files) {
    total_size += file->fd.GetFileSize();
  }
  return total_size;
}

void SortRunFilesBySmallest(const InternalKeyComparator* icmp,
                            std::vector<FileMetaData*>* files) {
  std::sort(files->begin(), files->end(),
            [icmp](const FileMetaData* lhs, const FileMetaData* rhs) {
              return icmp->Compare(lhs->smallest, rhs->smallest) < 0;
            });
}

std::string DescribeRemovedRuns(const DynAction& action) {
  std::string description;
  for (size_t level = 0; level < action.removed_runs.size(); ++level) {
    bool wrote_level = false;
    for (size_t run_idx = 0; run_idx < action.removed_runs[level].size();
         ++run_idx) {
      if (!action.removed_runs[level][run_idx]) {
        continue;
      }
      if (!wrote_level) {
        if (!description.empty()) {
          description.append(" ");
        }
        description.append("L");
        description.append(std::to_string(level));
        description.append(":[");
        wrote_level = true;
      } else {
        description.append(",");
      }
      description.append(std::to_string(run_idx));
    }
    if (wrote_level) {
      description.append("]");
    }
  }
  return description;
}

struct ArceRunCandidate {
  uint64_t size_bytes = 0;
  std::vector<FileMetaData*> files;
};

struct ArceDynamicTree {
  TreeState state;
  std::vector<std::vector<ArceRunCandidate>> level_runs;
};

ArceDynamicTree BuildDynamicTreeState(VersionStorageInfo* vstorage) {
  ArceDynamicTree dynamic_tree;
  dynamic_tree.state.level_runs.resize(vstorage->num_levels());
  dynamic_tree.level_runs.resize(vstorage->num_levels());

  for (int level = 0; level < vstorage->num_levels(); ++level) {
    if (level == 0) {
      for (auto* file : vstorage->LevelFiles(level)) {
        if (file->being_compacted) {
          continue;
        }
        ArceRunCandidate run;
        run.size_bytes = file->fd.GetFileSize();
        run.files.push_back(file);
        dynamic_tree.state.level_runs[level].push_back(run.size_bytes);
        dynamic_tree.level_runs[level].push_back(std::move(run));
        dynamic_tree.state.total_runs++;
      }
      dynamic_tree.state.max_level_runs =
          std::max(dynamic_tree.state.max_level_runs,
                   static_cast<int>(dynamic_tree.level_runs[level].size()));
      continue;
    }

    std::unordered_map<uint64_t, ArceRunCandidate> runs_by_id;
    std::unordered_map<uint64_t, bool> run_available;
    for (auto* file : vstorage->LevelFiles(level)) {
      auto& run = runs_by_id[file->sorted_run_id];
      run.files.push_back(file);
      auto& available = run_available[file->sorted_run_id];
      if (run.files.size() == 1) {
        available = true;
      }
      if (file->being_compacted) {
        available = false;
      }
    }

    std::vector<uint64_t> run_ids;
    run_ids.reserve(runs_by_id.size());
    for (const auto& [run_id, run] : runs_by_id) {
      (void)run;
      run_ids.push_back(run_id);
    }
    std::sort(run_ids.begin(), run_ids.end(), std::greater<uint64_t>());
    for (uint64_t run_id : run_ids) {
      auto run_it = runs_by_id.find(run_id);
      if (run_it == runs_by_id.end() || !run_available[run_id]) {
        continue;
      }
      auto run = std::move(run_it->second);
      SortRunFilesBySmallest(vstorage->InternalComparator(), &run.files);
      run.size_bytes = SumFileSizes(run.files);
      dynamic_tree.level_runs[level].push_back(std::move(run));
    }

    std::sort(dynamic_tree.level_runs[level].begin(),
              dynamic_tree.level_runs[level].end(),
              [](const ArceRunCandidate& lhs, const ArceRunCandidate& rhs) {
                return lhs.size_bytes > rhs.size_bytes;
              });
    for (const auto& run : dynamic_tree.level_runs[level]) {
      dynamic_tree.state.level_runs[level].push_back(run.size_bytes);
      dynamic_tree.state.total_runs++;
    }
    dynamic_tree.state.max_level_runs =
        std::max(dynamic_tree.state.max_level_runs,
                 static_cast<int>(dynamic_tree.level_runs[level].size()));
  }

  return dynamic_tree;
}

Compaction* BuildCompactionFromAction(
    const DynAction& action, const ArceDynamicTree& dynamic_tree,
    VersionStorageInfo* vstorage, const ImmutableOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options) {
  if (action.start_level < 0) {
    return nullptr;
  }

  std::vector<CompactionInputFiles> compaction_inputs;
  for (int level = action.start_level; level <= action.target_level; ++level) {
    if (level < 0 || level >= static_cast<int>(dynamic_tree.level_runs.size()) ||
        level >= static_cast<int>(action.removed_runs.size())) {
      continue;
    }

    CompactionInputFiles inputs;
    inputs.level = level;
    for (size_t run_idx = 0;
         run_idx < action.removed_runs[level].size() &&
         run_idx < dynamic_tree.level_runs[level].size();
         ++run_idx) {
      if (!action.removed_runs[level][run_idx]) {
        continue;
      }
      const auto& run = dynamic_tree.level_runs[level][run_idx];
      inputs.files.insert(inputs.files.end(), run.files.begin(), run.files.end());
    }
    if (!inputs.files.empty()) {
      compaction_inputs.push_back(std::move(inputs));
    }
  }

  if (compaction_inputs.empty()) {
    return nullptr;
  }

  auto* compaction = new Compaction(
      vstorage, ioptions, mutable_cf_options, mutable_db_options,
      std::move(compaction_inputs), action.target_level,
      MaxFileSizeForLevel(mutable_cf_options, action.target_level,
                          ioptions.compaction_style, vstorage->base_level(),
                          ioptions.level_compaction_dynamic_level_bytes),
      mutable_cf_options.max_compaction_bytes, GetArceOutputPathId(ioptions),
      GetCompressionType(vstorage, mutable_cf_options, action.target_level,
                         vstorage->base_level()),
      GetCompressionOptions(mutable_cf_options, vstorage, action.target_level),
      Temperature::kUnknown,
      /*max_subcompactions=*/0,
      /*grandparents=*/{},
      /*earliest_snapshot=*/std::nullopt,
      /*snapshot_checker=*/nullptr, CompactionReason::kLevelMaxLevelSize,
      /*trim_ts=*/"", /*score=*/0,
      /*l0_files_might_overlap=*/true);
  return compaction;
}

}  // namespace

bool ArceCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  assert(vstorage->IsArce());
  return CountArceSortedRuns(vstorage) > 1;
}

Compaction* ArceCompactionPicker::PickCompaction(
    const std::string& /*cf_name*/,
    const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options,
    const std::vector<SequenceNumber>& /*existing_snapshots*/,
    const SnapshotChecker* /*snapshot_checker*/, VersionStorageInfo* vstorage,
    LogBuffer* /*log_buffer*/, const std::string& /*full_history_ts_low*/,
    bool /*require_max_output_level*/) {
  assert(vstorage->IsArce());

  auto controller = mutable_cf_options.arce_compaction_controller;
  if (controller == nullptr) {
    controller = std::make_shared<ArceCompactionController>();
  }
  controller->SetBufferSize(
      static_cast<int64_t>(mutable_cf_options.write_buffer_size));

  if ((mutable_db_options.max_background_compactions > 1 ||
       mutable_db_options.max_background_jobs > 1) &&
      !controller->warned_parallelism.exchange(true)) {
    ROCKS_LOG_WARN(
        ioptions_.logger,
        "Arce dynamic compaction currently assumes a single active compaction "
        "decision stream. max_background_compactions=%d, "
        "max_background_jobs=%d may lead to unstable run indexing.",
        mutable_db_options.max_background_compactions,
        mutable_db_options.max_background_jobs);
  }

  auto dynamic_tree = BuildDynamicTreeState(vstorage);
  if (dynamic_tree.state.total_runs <= 1) {
    return nullptr;
  }
  dynamic_tree.state.EnumerateActions();
  auto best_action = controller->GetBestAction(dynamic_tree.state);
  ROCKS_LOG_INFO(
      ioptions_.logger,
      "Arce dynamic selected action start_level=%d target_level=%d reward=%.2f "
      "compaction_size=%.2f total_runs=%d removed_runs=%s",
      best_action.start_level, best_action.target_level, best_action.reward,
      best_action.compaction_size, dynamic_tree.state.total_runs,
      DescribeRemovedRuns(best_action).c_str());
  auto* compaction = BuildCompactionFromAction(
      best_action, dynamic_tree, vstorage, ioptions_, mutable_cf_options,
      mutable_db_options);
  if (compaction == nullptr) {
    return nullptr;
  }

  RegisterCompaction(compaction);
  return compaction;
}

}  // namespace ROCKSDB_NAMESPACE
