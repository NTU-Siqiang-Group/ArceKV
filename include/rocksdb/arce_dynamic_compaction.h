// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under both the GPLv2 (found in the
// COPYING file in the root directory) and Apache 2.0 License
// (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <mutex>
#include <numeric>
#include <tuple>
#include <utility>
#include <vector>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace ROCKSDB_NAMESPACE {
namespace ArceDynamicCompaction {

struct DynAction {
  std::vector<std::vector<int>> removed_runs;
  int start_level = -1;
  int target_level = -1;
  double reward = 0;
  int estimate_finished_idx = -1;
  double compaction_size = 0;

  explicit DynAction(size_t num_levels = 0) : removed_runs(num_levels) {}

  void ResizeToState(const std::vector<std::vector<int64_t>>& level_runs) {
    removed_runs.resize(level_runs.size());
    for (size_t level = 0; level < level_runs.size(); ++level) {
      removed_runs[level].assign(level_runs[level].size(), 0);
    }
  }

  void SetRemovedRuns(int level, int start_idx, int end_idx, int value = 1) {
    if (level < 0 || level >= static_cast<int>(removed_runs.size())) {
      return;
    }
    if (start_idx < 0 || end_idx < start_idx) {
      return;
    }
    end_idx = std::min(end_idx, static_cast<int>(removed_runs[level].size()) - 1);
    for (int idx = start_idx; idx <= end_idx; ++idx) {
      removed_runs[level][idx] = value;
    }
  }
};

struct TreeState {
  std::vector<std::vector<int64_t>> level_runs;
  int total_runs = 0;
  int max_level_runs = 0;
  std::vector<DynAction> actions;

  void EnumerateActions() {
    actions.clear();
    if (total_runs == 0 || level_runs.empty()) {
      return;
    }

    DynAction major_compaction(level_runs.size());
    DynAction empty(level_runs.size());
    major_compaction.ResizeToState(level_runs);
    empty.ResizeToState(level_runs);

    if (!level_runs[0].empty()) {
      major_compaction.start_level = 0;
      major_compaction.SetRemovedRuns(0, 0,
                                      static_cast<int>(level_runs[0].size()) - 1);
      major_compaction.compaction_size =
          static_cast<double>(std::accumulate(level_runs[0].begin(),
                                              level_runs[0].end(), int64_t{0}));
      major_compaction.reward = static_cast<double>(level_runs[0].size() - 1);
    }

    for (int level = 0; level < static_cast<int>(level_runs.size()); ++level) {
      int64_t level_size = 0;
      if (level_runs[level].empty()) {
        continue;
      }

      DynAction compact_to_cur_level = empty;
      compact_to_cur_level.reward = -1;

      auto cur_level_major = major_compaction;
      for (int run_idx = static_cast<int>(level_runs[level].size()) - 1;
           run_idx >= 0; --run_idx) {
        level_size += level_runs[level][run_idx];

        if (run_idx != 0) {
          compact_to_cur_level.removed_runs[level][run_idx] = 1;
          compact_to_cur_level.start_level = level;
          compact_to_cur_level.target_level = level == 0 ? 1 : level;
          compact_to_cur_level.compaction_size =
              static_cast<double>(level_size);
          compact_to_cur_level.reward += 1;
          actions.push_back(compact_to_cur_level);
        }

        if (level > 0 && cur_level_major.start_level >= 0) {
          cur_level_major.target_level = level;
          cur_level_major.removed_runs[level][run_idx] = 1;
          cur_level_major.compaction_size +=
              static_cast<double>(level_runs[level][run_idx]);
          cur_level_major.reward += 1;
          if (run_idx == 0 &&
              level + 1 < static_cast<int>(level_runs.size())) {
            cur_level_major.target_level = level + 1;
          }
          actions.push_back(cur_level_major);
        }
      }
      major_compaction = cur_level_major;

      auto compact_to_next = compact_to_cur_level;
      compact_to_next.start_level = level;
      compact_to_next.target_level =
          level + 1 < static_cast<int>(level_runs.size()) ? level + 1 : level;
      compact_to_next.reward =
          static_cast<double>(level_runs[level].size() - 1);
      compact_to_next.removed_runs[level][0] = 1;
      compact_to_next.compaction_size = static_cast<double>(level_size);
      actions.push_back(compact_to_next);

      if (level + 1 < static_cast<int>(level_runs.size()) &&
          !level_runs[level + 1].empty()) {
        auto compact_with_next = compact_to_next;
        compact_with_next.compaction_size +=
            static_cast<double>(level_runs[level + 1].back());
        compact_with_next.removed_runs[level + 1].back() = 1;
        compact_with_next.reward = static_cast<double>(level_runs[level].size());
        actions.push_back(compact_with_next);
      }
    }

    constexpr double kReadWriteRatio = 2.0;
    for (auto& action : actions) {
      action.compaction_size *= kReadWriteRatio / 4096.0;
    }
  }
};

inline void GetWindowAccumulatedIOs(int cur_total_runs, int max_offset,
                                    std::vector<double>* acc_ios,
                                    int64_t buffer_size, int write_stall,
                                    int r, int u, int p, double wait_io,
                                    double parallel_factor) {
  acc_ios->assign(max_offset, 0);
  double acc_io = 0;
  double factor = parallel_factor;
  if (factor > 1) {
    factor /= 2;
  }
  if (factor <= 0) {
    factor = 1;
  }

  for (int i = 0; i < max_offset; ++i) {
    acc_io += buffer_size * 1.0 / 4096 + r * cur_total_runs +
              p * (1 + cur_total_runs * 0.01);
    if (cur_total_runs >= write_stall) {
      acc_io += u * 4.0;
    }
    if (cur_total_runs >= write_stall * 4) {
      acc_io += u * 1e10;
    }
    acc_io += (r + u + p) * wait_io;
    (*acc_ios)[i] = acc_io / factor;
    cur_total_runs += 1;
  }
}

inline void GetRewardForAction(DynAction* action, int total_runs,
                               const std::vector<double>& acc_ios, int c, int M,
                               int r, int u, int p, double parallel_factor) {
  auto it = std::lower_bound(acc_ios.begin(), acc_ios.end(),
                             action->compaction_size);
  int idx = static_cast<int>(it - acc_ios.begin());
  if (it == acc_ios.end() || idx >= static_cast<int>(acc_ios.size()) - 1) {
    action->reward = 0;
    return;
  }

  double factor = parallel_factor;
  if (factor > 1) {
    factor /= 2;
  }
  if (factor <= 0) {
    factor = 1;
  }

  action->estimate_finished_idx = idx;
  action->reward *= (r + p * 0.01) * M;
  if (idx > 0) {
    double inc_rr = idx * 1.0 * r / 2;
    double inc_p = idx * 0.01 * p / 2;
    double inc_w = 0;
    if (idx + total_runs >= c) {
      inc_w = std::max(0, idx + total_runs - c) * 4.0 * u;
    }
    double remaining_comp = action->compaction_size - inc_rr - inc_p - inc_w;
    if (idx + total_runs >= 4 * c) {
      inc_w += std::max(0.0, remaining_comp) * factor;
    }
    action->reward -= inc_rr + inc_p + inc_w;
  }
}

inline void ApplyActionToTree(TreeState* state, const DynAction& action) {
  if (action.start_level < 0) {
    return;
  }
  int64_t total_compacted_size = 0;
  for (int level = action.start_level; level <= action.target_level; ++level) {
    if (level < 0 || level >= static_cast<int>(state->level_runs.size()) ||
        level >= static_cast<int>(action.removed_runs.size())) {
      continue;
    }
    for (size_t idx = 0; idx < action.removed_runs[level].size(); ++idx) {
      if (action.removed_runs[level][idx]) {
        total_compacted_size += state->level_runs[level][idx];
        state->level_runs[level][idx] = 0;
      }
    }
  }
  state->level_runs[action.target_level].push_back(total_compacted_size);

  int total_runs = 0;
  int max_level_runs = 0;
  std::vector<std::vector<int64_t>> new_level_runs;
  new_level_runs.reserve(state->level_runs.size());
  for (auto& level_runs : state->level_runs) {
    std::vector<int64_t> new_runs;
    for (int64_t run_size : level_runs) {
      if (run_size > 0) {
        new_runs.push_back(run_size);
        total_runs++;
      }
    }
    std::sort(new_runs.begin(), new_runs.end(), std::greater<int64_t>());
    max_level_runs = std::max(max_level_runs, static_cast<int>(new_runs.size()));
    new_level_runs.push_back(std::move(new_runs));
  }
  state->level_runs = std::move(new_level_runs);
  state->total_runs = total_runs;
  state->max_level_runs = max_level_runs;
}

inline DynAction GetBestActionWithForward(TreeState state, int M, int c,
                                          int64_t buffer_size, int r, int u,
                                          int p, double wait_io,
                                          double parallel_factor) {
  DynAction best_action(state.level_runs.size());
  if (state.actions.empty()) {
    return best_action;
  }

  std::vector<double> acc_ios;
  GetWindowAccumulatedIOs(state.total_runs, 500, &acc_ios, buffer_size, c, r,
                          u, p, wait_io, parallel_factor);
  for (auto& action : state.actions) {
    GetRewardForAction(&action, state.total_runs, acc_ios, c, M, r, u, p,
                       parallel_factor);
    if (action.reward > best_action.reward) {
      best_action = action;
    }
  }
  return best_action;
}

inline double GetCostForMc(const TreeState& latest_state, int M, int c,
                           int64_t buffer_size, int r, int u, int p,
                           double wait_io, int mc_search_len,
                           int remaining_window_cnt, double parallel_factor) {
  double cost = 0;
  int64_t ops = 0;
  TreeState tmp_state = latest_state;
  double factor = parallel_factor;
  if (factor > 1) {
    factor /= 2;
  }
  if (factor <= 0) {
    factor = 1;
  }

  for (int i = 0; i < mc_search_len; ++i) {
    tmp_state.EnumerateActions();
    auto action = GetBestActionWithForward(tmp_state, M, c, buffer_size, r, u,
                                           p, wait_io, parallel_factor);
    if (action.start_level < 0) {
      action.estimate_finished_idx = 0;
    }
    int64_t total_runs = tmp_state.total_runs;
    double remaining_compaction_size = action.compaction_size;
    int elapsed_window_cnt = action.estimate_finished_idx + 1;
    if (elapsed_window_cnt < remaining_window_cnt) {
      elapsed_window_cnt = remaining_window_cnt;
    }
    remaining_window_cnt -= elapsed_window_cnt;

    for (int j = 0; j < elapsed_window_cnt; ++j) {
      double window_cost =
          total_runs * r + (total_runs * 0.01 + 1) * p + buffer_size / 4096.0;
      cost += window_cost;
      remaining_compaction_size -= window_cost / factor;
      if (total_runs >= c && total_runs < c * 4) {
        cost += 1.0 * u * 4.0;
        remaining_compaction_size -= 1.0 * u * 4.0;
      }
      if (total_runs >= c * 4) {
        cost += std::max(0.0, remaining_compaction_size) * factor;
        remaining_compaction_size = 0;
      }
      total_runs++;
      ops += r + p + u;
    }

    ApplyActionToTree(&tmp_state, action);
    for (int k = 0; k < elapsed_window_cnt; ++k) {
      tmp_state.level_runs[0].push_back(buffer_size);
      tmp_state.total_runs++;
    }
    tmp_state.max_level_runs =
        std::max(tmp_state.max_level_runs,
                 static_cast<int>(tmp_state.level_runs[0].size()));
    if (remaining_window_cnt <= 0) {
      break;
    }
  }

  if (ops <= 0) {
    return 1e9;
  }
  double avg_cost = cost / ops;
  return avg_cost > 0 ? avg_cost : 1e9;
}

inline std::pair<int, int> FindBestMc(const TreeState& latest_state,
                                      int64_t buffer_size, int r, int u, int p,
                                      double wait_io, int mc_search_len,
                                      int remaining_window_cnt,
                                      double parallel_factor) {
  double best_cost = 1e9;
  std::pair<int, int> best_mc = {100, 2000};

  std::vector<int> c_candidates = {4};
  int upper = std::max(8, latest_state.total_runs * 2);
  for (int c = 4; c <= upper; c += 4) {
    c_candidates.push_back(c);
  }
  c_candidates.push_back(1000000);

  for (int M = 5; M < 100; M += 5) {
    for (int c : c_candidates) {
      double cost = GetCostForMc(latest_state, M, c, buffer_size, r, u, p,
                                 wait_io, mc_search_len, remaining_window_cnt,
                                 parallel_factor);
      if (cost < best_cost) {
        best_cost = cost;
        best_mc = {M, c};
      }
    }
  }
  return best_mc;
}

inline std::pair<int, int> FindBestMcParallel(const TreeState& latest_state,
                                              int64_t buffer_size, int r, int u,
                                              int p, double wait_io,
                                              int mc_search_len,
                                              int remaining_window_cnt,
                                              double parallel_factor) {
  std::vector<int> c_candidates = {4};
  int upper = std::max(8, latest_state.total_runs * 2);
  for (int c = 4; c <= upper; c += 4) {
    c_candidates.push_back(c);
  }
  c_candidates.push_back(1000000);

  struct CandidateCost {
    int M;
    int c;
    double cost;
  };
  std::vector<CandidateCost> results;
  results.reserve(static_cast<size_t>(19 * c_candidates.size()));
  for (int M = 5; M < 100; M += 5) {
    for (int c : c_candidates) {
      results.push_back({M, c, 0});
    }
  }

#ifdef _OPENMP
  omp_set_num_threads(16);
#pragma omp parallel for schedule(dynamic)
  for (int idx = 0; idx < static_cast<int>(results.size()); ++idx) {
    results[idx].cost = GetCostForMc(
        latest_state, results[idx].M, results[idx].c, buffer_size, r, u, p,
        wait_io, mc_search_len, remaining_window_cnt, parallel_factor);
  }
#else
  for (auto& result : results) {
    result.cost = GetCostForMc(latest_state, result.M, result.c, buffer_size,
                               r, u, p, wait_io, mc_search_len,
                               remaining_window_cnt, parallel_factor);
  }
#endif

  std::pair<int, int> best_mc = {100, 2000};
  double best_cost = 1e9;
  for (const auto& result : results) {
    if (result.cost < best_cost) {
      best_cost = result.cost;
      best_mc = {result.M, result.c};
    }
  }
  return best_mc;
}

class ArceCompactionController {
 public:
  double state_change_threshold = 0.1;
  int mc_search_len = 400;
  int remaining_window_cnt = 1000;
  int64_t buffer_size = 2L * (1 << 20);
  int64_t entry_size = 1024;
  double wait_io = 0;
  double parallel_factor = 1;
  bool enable_dynamic_parameter_selection = true;
  std::atomic<int> latest_run_num{0};
  std::atomic<bool> warned_parallelism{false};

  ArceCompactionController() = default;

  void SetWorkload(int r, int u, int p) {
    std::lock_guard<std::mutex> guard(mtx_);
    workload_ = std::make_tuple(r, u, p);
  }

  void SetWorkloadProportions(double r_ratio, double u_ratio, double p_ratio) {
    std::lock_guard<std::mutex> guard(mtx_);
    workload_ = NormalizeWorkloadLocked(r_ratio, u_ratio, p_ratio);
  }

  std::tuple<int, int, int> GetWorkload() const {
    std::lock_guard<std::mutex> guard(mtx_);
    return workload_;
  }

  void SetBufferSize(int64_t new_buffer_size) {
    std::lock_guard<std::mutex> guard(mtx_);
    buffer_size = std::max<int64_t>(1, new_buffer_size);
  }

  void SetEntrySize(int64_t new_entry_size) {
    std::lock_guard<std::mutex> guard(mtx_);
    entry_size = std::max<int64_t>(1, new_entry_size);
  }

  std::pair<int, int> GetMc() const {
    std::lock_guard<std::mutex> guard(mtx_);
    return mc_;
  }

  void SetMc(std::pair<int, int> mc) {
    std::lock_guard<std::mutex> guard(mtx_);
    mc_ = mc;
    mc_ready_ = true;
  }

  DynAction GetBestAction(const TreeState& state) {
    MaybeRefreshMc(state);
    auto [r, u, p] = GetWorkload();
    auto [M, c] = GetMc();
    TreeState state_copy = state;
    return GetBestActionWithForward(state_copy, M, c, buffer_size, r, u, p,
                                    wait_io, parallel_factor);
  }

 private:
  mutable std::mutex mtx_;
  int triggered_compaction_nums_ = 0;
  int total_run_nums_ = 0;
  double prev_avg_sorted_runs_ = 1;
  bool mc_ready_ = false;
  TreeState most_recent_state_;
  std::tuple<int, int, int> workload_{0, 2048, 0};
  std::pair<int, int> mc_{100, 2000};

  std::tuple<int, int, int> NormalizeWorkloadLocked(double r_ratio,
                                                    double u_ratio,
                                                    double p_ratio) const {
    if (u_ratio <= 0) {
      return {0, 0, 0};
    }
    const int normalized_u = static_cast<int>(
        std::max<int64_t>(1, buffer_size / std::max<int64_t>(1, entry_size)));
    const double scale = static_cast<double>(normalized_u) / u_ratio;
    const int normalized_r =
        static_cast<int>(std::llround(std::max(0.0, r_ratio * scale)));
    const int normalized_p =
        static_cast<int>(std::llround(std::max(0.0, p_ratio * scale)));
    return {normalized_r, normalized_u, normalized_p};
  }

  void MaybeRefreshMc(const TreeState& state) {
    bool should_refresh = false;
    int64_t local_buffer_size = 0;
    double local_wait_io = 0;
    double local_parallel_factor = 1;
    int local_search_len = 0;
    int local_remaining_windows = 0;
    std::tuple<int, int, int> local_workload;

    {
      std::lock_guard<std::mutex> guard(mtx_);
      most_recent_state_ = state;
      latest_run_num.store(state.total_runs);

      if (!enable_dynamic_parameter_selection) {
        mc_ready_ = true;
        return;
      }

      triggered_compaction_nums_++;
      total_run_nums_ += state.total_runs;
      if (!mc_ready_) {
        should_refresh = true;
      } else if (triggered_compaction_nums_ >= 10) {
        double cur_avg = 1.0 * total_run_nums_ / triggered_compaction_nums_;
        if (std::abs(cur_avg - prev_avg_sorted_runs_) /
                std::max(1e-9, prev_avg_sorted_runs_) >
            state_change_threshold) {
          prev_avg_sorted_runs_ = cur_avg;
          triggered_compaction_nums_ = 0;
          total_run_nums_ = 0;
          should_refresh = true;
        }
      }

      local_buffer_size = buffer_size;
      local_wait_io = wait_io;
      local_parallel_factor = parallel_factor;
      local_search_len = mc_search_len;
      local_remaining_windows = remaining_window_cnt;
      local_workload = workload_;
    }

    if (!should_refresh) {
      return;
    }

    auto [r, u, p] = local_workload;
    auto new_mc = FindBestMcParallel(
        state, local_buffer_size, r, u, p, local_wait_io, local_search_len,
        local_remaining_windows, local_parallel_factor);

    std::lock_guard<std::mutex> guard(mtx_);
    mc_ = new_mc;
    mc_ready_ = true;
  }
};

}  // namespace ArceDynamicCompaction

namespace UDPDynamicCompaction {
using ArceCompactionController = ArceDynamicCompaction::ArceCompactionController;
using UDPCompactionController = ArceDynamicCompaction::ArceCompactionController;
using DynAction = ArceDynamicCompaction::DynAction;
using TreeState = ArceDynamicCompaction::TreeState;
using ArceDynamicCompaction::FindBestMcParallel;
}  // namespace UDPDynamicCompaction

}  // namespace ROCKSDB_NAMESPACE
