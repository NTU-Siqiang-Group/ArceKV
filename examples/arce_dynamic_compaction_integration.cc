// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/listener.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksdb/arce_dynamic_compaction.h"

namespace {

using rocksdb::DB;
using rocksdb::FlushOptions;
using rocksdb::HistogramData;
using rocksdb::Iterator;
using rocksdb::LiveFileMetaData;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Status;
using rocksdb::ArceDynamicCompaction::FindBestMcParallel;
using rocksdb::ArceDynamicCompaction::TreeState;
using rocksdb::ArceDynamicCompaction::ArceCompactionController;
using rocksdb::WaitForCompactOptions;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

constexpr uint64_t kKeyBytes = 24;
constexpr uint64_t kValueBytes = 1000;
constexpr uint64_t kDefaultTotalBytes = 20ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultWriteBufferSize = 64ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultTargetFileSize = 32ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultMaxBytesForLevelBase = 128ULL * 1024ULL * 1024ULL;
constexpr int kDefaultMaxBytesForLevelMultiplier = 4;
constexpr uint64_t kDefaultCorrectnessOps = 30000;
constexpr uint64_t kDefaultRangeWidth = 16;
constexpr size_t kDefaultBatchSize = 128;
constexpr auto kDefaultRefreshInterval = std::chrono::seconds(2);
constexpr auto kDefaultConvergenceTimeout = std::chrono::minutes(20);
constexpr auto kDefaultStabilityWindow = std::chrono::seconds(10);
constexpr int kDefaultBackgroundJobs = 1;

constexpr double kLoadR = 0.01;
constexpr double kLoadU = 0.99;
constexpr double kLoadP = 0.01;
constexpr double kCompactR = 0.99;
constexpr double kCompactU = 0.01;
constexpr double kCompactP = 0.99;
constexpr double kVerifyR = 0.33;
constexpr double kVerifyU = 0.33;
constexpr double kVerifyP = 0.33;

#if defined(OS_WIN)
const char kDefaultDbPath[] =
    "C:\\Windows\\TEMP\\rocksdb_arce_dynamic_compaction_integration";
#else
const char kDefaultDbPath[] =
    "/tmp/rocksdb_arce_dynamic_compaction_integration";
#endif

struct Config {
  std::string engine = "arce";
  std::string db_path = kDefaultDbPath;
  std::string structure_output_path;
  std::string timeseries_output_path;
  std::string raw_stats_output_path;
  uint64_t total_bytes = kDefaultTotalBytes;
  uint64_t write_buffer_size = kDefaultWriteBufferSize;
  uint64_t target_file_size_base = kDefaultTargetFileSize;
  uint64_t max_bytes_for_level_base = kDefaultMaxBytesForLevelBase;
  int max_bytes_for_level_multiplier = kDefaultMaxBytesForLevelMultiplier;
  uint64_t correctness_ops = kDefaultCorrectnessOps;
  uint64_t range_width = kDefaultRangeWidth;
  uint64_t seed = 20260428;
  int max_background_jobs = kDefaultBackgroundJobs;
  uint64_t refresh_interval_ms =
      static_cast<uint64_t>(kDefaultRefreshInterval.count() * 1000);
  uint64_t sample_interval_ms = 1000;
  uint64_t correctness_log_interval = 100000;
  uint64_t convergence_timeout_s =
      static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::seconds>(
              kDefaultConvergenceTimeout)
              .count());
  uint64_t stability_window_s =
      static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::seconds>(
              kDefaultStabilityWindow)
              .count());
  bool keep_db = false;
  bool enable_bloom = true;
};

struct LatencySummary {
  double avg_micros = 0;
  double p50_micros = 0;
  double p95_micros = 0;
  double p99_micros = 0;
  double max_micros = 0;
};

struct LevelStructureSummary {
  int level = 0;
  uint64_t run_count = 0;
  uint64_t file_count = 0;
};

struct VerificationStats {
  std::vector<double> point_latencies;
  std::vector<double> range_latencies;
  std::vector<double> insert_latencies;
};

enum class RunPhase {
  kLoad = 0,
  kReadHeavy = 1,
  kCorrectness = 2,
};

enum class OperationKind {
  kPoint = 0,
  kRange = 1,
  kInsert = 2,
  kCount = 3,
};

struct TimeSeriesSample {
  double elapsed_seconds = 0;
  RunPhase phase = RunPhase::kLoad;
  int M = 0;
  int c = 0;
  uint64_t logical_runs = 0;
  uint64_t live_sst_files = 0;
  double point_avg_micros = 0;
  double range_avg_micros = 0;
  double insert_avg_micros = 0;
  uint64_t point_ops = 0;
  uint64_t range_ops = 0;
  uint64_t insert_ops = 0;
};

struct OperationCounters {
  std::atomic<uint64_t> count{0};
  std::atomic<uint64_t> total_nanos{0};
};

struct SampleRecorder {
  std::array<OperationCounters, static_cast<size_t>(OperationKind::kCount)>
      op_counters;
  std::mutex samples_mu;
  std::vector<TimeSeriesSample> samples;
};

[[noreturn]] void Die(const std::string& message) {
  std::cerr << "error: " << message << std::endl;
  std::exit(1);
}

uint64_t ParseUint64(const std::string& value, const char* name) {
  try {
    size_t parsed = 0;
    uint64_t result = std::stoull(value, &parsed, 10);
    if (parsed != value.size()) {
      Die("invalid numeric value for " + std::string(name) + ": " + value);
    }
    return result;
  } catch (const std::exception&) {
    Die("invalid numeric value for " + std::string(name) + ": " + value);
  }
}

int ParseInt(const std::string& value, const char* name) {
  try {
    size_t parsed = 0;
    int result = std::stoi(value, &parsed, 10);
    if (parsed != value.size()) {
      Die("invalid integer value for " + std::string(name) + ": " + value);
    }
    return result;
  } catch (const std::exception&) {
    Die("invalid integer value for " + std::string(name) + ": " + value);
  }
}

void PrintUsage(const char* argv0) {
  std::cout
      << "Usage: " << argv0 << " [options]\n"
      << "  --engine=arce|level\n"
      << "  --db_path=PATH\n"
      << "  --structure_output_path=PATH\n"
      << "  --timeseries_output_path=PATH\n"
      << "  --raw_stats_output_path=PATH\n"
      << "  --total_bytes=N\n"
      << "  --write_buffer_size=N\n"
      << "  --target_file_size_base=N\n"
      << "  --max_bytes_for_level_base=N\n"
      << "  --max_bytes_for_level_multiplier=N\n"
      << "  --correctness_ops=N\n"
      << "  --range_width=N\n"
      << "  --seed=N\n"
      << "  --max_background_jobs=N\n"
      << "  --refresh_interval_ms=N\n"
      << "  --sample_interval_ms=N\n"
      << "  --correctness_log_interval=N\n"
      << "  --convergence_timeout_s=N\n"
      << "  --stability_window_s=N\n"
      << "  --disable_bloom\n"
      << "  --keep_db\n"
      << "  --help\n";
}

Config ParseArgs(int argc, char** argv) {
  Config config;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(argv[0]);
      std::exit(0);
    } else if (arg == "--keep_db") {
      config.keep_db = true;
    } else if (arg == "--disable_bloom") {
      config.enable_bloom = false;
    } else if (arg.rfind("--engine=", 0) == 0) {
      config.engine = arg.substr(std::strlen("--engine="));
    } else if (arg.rfind("--db_path=", 0) == 0) {
      config.db_path = arg.substr(std::strlen("--db_path="));
    } else if (arg.rfind("--structure_output_path=", 0) == 0) {
      config.structure_output_path =
          arg.substr(std::strlen("--structure_output_path="));
    } else if (arg.rfind("--timeseries_output_path=", 0) == 0) {
      config.timeseries_output_path =
          arg.substr(std::strlen("--timeseries_output_path="));
    } else if (arg.rfind("--raw_stats_output_path=", 0) == 0) {
      config.raw_stats_output_path =
          arg.substr(std::strlen("--raw_stats_output_path="));
    } else if (arg.rfind("--total_bytes=", 0) == 0) {
      config.total_bytes =
          ParseUint64(arg.substr(std::strlen("--total_bytes=")), "total_bytes");
    } else if (arg.rfind("--write_buffer_size=", 0) == 0) {
      config.write_buffer_size = ParseUint64(
          arg.substr(std::strlen("--write_buffer_size=")), "write_buffer_size");
    } else if (arg.rfind("--target_file_size_base=", 0) == 0) {
      config.target_file_size_base = ParseUint64(
          arg.substr(std::strlen("--target_file_size_base=")),
          "target_file_size_base");
    } else if (arg.rfind("--max_bytes_for_level_base=", 0) == 0) {
      config.max_bytes_for_level_base = ParseUint64(
          arg.substr(std::strlen("--max_bytes_for_level_base=")),
          "max_bytes_for_level_base");
    } else if (arg.rfind("--max_bytes_for_level_multiplier=", 0) == 0) {
      config.max_bytes_for_level_multiplier = ParseInt(
          arg.substr(std::strlen("--max_bytes_for_level_multiplier=")),
          "max_bytes_for_level_multiplier");
    } else if (arg.rfind("--correctness_ops=", 0) == 0) {
      config.correctness_ops = ParseUint64(
          arg.substr(std::strlen("--correctness_ops=")), "correctness_ops");
    } else if (arg.rfind("--range_width=", 0) == 0) {
      config.range_width = ParseUint64(
          arg.substr(std::strlen("--range_width=")), "range_width");
    } else if (arg.rfind("--seed=", 0) == 0) {
      config.seed = ParseUint64(arg.substr(std::strlen("--seed=")), "seed");
    } else if (arg.rfind("--max_background_jobs=", 0) == 0) {
      config.max_background_jobs = ParseInt(
          arg.substr(std::strlen("--max_background_jobs=")),
          "max_background_jobs");
    } else if (arg.rfind("--refresh_interval_ms=", 0) == 0) {
      config.refresh_interval_ms = ParseUint64(
          arg.substr(std::strlen("--refresh_interval_ms=")),
          "refresh_interval_ms");
    } else if (arg.rfind("--sample_interval_ms=", 0) == 0) {
      config.sample_interval_ms = ParseUint64(
          arg.substr(std::strlen("--sample_interval_ms=")),
          "sample_interval_ms");
    } else if (arg.rfind("--correctness_log_interval=", 0) == 0) {
      config.correctness_log_interval = ParseUint64(
          arg.substr(std::strlen("--correctness_log_interval=")),
          "correctness_log_interval");
    } else if (arg.rfind("--convergence_timeout_s=", 0) == 0) {
      config.convergence_timeout_s = ParseUint64(
          arg.substr(std::strlen("--convergence_timeout_s=")),
          "convergence_timeout_s");
    } else if (arg.rfind("--stability_window_s=", 0) == 0) {
      config.stability_window_s = ParseUint64(
          arg.substr(std::strlen("--stability_window_s=")),
          "stability_window_s");
    } else {
      Die("unknown argument: " + arg);
    }
  }

  if (config.total_bytes < (kKeyBytes + kValueBytes)) {
    Die("total_bytes is too small to insert even one key-value pair");
  }
  if (config.max_bytes_for_level_multiplier <= 0) {
    Die("max_bytes_for_level_multiplier must be positive");
  }
  if (config.range_width == 0) {
    Die("range_width must be positive");
  }
  if (config.max_background_jobs <= 0) {
    Die("max_background_jobs must be positive");
  }
  if (config.engine != "arce" && config.engine != "level") {
    Die("engine must be either arce or level");
  }
  return config;
}

const char* PhaseName(RunPhase phase) {
  switch (phase) {
    case RunPhase::kLoad:
      return "load";
    case RunPhase::kReadHeavy:
      return "read_heavy";
    case RunPhase::kCorrectness:
      return "correctness";
  }
  return "unknown";
}

std::string FormatBytes(uint64_t bytes) {
  static const char* const suffixes[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  double value = static_cast<double>(bytes);
  size_t suffix = 0;
  while (value >= 1024.0 && suffix + 1 < std::size(suffixes)) {
    value /= 1024.0;
    ++suffix;
  }
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(value >= 100.0 ? 1 : 2) << value
      << suffixes[suffix];
  return oss.str();
}

std::string MakeKey(uint64_t id) {
  std::ostringstream oss;
  oss << std::setw(static_cast<int>(kKeyBytes)) << std::setfill('0') << id;
  return oss.str();
}

std::string MakeValue(uint64_t id) {
  std::string key = MakeKey(id);
  std::string prefix = "key:" + key + ":";
  std::string value = prefix;
  value.reserve(kValueBytes);
  while (value.size() < kValueBytes) {
    value.push_back(static_cast<char>('a' + (id + value.size()) % 26));
  }
  value.resize(kValueBytes);
  return value;
}

uint64_t ChoosePermutationStep(uint64_t n, uint64_t seed) {
  if (n <= 1) {
    return 1;
  }
  uint64_t step = (seed % (n - 1)) + 1;
  while (std::gcd(step, n) != 1) {
    ++step;
    if (step >= n) {
      step = 1;
    }
  }
  return step;
}

uint64_t PermutedId(uint64_t ordinal, uint64_t key_count, uint64_t seed,
                    uint64_t step) {
  if (key_count == 0) {
    return 0;
  }
  const uint64_t offset = seed % key_count;
  return (offset + ((ordinal % key_count) * step) % key_count) % key_count;
}

LatencySummary SummarizeLatencies(std::vector<double> latencies) {
  LatencySummary summary;
  if (latencies.empty()) {
    return summary;
  }
  std::sort(latencies.begin(), latencies.end());
  auto percentile = [&](double p) {
    const size_t index =
        static_cast<size_t>(p * static_cast<double>(latencies.size() - 1));
    return latencies[index];
  };
  const double sum = std::accumulate(latencies.begin(), latencies.end(), 0.0);
  summary.avg_micros = sum / static_cast<double>(latencies.size());
  summary.p50_micros = percentile(0.50);
  summary.p95_micros = percentile(0.95);
  summary.p99_micros = percentile(0.99);
  summary.max_micros = latencies.back();
  return summary;
}

void PrintLatencySummary(const std::string& label,
                         const LatencySummary& summary) {
  std::cout << label << " latency (micros): avg=" << std::fixed
            << std::setprecision(2) << summary.avg_micros
            << " p50=" << summary.p50_micros << " p95=" << summary.p95_micros
            << " p99=" << summary.p99_micros << " max=" << summary.max_micros
            << std::endl;
}

void PrintHistogram(const std::shared_ptr<rocksdb::Statistics>& statistics,
                    rocksdb::Histograms histogram,
                    const std::string& label) {
  HistogramData data;
  statistics->histogramData(histogram, &data);
  std::cout << label << " histogram: count=" << data.count
            << " avg=" << data.average << " p95=" << data.percentile95
            << " p99=" << data.percentile99 << " max=" << data.max
            << std::endl;
}

std::vector<LevelStructureSummary> SummarizeArceStructure(
    const std::vector<LiveFileMetaData>& files) {
  std::map<int, std::set<uint64_t>> runs_by_level;
  std::map<int, uint64_t> files_by_level;

  for (const auto& file : files) {
    ++files_by_level[file.level];
    uint64_t run_id = file.level == 0 ? file.file_number : file.sorted_run_id;
    if (run_id == 0) {
      run_id = file.file_number;
    }
    runs_by_level[file.level].insert(run_id);
  }

  std::vector<LevelStructureSummary> summary;
  for (const auto& level_entry : files_by_level) {
    summary.push_back(LevelStructureSummary{
        level_entry.first,
        static_cast<uint64_t>(runs_by_level[level_entry.first].size()),
        level_entry.second});
  }
  return summary;
}

std::vector<LevelStructureSummary> SummarizeLeveledStructure(
    const std::vector<LiveFileMetaData>& files) {
  std::map<int, uint64_t> files_by_level;

  for (const auto& file : files) {
    ++files_by_level[file.level];
  }

  std::vector<LevelStructureSummary> summary;
  for (const auto& level_entry : files_by_level) {
    const int level = level_entry.first;
    const uint64_t file_count = level_entry.second;
    summary.push_back(
        LevelStructureSummary{level, level == 0 ? file_count : 1, file_count});
  }
  return summary;
}

std::vector<LevelStructureSummary> SummarizeStructure(
    const std::vector<LiveFileMetaData>& files, bool arce_style) {
  return arce_style ? SummarizeArceStructure(files)
                    : SummarizeLeveledStructure(files);
}

uint64_t TotalLogicalRunCount(
    const std::vector<LevelStructureSummary>& summary) {
  uint64_t total = 0;
  for (const auto& level : summary) {
    total += level.run_count;
  }
  return total;
}

std::string DescribeTree(const std::vector<LiveFileMetaData>& files) {
  std::map<int, std::vector<LiveFileMetaData>> by_level;
  for (const auto& file : files) {
    by_level[file.level].push_back(file);
  }

  std::ostringstream oss;
  for (const auto& level_entry : by_level) {
    const int level = level_entry.first;
    const auto& level_files = level_entry.second;
    oss << "L" << level << ":";

    if (level == 0) {
      std::vector<LiveFileMetaData> sorted_files = level_files;
      std::sort(sorted_files.begin(), sorted_files.end(),
                [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
                  return lhs.file_number > rhs.file_number;
                });
      oss << "[";
      bool first = true;
      for (const auto& file : sorted_files) {
        if (!first) {
          oss << ",";
        }
        first = false;
        oss << "[" << file.relative_filename << "]";
      }
      oss << "]";
      continue;
    }

    std::map<uint64_t, std::vector<LiveFileMetaData>, std::greater<uint64_t>>
        runs;
    for (const auto& file : level_files) {
      uint64_t run_id = file.sorted_run_id == 0 ? file.file_number
                                                : file.sorted_run_id;
      runs[run_id].push_back(file);
    }

    oss << "[";
    bool first_run = true;
    for (auto& run_entry : runs) {
      auto& run_files = run_entry.second;
      std::sort(run_files.begin(), run_files.end(),
                [](const LiveFileMetaData& lhs, const LiveFileMetaData& rhs) {
                  if (lhs.smallestkey != rhs.smallestkey) {
                    return lhs.smallestkey < rhs.smallestkey;
                  }
                  return lhs.file_number < rhs.file_number;
                });
      if (!first_run) {
        oss << ",";
      }
      first_run = false;
      oss << "run=" << run_entry.first << ":[";
      bool first_file = true;
      for (const auto& file : run_files) {
        if (!first_file) {
          oss << ",";
        }
        first_file = false;
        oss << file.relative_filename;
      }
      oss << "]";
    }
    oss << "]";
  }
  return oss.str();
}

void WriteStructureReport(const Config& config,
                          const std::vector<LiveFileMetaData>& live_files,
                          const std::string& tree_description) {
  if (config.structure_output_path.empty()) {
    return;
  }

  std::ofstream out(config.structure_output_path);
  if (!out.is_open()) {
    Die("failed to open structure_output_path: " +
        config.structure_output_path);
  }

  const auto summary = SummarizeStructure(live_files, config.engine == "arce");
  out << "db_path=" << config.db_path << "\n";
  out << "live_sst_files=" << live_files.size() << "\n";
  out << "total_sorted_runs=" << TotalLogicalRunCount(summary) << "\n";
  for (const auto& level : summary) {
    out << "L" << level.level << ": runs=" << level.run_count
        << " files=" << level.file_count << "\n";
  }
  out << "tree=" << tree_description << "\n";
}

void WriteTimeSeriesReport(const Config& config,
                           const std::vector<TimeSeriesSample>& samples) {
  if (config.timeseries_output_path.empty()) {
    return;
  }

  std::ofstream out(config.timeseries_output_path);
  if (!out.is_open()) {
    Die("failed to open timeseries_output_path: " +
        config.timeseries_output_path);
  }

  out << "elapsed_seconds,phase,M,c,logical_runs,live_sst_files,point_avg_micros,"
         "range_avg_micros,insert_avg_micros,point_ops,range_ops,insert_ops\n";
  out << std::fixed << std::setprecision(3);
  for (const auto& sample : samples) {
    out << sample.elapsed_seconds << "," << PhaseName(sample.phase) << ","
        << sample.M << "," << sample.c << "," << sample.logical_runs << ","
        << sample.live_sst_files << "," << sample.point_avg_micros << ","
        << sample.range_avg_micros << ","
        << sample.insert_avg_micros << "," << sample.point_ops << ","
        << sample.range_ops << "," << sample.insert_ops << "\n";
  }
}

void WriteRawStatsReport(const Config& config, const std::string& db_stats,
                         const std::string& cf_stats) {
  if (config.raw_stats_output_path.empty()) {
    return;
  }

  std::ofstream out(config.raw_stats_output_path);
  if (!out.is_open()) {
    Die("failed to open raw_stats_output_path: " +
        config.raw_stats_output_path);
  }
  out << "rocksdb.stats\n" << db_stats << "\n";
  out << "rocksdb.cfstats\n" << cf_stats << "\n";
}

TreeState BuildTreeState(const std::vector<LiveFileMetaData>& files) {
  std::map<int, std::vector<LiveFileMetaData>> by_level;
  int max_level = 0;
  for (const auto& file : files) {
    by_level[file.level].push_back(file);
    max_level = std::max(max_level, file.level);
  }

  TreeState state;
  state.level_runs.resize(static_cast<size_t>(max_level + 1));
  for (int level = 0; level <= max_level; ++level) {
    auto iter = by_level.find(level);
    if (iter == by_level.end()) {
      continue;
    }

    const auto& level_files = iter->second;
    if (level == 0) {
      for (const auto& file : level_files) {
        state.level_runs[level].push_back(
            static_cast<int64_t>(file.size));
      }
      continue;
    }

    std::map<uint64_t, int64_t, std::greater<uint64_t>> run_sizes;
    for (const auto& file : level_files) {
      uint64_t run_id = file.sorted_run_id == 0 ? file.file_number
                                                : file.sorted_run_id;
      run_sizes[run_id] += static_cast<int64_t>(file.size);
    }
    for (const auto& [run_id, run_size] : run_sizes) {
      (void)run_id;
      state.level_runs[level].push_back(run_size);
    }
    std::sort(state.level_runs[level].begin(), state.level_runs[level].end(),
              std::greater<int64_t>());
  }

  state.total_runs = 0;
  state.max_level_runs = 0;
  for (const auto& level_runs : state.level_runs) {
    state.total_runs += static_cast<int>(level_runs.size());
    state.max_level_runs = std::max(
        state.max_level_runs, static_cast<int>(level_runs.size()));
  }
  return state;
}

void RefreshMcFromState(DB* db, ArceCompactionController* controller) {
  if (controller == nullptr) {
    return;
  }
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  TreeState state = BuildTreeState(live_files);
  if (state.level_runs.empty()) {
    return;
  }
  auto [r, u, p] = controller->GetWorkload();
  auto new_mc = FindBestMcParallel(
      state, controller->buffer_size, r, u, p, controller->wait_io,
      controller->mc_search_len, controller->remaining_window_cnt,
      controller->parallel_factor);
  controller->SetMc(new_mc);
  controller->latest_run_num.store(state.total_runs);
}

void LogControllerState(DB* db, ArceCompactionController* controller,
                        const std::string& label) {
  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  const auto summary = SummarizeStructure(live_files, controller != nullptr);
  if (controller == nullptr) {
    std::cout << "  [" << label << "] runs=" << TotalLogicalRunCount(summary)
              << " tree=" << DescribeTree(live_files) << std::endl;
    return;
  }
  const auto [r, u, p] = controller->GetWorkload();
  const auto [M, c] = controller->GetMc();
  std::cout << "  [" << label << "] runs=" << TotalLogicalRunCount(summary)
            << " workload=(" << r << "," << u << "," << p << ")"
            << " mc=(" << M << "," << c << ")"
            << " tree=" << DescribeTree(live_files) << std::endl;
}

void RecordLatencySample(SampleRecorder* recorder, OperationKind op_kind,
                         uint64_t nanos) {
  auto& counter = recorder->op_counters[static_cast<size_t>(op_kind)];
  counter.count.fetch_add(1, std::memory_order_relaxed);
  counter.total_nanos.fetch_add(nanos, std::memory_order_relaxed);
}

void SamplingLoop(DB* db, ArceCompactionController* controller,
                  SampleRecorder* recorder, std::atomic<bool>* stop,
                  std::atomic<int>* phase, uint64_t sample_interval_ms) {
  std::array<uint64_t, static_cast<size_t>(OperationKind::kCount)> last_counts{};
  std::array<uint64_t, static_cast<size_t>(OperationKind::kCount)> last_nanos{};
  const auto start = std::chrono::steady_clock::now();

  while (!stop->load()) {
    std::vector<LiveFileMetaData> live_files;
    db->GetLiveFilesMetaData(&live_files);
    const auto summary = SummarizeStructure(live_files, controller != nullptr);
    int M = 0;
    int c = 0;
    if (controller != nullptr) {
      std::tie(M, c) = controller->GetMc();
    }

    TimeSeriesSample sample;
    sample.elapsed_seconds =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - start)
            .count();
    sample.phase = static_cast<RunPhase>(phase->load(std::memory_order_relaxed));
    sample.M = M;
    sample.c = c;
    sample.logical_runs = TotalLogicalRunCount(summary);
    sample.live_sst_files = live_files.size();

    auto fill_op = [&](OperationKind kind, double* avg_micros, uint64_t* ops) {
      const size_t index = static_cast<size_t>(kind);
      const uint64_t count = recorder->op_counters[index].count.load(
          std::memory_order_relaxed);
      const uint64_t nanos = recorder->op_counters[index].total_nanos.load(
          std::memory_order_relaxed);
      const uint64_t delta_count = count - last_counts[index];
      const uint64_t delta_nanos = nanos - last_nanos[index];
      last_counts[index] = count;
      last_nanos[index] = nanos;
      *ops = delta_count;
      if (delta_count == 0) {
        *avg_micros = 0;
      } else {
        *avg_micros = static_cast<double>(delta_nanos) /
                      static_cast<double>(delta_count) / 1000.0;
      }
    };

    fill_op(OperationKind::kPoint, &sample.point_avg_micros, &sample.point_ops);
    fill_op(OperationKind::kRange, &sample.range_avg_micros, &sample.range_ops);
    fill_op(OperationKind::kInsert, &sample.insert_avg_micros,
            &sample.insert_ops);

    {
      std::lock_guard<std::mutex> lock(recorder->samples_mu);
      recorder->samples.push_back(sample);
    }

    std::cout << "  [sample] t=" << std::fixed << std::setprecision(1)
              << sample.elapsed_seconds << "s phase=" << PhaseName(sample.phase)
              << " runs=" << sample.logical_runs << " files="
              << sample.live_sst_files << " mc=(" << sample.M << ","
              << sample.c << ") point_avg_us=" << std::setprecision(2)
              << sample.point_avg_micros << " range_avg_us="
              << sample.range_avg_micros << " insert_avg_us="
              << sample.insert_avg_micros << " point_ops=" << sample.point_ops
              << " range_ops=" << sample.range_ops
              << " insert_ops=" << sample.insert_ops << std::endl;

    std::this_thread::sleep_for(std::chrono::milliseconds(sample_interval_ms));
  }
}

void McRefreshLoop(DB* db, ArceCompactionController* controller,
                   std::atomic<bool>* stop, uint64_t refresh_interval_ms) {
  if (controller == nullptr) {
    return;
  }
  std::pair<int, int> last_mc = controller->GetMc();
  int last_runs = -1;
  while (!stop->load()) {
    RefreshMcFromState(db, controller);
    auto new_mc = controller->GetMc();
    const int new_runs = controller->latest_run_num.load();
    if (new_mc != last_mc || new_runs != last_runs) {
      LogControllerState(db, controller, "refresh");
      last_mc = new_mc;
      last_runs = new_runs;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(refresh_interval_ms));
  }
}

class IntegrationListener : public rocksdb::EventListener {
 public:
  explicit IntegrationListener(ArceCompactionController* controller)
      : controller_(controller) {}

  void OnCompactionCompleted(rocksdb::DB* db,
                             const rocksdb::CompactionJobInfo& info) override {
    std::cout << "  [compaction-complete] output_level=" << info.output_level
              << " input_files=" << info.input_files.size()
              << " output_files=" << info.output_files.size() << std::endl;
    LogControllerState(db, controller_, "compaction-complete");
  }

  void OnFlushCompleted(rocksdb::DB* db,
                        const rocksdb::FlushJobInfo& info) override {
    std::cout << "  [flush-complete] file=" << info.file_path << std::endl;
    LogControllerState(db, controller_, "flush-complete");
  }

 private:
  ArceCompactionController* controller_;
};

uint64_t PrefillDatabase(DB* db, uint64_t key_count, uint64_t seed,
                         size_t batch_size) {
  const uint64_t permutation_step =
      ChoosePermutationStep(key_count, seed ^ 0x94d049bb133111ebULL);
  WriteOptions write_options;
  uint64_t inserted = 0;
  while (inserted < key_count) {
    WriteBatch batch;
    const uint64_t batch_end =
        std::min<uint64_t>(inserted + batch_size, key_count);
    for (; inserted < batch_end; ++inserted) {
      const uint64_t id = PermutedId(inserted, key_count, seed, permutation_step);
      batch.Put(MakeKey(id), MakeValue(id));
    }
    Status status = db->Write(write_options, &batch);
    if (!status.ok()) {
      Die("Write failed during prefill: " + status.ToString());
    }
    if (inserted % (1ULL << 18) == 0 || inserted == key_count) {
      std::cout << "  inserted " << inserted << "/" << key_count << " keys ("
                << FormatBytes(inserted * (kKeyBytes + kValueBytes)) << ")"
                << std::endl;
    }
  }
  return inserted;
}

void VerifyPointLookup(DB* db, uint64_t id, std::vector<double>* latencies) {
  std::string value;
  const auto start = std::chrono::steady_clock::now();
  Status status = db->Get(ReadOptions(), MakeKey(id), &value);
  const auto end = std::chrono::steady_clock::now();
  latencies->push_back(
      std::chrono::duration<double, std::micro>(end - start).count());
  if (!status.ok()) {
    Die("Get failed for key " + MakeKey(id) + ": " + status.ToString());
  }
  if (value != MakeValue(id)) {
    Die("Get returned incorrect value for key " + MakeKey(id));
  }
}

void VerifyRangeLookup(DB* db, uint64_t start_id, uint64_t width,
                       std::vector<double>* latencies) {
  const auto begin = std::chrono::steady_clock::now();
  std::unique_ptr<Iterator> it(db->NewIterator(ReadOptions()));
  it->Seek(MakeKey(start_id));
  for (uint64_t offset = 0; offset < width; ++offset) {
    if (!it->Valid()) {
      Die("iterator ended early while verifying range lookup");
    }
    const uint64_t id = start_id + offset;
    if (it->key().ToString() != MakeKey(id)) {
      Die("iterator returned incorrect key inside range lookup");
    }
    if (it->value().ToString() != MakeValue(id)) {
      Die("iterator returned incorrect value inside range lookup");
    }
    it->Next();
  }
  const Status status = it->status();
  const auto end = std::chrono::steady_clock::now();
  latencies->push_back(
      std::chrono::duration<double, std::micro>(end - begin).count());
  if (!status.ok()) {
    Die("iterator status is not ok: " + status.ToString());
  }
}

void InsertAndVerify(DB* db, uint64_t id, std::vector<double>* latencies,
                     SampleRecorder* recorder) {
  const std::string key = MakeKey(id);
  const std::string value = MakeValue(id);
  const auto begin = std::chrono::steady_clock::now();
  Status status = db->Put(WriteOptions(), key, value);
  const auto end = std::chrono::steady_clock::now();
  const auto nanos =
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - begin).count();
  latencies->push_back(static_cast<double>(nanos) / 1000.0);
  RecordLatencySample(recorder, OperationKind::kInsert,
                      static_cast<uint64_t>(nanos));
  if (!status.ok()) {
    Die("Put failed for key " + key + ": " + status.ToString());
  }
  std::string fetched;
  status = db->Get(ReadOptions(), key, &fetched);
  if (!status.ok() || fetched != value) {
    Die("verification Get failed after insert for key " + key);
  }
}

VerificationStats RunMixedCorrectnessWorkload(DB* db, uint64_t operations,
                                              uint64_t range_width,
                                              uint64_t seed, uint64_t log_interval,
                                              ArceCompactionController* controller,
                                              uint64_t* next_key_id,
                                              SampleRecorder* recorder) {
  VerificationStats stats;
  std::mt19937_64 rng(seed ^ 0xfeedfacedeadbeefULL);
  std::uniform_int_distribution<int> op_dist(0, 2);

  for (uint64_t i = 0; i < operations; ++i) {
    const int op = op_dist(rng);
    if (op == 0) {
      std::uniform_int_distribution<uint64_t> id_dist(0, *next_key_id - 1);
      const auto before = stats.point_latencies.size();
      VerifyPointLookup(db, id_dist(rng), &stats.point_latencies);
      if (stats.point_latencies.size() > before) {
        RecordLatencySample(
            recorder, OperationKind::kPoint,
            static_cast<uint64_t>(stats.point_latencies.back() * 1000.0));
      }
    } else if (op == 1) {
      const uint64_t width = std::min<uint64_t>(range_width, *next_key_id);
      const uint64_t max_start = *next_key_id - width;
      std::uniform_int_distribution<uint64_t> start_dist(0, max_start);
      const auto before = stats.range_latencies.size();
      VerifyRangeLookup(db, start_dist(rng), width, &stats.range_latencies);
      if (stats.range_latencies.size() > before) {
        RecordLatencySample(
            recorder, OperationKind::kRange,
            static_cast<uint64_t>(stats.range_latencies.back() * 1000.0));
      }
    } else {
      InsertAndVerify(db, *next_key_id, &stats.insert_latencies, recorder);
      ++(*next_key_id);
    }
    if (log_interval > 0 && ((i + 1) % log_interval == 0 || i + 1 == operations)) {
      std::cout << "  [correctness-progress] ops=" << (i + 1)
                << " next_key_id=" << *next_key_id << std::endl;
      LogControllerState(db, controller, "correctness-progress");
    }
  }
  return stats;
}

bool WaitForRunCountImprovement(DB* db, uint64_t initial_total_runs,
                                uint64_t stability_window_s,
                                uint64_t timeout_s, uint64_t* final_runs,
                                std::vector<LiveFileMetaData>* final_live_files) {
  const uint64_t target_runs =
      std::max<uint64_t>(2, std::min<uint64_t>(8, initial_total_runs / 2));
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(timeout_s);
  const auto stable_window = std::chrono::seconds(stability_window_s);

  uint64_t best_runs = initial_total_runs;
  auto best_time = std::chrono::steady_clock::now();

  while (std::chrono::steady_clock::now() < deadline) {
    final_live_files->clear();
    db->GetLiveFilesMetaData(final_live_files);
    const uint64_t runs = TotalLogicalRunCount(
        SummarizeStructure(*final_live_files, true));
    if (runs < best_runs) {
      best_runs = runs;
      best_time = std::chrono::steady_clock::now();
      std::cout << "  run count improved to " << best_runs << std::endl;
    }
    if (best_runs <= target_runs &&
        std::chrono::steady_clock::now() - best_time >= stable_window) {
      *final_runs = best_runs;
      return true;
    }
    std::this_thread::sleep_for(std::chrono::seconds(2));
  }

  *final_runs = best_runs;
  return best_runs < initial_total_runs;
}

}  // namespace

int main(int argc, char** argv) {
  const Config config = ParseArgs(argc, argv);
  const uint64_t record_bytes = kKeyBytes + kValueBytes;
  const uint64_t initial_key_count = config.total_bytes / record_bytes;
  const uint64_t logical_bytes = initial_key_count * record_bytes;

  std::cout << "Compaction integration run" << std::endl;
  std::cout << "  engine: " << config.engine << std::endl;
  std::cout << "  db_path: " << config.db_path << std::endl;
  std::cout << "  logical bytes: " << logical_bytes << " ("
            << FormatBytes(logical_bytes) << ")" << std::endl;
  std::cout << "  initial key count: " << initial_key_count << std::endl;
  std::cout << "  correctness_ops: " << config.correctness_ops << std::endl;
  std::cout << "  range_width: " << config.range_width << std::endl;
  std::cout << "  write_buffer_size: "
            << FormatBytes(config.write_buffer_size) << std::endl;
  std::cout << "  max_background_jobs: " << config.max_background_jobs
            << std::endl;
  std::cout << "  correctness_log_interval: "
            << config.correctness_log_interval << std::endl;
  std::cout << "  sample_interval_ms: " << config.sample_interval_ms
            << std::endl;
  std::cout << "  bloom_filter: "
            << (config.enable_bloom ? "enabled" : "disabled") << std::endl;

  std::shared_ptr<ArceCompactionController> controller;
  if (config.engine == "arce") {
    controller = std::make_shared<ArceCompactionController>();
    controller->enable_dynamic_parameter_selection = false;
    controller->SetBufferSize(static_cast<int64_t>(config.write_buffer_size));
    controller->SetEntrySize(static_cast<int64_t>(kKeyBytes + kValueBytes));
    controller->parallel_factor = config.max_background_jobs;
    controller->SetWorkloadProportions(kLoadR, kLoadU, kLoadP);
    controller->SetMc({100, 1000000});
  }

  Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.compression = rocksdb::kNoCompression;
  options.compaction_style = config.engine == "arce"
                                 ? rocksdb::kCompactionStyleArce
                                 : rocksdb::kCompactionStyleLevel;
  options.disable_auto_compactions = false;
  options.write_buffer_size = config.write_buffer_size;
  options.target_file_size_base = config.target_file_size_base;
  options.max_bytes_for_level_base = config.max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier =
      config.max_bytes_for_level_multiplier;
  options.max_background_jobs = config.max_background_jobs;
  options.IncreaseParallelism(config.max_background_jobs);
  options.max_write_buffer_number = 4;
  options.level0_file_num_compaction_trigger = 1000000;
  options.level0_slowdown_writes_trigger = 1000000;
  options.level0_stop_writes_trigger = 1000000;
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->set_stats_level(rocksdb::StatsLevel::kAll);
  if (config.engine == "arce") {
    options.arce_compaction_controller = controller;
  }
  options.listeners.emplace_back(
      std::make_shared<IntegrationListener>(controller.get()));
  if (config.enable_bloom) {
    rocksdb::BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
    table_options.whole_key_filtering = true;
    options.table_factory.reset(
        rocksdb::NewBlockBasedTableFactory(table_options));
  }

  Status status = rocksdb::DestroyDB(config.db_path, options);
  if (!status.ok() && !status.IsNotFound()) {
    Die("DestroyDB failed before run: " + status.ToString());
  }

  std::unique_ptr<DB> db;
  status = DB::Open(options, config.db_path, &db);
  if (!status.ok()) {
    Die("DB::Open failed: " + status.ToString());
  }

  std::atomic<bool> stop_refresh{false};
  std::atomic<bool> stop_sampling{false};
  std::atomic<int> phase{static_cast<int>(RunPhase::kLoad)};
  SampleRecorder recorder;
  std::thread refresh_thread([&] {
    McRefreshLoop(db.get(), controller.get(), &stop_refresh,
                  config.refresh_interval_ms);
  });
  std::thread sampling_thread([&] {
    SamplingLoop(db.get(), controller.get(), &recorder, &stop_sampling, &phase,
                 config.sample_interval_ms);
  });

  const auto write_start = std::chrono::steady_clock::now();
  PrefillDatabase(db.get(), initial_key_count, config.seed, kDefaultBatchSize);
  FlushOptions flush_options;
  flush_options.wait = true;
  status = db->Flush(flush_options);
  if (!status.ok()) {
    Die("Flush failed after prefill: " + status.ToString());
  }
  const auto write_end = std::chrono::steady_clock::now();
  const double write_seconds =
      std::chrono::duration<double>(write_end - write_start).count();

  std::vector<LiveFileMetaData> initial_live_files;
  db->GetLiveFilesMetaData(&initial_live_files);
  const uint64_t initial_total_runs =
      TotalLogicalRunCount(SummarizeStructure(initial_live_files, true));
  std::cout << "  initial total runs after prefill: " << initial_total_runs
            << std::endl;

  if (controller != nullptr) {
    controller->SetWorkloadProportions(kCompactR, kCompactU, kCompactP);
  }
  phase.store(static_cast<int>(RunPhase::kReadHeavy), std::memory_order_relaxed);
  if (controller != nullptr) {
    std::cout << "  switched workload to read-heavy compaction pressure ("
              << kCompactR << "," << kCompactU << "," << kCompactP << ")"
              << std::endl;

    uint64_t converged_runs = initial_total_runs;
    std::vector<LiveFileMetaData> converged_live_files;
    if (!WaitForRunCountImprovement(db.get(), initial_total_runs,
                                    config.stability_window_s,
                                    config.convergence_timeout_s,
                                    &converged_runs,
                                    &converged_live_files)) {
      Die("run count did not improve under read-heavy workload");
    }
    if (converged_runs >= initial_total_runs) {
      Die("read-heavy workload did not reduce total run count");
    }
  } else {
    std::cout << "  default leveled engine: waiting for background compaction before correctness phase"
              << std::endl;
  }

  WaitForCompactOptions wait_options;
  wait_options.timeout =
      std::chrono::seconds(config.convergence_timeout_s);
  status = db->WaitForCompact(wait_options);
  if (!status.ok() && !status.IsTimedOut()) {
    Die("WaitForCompact failed: " + status.ToString());
  }

  if (controller != nullptr) {
    controller->SetWorkloadProportions(kVerifyR, kVerifyU, kVerifyP);
  }
  phase.store(static_cast<int>(RunPhase::kCorrectness),
              std::memory_order_relaxed);
  std::cout << "  switched workload to correctness mix (" << kVerifyR << ","
            << kVerifyU << "," << kVerifyP << ")" << std::endl;

  uint64_t next_key_id = initial_key_count;
  VerificationStats verification =
      RunMixedCorrectnessWorkload(db.get(), config.correctness_ops,
                                  config.range_width, config.seed,
                                  config.correctness_log_interval,
                                  controller.get(),
                                  &next_key_id, &recorder);

  stop_refresh.store(true);
  stop_sampling.store(true);
  refresh_thread.join();
  sampling_thread.join();

  std::vector<LiveFileMetaData> final_live_files;
  db->GetLiveFilesMetaData(&final_live_files);
  const std::string tree_description = DescribeTree(final_live_files);
  const auto structure_summary =
      SummarizeStructure(final_live_files, config.engine == "arce");
  std::cout << "Live SST files: " << final_live_files.size() << std::endl;
  std::cout << "Total sorted runs: " << TotalLogicalRunCount(structure_summary)
            << std::endl;
  for (const auto& level : structure_summary) {
    std::cout << "  L" << level.level << ": runs=" << level.run_count
              << " files=" << level.file_count << std::endl;
  }
  if (controller != nullptr) {
    auto final_mc = controller->GetMc();
    std::cout << "Final (M,c)=(" << final_mc.first << "," << final_mc.second
              << ")" << std::endl;
    std::cout << "Arce dynamic tree: " << tree_description << std::endl;
  } else {
    std::cout << "Final tree: " << tree_description << std::endl;
  }
  WriteStructureReport(config, final_live_files, tree_description);
  {
    std::lock_guard<std::mutex> lock(recorder.samples_mu);
    WriteTimeSeriesReport(config, recorder.samples);
  }

  PrintLatencySummary("Point lookup",
                      SummarizeLatencies(verification.point_latencies));
  PrintLatencySummary("Range lookup",
                      SummarizeLatencies(verification.range_latencies));
  PrintLatencySummary("Insert", SummarizeLatencies(verification.insert_latencies));

  std::cout << "Prefill throughput: "
            << (write_seconds > 0.0
                    ? FormatBytes(
                          static_cast<uint64_t>(logical_bytes / write_seconds))
                    : "inf")
            << "/s" << std::endl;

  PrintHistogram(options.statistics, rocksdb::DB_WRITE, "DB_WRITE");
  PrintHistogram(options.statistics, rocksdb::DB_GET, "DB_GET");
  PrintHistogram(options.statistics, rocksdb::DB_SEEK, "DB_SEEK");
  PrintHistogram(options.statistics, rocksdb::FILE_READ_GET_MICROS,
                 "FILE_READ_GET_MICROS");
  PrintHistogram(options.statistics, rocksdb::FILE_READ_DB_ITERATOR_MICROS,
                 "FILE_READ_DB_ITERATOR_MICROS");

  std::string db_stats;
  if (db->GetProperty("rocksdb.stats", &db_stats)) {
    std::cout << "rocksdb.stats\n" << db_stats << std::endl;
  }

  std::string cf_stats;
  if (db->GetProperty("rocksdb.cfstats", &cf_stats)) {
    std::cout << "rocksdb.cfstats\n" << cf_stats << std::endl;
  }
  WriteRawStatsReport(config, db_stats, cf_stats);

  db.reset();

  if (!config.keep_db) {
    status = rocksdb::DestroyDB(config.db_path, options);
    if (!status.ok()) {
      Die("DestroyDB failed after run: " + status.ToString());
    }
  }

  std::cout << "integration run completed successfully" << std::endl;
  return 0;
}
