// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <fstream>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/metadata.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/statistics.h"

namespace {

using rocksdb::CompactRangeOptions;
using rocksdb::DB;
using rocksdb::FlushOptions;
using rocksdb::HistogramData;
using rocksdb::Iterator;
using rocksdb::LiveFileMetaData;
using rocksdb::Options;
using rocksdb::ReadOptions;
using rocksdb::Status;
using rocksdb::WaitForCompactOptions;
using rocksdb::WriteBatch;
using rocksdb::WriteOptions;

constexpr uint64_t kKeyBytes = 24;
constexpr uint64_t kValueBytes = 1000;
constexpr uint64_t kDefaultTotalBytes = 20ULL * 1024ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultWriteBufferSize = 2ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultTargetFileSize = 1ULL * 1024ULL * 1024ULL;
constexpr uint64_t kDefaultMaxBytesForLevelBase = 4ULL * 1024ULL * 1024ULL;
constexpr int kDefaultMaxBytesForLevelMultiplier = 4;
constexpr uint64_t kDefaultPointLookups = 5000;
constexpr uint64_t kDefaultRangeLookups = 200;
constexpr uint64_t kDefaultRangeWidth = 128;
constexpr size_t kDefaultBatchSize = 128;

#if defined(OS_WIN)
const char kDefaultDbPath[] =
    "C:\\Windows\\TEMP\\rocksdb_tiered_compaction_integration";
#else
const char kDefaultDbPath[] = "/tmp/rocksdb_tiered_compaction_integration";
#endif

struct Config {
  std::string db_path = kDefaultDbPath;
  std::string structure_output_path;
  uint64_t total_bytes = kDefaultTotalBytes;
  uint64_t write_buffer_size = kDefaultWriteBufferSize;
  uint64_t target_file_size_base = kDefaultTargetFileSize;
  uint64_t max_bytes_for_level_base = kDefaultMaxBytesForLevelBase;
  int max_bytes_for_level_multiplier = kDefaultMaxBytesForLevelMultiplier;
  uint64_t point_lookups = kDefaultPointLookups;
  uint64_t range_lookups = kDefaultRangeLookups;
  uint64_t range_width = kDefaultRangeWidth;
  uint64_t seed = 20260422;
  std::optional<int> max_background_jobs;
  bool keep_db = false;
};

struct LatencySummary {
  double avg_micros = 0.0;
  double p50_micros = 0.0;
  double p95_micros = 0.0;
  double p99_micros = 0.0;
  double max_micros = 0.0;
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
      << "  --db_path=PATH\n"
      << "  --structure_output_path=PATH Persist final tree and run counts.\n"
      << "  --total_bytes=N               Total logical KV bytes to insert.\n"
      << "  --write_buffer_size=N         Memtable size in bytes.\n"
      << "  --target_file_size_base=N\n"
      << "  --max_bytes_for_level_base=N\n"
      << "  --max_bytes_for_level_multiplier=N\n"
      << "  --max_background_jobs=N        Override RocksDB background jobs.\n"
      << "  --point_lookups=N\n"
      << "  --range_lookups=N\n"
      << "  --range_width=N\n"
      << "  --seed=N\n"
      << "  --keep_db                     Do not destroy the DB on exit.\n"
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
    } else if (arg.rfind("--db_path=", 0) == 0) {
      config.db_path = arg.substr(std::strlen("--db_path="));
    } else if (arg.rfind("--structure_output_path=", 0) == 0) {
      config.structure_output_path =
          arg.substr(std::strlen("--structure_output_path="));
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
    } else if (arg.rfind("--max_background_jobs=", 0) == 0) {
      config.max_background_jobs = ParseInt(
          arg.substr(std::strlen("--max_background_jobs=")),
          "max_background_jobs");
    } else if (arg.rfind("--point_lookups=", 0) == 0) {
      config.point_lookups = ParseUint64(
          arg.substr(std::strlen("--point_lookups=")), "point_lookups");
    } else if (arg.rfind("--range_lookups=", 0) == 0) {
      config.range_lookups = ParseUint64(
          arg.substr(std::strlen("--range_lookups=")), "range_lookups");
    } else if (arg.rfind("--range_width=", 0) == 0) {
      config.range_width = ParseUint64(
          arg.substr(std::strlen("--range_width=")), "range_width");
    } else if (arg.rfind("--seed=", 0) == 0) {
      config.seed = ParseUint64(arg.substr(std::strlen("--seed=")), "seed");
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
  if (config.max_background_jobs.has_value() &&
      *config.max_background_jobs <= 0) {
    Die("max_background_jobs must be positive");
  }
  return config;
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
  std::string value = "value:" + key + ":";
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
  const double sum =
      std::accumulate(latencies.begin(), latencies.end(), 0.0);
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
      uint64_t run_id = file.sorted_run_id;
      if (run_id == 0) {
        run_id = file.file_number;
      }
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

struct LevelStructureSummary {
  int level = 0;
  uint64_t run_count = 0;
  uint64_t file_count = 0;
};

std::vector<LevelStructureSummary> SummarizeStructure(
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
    LevelStructureSummary level_summary;
    level_summary.level = level_entry.first;
    level_summary.file_count = level_entry.second;
    level_summary.run_count =
        static_cast<uint64_t>(runs_by_level[level_entry.first].size());
    summary.push_back(level_summary);
  }
  return summary;
}

uint64_t TotalRunCount(const std::vector<LevelStructureSummary>& summary) {
  uint64_t total = 0;
  for (const auto& level : summary) {
    total += level.run_count;
  }
  return total;
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

  const auto summary = SummarizeStructure(live_files);
  out << "db_path=" << config.db_path << "\n";
  out << "live_sst_files=" << live_files.size() << "\n";
  out << "total_sorted_runs=" << TotalRunCount(summary) << "\n";
  for (const auto& level : summary) {
    out << "L" << level.level << ": runs=" << level.run_count
        << " files=" << level.file_count << "\n";
  }
  out << "tree=" << tree_description << "\n";
  out.close();
  if (!out) {
    Die("failed while writing structure_output_path: " +
        config.structure_output_path);
  }
}

void VerifyPointLookups(DB* db, uint64_t key_count, uint64_t sample_count,
                        uint64_t seed, std::vector<double>* latencies) {
  ReadOptions read_options;
  uint64_t step = ChoosePermutationStep(key_count, seed ^ 0x9e3779b97f4a7c15ULL);

  for (uint64_t i = 0; i < sample_count; ++i) {
    const uint64_t id = PermutedId(i, key_count, seed + 17, step);
    const std::string key = MakeKey(id);
    const std::string expected_value = MakeValue(id);

    std::string value;
    const auto start = std::chrono::steady_clock::now();
    Status status = db->Get(read_options, key, &value);
    const auto end = std::chrono::steady_clock::now();
    latencies->push_back(
        std::chrono::duration<double, std::micro>(end - start).count());

    if (!status.ok()) {
      Die("Get failed for key " + key + ": " + status.ToString());
    }
    if (value != expected_value) {
      Die("Get returned incorrect value for key " + key);
    }
  }
}

void VerifyRangeLookups(DB* db, uint64_t key_count, uint64_t sample_count,
                        uint64_t width, uint64_t seed,
                        std::vector<double>* latencies) {
  if (key_count == 0) {
    return;
  }
  if (width > key_count) {
    width = key_count;
  }

  ReadOptions read_options;
  uint64_t step = ChoosePermutationStep(key_count, seed ^ 0xd1b54a32d192ed03ULL);

  for (uint64_t i = 0; i < sample_count; ++i) {
    const uint64_t max_start = key_count - width;
    const uint64_t start_id =
        max_start == 0 ? 0 : PermutedId(i, max_start + 1, seed + 31, step);
    const std::string start_key = MakeKey(start_id);

    const auto begin = std::chrono::steady_clock::now();
    std::unique_ptr<Iterator> it(db->NewIterator(read_options));
    it->Seek(start_key);

    for (uint64_t offset = 0; offset < width; ++offset) {
      if (!it->Valid()) {
        Die("iterator ended early while verifying range lookup");
      }
      const uint64_t id = start_id + offset;
      const std::string expected_key = MakeKey(id);
      const std::string expected_value = MakeValue(id);
      if (it->key().ToString() != expected_key) {
        Die("iterator returned incorrect key inside range lookup");
      }
      if (it->value().ToString() != expected_value) {
        Die("iterator returned incorrect value inside range lookup");
      }
      it->Next();
    }
    Status status = it->status();
    const auto end = std::chrono::steady_clock::now();
    latencies->push_back(
        std::chrono::duration<double, std::micro>(end - begin).count());
    if (!status.ok()) {
      Die("iterator status is not ok: " + status.ToString());
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  const Config config = ParseArgs(argc, argv);
  const uint64_t record_bytes = kKeyBytes + kValueBytes;
  const uint64_t key_count = config.total_bytes / record_bytes;
  const uint64_t logical_bytes = key_count * record_bytes;
  const uint64_t permutation_step =
      ChoosePermutationStep(key_count, config.seed ^ 0x94d049bb133111ebULL);

  std::cout << "Tiered compaction integration run" << std::endl;
  std::cout << "  db_path: " << config.db_path << std::endl;
  if (!config.structure_output_path.empty()) {
    std::cout << "  structure_output_path: " << config.structure_output_path
              << std::endl;
  }
  std::cout << "  total logical bytes: " << logical_bytes << " ("
            << FormatBytes(logical_bytes) << ")" << std::endl;
  std::cout << "  key count: " << key_count << std::endl;
  std::cout << "  key size: " << kKeyBytes << "B" << std::endl;
  std::cout << "  value size: " << kValueBytes << "B" << std::endl;
  std::cout << "  write_buffer_size: "
            << FormatBytes(config.write_buffer_size) << std::endl;
  std::cout << "  tiered run threshold T: "
            << config.max_bytes_for_level_multiplier << std::endl;
  std::cout << "  max_background_jobs: ";
  if (config.max_background_jobs.has_value()) {
    std::cout << *config.max_background_jobs << " (override)";
  } else {
    std::cout << "default";
  }
  std::cout << std::endl;

  Options options;
  options.create_if_missing = true;
  options.error_if_exists = true;
  options.compression = rocksdb::kNoCompression;
  options.compaction_style = rocksdb::kCompactionStyleTiered;
  options.write_buffer_size = config.write_buffer_size;
  options.target_file_size_base = config.target_file_size_base;
  options.max_bytes_for_level_base = config.max_bytes_for_level_base;
  options.max_bytes_for_level_multiplier =
      config.max_bytes_for_level_multiplier;
  options.level0_file_num_compaction_trigger = 1000;
  options.max_write_buffer_number = 4;
  if (config.max_background_jobs.has_value()) {
    options.max_background_jobs = *config.max_background_jobs;
    options.IncreaseParallelism(*config.max_background_jobs);
  }
  options.statistics = rocksdb::CreateDBStatistics();
  options.statistics->set_stats_level(rocksdb::StatsLevel::kAll);

  Status status = rocksdb::DestroyDB(config.db_path, options);
  if (!status.ok() && !status.IsNotFound()) {
    Die("DestroyDB failed before run: " + status.ToString());
  }

  std::unique_ptr<DB> db;
  status = DB::Open(options, config.db_path, &db);
  if (!status.ok()) {
    Die("DB::Open failed: " + status.ToString());
  }

  const auto write_start = std::chrono::steady_clock::now();
  uint64_t inserted = 0;
  WriteOptions write_options;
  while (inserted < key_count) {
    WriteBatch batch;
    const uint64_t batch_end =
        std::min<uint64_t>(inserted + kDefaultBatchSize, key_count);
    for (; inserted < batch_end; ++inserted) {
      const uint64_t id =
          PermutedId(inserted, key_count, config.seed, permutation_step);
      batch.Put(MakeKey(id), MakeValue(id));
    }
    status = db->Write(write_options, &batch);
    if (!status.ok()) {
      Die("Write failed: " + status.ToString());
    }

    if (inserted % (1ULL << 18) == 0 || inserted == key_count) {
      std::cout << "  inserted " << inserted << "/" << key_count << " keys ("
                << FormatBytes(inserted * record_bytes) << ")" << std::endl;
    }
  }
  const auto write_end = std::chrono::steady_clock::now();
  const double write_seconds =
      std::chrono::duration<double>(write_end - write_start).count();

  FlushOptions flush_options;
  flush_options.wait = true;
  status = db->Flush(flush_options);
  if (!status.ok()) {
    Die("Flush failed: " + status.ToString());
  }

  WaitForCompactOptions wait_options;
  status = db->WaitForCompact(wait_options);
  if (!status.ok()) {
    Die("WaitForCompact failed: " + status.ToString());
  }

  std::vector<LiveFileMetaData> live_files;
  db->GetLiveFilesMetaData(&live_files);
  const std::string tree_description = DescribeTree(live_files);
  const auto structure_summary = SummarizeStructure(live_files);
  std::cout << "Live SST files: " << live_files.size() << std::endl;
  std::cout << "Total sorted runs: " << TotalRunCount(structure_summary)
            << std::endl;
  for (const auto& level : structure_summary) {
    std::cout << "  L" << level.level << ": runs=" << level.run_count
              << " files=" << level.file_count << std::endl;
  }
  std::cout << "Tiered tree: " << tree_description << std::endl;
  WriteStructureReport(config, live_files, tree_description);

  std::vector<double> point_latencies;
  VerifyPointLookups(db.get(), key_count, config.point_lookups, config.seed,
                     &point_latencies);
  PrintLatencySummary("Point lookup", SummarizeLatencies(point_latencies));

  std::vector<double> range_latencies;
  VerifyRangeLookups(db.get(), key_count, config.range_lookups,
                     config.range_width, config.seed, &range_latencies);
  PrintLatencySummary("Range lookup", SummarizeLatencies(range_latencies));

  std::cout << "Write throughput: "
            << (write_seconds > 0.0
                    ? FormatBytes(static_cast<uint64_t>(logical_bytes /
                                                       write_seconds))
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
