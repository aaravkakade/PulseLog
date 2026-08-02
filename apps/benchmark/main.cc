// pulselog-bench: the end-to-end benchmark driver.
//
// Measurement discipline, because it is what makes the numbers mean anything:
//
//  * **Warm-up is discarded.** The first records pay for connection setup,
//    segment creation, page-cache cold misses and buffer-pool growth. Those
//    are real costs but they are not steady-state costs, so they are measured
//    separately and excluded from the reported distribution.
//  * **Latency is recorded per request into an HDR histogram**, not averaged.
//    Percentiles are computed over every sample, not over a sliding window.
//  * **Throughput is measured over the steady-state window only**, wall-clock,
//    from the first post-warm-up send to the last acknowledgement.
//  * **Coordinated omission is not corrected for.** This driver is closed-loop:
//    each producer thread waits for its acknowledgement before sending again,
//    so a stall suppresses subsequent samples rather than showing up as a long
//    one. The reported percentiles therefore describe *service time under this
//    offered load*, not what an open-loop arrival process would see. This is
//    stated in every result file.
//  * **Every run records its own configuration and host**, so a number can
//    never be quoted without the conditions that produced it.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "pulselog/base/clock.h"
#include "pulselog/base/config.h"
#include "pulselog/base/crc32c.h"
#include "pulselog/base/logging.h"
#include "pulselog/client/client.h"
#include "pulselog/metrics/histogram.h"
#include "pulselog/metrics/process_stats.h"
#include "pulselog/net/poller.h"

namespace {

using namespace pulselog;

struct BenchConfig {
  std::string scenario = "produce";
  std::vector<std::string> brokers = {"127.0.0.1:9092"};
  std::string topic = "bench";
  std::int32_t partitions = 1;
  std::int16_t replication_factor = 1;
  int producers = 1;
  int consumers = 0;
  std::int64_t records = 200'000;
  std::size_t record_size = 128;
  std::size_t batch_size = 1;
  AckMode acks = AckMode::kLeader;
  std::int64_t warmup_records = 10'000;
  int trials = 1;
  bool keyed = false;
  std::string output;
  std::string label;
};

struct TrialResult {
  std::int64_t records = 0;
  std::int64_t bytes = 0;
  double duration_seconds = 0.0;
  double records_per_second = 0.0;
  double megabytes_per_second = 0.0;
  metrics::HistogramSnapshot latency;  // Nanoseconds, per request.
  std::int64_t errors = 0;
  double cpu_percent = 0.0;
  std::uint64_t peak_rss_bytes = 0;
};

std::string JsonEscape(std::string_view value) {
  std::string out;
  for (const char c : value) {
    if (c == '"' || c == '\\') {
      out.push_back('\\');
      out.push_back(c);
    } else if (c == '\n') {
      out += "\\n";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

// Builds a value of `size` bytes with a recognisable prefix, so a hexdump of
// the log during a failed run shows what produced it.
std::string MakePayload(std::size_t size, int producer_id) {
  std::string payload = "pulselog-bench-p" + std::to_string(producer_id) + "-";
  if (payload.size() >= size) return payload.substr(0, size);
  payload.resize(size, 'x');
  return payload;
}

// --- the scenarios ---------------------------------------------------------

TrialResult RunProduce(const BenchConfig& config, bool with_consumers) {
  TrialResult result;
  metrics::Histogram latency(/*max_trackable=*/60'000'000'000, /*significant_digits=*/3);

  std::atomic<std::int64_t> produced{0};
  std::atomic<std::int64_t> bytes{0};
  std::atomic<std::int64_t> errors{0};
  std::atomic<std::int64_t> consumed{0};
  std::atomic<bool> consumers_stop{false};
  std::atomic<int> warm_producers{0};
  std::atomic<bool> measuring{false};

  const std::int64_t per_producer = std::max<std::int64_t>(1, config.records / config.producers);
  const std::int64_t warmup_per_producer =
      std::max<std::int64_t>(0, config.warmup_records / config.producers);

  std::vector<std::thread> consumer_threads;
  if (with_consumers) {
    consumer_threads.reserve(static_cast<std::size_t>(config.consumers));
    for (int c = 0; c < config.consumers; ++c) {
      consumer_threads.emplace_back([&, c] {
        client::ClientConfig client_config;
        client_config.bootstrap_servers = config.brokers;
        client::ClientContext context(client_config);
        client::ConsumerConfig consumer_config;
        consumer_config.max_wait_ms = 20;
        client::Consumer consumer(context, consumer_config);

        // Each consumer owns a slice of the partitions.
        std::vector<PartitionIndex> owned;
        for (std::int32_t p = c; p < config.partitions; p += config.consumers) {
          owned.push_back(PartitionIndex{p});
        }
        std::vector<Offset> positions(owned.size(), 0);

        while (!consumers_stop.load(std::memory_order_relaxed)) {
          for (std::size_t i = 0; i < owned.size(); ++i) {
            auto records = consumer.Fetch(config.topic, owned[i], positions[i]);
            if (!records.ok()) {
              std::this_thread::sleep_for(std::chrono::milliseconds(2));
              continue;
            }
            if (records->empty()) continue;
            positions[i] = records->back().offset + 1;
            consumed.fetch_add(static_cast<std::int64_t>(records->size()),
                               std::memory_order_relaxed);
          }
        }
      });
    }
  }

  const Stopwatch total_clock;
  std::int64_t measure_start_nanos = 0;

  std::vector<std::thread> producer_threads;
  producer_threads.reserve(static_cast<std::size_t>(config.producers));
  for (int p = 0; p < config.producers; ++p) {
    producer_threads.emplace_back([&, p] {
      client::ClientConfig client_config;
      client_config.bootstrap_servers = config.brokers;
      client_config.request_timeout_ms = 30'000;
      client::ClientContext context(client_config);

      client::ProducerConfig producer_config;
      producer_config.acks = config.acks;
      producer_config.batch_records = config.batch_size;
      producer_config.batch_bytes = 16 * 1024 * 1024;  // Records, not bytes, drive batching here.
      producer_config.request_timeout_ms = 30'000;
      client::Producer producer(context, producer_config);

      const std::string payload = MakePayload(config.record_size, p);
      const std::string key = config.keyed ? ("key-" + std::to_string(p)) : std::string();

      // Warm-up: same work, discarded. Connections open, the topic's segments
      // get created, the buffer pools reach steady size.
      for (std::int64_t i = 0; i < warmup_per_producer; ++i) {
        client::OutboundRecord record;
        record.value = payload;
        record.key = key;
        record.key_is_null = !config.keyed;
        auto sent = producer.Send(config.topic, record);
        if (!sent.ok()) errors.fetch_add(1, std::memory_order_relaxed);
      }
      auto flushed = producer.Flush();
      if (!flushed.ok()) errors.fetch_add(1, std::memory_order_relaxed);

      // Barrier: no producer starts measuring until all have warmed up, so the
      // measured window has the full offered load throughout.
      warm_producers.fetch_add(1, std::memory_order_release);
      while (warm_producers.load(std::memory_order_acquire) < config.producers) {
        std::this_thread::yield();
      }
      if (!measuring.exchange(true, std::memory_order_acq_rel)) {
        measure_start_nanos = MonotonicNanos();
      }

      // Accounting comes from the producer's own counters rather than from
      // summing return values: a Send can flush a batch buffered by an earlier
      // call, and the counters cannot miss one.
      const auto records_before = producer.stats().records_sent;
      const auto bytes_before = producer.stats().bytes_sent;

      for (std::int64_t i = 0; i < per_producer; ++i) {
        client::OutboundRecord record;
        record.value = payload;
        record.key = key;
        record.key_is_null = !config.keyed;

        const std::int64_t started = MonotonicNanos();
        auto sent = producer.Send(config.topic, record);
        if (!sent.ok()) {
          errors.fetch_add(1, std::memory_order_relaxed);
          continue;
        }
        // With batching, only the send that actually shipped a batch has a
        // meaningful latency; buffered sends report zero records. The recorded
        // value is therefore per *request*, not per record -- stated in the
        // result file so a batched p50 is never read as a per-record number.
        if (sent->record_count > 0) latency.Record(MonotonicNanos() - started);
      }

      const std::int64_t started = MonotonicNanos();
      auto final_flush = producer.Flush();
      if (final_flush.ok() && final_flush->record_count > 0) {
        latency.Record(MonotonicNanos() - started);
      } else if (!final_flush.ok()) {
        errors.fetch_add(1, std::memory_order_relaxed);
      }

      produced.fetch_add(static_cast<std::int64_t>(producer.stats().records_sent - records_before),
                         std::memory_order_relaxed);
      bytes.fetch_add(static_cast<std::int64_t>(producer.stats().bytes_sent - bytes_before),
                      std::memory_order_relaxed);
    });
  }

  for (auto& thread : producer_threads) thread.join();
  const std::int64_t measure_end_nanos = MonotonicNanos();

  consumers_stop.store(true, std::memory_order_relaxed);
  for (auto& thread : consumer_threads) thread.join();

  result.records = produced.load();
  result.bytes = bytes.load();
  result.errors = errors.load();
  result.duration_seconds = static_cast<double>(measure_end_nanos - measure_start_nanos) / 1e9;
  if (result.duration_seconds > 0) {
    result.records_per_second = static_cast<double>(result.records) / result.duration_seconds;
    result.megabytes_per_second =
        static_cast<double>(result.bytes) / result.duration_seconds / (1024.0 * 1024.0);
  }
  result.latency = latency.GetSnapshot();
  (void)total_clock;
  if (with_consumers) {
    std::cerr << "  (consumers read " << consumed.load() << " records during the run)\n";
  }
  return result;
}

// Baseline: a mutex-protected in-memory queue with no networking, no
// protocol, no durability. It is the ceiling this design gives up in exchange
// for being a broker, and it is here so the engine's numbers can be read
// against something rather than admired in isolation.
TrialResult RunMutexQueueBaseline(const BenchConfig& config) {
  struct Record {
    std::string value;
    std::int64_t offset = 0;
  };

  std::mutex mutex;
  std::vector<Record> log;
  log.reserve(static_cast<std::size_t>(config.records));

  metrics::Histogram latency(60'000'000'000, 3);
  std::atomic<std::int64_t> produced{0};
  std::atomic<std::int64_t> bytes{0};
  std::atomic<std::int64_t> next_offset{0};

  const std::int64_t per_producer = std::max<std::int64_t>(1, config.records / config.producers);
  const std::string payload(config.record_size, 'x');

  const std::int64_t started = MonotonicNanos();
  std::vector<std::thread> threads;
  threads.reserve(static_cast<std::size_t>(config.producers));
  for (int p = 0; p < config.producers; ++p) {
    threads.emplace_back([&] {
      std::int64_t local = 0;
      for (std::int64_t i = 0; i < per_producer; ++i) {
        const std::int64_t send_start = MonotonicNanos();
        {
          std::lock_guard<std::mutex> lock(mutex);
          log.push_back(Record{payload, next_offset++});
        }
        latency.Record(MonotonicNanos() - send_start);
        ++local;
      }
      produced.fetch_add(local, std::memory_order_relaxed);
      bytes.fetch_add(local * static_cast<std::int64_t>(payload.size()), std::memory_order_relaxed);
    });
  }
  for (auto& thread : threads) thread.join();
  const std::int64_t ended = MonotonicNanos();

  TrialResult result;
  result.records = produced.load();
  result.bytes = bytes.load();
  result.duration_seconds = static_cast<double>(ended - started) / 1e9;
  if (result.duration_seconds > 0) {
    result.records_per_second = static_cast<double>(result.records) / result.duration_seconds;
    result.megabytes_per_second =
        static_cast<double>(result.bytes) / result.duration_seconds / (1024.0 * 1024.0);
  }
  result.latency = latency.GetSnapshot();
  return result;
}

// --- reporting -------------------------------------------------------------

void WriteJson(std::ostream& out,
               const BenchConfig& config,
               const std::vector<TrialResult>& trials) {
  const auto host = metrics::ProcessStatsSampler::DescribeHost();

  // The median trial is reported as the headline; every trial is included so a
  // reader can see the spread rather than trusting one number.
  std::vector<double> throughputs;
  for (const auto& trial : trials) throughputs.push_back(trial.records_per_second);
  std::vector<double> sorted = throughputs;
  std::sort(sorted.begin(), sorted.end());
  const double median = sorted.empty() ? 0.0 : sorted[sorted.size() / 2];

  out << "{\n";
  out << "  \"scenario\": \"" << JsonEscape(config.scenario) << "\",\n";
  out << "  \"label\": \"" << JsonEscape(config.label.empty() ? config.scenario : config.label)
      << "\",\n";
  out << "  \"config\": {\n";
  out << "    \"topic\": \"" << JsonEscape(config.topic) << "\",\n";
  out << "    \"partitions\": " << config.partitions << ",\n";
  out << "    \"replication_factor\": " << config.replication_factor << ",\n";
  out << "    \"producers\": " << config.producers << ",\n";
  out << "    \"consumers\": " << config.consumers << ",\n";
  out << "    \"records\": " << config.records << ",\n";
  out << "    \"record_size_bytes\": " << config.record_size << ",\n";
  out << "    \"batch_size\": " << config.batch_size << ",\n";
  out << "    \"acks\": \"" << AckModeName(config.acks) << "\",\n";
  out << "    \"warmup_records\": " << config.warmup_records << ",\n";
  out << "    \"trials\": " << config.trials << ",\n";
  out << "    \"keyed\": " << (config.keyed ? "true" : "false") << ",\n";
  out << "    \"brokers\": " << config.brokers.size() << "\n";
  out << "  },\n";
  out << "  \"environment\": {\n";
  out << "    \"os\": \"" << JsonEscape(host.os) << "\",\n";
  out << "    \"kernel\": \"" << JsonEscape(host.kernel) << "\",\n";
  out << "    \"architecture\": \"" << JsonEscape(host.architecture) << "\",\n";
  out << "    \"cpu_model\": \"" << JsonEscape(host.cpu_model) << "\",\n";
  out << "    \"cpu_count\": " << host.cpu_count << ",\n";
  out << "    \"total_memory_bytes\": " << host.total_memory_bytes << ",\n";
  out << "    \"compiler\": \"" <<
#if defined(__clang__)
      "clang " << __clang_major__ << "." << __clang_minor__ << "." << __clang_patchlevel__
#elif defined(__GNUC__)
      "gcc " << __GNUC__ << "." << __GNUC_MINOR__
#else
      "unknown"
#endif
      << "\",\n";
  out << "    \"cxx_standard\": " << __cplusplus << ",\n";
  out << "    \"poller\": \"" << net::PollerBackendName() << "\",\n";
  out << "    \"checksum\": \"" << Crc32cImplementationName() << "\"\n";
  out << "  },\n";
  out << "  \"method\": {\n";
  out << "    \"loop\": \"closed\",\n";
  out << "    \"coordinated_omission\": \"not corrected; latencies are service times under this "
         "offered load\",\n";
  out << "    \"warmup\": \"discarded before the measured window\",\n";
  out << "    \"latency_unit\": \"nanoseconds\",\n";
  out << "    \"latency_scope\": \"per produce request (a batch), not per record\"\n";
  out << "  },\n";
  out << "  \"median_records_per_second\": " << std::fixed << std::setprecision(1) << median
      << ",\n";
  out << "  \"trials\": [\n";
  for (std::size_t i = 0; i < trials.size(); ++i) {
    const auto& trial = trials[i];
    out << "    {\n";
    out << "      \"records\": " << trial.records << ",\n";
    out << "      \"bytes\": " << trial.bytes << ",\n";
    out << "      \"duration_seconds\": " << std::fixed << std::setprecision(4)
        << trial.duration_seconds << ",\n";
    out << "      \"records_per_second\": " << std::fixed << std::setprecision(1)
        << trial.records_per_second << ",\n";
    out << "      \"megabytes_per_second\": " << std::fixed << std::setprecision(2)
        << trial.megabytes_per_second << ",\n";
    out << "      \"errors\": " << trial.errors << ",\n";
    out << "      \"latency_nanos\": {\n";
    out << "        \"count\": " << trial.latency.count << ",\n";
    out << "        \"min\": " << trial.latency.min << ",\n";
    out << "        \"p50\": " << trial.latency.p50 << ",\n";
    out << "        \"p90\": " << trial.latency.p90 << ",\n";
    out << "        \"p95\": " << trial.latency.p95 << ",\n";
    out << "        \"p99\": " << trial.latency.p99 << ",\n";
    out << "        \"p999\": " << trial.latency.p999 << ",\n";
    out << "        \"max\": " << trial.latency.max << ",\n";
    out << "        \"mean\": " << static_cast<std::int64_t>(trial.latency.mean) << "\n";
    out << "      },\n";
    out << "      \"cpu_percent\": " << std::fixed << std::setprecision(1) << trial.cpu_percent
        << ",\n";
    out << "      \"peak_rss_bytes\": " << trial.peak_rss_bytes << "\n";
    out << "    }" << (i + 1 < trials.size() ? "," : "") << "\n";
  }
  out << "  ]\n";
  out << "}\n";
}

void PrintHuman(const BenchConfig& config, const TrialResult& trial) {
  std::cerr << "\n  scenario         : " << config.scenario << '\n'
            << "  records          : " << trial.records << " x " << config.record_size << " B"
            << " (batch " << config.batch_size << ", acks " << AckModeName(config.acks) << ")\n"
            << "  duration         : " << std::fixed << std::setprecision(3)
            << trial.duration_seconds << " s\n"
            << "  throughput       : " << std::fixed << std::setprecision(0)
            << trial.records_per_second << " records/s  (" << std::setprecision(1)
            << trial.megabytes_per_second << " MiB/s)\n"
            << "  latency p50      : " << trial.latency.p50 / 1000 << " us\n"
            << "  latency p95      : " << trial.latency.p95 / 1000 << " us\n"
            << "  latency p99      : " << trial.latency.p99 / 1000 << " us\n"
            << "  latency p99.9    : " << trial.latency.p999 / 1000 << " us\n"
            << "  latency max      : " << trial.latency.max / 1000 << " us\n"
            << "  errors           : " << trial.errors << '\n';
}

void PrintUsage() {
  std::cout << R"(pulselog-bench - benchmark driver

Usage:
  pulselog-bench --scenario=NAME [options]

Scenarios:
  produce            producers only
  produce-consume    producers with concurrent consumers
  baseline-mutex     in-process mutex-protected queue (no network, no disk)

Options:
  --brokers=LIST         bootstrap brokers (default 127.0.0.1:9092)
  --topic=NAME           topic to use (default bench)
  --partitions=N         partitions to create (default 1)
  --replication=N        replication factor (default 1)
  --producers=N          producer threads (default 1)
  --consumers=N          consumer threads (default 0)
  --records=N            records per trial, across all producers (default 200000)
  --record-size=N        value bytes per record (default 128)
  --batch-size=N         records per produce request (default 1)
  --acks=none|leader|quorum
  --warmup=N             records discarded before measuring (default 10000)
  --trials=N             repeats; the median is reported (default 1)
  --keyed                use keys, so routing is by hash rather than round-robin
  --label=TEXT           label recorded in the result file
  --output=FILE          write JSON here (stdout if omitted)

Every result file records the host, compiler, configuration and measurement
method. See docs/BENCHMARKING.md.
)";
}

}  // namespace

int main(int argc, char** argv) {
  using namespace pulselog;

  ConfigStore flags;
  const auto positional = flags.LoadCommandLine(argc, argv);
  (void)positional;
  if (flags.Contains("help")) {
    PrintUsage();
    return 0;
  }

  SetLogLevel(LogLevel::kWarn);

  BenchConfig config;
  config.scenario = flags.GetString("scenario", "produce");
  const auto brokers = flags.GetList("brokers");
  if (!brokers.empty()) config.brokers = brokers;
  config.topic = flags.GetString("topic", "bench");
  config.label = flags.GetString("label", "");
  config.output = flags.GetString("output", "");
  config.keyed = flags.GetBool("keyed", false).value_or(false);

  const auto read_int = [&flags](std::string_view key, std::int64_t fallback) -> std::int64_t {
    auto value = flags.GetInt(key, fallback);
    if (!value.ok()) {
      std::cerr << "pulselog-bench: " << value.status().ToString() << '\n';
      std::exit(1);
    }
    return value.value();
  };

  config.partitions = static_cast<std::int32_t>(read_int("partitions", 1));
  config.replication_factor = static_cast<std::int16_t>(read_int("replication", 1));
  config.producers = static_cast<int>(read_int("producers", 1));
  config.consumers = static_cast<int>(read_int("consumers", 0));
  config.records = read_int("records", 200'000);
  config.record_size = static_cast<std::size_t>(read_int("record-size", 128));
  config.batch_size = static_cast<std::size_t>(read_int("batch-size", 1));
  config.warmup_records = read_int("warmup", 10'000);
  config.trials = static_cast<int>(read_int("trials", 1));

  if (!ParseAckMode(flags.GetString("acks", "leader"), config.acks)) {
    std::cerr << "pulselog-bench: --acks must be none, leader or quorum\n";
    return 1;
  }
  if (config.producers < 1) {
    std::cerr << "pulselog-bench: --producers must be at least 1\n";
    return 1;
  }
  if (config.scenario == "produce-consume" && config.consumers < 1) config.consumers = 1;

  // Create the topic up front so no trial pays for topic creation.
  if (config.scenario != "baseline-mutex") {
    client::ClientConfig client_config;
    client_config.bootstrap_servers = config.brokers;
    client::ClientContext context(client_config);
    client::AdminClient admin(context);
    const Status created =
        admin.CreateTopic(config.topic, config.partitions, config.replication_factor);
    if (!created.ok() && created.code() != ErrorCode::kAlreadyExists) {
      std::cerr << "pulselog-bench: cannot create topic: " << created.ToString() << '\n';
      return 1;
    }
  }

  std::vector<TrialResult> trials;
  metrics::ProcessStatsSampler sampler;
  for (int trial = 0; trial < config.trials; ++trial) {
    std::cerr << "trial " << (trial + 1) << " of " << config.trials << "...";
    (void)sampler.Sample();  // Reset the CPU delta window.

    TrialResult result;
    if (config.scenario == "baseline-mutex") {
      result = RunMutexQueueBaseline(config);
    } else if (config.scenario == "produce-consume") {
      result = RunProduce(config, /*with_consumers=*/true);
    } else if (config.scenario == "produce") {
      result = RunProduce(config, /*with_consumers=*/false);
    } else {
      std::cerr << "\npulselog-bench: unknown scenario '" << config.scenario << "'\n";
      return 1;
    }

    const auto sample = sampler.Sample();
    result.cpu_percent = sample.cpu_percent;
    result.peak_rss_bytes = sample.resident_bytes;
    PrintHuman(config, result);
    trials.push_back(result);
  }

  if (config.output.empty()) {
    WriteJson(std::cout, config, trials);
  } else {
    std::ofstream out(config.output);
    if (!out) {
      std::cerr << "pulselog-bench: cannot write " << config.output << '\n';
      return 1;
    }
    WriteJson(out, config, trials);
    std::cerr << "\nwrote " << config.output << '\n';
  }
  return 0;
}
