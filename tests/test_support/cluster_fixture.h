// Test harness that runs real brokers in-process.
//
// These are not mocks: each Broker here is the same object `pulselog-broker`
// runs, with real sockets, real threads and real files. The only concession to
// testing is that they live in the test process so a failure produces a usable
// stack trace and the sanitizers can see across the whole system.
#ifndef PULSELOG_TESTS_TEST_SUPPORT_CLUSTER_FIXTURE_H_
#define PULSELOG_TESTS_TEST_SUPPORT_CLUSTER_FIXTURE_H_

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "test_support/temp_dir.h"

#include "pulselog/broker/broker.h"
#include "pulselog/client/client.h"
#include "pulselog/net/socket.h"

namespace pulselog::testing {

// Binds an ephemeral port, records it, and closes. There is a small race
// window before the broker binds it, which is why callers retry.
inline std::uint16_t PickFreePort() {
  auto socket = net::TcpSocket::Listen(net::Endpoint{"127.0.0.1", 0});
  if (!socket.ok()) return 0;
  auto endpoint = socket->LocalEndpoint();
  return endpoint.ok() ? endpoint->port : 0;
}

// Polls `predicate` until it holds or the timeout expires. Tests assert on the
// return value rather than sleeping a fixed amount, so they stay reliable on a
// loaded machine without being slow on an idle one.
template<typename Predicate>
bool WaitUntil(Predicate predicate, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }
  return predicate();
}

// A cluster of in-process brokers sharing one temp directory.
struct ClusterOptions {
  std::size_t broker_count = 1;
  std::size_t io_threads = 2;
  std::size_t worker_threads = 2;
  std::int64_t segment_bytes = 1 << 20;
  bool sync_on_append = false;
  std::int64_t flush_interval_ms = 5;
  std::int32_t default_partitions = 1;
  std::int16_t default_replication_factor = 1;
  bool metrics = false;
  std::int64_t replication_interval_ms = 2;
  std::int64_t group_session_timeout_ms = 10'000;
  bool auto_create_topics = true;
};

class ClusterFixture {
 public:
  using Options = ClusterOptions;

  ClusterFixture() : ClusterFixture(Options{}) {}

  explicit ClusterFixture(const Options& options) : options_(options) {
    ports_.reserve(options_.broker_count);
    for (std::size_t i = 0; i < options_.broker_count; ++i) {
      ports_.push_back(PickFreePort());
    }
    for (std::size_t i = 0; i < options_.broker_count; ++i) {
      specs_.push_back(std::to_string(i) + "@127.0.0.1:" + std::to_string(ports_[i]));
    }
  }

  ClusterFixture(const ClusterFixture&) = delete;
  ClusterFixture& operator=(const ClusterFixture&) = delete;

  ~ClusterFixture() { StopAll(); }

  [[nodiscard]] Status StartAll() {
    for (std::size_t i = 0; i < options_.broker_count; ++i) {
      PL_RETURN_IF_ERROR(StartBroker(i));
    }
    return OkStatus();
  }

  [[nodiscard]] Status StartBroker(std::size_t index) {
    if (index >= options_.broker_count) return InvalidArgument("broker index out of range");
    if (brokers_.size() <= index) brokers_.resize(options_.broker_count);
    if (brokers_[index] != nullptr) return OkStatus();

    broker::BrokerConfig config;
    config.broker_id = BrokerId{static_cast<std::int32_t>(index)};
    config.listen = net::Endpoint{"127.0.0.1", ports_[index]};
    config.advertised_host = "127.0.0.1";
    config.advertised_port = ports_[index];
    config.data_dir = (dir_.path() / ("broker-" + std::to_string(index))).string();
    config.io_threads = options_.io_threads;
    config.worker_threads = options_.worker_threads;
    config.segment_bytes = options_.segment_bytes;
    config.preallocate_segments = false;  // Keeps test artefacts small.
    config.flush.sync_on_append = options_.sync_on_append;
    config.flush.interval_ms = options_.flush_interval_ms;
    config.flush.max_unflushed_bytes = 64 * 1024;
    config.flush.max_unflushed_records = 100;
    config.flusher_interval_ms = 2;
    config.default_partitions = options_.default_partitions;
    config.default_replication_factor = options_.default_replication_factor;
    config.metrics_enabled = options_.metrics;
    config.metrics_port = 0;  // Ephemeral.
    config.replication_interval_ms = options_.replication_interval_ms;
    config.group_session_timeout_ms = options_.group_session_timeout_ms;
    config.auto_create_topics = options_.auto_create_topics;
    config.retention_check_interval_ms = 1000;
    config.log_level = GetLogLevel();
    if (options_.broker_count > 1) config.cluster_brokers = specs_;

    auto instance = std::make_unique<broker::Broker>(std::move(config));
    PL_RETURN_IF_ERROR(instance->Start());
    brokers_[index] = std::move(instance);
    return OkStatus();
  }

  // Stops a broker cleanly (flushes, joins threads), leaving its data intact.
  void StopBroker(std::size_t index) {
    if (index >= brokers_.size() || brokers_[index] == nullptr) return;
    brokers_[index]->Stop();
    brokers_[index].reset();
  }

  // Restarts a broker, exercising the recovery path against real files.
  [[nodiscard]] Status RestartBroker(std::size_t index) {
    StopBroker(index);
    return StartBroker(index);
  }

  void StopAll() {
    for (std::size_t i = 0; i < brokers_.size(); ++i) StopBroker(i);
    brokers_.clear();
  }

  [[nodiscard]] broker::Broker* broker(std::size_t index) const {
    return index < brokers_.size() ? brokers_[index].get() : nullptr;
  }

  [[nodiscard]] std::vector<std::string> BootstrapServers() const {
    std::vector<std::string> servers;
    servers.reserve(ports_.size());
    for (const auto port : ports_) servers.push_back("127.0.0.1:" + std::to_string(port));
    return servers;
  }

  [[nodiscard]] std::string BootstrapFor(std::size_t index) const {
    return "127.0.0.1:" + std::to_string(ports_[index]);
  }

  // A client context pointed at the whole cluster.
  [[nodiscard]] std::unique_ptr<client::ClientContext> MakeClient() const {
    client::ClientConfig config;
    config.bootstrap_servers = BootstrapServers();
    config.request_timeout_ms = 15'000;
    config.retry.max_attempts = 5;
    return std::make_unique<client::ClientContext>(config);
  }

  [[nodiscard]] const std::filesystem::path& data_root() const { return dir_.path(); }

  [[nodiscard]] std::filesystem::path PartitionDir(std::size_t broker_index,
                                                   const std::string& topic,
                                                   std::int32_t partition) const {
    return dir_.path() / ("broker-" + std::to_string(broker_index)) /
           (topic + "-" + std::to_string(partition));
  }

 private:
  Options options_;
  TempDir dir_{"pulselog-cluster"};
  std::vector<std::uint16_t> ports_;
  std::vector<std::string> specs_;
  std::vector<std::unique_ptr<broker::Broker>> brokers_;
};

// Produces `count` records to one partition and returns the last offset.
inline Result<Offset> ProduceRecords(client::Producer& producer,
                                     const std::string& topic,
                                     PartitionIndex partition,
                                     int count,
                                     const std::string& value_prefix = "v",
                                     std::size_t value_padding = 0) {
  std::vector<client::OutboundRecord> records;
  records.reserve(static_cast<std::size_t>(count));
  std::vector<std::string> values;
  values.reserve(static_cast<std::size_t>(count));

  for (int i = 0; i < count; ++i) {
    values.push_back(value_prefix + std::to_string(i) + std::string(value_padding, 'x'));
  }
  for (int i = 0; i < count; ++i) {
    client::OutboundRecord record;
    record.value = values[static_cast<std::size_t>(i)];
    records.push_back(record);
  }

  auto result = producer.SendBatch(topic, partition, records);
  if (!result.ok()) return result.status();
  return result->last_offset;
}

// Reads every record from `from` up to the current end, following the log.
inline Result<std::vector<std::string>> ConsumeAll(client::Consumer& consumer,
                                                   const std::string& topic,
                                                   PartitionIndex partition,
                                                   Offset from,
                                                   int expected) {
  std::vector<std::string> values;
  Offset cursor = from;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);

  while (static_cast<int>(values.size()) < expected &&
         std::chrono::steady_clock::now() < deadline) {
    auto records = consumer.Fetch(topic, partition, cursor);
    if (!records.ok()) return records.status();
    if (records->empty()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      continue;
    }
    for (const auto& record : *records) {
      values.emplace_back(record.value);
      cursor = record.offset + 1;
    }
  }
  return values;
}

}  // namespace pulselog::testing

#endif  // PULSELOG_TESTS_TEST_SUPPORT_CLUSTER_FIXTURE_H_
