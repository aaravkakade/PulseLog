// pulselog-cli: an operator and demo client.
//
//   pulselog-cli create-topic <topic> [--partitions=N] [--replication=N]
//   pulselog-cli delete-topic <topic>
//   pulselog-cli list-topics
//   pulselog-cli metadata [topic]
//   pulselog-cli health
//   pulselog-cli describe-cluster
//   pulselog-cli produce <topic> [--key=K] [--value=V] [--count=N] [--acks=...]
//   pulselog-cli consume <topic> [--partition=N] [--offset=N] [--follow]
//   pulselog-cli consume-group <topic> --group=G [--max=N]
//   pulselog-cli offsets <topic> [--partition=N]
#include <chrono>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "pulselog/base/config.h"
#include "pulselog/base/logging.h"
#include "pulselog/client/client.h"

namespace {

using namespace pulselog;

void PrintUsage() {
  std::cout << R"(pulselog-cli - operate and exercise a PulseLog cluster

Usage:
  pulselog-cli <command> [args] [--brokers=host:port,...]

Commands:
  create-topic <topic> [--partitions=N] [--replication=N]
  delete-topic <topic>
  list-topics
  metadata [topic]
  health
  describe-cluster
  produce <topic> [--key=K] [--value=V] [--count=N] [--acks=none|leader|quorum]
                  [--batch=N] [--partition=N]
  consume <topic> [--partition=N] [--offset=N|earliest|latest] [--max=N] [--follow]
  consume-group <topic> --group=G [--max=N] [--timeout-ms=N]
  offsets <topic> [--partition=N]

Options:
  --brokers=LIST   bootstrap brokers (default 127.0.0.1:9092)
)";
}

int Fail(const Status& status) {
  std::cerr << "error: " << status.ToString() << '\n';
  return 1;
}

std::string FormatBytes(std::int64_t bytes) {
  static const char* units[] = {"B", "KiB", "MiB", "GiB", "TiB"};
  auto value = static_cast<double>(bytes);
  int unit = 0;
  while (value >= 1024.0 && unit < 4) {
    value /= 1024.0;
    ++unit;
  }
  std::ostringstream out;
  out << std::fixed << std::setprecision(value < 10 ? 1 : 0) << value << ' ' << units[unit];
  return out.str();
}

int CmdCreateTopic(client::ClientContext& context,
                   const ConfigStore& flags,
                   const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli create-topic <topic> [--partitions=N]\n";
    return 1;
  }
  const auto partitions = flags.GetInt("partitions", 1);
  const auto replication = flags.GetInt("replication", 1);
  if (!partitions.ok()) return Fail(partitions.status());
  if (!replication.ok()) return Fail(replication.status());

  client::AdminClient admin(context);
  const Status status = admin.CreateTopic(args[1],
                                          static_cast<std::int32_t>(partitions.value()),
                                          static_cast<std::int16_t>(replication.value()));
  if (!status.ok()) return Fail(status);

  std::cout << "created topic " << args[1] << " with " << partitions.value()
            << " partition(s), replication factor " << replication.value() << '\n';
  return 0;
}

int CmdDeleteTopic(client::ClientContext& context, const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli delete-topic <topic>\n";
    return 1;
  }
  client::AdminClient admin(context);
  const Status status = admin.DeleteTopic(args[1]);
  if (!status.ok()) return Fail(status);
  std::cout << "deleted topic " << args[1] << '\n';
  return 0;
}

int CmdListTopics(client::ClientContext& context) {
  client::AdminClient admin(context);
  auto response = admin.ListTopics();
  if (!response.ok()) return Fail(response.status());

  if (response->topics.empty()) {
    std::cout << "no topics\n";
    return 0;
  }
  std::cout << std::left << std::setw(28) << "TOPIC" << std::setw(12) << "PARTITIONS"
            << std::setw(14) << "RECORDS" << "SIZE\n";
  for (const auto& topic : response->topics) {
    std::cout << std::left << std::setw(28) << topic.name << std::setw(12) << topic.partitions
              << std::setw(14) << topic.total_records << FormatBytes(topic.total_bytes) << '\n';
  }
  return 0;
}

int CmdMetadata(client::ClientContext& context, const std::vector<std::string>& args) {
  std::vector<std::string> topics;
  if (args.size() >= 2) topics.push_back(args[1]);

  client::AdminClient admin(context);
  auto response = admin.GetMetadata(topics);
  if (!response.ok()) return Fail(response.status());

  std::cout << "controller: broker " << response->controller_id.value() << "\nbrokers:\n";
  for (const auto& broker : response->brokers) {
    std::cout << "  " << broker.id.value() << " -> " << broker.host << ':' << broker.port << '\n';
  }
  for (const auto& topic : response->topics) {
    std::cout << "topic " << topic.name << ":\n";
    for (const auto& partition : topic.partitions) {
      std::cout << "  partition " << partition.index.value() << " leader "
                << partition.leader.value() << " epoch " << partition.leader_epoch << " replicas [";
      for (std::size_t i = 0; i < partition.replicas.size(); ++i) {
        if (i > 0) std::cout << ',';
        std::cout << partition.replicas[i].value();
      }
      std::cout << "] isr [";
      for (std::size_t i = 0; i < partition.in_sync_replicas.size(); ++i) {
        if (i > 0) std::cout << ',';
        std::cout << partition.in_sync_replicas[i].value();
      }
      std::cout << "]\n";
    }
  }
  return 0;
}

int CmdHealth(client::ClientContext& context) {
  client::AdminClient admin(context);
  auto response = admin.Health();
  if (!response.ok()) return Fail(response.status());
  std::cout << "broker " << response->broker_id.value() << " version " << response->version
            << "\n  ready: " << (response->ready ? "yes" : "no")
            << "\n  uptime: " << (response->uptime_ms / 1000) << "s"
            << "\n  partitions hosted: " << response->hosted_partitions
            << "\n  partitions led: " << response->leader_partitions << '\n';
  return 0;
}

int CmdDescribeCluster(client::ClientContext& context) {
  client::AdminClient admin(context);
  auto response = admin.DescribeCluster();
  if (!response.ok()) return Fail(response.status());
  std::cout << "controller: " << response->controller_id.value() << "\nbrokers:\n";
  for (const auto& broker : response->brokers) {
    bool live = false;
    for (const auto& id : response->live_brokers) {
      if (id == broker.id) live = true;
    }
    std::cout << "  " << broker.id.value() << " " << broker.host << ':' << broker.port << "  "
              << (live ? "reachable" : "unknown") << '\n';
  }
  return 0;
}

int CmdProduce(client::ClientContext& context,
               const ConfigStore& flags,
               const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli produce <topic> [--value=V] [--count=N]\n";
    return 1;
  }
  const auto count = flags.GetInt("count", 1);
  const auto batch = flags.GetInt("batch", 1);
  const auto partition = flags.GetInt("partition", -1);
  if (!count.ok()) return Fail(count.status());
  if (!batch.ok()) return Fail(batch.status());
  if (!partition.ok()) return Fail(partition.status());

  AckMode acks = AckMode::kLeader;
  const std::string acks_text = flags.GetString("acks", "leader");
  if (!ParseAckMode(acks_text, acks)) {
    std::cerr << "error: --acks must be none, leader or quorum\n";
    return 1;
  }

  client::ProducerConfig producer_config;
  producer_config.acks = acks;
  producer_config.batch_records =
      static_cast<std::size_t>(std::max<std::int64_t>(1, batch.value()));
  producer_config.forced_partition = static_cast<std::int32_t>(partition.value());
  client::Producer producer(context, producer_config);

  const std::string key = flags.GetString("key", "");
  const std::string value = flags.GetString("value", "hello from pulselog-cli");
  const bool key_is_null = key.empty();

  Offset last_offset = kInvalidOffset;
  for (std::int64_t i = 0; i < count.value(); ++i) {
    const std::string indexed_value = count.value() > 1 ? value + " #" + std::to_string(i) : value;
    client::OutboundRecord record;
    record.key = key;
    record.key_is_null = key_is_null;
    record.value = indexed_value;

    auto result = producer.Send(args[1], record);
    if (!result.ok()) return Fail(result.status());
    if (result->record_count > 0) last_offset = result->last_offset;
  }

  auto flushed = producer.Flush();
  if (!flushed.ok()) return Fail(flushed.status());
  if (flushed->record_count > 0) last_offset = flushed->last_offset;

  std::cout << "produced " << producer.stats().records_sent << " record(s) in "
            << producer.stats().batches_sent << " batch(es); last offset " << last_offset << '\n';
  return 0;
}

int CmdConsume(client::ClientContext& context,
               const ConfigStore& flags,
               const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli consume <topic> [--partition=N] [--offset=N]\n";
    return 1;
  }
  const auto partition = flags.GetInt("partition", 0);
  const auto max_records = flags.GetInt("max", 100);
  if (!partition.ok()) return Fail(partition.status());
  if (!max_records.ok()) return Fail(max_records.status());
  const bool follow = flags.GetBool("follow", false).value_or(false);

  client::Consumer consumer(context);
  const PartitionIndex index{static_cast<std::int32_t>(partition.value())};

  Offset offset = 0;
  const std::string offset_text = flags.GetString("offset", "earliest");
  if (offset_text == "earliest") {
    auto resolved = consumer.ListOffset(args[1], index, kEarliestOffset);
    if (!resolved.ok()) return Fail(resolved.status());
    offset = resolved.value();
  } else if (offset_text == "latest") {
    auto resolved = consumer.ListOffset(args[1], index, kLatestOffset);
    if (!resolved.ok()) return Fail(resolved.status());
    offset = resolved.value();
  } else {
    offset = std::strtoll(offset_text.c_str(), nullptr, 10);
  }

  std::int64_t printed = 0;
  while (printed < max_records.value()) {
    auto records = consumer.Fetch(args[1], index, offset);
    if (!records.ok()) return Fail(records.status());

    if (records->empty()) {
      if (!follow) break;
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }
    for (const auto& record : *records) {
      std::cout << record.offset << '\t' << (record.key_is_null ? "<null>" : record.key) << '\t'
                << record.value << '\n';
      ++printed;
      offset = record.offset + 1;
      if (printed >= max_records.value()) break;
    }
  }
  std::cout << "-- " << printed << " record(s), next offset " << offset << '\n';
  return 0;
}

int CmdConsumeGroup(client::ClientContext& context,
                    const ConfigStore& flags,
                    const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli consume-group <topic> --group=G\n";
    return 1;
  }
  const std::string group = flags.GetString("group", "");
  if (group.empty()) {
    std::cerr << "error: --group is required\n";
    return 1;
  }
  const auto max_records = flags.GetInt("max", 100);
  const auto timeout_ms = flags.GetDurationMs("timeout-ms", 5000);
  if (!max_records.ok()) return Fail(max_records.status());
  if (!timeout_ms.ok()) return Fail(timeout_ms.status());

  client::ConsumerConfig consumer_config;
  consumer_config.group_id = group;
  consumer_config.topics = {args[1]};
  consumer_config.max_wait_ms = 200;
  client::Consumer consumer(context, consumer_config);

  const Status joined = consumer.Join();
  if (!joined.ok()) return Fail(joined);

  std::cout << "joined group " << group << " as " << consumer.member_id() << " (generation "
            << consumer.generation().value() << ")\nassigned partitions:";
  for (const auto& topic_partition : consumer.assignment()) {
    std::cout << ' ' << topic_partition.ToString();
  }
  std::cout << '\n';

  std::int64_t printed = 0;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms.value());
  while (printed < max_records.value() && std::chrono::steady_clock::now() < deadline) {
    auto records = consumer.Poll();
    if (!records.ok()) return Fail(records.status());
    for (const auto& record : *records) {
      std::cout << record.offset << '\t' << (record.key_is_null ? "<null>" : record.key) << '\t'
                << record.value << '\n';
      ++printed;
    }
    if (!records->empty()) {
      const Status committed = consumer.Commit();
      if (!committed.ok()) return Fail(committed);
    }
  }

  std::cout << "-- " << printed << " record(s); committed positions:\n";
  for (const auto& [topic_partition, position] : consumer.positions()) {
    std::cout << "   " << topic_partition.ToString() << " -> " << position << '\n';
  }
  const Status left = consumer.Leave();
  if (!left.ok()) return Fail(left);
  return 0;
}

int CmdOffsets(client::ClientContext& context,
               const ConfigStore& flags,
               const std::vector<std::string>& args) {
  if (args.size() < 2) {
    std::cerr << "usage: pulselog-cli offsets <topic> [--partition=N]\n";
    return 1;
  }
  const auto partition = flags.GetInt("partition", -1);
  if (!partition.ok()) return Fail(partition.status());

  client::AdminClient admin(context);
  auto metadata = admin.GetMetadata({args[1]});
  if (!metadata.ok()) return Fail(metadata.status());
  if (metadata->topics.empty()) {
    std::cerr << "error: unknown topic " << args[1] << '\n';
    return 1;
  }

  client::Consumer consumer(context);
  std::cout << std::left << std::setw(12) << "PARTITION" << std::setw(14) << "EARLIEST"
            << "LATEST\n";
  for (const auto& meta : metadata->topics[0].partitions) {
    if (partition.value() >= 0 && meta.index.value() != partition.value()) continue;
    auto earliest = consumer.ListOffset(args[1], meta.index, kEarliestOffset);
    auto latest = consumer.ListOffset(args[1], meta.index, kLatestOffset);
    if (!earliest.ok()) return Fail(earliest.status());
    if (!latest.ok()) return Fail(latest.status());
    std::cout << std::left << std::setw(12) << meta.index.value() << std::setw(14)
              << earliest.value() << latest.value() << '\n';
  }
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  using namespace pulselog;

  if (argc < 2) {
    PrintUsage();
    return 1;
  }

  ConfigStore flags;
  const auto positional = flags.LoadCommandLine(argc, argv);
  if (positional.empty() || positional[0] == "help") {
    PrintUsage();
    return positional.empty() ? 1 : 0;
  }

  LogLevel level = LogLevel::kWarn;  // The CLI's own output is the product.
  (void)ParseLogLevel(flags.GetString("log.level", "warn"), level);
  SetLogLevel(level);

  client::ClientConfig config;
  const auto brokers = flags.GetList("brokers");
  if (!brokers.empty()) config.bootstrap_servers = brokers;
  client::ClientContext context(config);

  const std::string& command = positional[0];
  if (command == "create-topic") return CmdCreateTopic(context, flags, positional);
  if (command == "delete-topic") return CmdDeleteTopic(context, positional);
  if (command == "list-topics") return CmdListTopics(context);
  if (command == "metadata") return CmdMetadata(context, positional);
  if (command == "health") return CmdHealth(context);
  if (command == "describe-cluster") return CmdDescribeCluster(context);
  if (command == "produce") return CmdProduce(context, flags, positional);
  if (command == "consume") return CmdConsume(context, flags, positional);
  if (command == "consume-group") return CmdConsumeGroup(context, flags, positional);
  if (command == "offsets") return CmdOffsets(context, flags, positional);

  std::cerr << "unknown command: " << command << "\n\n";
  PrintUsage();
  return 1;
}
