// The broker: lifecycle, request routing, and the background threads.
//
// Thread inventory (see docs/CONCURRENCY_MODEL.md for the full ownership map):
//
//   1 accept thread      owns the listening socket
//   N io threads         own connections; parse frames; send responses
//   M worker threads     own partitions; append, read, assign offsets
//   1 flusher thread     batches fsync; completes durability waiters
//   1 maintenance thread retention, group session expiry, waiter timeouts
//   1 metrics thread     samples process stats; serves the HTTP endpoint
//   R replication threads one per followed leader (created by Replicator)
#ifndef PULSELOG_BROKER_BROKER_H_
#define PULSELOG_BROKER_BROKER_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "pulselog/base/status.h"
#include "pulselog/broker/broker_config.h"
#include "pulselog/broker/partition_manager.h"
#include "pulselog/broker/worker.h"
#include "pulselog/concurrency/thread_util.h"
#include "pulselog/consumer/group_coordinator.h"
#include "pulselog/metadata/cluster_metadata.h"
#include "pulselog/metrics/exporter.h"
#include "pulselog/metrics/process_stats.h"
#include "pulselog/metrics/registry.h"
#include "pulselog/net/sync_client.h"
#include "pulselog/net/tcp_server.h"
#include "pulselog/replication/replicator.h"

namespace pulselog::broker {

class Broker final : public RequestExecutor {
 public:
  explicit Broker(BrokerConfig config);

  Broker(const Broker&) = delete;
  Broker& operator=(const Broker&) = delete;

  ~Broker() override;

  [[nodiscard]] Status Start();

  void Stop();

  [[nodiscard]] bool running() const noexcept { return running_.load(std::memory_order_acquire); }

  // Endpoint actually bound; resolves an ephemeral port when 0 was requested.
  [[nodiscard]] net::Endpoint endpoint() const;

  [[nodiscard]] std::uint16_t metrics_port() const;

  [[nodiscard]] const BrokerConfig& config() const noexcept { return config_; }

  [[nodiscard]] metadata::ClusterMetadata& cluster() noexcept { return cluster_; }

  [[nodiscard]] PartitionManager& partitions() noexcept { return partitions_; }

  [[nodiscard]] metrics::MetricRegistry& metric_registry() noexcept { return registry_; }

  [[nodiscard]] std::int64_t uptime_ms() const;

  // RequestExecutor: runs on a worker thread.
  void Execute(WorkerRequest& request) override;

 private:
  // --- io-loop-thread entry points -----------------------------------------
  void OnFrame(net::Connection& connection, const protocol::FrameDecoder::Frame& frame);

  void OnConnectionClosed(net::Connection& connection, const Status& reason);

  // Handles the operations that need no partition ownership, inline on the io
  // thread: metadata, health, topic administration, group coordination. They
  // are short and touch only mutex-guarded shared state.
  [[nodiscard]] bool HandleInline(net::Connection& connection,
                                  const protocol::FrameDecoder::Frame& frame);

  // Copies the payload and routes to the owning worker. Answers the client
  // directly with BACKPRESSURE when that worker's queue is full.
  void RouteToWorker(net::Connection& connection,
                     const protocol::FrameDecoder::Frame& frame,
                     std::size_t worker_index);

  // Which worker owns the partition addressed by this request, or nullopt when
  // the payload cannot be parsed far enough to tell.
  [[nodiscard]] std::optional<std::size_t> WorkerForRequest(
      const protocol::FrameDecoder::Frame& frame);

  // --- worker-thread handlers ----------------------------------------------
  void ExecuteProduce(WorkerRequest& request);

  void ExecuteFetch(WorkerRequest& request);

  void ExecuteListOffsets(WorkerRequest& request);

  void ExecuteReplicate(WorkerRequest& request);

  void ExecuteReplicaFetch(WorkerRequest& request);

  void ExecuteReplicaAck(WorkerRequest& request);

  void ExecuteCreateTopic(WorkerRequest& request);

  void ExecuteDeleteTopic(WorkerRequest& request);

  // Sends a response frame back on the connection that made the request, by
  // posting onto that connection's io loop. Safe if the connection is gone.
  void Respond(std::size_t loop_index,
               std::uint64_t connection_id,
               protocol::OpCode opcode,
               RequestId request_id,
               ByteBuffer&& payload);

  void RespondError(std::size_t loop_index,
                    std::uint64_t connection_id,
                    protocol::OpCode opcode,
                    RequestId request_id,
                    ErrorCode code,
                    std::string_view message);

  // Finds or auto-creates the partition a request addresses.
  [[nodiscard]] Result<PartitionReplica*> ResolvePartition(const std::string& topic,
                                                           PartitionIndex partition,
                                                           bool auto_create);

  // --- background loops -----------------------------------------------------
  void FlusherLoop();

  void MaintenanceLoop();

  void MetricsLoop();

  // Topic creation has exactly one owner: the controller (the lowest broker
  // ID). Any broker that needs a topic created forwards the request there and
  // then reconciles. Without a single owner, two brokers can create the same
  // topic with different partition counts -- which silently reroutes keys and
  // makes replication fail with "this broker does not host X".
  // `apply_locally` is set when the controller pushed this topic to us: the
  // decision is already made, so it is applied here rather than forwarded.
  [[nodiscard]] Result<metadata::TopicDescriptor> EnsureTopic(const std::string& topic,
                                                              std::int32_t partitions,
                                                              std::int16_t replication_factor,
                                                              bool apply_locally = false);

  // Pushes a newly created topic to every peer, synchronously, so that a
  // successful CreateTopic means every reachable broker can already serve and
  // replicate it. Unreachable peers pick it up through reconciliation.
  void BroadcastTopic(const metadata::TopicDescriptor& descriptor);

  // Pulls topic metadata from the controller and adopts anything unknown.
  // Assignments are deterministic, so adopting the controller's view is the
  // same as recomputing it -- but taking it verbatim also carries leader
  // epochs, which are not derivable.
  [[nodiscard]] Status ReconcileMetadataFromController();

  [[nodiscard]] Status LoadOrInitialiseMetadata();

  [[nodiscard]] Status PersistMetadata();

  [[nodiscard]] std::string BuildTopologyJson() const;

  BrokerConfig config_;
  metrics::MetricRegistry registry_;
  std::unique_ptr<metrics::BrokerMetrics> metrics_;
  metadata::ClusterMetadata cluster_;
  PartitionManager partitions_;

  std::unique_ptr<net::TcpServer> server_;
  std::vector<std::unique_ptr<PartitionWorker>> workers_;
  std::unique_ptr<replication::Replicator> replicator_;
  std::unique_ptr<consumer::GroupCoordinator> groups_;
  std::unique_ptr<metrics::MetricsExporter> exporter_;

  // Per-io-loop connection registries. Each is touched only by its own loop
  // thread, so no lock is needed; the outer vector is sized once at start-up.
  std::vector<std::unordered_map<std::uint64_t, net::Connection*>> loop_connections_;

  // Buffers for request payload copies and response encoding. One pool per
  // worker so workers never contend on the pool mutex.
  std::vector<std::unique_ptr<BufferPool>> worker_pools_;
  std::vector<std::unique_ptr<BufferPool>> loop_pools_;

  NamedThread flusher_thread_;
  NamedThread maintenance_thread_;
  NamedThread metrics_thread_;

  // Connection to the controller, used only by the maintenance thread and by
  // topic-creation forwarding on worker threads. Guarded because both touch it.
  std::mutex controller_mutex_;
  std::unique_ptr<net::SyncClient> controller_client_;
  std::atomic<RequestId> control_request_id_{1};

  std::atomic<bool> running_{false};
  std::atomic<bool> stopping_{false};
  std::int64_t start_nanos_ = 0;
  std::atomic<std::uint64_t> round_robin_counter_{0};
};

}  // namespace pulselog::broker

#endif  // PULSELOG_BROKER_BROKER_H_
