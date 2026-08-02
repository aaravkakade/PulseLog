// pulselog-broker: the broker process.
//
// Usage:
//   pulselog-broker [--config=FILE] [--key=value ...]
//
// Configuration precedence is file < environment (PULSELOG_*) < flags. Every
// key is validated at start-up; an unparseable value is a start-up failure,
// not a silent fallback.
#include <csignal>
#include <cstdlib>
#include <iostream>
#include <string>

#include "pulselog/base/config.h"
#include "pulselog/base/logging.h"
#include "pulselog/broker/broker.h"

namespace {

// Written from a signal handler, so it must be a lock-free atomic of a type
// the standard guarantees is safe there.
volatile std::sig_atomic_t g_shutdown_requested = 0;

extern "C" void HandleSignal(int signal) {
  (void)signal;
  g_shutdown_requested = 1;
}

void PrintUsage() {
  std::cout << R"(pulselog-broker - a distributed event-streaming broker

Usage:
  pulselog-broker [--config=FILE] [--key=value ...]

Common options:
  --config=FILE                     properties file to load first
  --broker.id=N                     broker identity (default 0)
  --net.listen=HOST:PORT            bind address (default 0.0.0.0:9092)
  --net.advertised.host=HOST        address peers should use to reach us
  --net.advertised.port=PORT        port peers should use to reach us
  --cluster.brokers=1@h:p,2@h:p     static cluster membership
  --broker.data.dir=PATH            data directory (default ./pulselog-data)
  --net.io.threads=N                io loop threads (default 2)
  --broker.worker.threads=N         partition worker threads (default 2)
  --storage.flush.sync.on.append    fsync inside every append (slow, durable)
  --storage.write.mode=write|writev|mmap
  --metrics.port=PORT               Prometheus endpoint (default 9644)
  --log.level=trace|debug|info|warn|error
  --help                            print this message

Configuration precedence: file < PULSELOG_* environment < command line.
Documentation: docs/ARCHITECTURE.md
)";
}

}  // namespace

int main(int argc, char** argv) {
  using namespace pulselog;

  ConfigStore store;
  // The config file has to be located before anything else is parsed, since it
  // is the lowest-precedence source.
  for (int i = 1; i < argc; ++i) {
    const std::string arg(argv[i]);
    if (arg == "--help" || arg == "-h") {
      PrintUsage();
      return 0;
    }
    if (arg.starts_with("--config=")) {
      const std::string path = arg.substr(9);
      const Status status = store.LoadFile(path);
      if (!status.ok()) {
        std::cerr << "pulselog-broker: " << status.ToString() << '\n';
        return 1;
      }
    }
  }

  store.LoadEnvironment();
  (void)store.LoadCommandLine(argc, argv);

  auto config = broker::BrokerConfig::FromStore(store);
  if (!config.ok()) {
    std::cerr << "pulselog-broker: invalid configuration: " << config.status().ToString() << '\n';
    return 1;
  }

  // A key nobody reads is a misconfiguration, not a harmless extra. Starting
  // anyway means running on defaults while looking configured, which is how a
  // three-broker Docker cluster silently ran as three single-broker clusters:
  // the compose file wrote PULSELOG_CLUSTER__BROKERS, which maps to
  // `cluster_brokers`, and no one read it.
  //
  // This runs after FromStore, so every key the broker consumes has been read.
  if (const auto unread = store.UnreadKeys(); !unread.empty()) {
    std::cerr << "pulselog-broker: unrecognised configuration key(s):\n";
    for (const auto& key : unread) std::cerr << "  " << key << '\n';
    std::cerr << "Environment variables map PULSELOG_A_B to key `a.b`; a doubled "
                 "underscore\nis a literal underscore, not a separator. Run --help "
                 "for the key list.\n";
    return 1;
  }

  // Handle the signals a container runtime and a shell actually send. The
  // previous handler is discarded on purpose: nothing here restores it, and
  // the process owns these signals for its whole lifetime.
  (void)std::signal(SIGINT, HandleSignal);
  (void)std::signal(SIGTERM, HandleSignal);
  // Sockets are configured to suppress SIGPIPE, but ignoring it globally
  // guards any path that is not.
  (void)std::signal(SIGPIPE, SIG_IGN);

  broker::Broker instance(std::move(config).value());
  const Status started = instance.Start();
  if (!started.ok()) {
    std::cerr << "pulselog-broker: failed to start: " << started.ToString() << '\n';
    return 1;
  }

  PL_INFO("main") << "ready; send SIGINT or SIGTERM to shut down";
  while (g_shutdown_requested == 0 && instance.running()) {
    // Coarse polling: shutdown latency of up to 100 ms is irrelevant next to
    // the flush and join work that follows.
    struct timespec sleep_time{};
    sleep_time.tv_sec = 0;
    sleep_time.tv_nsec = 100'000'000;
    ::nanosleep(&sleep_time, nullptr);
  }

  PL_INFO("main") << "shutdown signal received";
  instance.Stop();
  return 0;
}
