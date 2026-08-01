// Consumer-group partition assignment.
//
// Both strategies are deterministic functions of (sorted member IDs, sorted
// partitions). Determinism matters for two reasons: every member computes the
// same answer without extra coordination, and a rebalance that changes nothing
// produces an identical assignment rather than shuffling partitions for no
// reason.
//
// The core invariant, enforced by construction and checked in tests: within
// one generation, every assigned partition goes to exactly one member.
#ifndef PULSELOG_CONSUMER_ASSIGNOR_H_
#define PULSELOG_CONSUMER_ASSIGNOR_H_

#include <map>
#include <string>
#include <vector>

#include "pulselog/base/types.h"
#include "pulselog/protocol/messages.h"

namespace pulselog::consumer {

using protocol::AssignmentStrategy;

// member id -> partitions.
using Assignment = std::map<std::string, std::vector<TopicPartition>>;

// Range assignment: partitions of each topic are split into contiguous blocks,
// one block per member. Members subscribed to the same topics end up with
// adjacent partitions, which keeps a consumer's fetches on a small number of
// brokers. The cost is uneven distribution when partitions % members != 0 --
// the first (partitions % members) members get one extra.
[[nodiscard]] Assignment AssignRange(const std::vector<std::string>& member_ids,
                                     const std::vector<TopicPartition>& partitions);

// Round-robin assignment: partitions are dealt out one at a time. Distribution
// is even to within one partition, but a member's partitions are scattered
// across topics and brokers.
[[nodiscard]] Assignment AssignRoundRobin(const std::vector<std::string>& member_ids,
                                          const std::vector<TopicPartition>& partitions);

[[nodiscard]] Assignment Assign(AssignmentStrategy strategy,
                                const std::vector<std::string>& member_ids,
                                const std::vector<TopicPartition>& partitions);

}  // namespace pulselog::consumer

#endif  // PULSELOG_CONSUMER_ASSIGNOR_H_
