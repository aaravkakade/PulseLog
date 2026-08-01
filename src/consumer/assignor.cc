#include "pulselog/consumer/assignor.h"

#include <algorithm>

namespace pulselog::consumer {
namespace {

// Both strategies sort their inputs so the result depends only on the *set* of
// members and partitions, never on the order they happened to arrive in.
std::vector<std::string> SortedMembers(const std::vector<std::string>& members) {
  std::vector<std::string> sorted = members;
  std::sort(sorted.begin(), sorted.end());
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
  return sorted;
}

std::vector<TopicPartition> SortedPartitions(const std::vector<TopicPartition>& partitions) {
  std::vector<TopicPartition> sorted = partitions;
  std::sort(sorted.begin(), sorted.end(), [](const TopicPartition& a, const TopicPartition& b) {
    if (a.topic != b.topic) return a.topic < b.topic;
    return a.partition < b.partition;
  });
  sorted.erase(std::unique(sorted.begin(), sorted.end()), sorted.end());
  return sorted;
}

}  // namespace

Assignment AssignRange(const std::vector<std::string>& member_ids,
                       const std::vector<TopicPartition>& partitions) {
  Assignment assignment;
  const auto members = SortedMembers(member_ids);
  if (members.empty()) return assignment;
  for (const auto& member : members) assignment[member];  // Members with nothing still appear.

  const auto sorted = SortedPartitions(partitions);

  // Group by topic so each topic is divided independently -- that is what
  // makes a member's partitions contiguous within a topic.
  std::map<std::string, std::vector<TopicPartition>> by_topic;
  for (const auto& partition : sorted) by_topic[partition.topic].push_back(partition);

  for (const auto& [topic, topic_partitions] : by_topic) {
    const std::size_t count = topic_partitions.size();
    const std::size_t per_member = count / members.size();
    const std::size_t remainder = count % members.size();

    std::size_t cursor = 0;
    for (std::size_t m = 0; m < members.size(); ++m) {
      // The first `remainder` members take one extra partition.
      const std::size_t take = per_member + (m < remainder ? 1 : 0);
      for (std::size_t i = 0; i < take && cursor < count; ++i, ++cursor) {
        assignment[members[m]].push_back(topic_partitions[cursor]);
      }
    }
  }
  return assignment;
}

Assignment AssignRoundRobin(const std::vector<std::string>& member_ids,
                            const std::vector<TopicPartition>& partitions) {
  Assignment assignment;
  const auto members = SortedMembers(member_ids);
  if (members.empty()) return assignment;
  for (const auto& member : members) assignment[member];

  const auto sorted = SortedPartitions(partitions);
  for (std::size_t i = 0; i < sorted.size(); ++i) {
    assignment[members[i % members.size()]].push_back(sorted[i]);
  }
  return assignment;
}

Assignment Assign(AssignmentStrategy strategy, const std::vector<std::string>& member_ids,
                  const std::vector<TopicPartition>& partitions) {
  switch (strategy) {
    case AssignmentStrategy::kRoundRobin:
      return AssignRoundRobin(member_ids, partitions);
    case AssignmentStrategy::kRange:
      break;
  }
  return AssignRange(member_ids, partitions);
}

}  // namespace pulselog::consumer
