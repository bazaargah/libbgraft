// Three-node in-process demo: elects a leader, commits a handful of
// entries, snapshots, and shuts everything down again.

#include <bg/raft/raft.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstdio>
#include <format>
#include <memory>
#include <print>
#include <span>
#include <string>
#include <thread>
#include <vector>

using namespace bg::raft;
using namespace std::chrono_literals;

namespace {

class CounterSm final : public StateMachine {
 public:
  auto on_apply(const LogEntry& entry) -> void override {
    applied_ = entry.index;
    count_ += 1;
  }

  auto take_snapshot(int fd) -> Result<void> override {
    const auto blob =
        std::format("{} {} {}\n", applied_, count_, term_at_snapshot_);
    if (::write(fd, blob.data(), blob.size()) !=
        static_cast<::ssize_t>(blob.size())) {
      return std::unexpected(Error::io_failure);
    }
    term_at_snapshot_ = applied_;
    return {};
  }

  auto restore_snapshot(std::string_view path, const SnapshotInfo& info,
                        bool out_of_band) -> Result<void> override {
    if (out_of_band) {  // pretend the bulk sync happened elsewhere
      applied_ = info.last_included_index;
      return {};
    }
    const std::string file(path);
    std::FILE* handle = std::fopen(file.c_str(), "r");
    if (handle == nullptr) {
      return std::unexpected(Error::restore_failed);
    }
    unsigned long long applied = 0;
    unsigned long long count = 0;
    // Return value checked immediately below; scanf is deliberate here.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,bugprone-unchecked-string-to-number-conversion,cert-err34-c)
    if (std::fscanf(handle, "%llu %llu", &applied, &count) != 2) {
      std::fclose(handle);
      return std::unexpected(Error::restore_failed);
    }
    std::fclose(handle);
    applied_ = applied;
    count_ = count;
    return {};
  }

  [[nodiscard]] auto applied() const -> std::uint64_t { return applied_; }

 private:
  std::uint64_t applied_{};
  std::uint64_t count_{};
  std::uint64_t term_at_snapshot_{};
};

auto temp_dir() -> std::string {
  std::array<char, 64> tmpl{"/tmp/bgraft_demo_XXXXXX"};
  const char* dir = mkdtemp(tmpl.data());
  return std::string{dir != nullptr ? dir : "/tmp"};
}

}  // namespace

auto main() -> int {
  const auto dir = temp_dir();
  const std::vector<std::uint16_t> ports{40601, 40602, 40603};

  std::vector<std::unique_ptr<CounterSm>> machines;
  std::vector<std::unique_ptr<Node>> nodes;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    NodeConfig cfg;
    cfg.node_id = id;
    cfg.cluster_id = 1;
    cfg.listen = Endpoint{.host = "127.0.0.1", .port = ports[id - 1]};
    for (std::uint64_t peer = 1; peer <= 3; ++peer) {
      cfg.peers.push_back(PeerConfig{
          .id = peer,
          .endpoint = Endpoint{.host = "127.0.0.1", .port = ports[peer - 1]},
      });
    }
    cfg.election_timeout_min = 300ms;
    cfg.election_timeout_max = 600ms;
    cfg.heartbeat_period = 100ms;
    cfg.log_path = std::format("{}/node{}.log", dir, id);
    cfg.snapshot_dir = dir;
    machines.push_back(std::make_unique<CounterSm>());
    nodes.push_back(std::make_unique<Node>(cfg, *machines.back()));
    if (auto r = nodes.back()->start(); !r) {
      std::println(stderr, "node {} failed to start: {}", id,
                   to_string(r.error()));
      return 1;
    }
  }

  // Wait for a leader.
  Node* leader = nullptr;
  for (int i = 0; i < 100 && leader == nullptr; ++i) {
    for (auto& node : nodes) {
      if (node->role() == Role::leader) {
        leader = node.get();
        break;
      }
    }
    std::this_thread::sleep_for(100ms);
  }
  if (leader == nullptr) {
    std::println(stderr, "no leader elected");
    return 1;
  }
  const auto leader_id = leader->leader_id().value_or(0);
  std::println("leader is node {} in term {}", leader_id,
               leader->current_term());

  for (std::uint64_t i = 1; i <= 5; ++i) {
    const auto data = std::format("command-{}", i);
    const std::span<const std::byte> bytes{
        reinterpret_cast<const std::byte*>(data.data()), data.size()};
    auto result = leader->submit(bytes);
    if (!result) {
      std::println(stderr, "submit failed: {}", to_string(result.error()));
      break;
    }
    std::println("committed index {} (term {})", result->index, result->term);
  }

  // Wait until every replica applied everything.
  const auto target = leader->last_applied();
  for (std::size_t i = 0; i < nodes.size(); ++i) {
    for (int w = 0; w < 100 && nodes[i]->last_applied() < target; ++w) {
      std::this_thread::sleep_for(50ms);
    }
    std::println("node {} applied {} entries", i + 1, nodes[i]->last_applied());
  }

  if (auto snap = leader->make_snapshot(); snap) {
    std::println("snapshot at index {}", *snap);
  }

  for (auto& node : nodes) {
    node->shutdown();
  }
  std::println("demo complete (data in {})", dir);
  return 0;
}
