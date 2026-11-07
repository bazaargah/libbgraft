// Leader-death churn: a new leader MUST be able to commit when its log
// carries an uncommitted entry from the previous term below the
// current-term entries a quorum now holds (CR-5 regression — the
// current-term walk-up gate stranded commit forever in exactly this
// shape).

#include <arpa/inet.h>
#include <bg/raft/raft.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <format>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "harness/check.h"

using namespace bg::raft;
using namespace std::chrono_literals;

namespace {

class ChurnSm final : public StateMachine {
 public:
  auto on_apply(const LogEntry& entry) -> void override {
    applied_.store(entry.index);
  }
  auto durable_watermark() const -> std::uint64_t override {
    return applied_.load();
  }
  auto take_snapshot(int fd) -> Result<void> override {
    const auto blob = std::format("{}\n", applied_.load());
    if (::write(fd, blob.data(), blob.size()) !=
        static_cast<ssize_t>(blob.size())) {
      return std::unexpected(Error::io_failure);
    }
    return {};
  }
  auto restore_snapshot(std::string_view, const SnapshotInfo& info, bool)
      -> Result<void> override {
    applied_.store(info.last_included_index);
    return {};
  }
  auto applied() const -> std::uint64_t { return applied_.load(); }

 private:
  std::atomic<std::uint64_t> applied_{};
};

struct Member {
  std::uint64_t id;
  std::unique_ptr<ChurnSm> sm;
  std::unique_ptr<Node> node;
};

auto probe_ports(std::size_t count) -> std::vector<std::uint16_t> {
  std::vector<std::uint16_t> ports;
  for (std::size_t i = 0; i < count; ++i) {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    CHECK(::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
    socklen_t len = sizeof(addr);
    CHECK(::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
    ports.push_back(ntohs(addr.sin_port));
    ::close(fd);
  }
  return ports;
}

auto make_config(std::uint64_t id, std::uint64_t cluster_id,
                 const std::vector<std::uint16_t>& ports,
                 const std::string& root) -> NodeConfig {
  NodeConfig cfg;
  cfg.node_id = id;
  cfg.cluster_id = cluster_id;
  cfg.listen = {"127.0.0.1", ports[id - 1U]};
  for (std::size_t i = 0; i < ports.size(); ++i) {
    cfg.peers.push_back({i + 1U, {"127.0.0.1", ports[i]}});
  }
  cfg.log_path = root + "/" + std::to_string(id) + "/raft.wal";
  cfg.snapshot_dir = root + "/" + std::to_string(id) + "/snapshots";
  std::filesystem::create_directories(cfg.snapshot_dir);
  cfg.fsync = FsyncMode::batched;
  return cfg;
}

template <typename Pred>
auto wait_until(Pred pred, std::uint64_t max_ms) -> bool {
  for (std::uint64_t waited = 0; waited < max_ms; waited += 20) {
    if (pred()) {
      return true;
    }
    std::this_thread::sleep_for(20ms);
  }
  return pred();
}

auto submit_one(Node& node, std::string_view text) -> Result<Node::CommitInfo> {
  std::vector<std::byte> data(text.size());
  for (std::size_t i = 0; i < text.size(); ++i) {
    data[i] = static_cast<std::byte>(text[i]);
  }
  return node.submit(std::span<const std::byte>{data});
}

auto leader_of(std::vector<std::unique_ptr<Member>>& ms) -> Member* {
  for (auto& m : ms) {
    if (m->node && m->node->role() == Role::leader) {
      return m.get();
    }
  }
  return nullptr;
}

void test_leader_death_with_stranded_term() {
  const auto ports = probe_ports(3);
  const auto root =
      (std::filesystem::temp_directory_path() /
       ("bgraft_churn_" + std::to_string(static_cast<long>(::getpid()))))
          .string();
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);

  std::vector<std::unique_ptr<Member>> ms;
  for (std::uint64_t id = 1; id <= 3; ++id) {
    auto m = std::make_unique<Member>();
    m->id = id;
    m->sm = std::make_unique<ChurnSm>();
    m->node = std::make_unique<Node>(make_config(id, 51, ports, root), *m->sm);
    CHECK(m->node->start().has_value());
    ms.push_back(std::move(m));
  }

  CHECK(wait_until([&] { return leader_of(ms) != nullptr; }, 10000));
  auto* first = leader_of(ms);
  CHECK(first != nullptr);
  if (first == nullptr) {
    return;
  }
  const auto c1 = submit_one(*first->node, "before-death");
  CHECK(c1.has_value());
  CHECK(wait_until(
      [&] {
        return ms[0]->sm->applied() >= 1U && ms[1]->sm->applied() >= 1U &&
               ms[2]->sm->applied() >= 1U;
      },
      8000));

  // Kill the leader WITHOUT letting every follower drain to its commit
  // index — an old-term entry stays uncommitted below the successor's
  // log tail. This is the CR-5 trigger.
  const auto dead_id = first->id;
  first->node->shutdown();
  first->node.reset();
  ms.erase(std::remove_if(ms.begin(), ms.end(),
                          [&](const std::unique_ptr<Member>& m) {
                            return m->id == dead_id;
                          }),
           ms.end());

  CHECK(wait_until(
      [&] {
        auto* l = leader_of(ms);
        return l != nullptr && l->id != dead_id;
      },
      10000));
  auto* second = leader_of(ms);
  CHECK(second != nullptr && second->id != dead_id);
  if (second == nullptr || second->id == dead_id) {
    return;
  }

  // The regression: this submit never committed under the bug.
  const auto c2 = submit_one(*second->node, "after-election");
  CHECK(c2.has_value());
  if (!c2) {
    return;
  }
  CHECK(wait_until(
      [&] {
        for (auto& m : ms) {
          if (m->id != dead_id && m->sm->applied() < c2->index) {
            return false;
          }
        }
        return true;
      },
      8000));

  // Restart the dead member; it must catch up and commit proceeds with
  // full membership.
  auto revived = std::make_unique<Member>();
  revived->id = dead_id;
  revived->sm = std::make_unique<ChurnSm>();
  revived->node = std::make_unique<Node>(make_config(dead_id, 51, ports, root),
                                         *revived->sm);
  CHECK(revived->node->start().has_value());
  auto* revived_ptr = revived.get();
  ms.push_back(std::move(revived));
  CHECK(wait_until([&] { return revived_ptr->sm->applied() >= c2->index; },
                   10000));

  auto* third = leader_of(ms);
  CHECK(third != nullptr);
  if (third != nullptr) {
    const auto c3 = submit_one(*third->node, "full-mesh");
    CHECK(c3.has_value());
    if (c3) {
      const auto ok = wait_until(
          [&] {
            for (auto& m : ms) {
              if (m->sm->applied() < c3->index) {
                return false;
              }
            }
            return true;
          },
          8000);
      CHECK(ok);
    }
  }

  for (auto& m : ms) {
    if (m->node) {
      m->node->shutdown();
    }
  }
}

}  // namespace

auto main() -> int {
  test_leader_death_with_stranded_term();
  return bgraft_test::summary("churn");
}
