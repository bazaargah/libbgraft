#include <arpa/inet.h>
#include <bg/raft/raft.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "harness/check.h"

using namespace bg::raft;
using namespace std::chrono_literals;

namespace {

// In-memory state machine with a simple text snapshot format. Out-of-band
// restores succeed by pretending the bulk sync already happened.
class TestSm final : public StateMachine {
 public:
  auto on_apply(const LogEntry& entry) -> void override {
    std::scoped_lock lock(mutex_);
    std::string data;
    for (const auto b : entry.data) {
      data.push_back(static_cast<char>(b));
    }
    applied_.emplace_back(entry.index, std::move(data));
    applied_index_ = entry.index;
  }

  auto take_snapshot(int fd) -> Result<void> override {
    std::scoped_lock lock(mutex_);
    std::string blob = std::format("{}\n", applied_index_);
    for (const auto& [index, data] : applied_) {
      blob += std::format("{}|{}\n", index, data);
    }
    if (::write(fd, blob.data(), blob.size()) !=
        static_cast<::ssize_t>(blob.size())) {
      return std::unexpected(Error::io_failure);
    }
    return {};
  }

  auto restore_snapshot(std::string_view path, const SnapshotInfo& info,
                        bool out_of_band) -> Result<void> override {
    if (out_of_band) {
      std::scoped_lock lock(mutex_);
      applied_.clear();
      applied_index_ = info.last_included_index;
      return {};
    }
    const std::string file(path);
    auto* handle = std::fopen(file.c_str(), "r");
    if (handle == nullptr) {
      return std::unexpected(Error::restore_failed);
    }
    std::string blob;
    std::vector<char> buffer(4096);
    while (std::fgets(buffer.data(), static_cast<int>(buffer.size()), handle) !=
           nullptr) {
      blob.append(buffer.data());
    }
    std::fclose(handle);
    std::scoped_lock lock(mutex_);
    applied_.clear();
    const auto nl = blob.find('\n');
    if (nl == std::string::npos) {
      return std::unexpected(Error::restore_failed);
    }
    applied_index_ = std::stoull(blob.substr(0, nl));
    std::string rest = blob.substr(nl + 1);
    while (!rest.empty()) {
      const auto line_end = rest.find('\n');
      const auto line = rest.substr(0, line_end);
      const auto bar = line.find('|');
      if (bar == std::string::npos) {
        break;
      }
      applied_.emplace_back(std::stoull(line.substr(0, bar)),
                            line.substr(bar + 1));
      if (line_end == std::string::npos) {
        break;
      }
      rest = rest.substr(line_end + 1);
    }
    return {};
  }

  [[nodiscard]] auto history() const
      -> std::vector<std::pair<std::uint64_t, std::string>> {
    std::scoped_lock lock(mutex_);
    return applied_;
  }

 private:
  mutable std::mutex mutex_;
  std::vector<std::pair<std::uint64_t, std::string>> applied_;
  std::uint64_t applied_index_{};
};

// Ask the OS for free loopback ports. A small race window exists but is
// acceptable for a local test.
auto probe_ports(std::size_t count) -> std::vector<std::uint16_t> {
  std::vector<std::uint16_t> ports;
  for (std::size_t i = 0; i < count; ++i) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    CHECK(fd >= 0);
    if (fd < 0) {
      break;  // never bind()/close() an invalid fd; later phases fail loudly
    }
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

auto wait_for(const std::function<bool()>& predicate,
              std::chrono::milliseconds budget) -> bool {
  const auto deadline = std::chrono::steady_clock::now() + budget;
  while (std::chrono::steady_clock::now() < deadline) {
    if (predicate()) {
      return true;
    }
    std::this_thread::sleep_for(10ms);
  }
  return predicate();
}

struct Member {
  std::unique_ptr<TestSm> sm{std::make_unique<TestSm>()};
  std::unique_ptr<Node> node;
  NodeConfig cfg;
};

auto make_config(std::uint64_t id, std::uint64_t cluster_id,
                 const std::vector<std::uint16_t>& ports,
                 const std::string& dir) -> NodeConfig {
  NodeConfig cfg;
  cfg.node_id = id;
  cfg.cluster_id = cluster_id;
  cfg.listen = Endpoint{.host = "127.0.0.1", .port = ports[id - 1]};
  for (std::uint64_t peer = 1; peer <= ports.size(); ++peer) {
    cfg.peers.push_back(PeerConfig{
        .id = peer,
        .endpoint = Endpoint{.host = "127.0.0.1", .port = ports[peer - 1]},
    });
  }
  cfg.election_timeout_min = 300ms;
  cfg.election_timeout_max = 600ms;
  cfg.heartbeat_period = 100ms;
  cfg.log_path = dir + "/node" + std::to_string(id) + ".log";
  cfg.snapshot_dir = dir;
  cfg.fsync = FsyncMode::batched;
  return cfg;
}

auto temp_dir(std::string_view name) -> std::string {
  const std::filesystem::path base{
      std::filesystem::temp_directory_path() / ("bgraft_" + std::string{name}),
  };
  std::filesystem::remove_all(base);
  std::filesystem::create_directories(base);
  return base.string() + "/";
}

// Issues a throwaway CA plus one certificate per node using the openssl
// CLI. Returns false when the CLI is unavailable, letting the TLS phase
// skip itself instead of failing on machines without openssl installed.
auto generate_certs(const std::string& dir, std::uint64_t count) -> bool {
  const std::string quiet = " >/dev/null 2>&1";
  auto run = [&](std::string_view cmd) -> bool {
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c,concurrency-mt-unsafe)
    return std::system((std::string{cmd} + quiet).c_str()) == 0;
  };
  if (!run("openssl version")) {
    return false;
  }
  if (!run(std::format(
          "openssl req -x509 -newkey rsa:2048 -nodes -keyout {}ca.key "
          "-out {}ca.crt -days 2 -subj /CN=bgraft-test-ca",
          dir, dir))) {
    return false;
  }
  for (std::uint64_t id = 1; id <= count; ++id) {
    const std::string base = dir + "node" + std::to_string(id);
    if (!run(std::format(
            "openssl req -new -newkey rsa:2048 -nodes -keyout {}.key "
            "-out {}.csr -subj /CN=node{}",
            base, base, id))) {
      return false;
    }
    if (!run(std::format(
            "openssl x509 -req -in {}.csr -CA {}ca.crt -CAkey {}ca.key "
            "-CAcreateserial -days 2 -out {}.crt",
            base, dir, dir, base))) {
      return false;
    }
  }
  return true;
}

auto leader_of(std::vector<std::unique_ptr<Member>>& members) -> Member* {
  for (const auto& m : members) {
    if (m->node != nullptr && m->node->role() == Role::leader) {
      return m.get();
    }
  }
  return nullptr;
}

auto to_bytes(std::string_view text) -> std::vector<std::byte> {
  std::vector<std::byte> out;
  out.reserve(text.size());
  for (const char c : text) {
    out.push_back(static_cast<std::byte>(c));
  }
  return out;
}

auto submit_n(Member& member, std::uint64_t count, std::string_view tag)
    -> std::uint64_t {
  std::uint64_t last_index = 0;
  for (std::uint64_t i = 0; i < count; ++i) {
    const auto data = to_bytes(std::format("{}{}", tag, i));
    Result<Node::CommitInfo> result{std::unexpected(Error::not_leader)};
    // Leadership can change underneath the caller; a real client retries.
    for (int attempt = 0; attempt < 20 && !result.has_value(); ++attempt) {
      result = member.node->submit(data);
      if (!result) {
        std::this_thread::sleep_for(250ms);
      }
    }
    CHECK(result.has_value());
    if (!result) {
      std::println(
          stderr, "submit {} failed: {} role={} term={}",
          static_cast<unsigned long long>(member.cfg.node_id),
          to_string(result.error()).data(),
          static_cast<int>(member.node->role()),
          static_cast<unsigned long long>(member.node->current_term()));
    }
    if (result) {
      CHECK(result->index > last_index);
      last_index = result->index;
    }
  }
  return last_index;
}

class Cluster {
 public:
  explicit Cluster(std::string_view name, std::size_t count = 3,
                   std::uint64_t cluster_id = 77,
                   const std::string& certs_dir = "")
      : dir_(temp_dir(name)), ports_(probe_ports(count)) {
    for (std::uint64_t id = 1; id <= count; ++id) {
      auto member = std::make_unique<Member>();
      member->cfg = make_config(id, cluster_id, ports_, dir_);
      if (!certs_dir.empty()) {
        const std::string base = certs_dir + "node" + std::to_string(id);
        member->cfg.tls = TlsConfig{.cert_path = base + ".crt",
                                    .key_path = base + ".key",
                                    .ca_path = certs_dir + "ca.crt",
                                    };
      }
      members_.push_back(std::move(member));
    }
  }

  auto start_all() -> void {
    for (auto& member : members_) {
      member->node = std::make_unique<Node>(member->cfg, *member->sm);
      CHECK(member->node->start().has_value());
    }
  }

  auto kill(std::size_t index) -> void { members_[index]->node.reset(); }

  // Replaces the member with a fresh node + state machine over the same
  // files on disk.
  auto restart(std::size_t index) -> Member& {
    auto& member = *members_[index];
    member.sm = std::make_unique<TestSm>();
    member.node = std::make_unique<Node>(member.cfg, *member.sm);
    CHECK(member.node->start().has_value());
    return member;
  }

  auto members() -> std::vector<std::unique_ptr<Member>>& { return members_; }
  auto port(std::size_t index) const -> std::uint16_t { return ports_[index]; }
  auto dir() const -> std::string { return dir_; }

 private:
  std::string dir_;
  std::vector<std::uint16_t> ports_;
  std::vector<std::unique_ptr<Member>> members_;
};

void test_election_and_replication(Cluster& cluster) {
  cluster.start_all();
  CHECK(wait_for(
      [&] -> bool { return leader_of(cluster.members()) != nullptr; }, 10s));
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }
  CHECK_EQ(leader->node->current_term(), 1U);

  const auto last = submit_n(*leader, 10, "op");
  CHECK(last >= 11U);  // index 1 is the leader's no-op entry
  CHECK(wait_for(
      [&] -> bool {
        for (auto& member : cluster.members()) {
          if (member->node->last_applied() < last) {
            return false;
          }
        }
        return true;
      },
      15s));
  // Every node replayed an identical history.
  const auto expected = cluster.members()[0]->sm->history();
  CHECK(expected == cluster.members()[1]->sm->history());
  CHECK(expected == cluster.members()[2]->sm->history());
  CHECK_EQ(leader->node->leader_id().value_or(0U), leader->cfg.node_id);

  // Entry size boundary: the maximum legal entry must replicate and
  // commit end-to-end (this exercises the exact frame-size math), while
  // one byte more is rejected up front with a distinct error.
  std::vector<std::byte> exact(kMaxEntryDataBytes, std::byte{7U});
  const auto big = leader->node->submit(std::span<const std::byte>(exact));
  CHECK(big.has_value());
  std::vector<std::byte> too_big(kMaxEntryDataBytes + 1U, std::byte{0U});
  const auto over = leader->node->submit(std::span<const std::byte>(too_big));
  CHECK(!over.has_value());
  if (!over) {
    CHECK(over.error() == Error::entry_too_large);
  }
  if (big) {
    CHECK(wait_for(
        [&] -> bool {
          for (auto& member : cluster.members()) {
            if (member->node->last_applied() < big->index) {
              return false;
            }
          }
          return true;
        },
        15s));
  }
}

void test_leader_failure(Cluster& cluster) {
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }
  const auto dead_index = static_cast<std::size_t>(leader->cfg.node_id - 1);
  cluster.kill(dead_index);
  CHECK(wait_for(
      [&] -> bool {
        auto* now = leader_of(cluster.members());
        return now != nullptr && now->cfg.node_id != leader->cfg.node_id;
      },
      15s));
  auto* new_leader = leader_of(cluster.members());
  CHECK(new_leader != nullptr);
  if (new_leader == nullptr) {
    return;
  }
  CHECK(new_leader->node->current_term() >= 2U);
  const auto last = submit_n(*new_leader, 3, "after");
  CHECK(wait_for(
      [&] -> bool {
        for (auto& member : cluster.members()) {
          if (member->node != nullptr && member->node->last_applied() < last) {
            return false;
          }
        }
        return true;
      },
      15s));
}

void test_restart_replays(Cluster& cluster) {
  // Bring the previously dead member back with a fresh state machine; the
  // WAL re-delivers committed entries.
  std::size_t dead_index = SIZE_MAX;
  for (std::size_t i = 0; i < cluster.members().size(); ++i) {
    if (cluster.members()[i]->node == nullptr) {
      dead_index = i;
    }
  }
  CHECK(dead_index != SIZE_MAX);
  if (dead_index == SIZE_MAX) {
    return;
  }
  auto& revived = cluster.restart(dead_index);
  CHECK(wait_for(
      [&] -> bool { return leader_of(cluster.members()) != nullptr; }, 15s));
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }
  const auto last = submit_n(*leader, 1, "revived");
  CHECK(wait_for([&] -> bool { return revived.node->last_applied() >= last; },
                 15s));
  CHECK(wait_for([&] -> bool { return revived.node->role() == Role::follower; },
                 10s));
  CHECK(wait_for(
      [&] -> bool {
        return revived.node->leader_id().value_or(0U) == leader->cfg.node_id;
      },
      10s));
  const auto revived_hist = revived.sm->history();
  const auto leader_hist = leader->sm->history();
  if (revived_hist != leader_hist) {
    std::println(stderr, "--- revived({}) vs leader({}) histories ---",
                 static_cast<unsigned long long>(revived.cfg.node_id),
                 static_cast<unsigned long long>(leader->cfg.node_id));
    for (const auto& [index, value] : revived_hist) {
      std::println(stderr, "R {} {}", static_cast<unsigned long long>(index),
                   value);
    }
    for (const auto& [index, value] : leader_hist) {
      std::println(stderr, "L {} {}", static_cast<unsigned long long>(index),
                   value);
    }
  }
  CHECK(revived_hist == leader_hist);
}

void test_out_of_band_snapshot(Cluster& cluster) {
  // Isolate a follower (never the leader), advance the cluster, snapshot on
  // the leader, then bring the member back: it lags behind the compacted
  // prefix, so the leader offers an out-of-band install which the test sm
  // accepts.
  CHECK(wait_for(
      [&] -> bool { return leader_of(cluster.members()) != nullptr; }, 15s));
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }
  const auto leader_index = static_cast<std::size_t>(leader->cfg.node_id - 1U);
  const std::size_t isolate = leader_index == 2U ? 1U : 2U;
  cluster.kill(isolate);
  const auto before = submit_n(*leader, 4, "solo");
  const auto snapped = leader->node->make_snapshot();
  CHECK(snapped.has_value());
  if (!snapped) {
    return;
  }
  const auto snap_index = *snapped;
  CHECK(snap_index >= before);
  const auto snap_file = std::filesystem::path{
      std::format("{}/snapshot.{}.bin", cluster.dir(), snap_index)};
  CHECK(std::filesystem::exists(snap_file));

  auto& rejoined = cluster.restart(isolate);
  CHECK(wait_for(
      [&] -> bool { return rejoined.node->last_applied() >= snap_index; },
      15s));
  // Replication resumes normally on top of the adopted snapshot.
  const auto last = submit_n(*leader, 2, "post-snap");
  CHECK(wait_for(
      [&] -> bool {
        for (auto& member : cluster.members()) {
          if (member->node != nullptr && member->node->last_applied() < last) {
            return false;
          }
        }
        return true;
      },
      15s));
}

void test_foreign_cluster_dropped(Cluster& cluster) {
  CHECK(wait_for(
      [&] -> bool { return leader_of(cluster.members()) != nullptr; }, 15s));
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }
  // The leader may currently be member index 0, 1 or 2; find its id for the
  // rogue's peer list. Real config indexes match node ids, but ports are
  // per member, so locate the leader's own port by id.
  const auto rogue_ports = probe_ports(1);
  NodeConfig rogue_cfg;
  rogue_cfg.node_id = 9;
  rogue_cfg.cluster_id = 999;  // different cluster on purpose
  rogue_cfg.listen = Endpoint{.host = "127.0.0.1", .port = rogue_ports[0]};
  for (const auto& member : cluster.members()) {
    // Give the rogue the whole real cluster as its (mismatched) peers so it
    // actively dials; ids 1..3 under cluster 77.
    (void)member;
  }
  const auto& real_cfg = cluster.members()[0]->cfg;
  auto rogue_peers = real_cfg.peers;
  // Static membership requires self in the peer list; id 9 on its own port.
  rogue_peers.push_back(PeerConfig{
      .id = 9,
      .endpoint = Endpoint{.host = "127.0.0.1", .port = rogue_ports[0]},
  });
  rogue_cfg.peers = rogue_peers;
  rogue_cfg.election_timeout_min = 300ms;
  rogue_cfg.election_timeout_max = 600ms;
  rogue_cfg.heartbeat_period = 100ms;
  const auto rogue_dir = temp_dir("rogue");
  rogue_cfg.log_path = rogue_dir + "/node9.log";
  rogue_cfg.snapshot_dir = rogue_dir;

  TestSm rogue_sm;
  {
    Node rogue{rogue_cfg, rogue_sm};
    CHECK(rogue.start().has_value());
    std::this_thread::sleep_for(1500ms);
    CHECK(rogue.role() != Role::leader);
    CHECK(!rogue.leader_id().has_value());
  }
  // The real cluster is unharmed.
  const auto last = submit_n(*leader, 1, "healthy");
  CHECK(wait_for(
      [&] -> bool {
        for (auto& member : cluster.members()) {
          if (member->node != nullptr && member->node->last_applied() < last) {
            return false;
          }
        }
        return true;
      },
      10s));
}

// Mutual TLS: an encrypted cluster must elect, replicate, and stay sealed
// against a plaintext peer and against a peer holding valid certificates
// from a foreign CA — both with the correct cluster id, so only the
// transport layer can keep them out.
void test_tls_transport() {
  const std::string certs = temp_dir("tls_certs");
  const std::string foreign = temp_dir("tls_foreign_certs");
  if (!generate_certs(certs, 3)) {
    std::println(stderr, "SKIP tls: openssl CLI unavailable");
    return;
  }
  const bool foreign_ok = generate_certs(foreign, 1);

  Cluster cluster{"tls_cluster", 3, 88, certs};
  cluster.start_all();
  CHECK(wait_for(
      [&] -> bool { return leader_of(cluster.members()) != nullptr; }, 15s));
  auto* leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader == nullptr) {
    return;
  }

  const auto last = submit_n(*leader, 4, "tls");
  CHECK(wait_for(
      [&] -> bool {
        for (auto& member : cluster.members()) {
          if (member->node->last_applied() < last) {
            return false;
          }
        }
        return true;
      },
      15s));

  auto spawn_rogue = [&](std::uint64_t id, const TlsConfig& tls) -> void {
    const auto rogue_ports = probe_ports(1);
    NodeConfig cfg;
    cfg.node_id = id;
    cfg.cluster_id = 88;  // deliberately correct: TLS alone must reject it
    cfg.listen = Endpoint{.host = "127.0.0.1", .port = rogue_ports[0]};
    cfg.peers = cluster.members()[0]->cfg.peers;
    cfg.peers.push_back(PeerConfig{
        .id = id,
        .endpoint = Endpoint{.host = "127.0.0.1", .port = rogue_ports[0]},
    });
    cfg.election_timeout_min = 300ms;
    cfg.election_timeout_max = 600ms;
    cfg.heartbeat_period = 100ms;
    const auto rogue_dir = temp_dir(std::format("tls_rogue{}", id));
    cfg.log_path = rogue_dir + "/node.log";
    cfg.snapshot_dir = rogue_dir;
    cfg.tls = tls;

    TestSm rogue_sm;
    {
      Node rogue{cfg, rogue_sm};
      CHECK(rogue.start().has_value());
      std::this_thread::sleep_for(1500ms);
      CHECK(rogue.role() != Role::leader);
      CHECK(!rogue.leader_id().has_value());
      CHECK(rogue_sm.history().empty());
    }
  };

  spawn_rogue(9, TlsConfig{});  // plain TCP against a TLS cluster
  if (foreign_ok) {
    spawn_rogue(10, TlsConfig{.cert_path = foreign + "node1.crt",
                              .key_path = foreign + "node1.key",
                              .ca_path = foreign + "ca.crt",
                              });
  }

  // The encrypted cluster is unharmed by both attacks.
  leader = leader_of(cluster.members());
  CHECK(leader != nullptr);
  if (leader != nullptr) {
    const auto later = submit_n(*leader, 2, "after");
    CHECK(wait_for(
        [&] -> bool {
          for (auto& member : cluster.members()) {
            if (member->node->last_applied() < later) {
              return false;
            }
          }
          return true;
        },
        15s));
  }
  for (auto& member : cluster.members()) {
    member->node->shutdown();
  }
}

}  // namespace

auto main() -> int {
  Cluster cluster{"cluster"};
  test_election_and_replication(cluster);
  test_leader_failure(cluster);
  test_restart_replays(cluster);
  test_out_of_band_snapshot(cluster);
  test_foreign_cluster_dropped(cluster);
  test_tls_transport();
  return bgraft_test::summary("cluster");
}
