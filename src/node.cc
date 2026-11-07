#include <bg/raft/raft.h>

#include <algorithm>
#include <future>
#include <set>
#include <string>
#include <string_view>
#include <utility>

#include "diag.h"
#include "log.h"
#include "loop.h"
#include "state.h"
#include "transport.h"

namespace bg::raft {

auto to_string(Error error) -> std::string_view {
  switch (error) {
    case Error::unknown:
      return "unknown";
    case Error::not_leader:
      return "not_leader";
    case Error::io_failure:
      return "io_failure";
    case Error::invalid_config:
      return "invalid_config";
    case Error::shutting_down:
      return "shutting_down";
    case Error::timeout:
      return "timeout";
    case Error::protocol_violation:
      return "protocol_violation";
    case Error::restore_failed:
      return "restore_failed";
    case Error::snapshot_failed:
      return "snapshot_failed";
    case Error::not_found:
      return "not_found";
    case Error::busy:
      return "busy";
    case Error::entry_too_large:
      return "entry_too_large";
    case Error::not_certain:
      return "not_certain";
  }
  return "invalid_error";
}

auto NodeConfig::validate() const -> Result<void> {
  if (peers.empty()) {
    return std::unexpected(Error::invalid_config);
  }
  if (node_id == 0U) {
    return std::unexpected(Error::invalid_config);  // 0 is reserved (no vote)
  }
  std::set<std::uint64_t> ids;
  for (const auto& peer : peers) {
    if (peer.id == 0U || !ids.insert(peer.id).second) {
      return std::unexpected(Error::invalid_config);  // duplicate/zero id
    }
    if (peer.endpoint.port == 0U && peer.id != node_id) {
      return std::unexpected(Error::invalid_config);
    }
    // A self entry is allowed but must not contradict the listen socket.
  }
  if (!ids.contains(node_id)) {
    return std::unexpected(Error::invalid_config);  // must include self
  }
  if (election_timeout_min <= std::chrono::milliseconds::zero() ||
      election_timeout_max < election_timeout_min ||
      heartbeat_period <= std::chrono::milliseconds::zero()) {
    return std::unexpected(Error::invalid_config);
  }
  if (log_path.empty() || snapshot_dir.empty()) {
    return std::unexpected(Error::invalid_config);
  }
  if (entry_batch_max_bytes == 0U) {
    return std::unexpected(Error::invalid_config);
  }
  if (tls.enabled() &&
      (tls.cert_path.empty() || tls.key_path.empty() || tls.ca_path.empty())) {
    return std::unexpected(Error::invalid_config);  // partial TLS material
  }
  return {};
}

struct Node::Impl {
  explicit Impl(NodeConfig config, StateMachine& state_machine)
      : cfg(std::move(config)),
        sm(state_machine),

        transport(loop),
        state(cfg, sm, log, loop, transport, shared) {}

  // Runs before member destructors: joins the loop thread so no deferred
  // task can touch Transport (or anything else) mid-teardown.
  ~Impl() { loop.quit(); }

  NodeConfig cfg;
  StateMachine& sm;
  detail::Loop loop;
  detail::Log log;
  detail::Transport transport;
  detail::SharedFlags shared;
  detail::State state;
  Endpoint bound;
  std::atomic<bool> running{false};
};

Node::Node(NodeConfig config, StateMachine& state_machine)
    : impl_(std::make_unique<Impl>(std::move(config), state_machine)) {}

Node::~Node() = default;

auto Node::start() -> Result<void> {
  if (impl_->running.load()) {
    return std::unexpected(Error::busy);
  }
  if (auto r = impl_->cfg.validate(); !r) {
    return std::unexpected(r.error());
  }
  if (auto r = impl_->log.open(impl_->cfg.log_path); !r) {
    return std::unexpected(r.error());
  }

  impl_->loop.spawn();
  impl_->running.store(true);

  std::promise<Result<void>> ready;
  auto future = ready.get_future();
  impl_->loop.post([this, &ready] -> void {
    impl_->transport.set_handler(
        [state = &impl_->state](std::uint64_t src,
                                const detail::WireMessage& msg) -> void {
          state->on_message(src, msg);
        });
    impl_->transport.set_peer_down(
        [state = &impl_->state](std::uint64_t peer) -> void {
          state->on_peer_down(peer);
        });
    if (impl_->cfg.tls.enabled()) {
      if (auto tls = impl_->transport.set_tls(impl_->cfg.tls); !tls) {
        ready.set_value(std::unexpected(tls.error()));
        return;
      }
    }
    auto bound = impl_->transport.listen(
        impl_->cfg.listen, impl_->cfg.cluster_id, impl_->cfg.node_id);
    if (!bound) {
      ready.set_value(std::unexpected(bound.error()));
      return;
    }
    impl_->bound = *bound;
    for (const auto& peer : impl_->cfg.peers) {
      impl_->transport.add_peer(peer);
    }
    ready.set_value(impl_->state.start());
  });
  auto result = future.get();
  if (!result) {
    impl_->loop.quit();
    impl_->running.store(false);
  }
  return result;
}

auto Node::shutdown() -> void {
  if (!impl_->running.exchange(false)) {
    return;
  }
  impl_->loop.post([state = &impl_->state] -> void { state->stop(); });
  impl_->loop.quit();
}

auto Node::bound_endpoint() const -> Endpoint { return impl_->bound; }

auto Node::submit(std::span<const std::byte> data, Completion completion)
    -> void {
  std::vector<std::byte> copy(data.begin(), data.end());
  if (!impl_->running.load()) {
    completion(std::unexpected(Error::shutting_down));
    return;
  }
  impl_->loop.post([state = &impl_->state, copy = std::move(copy),
                    completion = std::move(completion)] mutable -> void {
    state->submit(std::move(copy), std::move(completion));
  });
}

auto Node::submit(std::span<const std::byte> data) -> Result<CommitInfo> {
  std::promise<Result<CommitInfo>> done;
  auto future = done.get_future();
  submit(data, [&done](Result<CommitInfo> result) -> void {
    done.set_value(std::move(result));
  });
  return future.get();
}

auto Node::role() const -> Role {
  return static_cast<Role>(impl_->shared.role.load(std::memory_order_relaxed));
}

auto Node::current_term() const -> std::uint64_t {
  return impl_->shared.term.load(std::memory_order_relaxed);
}

auto Node::leader_id() const -> std::optional<std::uint64_t> {
  const auto id = impl_->shared.leader.load(std::memory_order_relaxed);
  if (id == 0U) {
    return std::nullopt;
  }
  return id;
}

auto Node::last_applied() const -> std::uint64_t {
  return impl_->shared.applied.load(std::memory_order_relaxed);
}

auto Node::durable_watermark(std::uint64_t id) const -> Result<std::uint64_t> {
  auto& shared = impl_->shared;
  if (id == impl_->cfg.node_id) {
    return shared.sm_durable.load(std::memory_order_relaxed);
  }
  const auto role = static_cast<Role>(shared.role.load(std::memory_order_relaxed));
  if (role != Role::leader) {
    return std::unexpected(Error::not_certain);  // only the leader tracks peers
  }
  const std::lock_guard<std::mutex> lock(shared.meta_mtx);
  const auto it = shared.peer_durable.find(id);
  if (it == shared.peer_durable.end()) {
    return std::unexpected(Error::not_certain);
  }
  return it->second;
}

auto Node::watch_durable_watermark(DurableWatermarkCallback callback)
    -> std::uint64_t {
  const std::lock_guard<std::mutex> lock(impl_->shared.meta_mtx);
  const auto id = ++impl_->shared.durable_watch_seq;
  impl_->shared.durable_watch.emplace(id, std::move(callback));
  return id;
}

auto Node::unsubscribe_durable_watermark(std::uint64_t id) -> void {
  const std::lock_guard<std::mutex> lock(impl_->shared.meta_mtx);
  impl_->shared.durable_watch.erase(id);
}

auto Node::make_snapshot() -> Result<std::uint64_t> {
  if (!impl_->running.load()) {
    return std::unexpected(Error::shutting_down);
  }
  std::promise<Result<std::uint64_t>> done;
  auto future = done.get_future();
  impl_->loop.post([state = &impl_->state, &done] -> void {
    done.set_value(state->snapshot());
  });
  return future.get();
}

}  // namespace bg::raft
