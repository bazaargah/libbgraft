#pragma once

#include <bg/raft/raft.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <span>
#include <string_view>
#include <vector>

#include "log.h"
#include "loop.h"
#include "protocol.h"
#include "transport.h"

// The Raft machine. Every method here runs on the loop thread only.

namespace bg::raft::detail {

struct SharedFlags {
  std::atomic<unsigned> role{static_cast<unsigned>(Role::follower)};
  std::atomic<std::uint64_t> term;
  std::atomic<std::uint64_t> leader;  // 0 = unknown
  std::atomic<std::uint64_t> applied;

  // Durable state-machine watermarks. sm_durable is this node's own;
  // durable_min is the leader-computed minimum over all members, valid only
  // while every member has reported in the current term (0 otherwise).
  // peer_durable mirrors the leader's per-member view for cross-thread
  // reads. meta_mtx guards peer_durable and the durable watch registry.
  std::atomic<std::uint64_t> sm_durable{0};
  std::atomic<std::uint64_t> durable_min{0};
  std::mutex meta_mtx;
  std::map<std::uint64_t, std::uint64_t> peer_durable;
  std::map<std::uint64_t, Node::DurableWatermarkCallback> durable_watch;
  std::uint64_t durable_watch_seq = 0;
};

class State {
 public:
  State(NodeConfig config, StateMachine& state_machine, Log& log, Loop& loop,
        Transport& transport, SharedFlags& shared);

  auto start() -> Result<void>;
  auto stop() -> void;
  auto on_message(std::uint64_t src, const WireMessage& msg) -> void;

  // Last connection to a peer died: clear per-peer in-flight expectations
  // so replication (or snapshot streaming) restarts cleanly on reconnect.
  auto on_peer_down(std::uint64_t peer) -> void;
  auto submit(std::vector<std::byte> data, Node::Completion completion) -> void;
  auto snapshot() -> Result<std::uint64_t>;

 private:
  struct Waiter {
    std::uint64_t index;
    Node::Completion completion;
  };

  struct Tx {
    int fd{-1};  // snapshot file being streamed (leader side)
    std::uint64_t index{};
    std::uint64_t term{};
    std::uint64_t offset{};
    std::uint64_t last_len{};
    bool active{};
    bool offer_pending{};
    bool final_pending{};
  };

  struct PeerTrack {
    std::uint64_t next_index{1};
    std::uint64_t match_index{};
    std::uint64_t inflight_prev{};
    std::uint64_t inflight_count{};
    bool inflight{};
    Tx tx;
  };

  struct Rx {
    int fd{-1};
    std::uint64_t term{};
    std::uint64_t index{};
    std::uint64_t written{};
  };

  // elections
  auto start_prevote() -> void;
  auto become_follower(std::uint64_t new_term, std::uint64_t new_leader)
      -> void;
  auto become_candidate() -> void;
  auto become_leader() -> void;
  // Accept a same-term leader heartbeat: confirm authority and restart the
  // election deadline without touching term/vote state.
  auto ack_leader(std::uint64_t src) -> void;
  auto reset_election_timer() -> void;
  auto random_election_delay() -> std::chrono::milliseconds;

  // wire
  auto send_msg(std::uint64_t peer_id, MsgType type, std::uint8_t flags,
                std::uint64_t term, std::span<const std::byte> payload)
      -> bool;

  // replication
  auto broadcast_append() -> void;
  auto send_append_to(std::uint64_t peer_id, PeerTrack& track) -> void;
  auto on_append(std::uint64_t src, const WireMessage& msg) -> void;
  auto on_append_resp(std::uint64_t src, const WireMessage& msg) -> void;
  auto advance_commit() -> void;
  auto apply_upto() -> void;
  // Mirror this node's durable watermark and the leader's cluster picture
  // into the shared flags; fire the durable watch when the picture changes.
  auto sync_durable() -> void;
  auto flush_waiters_committed() -> void;
  auto flush_waiters(Error error) -> void;
  auto hint_for(std::uint64_t prev_term, std::uint64_t prev_index) const
      -> std::uint64_t;
  auto quorum() const -> std::size_t;

  // votes
  auto on_request_vote(std::uint64_t src, const WireMessage& msg) -> void;
  auto on_vote_resp(std::uint64_t src, const WireMessage& msg) -> void;
  auto log_up_to_date(std::uint64_t term, std::uint64_t index) const -> bool;

  // snapshots
  auto start_install(std::uint64_t peer_id, PeerTrack& track) -> void;
  auto send_snapshot_chunk(std::uint64_t peer_id, PeerTrack& track) -> void;
  auto on_install(std::uint64_t src, const WireMessage& msg) -> void;
  auto on_install_resp(std::uint64_t src, const WireMessage& msg) -> void;
  auto finish_adopt(std::uint64_t index, std::uint64_t term,
                    std::string_view adopted_path) -> Result<void>;
  auto snapshot_path(std::uint64_t index) const -> std::string;

  void publish();

  NodeConfig cfg_;
  StateMachine& sm_;
  Log& log_;
  Loop& loop_;
  Transport& xport_;
  SharedFlags& shared_;

  // Cluster members other than ourselves; static membership, computed once.
  std::vector<std::uint64_t> cluster_peers_;

  Role role_{Role::follower};
  std::uint64_t term_{};
  std::uint64_t voted_for_{};  // 0 = none
  std::uint64_t leader_id_{};  // 0 = unknown
  std::uint64_t commit_{};
  std::uint64_t applied_{};

  std::unique_ptr<Timer> election_timer_;
  std::unique_ptr<Timer> heartbeat_timer_;

  std::uint64_t votes_{};
  std::uint64_t probe_term_{};
  bool prevote_phase_{};

  std::deque<Waiter> waiters_;
  std::map<std::uint64_t, PeerTrack> tracks_;
  std::map<std::uint64_t, std::uint64_t> vote_granted_;  // peer -> term
  // Leader-side: peer -> state-machine durable watermark, populated only
  // from current-term AppendEntries responses; cleared on every leader term
  // change so a stale leader's last picture never survives into a new term.
  std::map<std::uint64_t, std::uint64_t> peer_durable_;
  std::uint64_t durable_pub_{};   // last value mirrored to shared_.durable_min
  Rx rx_;
  std::mt19937_64 rng_{std::random_device{}()};
};

}  // namespace bg::raft::detail
