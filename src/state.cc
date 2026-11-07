#include "state.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <filesystem>
#include <string>
#include <utility>

#include "diag.h"
#include "fs.h"
#include "wire.h"

namespace bg::raft::detail {

namespace {
constexpr std::uint32_t kMaxEntriesPerAppend = 512U;
constexpr std::uint64_t kAppendDataBudget = 52U
                                            << 10U;  // keeps frames < 64 KiB
constexpr std::size_t kSnapshotChunk = 52U << 10U;
constexpr std::uint32_t kReadAll = 0xFFFFFFFFU;
}  // namespace

State::State(NodeConfig config, StateMachine& state_machine, Log& log,
             Loop& loop, Transport& transport, SharedFlags& shared)
    : cfg_(std::move(config)),
      sm_(state_machine),
      log_(log),
      loop_(loop),
      xport_(transport),
      shared_(shared) {
  for (const auto& peer : cfg_.peers) {
    if (peer.id != cfg_.node_id) {
      cluster_peers_.push_back(peer.id);
    }
  }
}

auto State::start() -> Result<void> {
  term_ = log_.current_term();
  voted_for_ = log_.voted_for();
  commit_ = log_.last_included_index();
  applied_ = log_.last_included_index();
  role_ = Role::follower;
  leader_id_ = 0U;
  publish();

  election_timer_ = std::make_unique<Timer>(loop_, [this] -> void {
    if (role_ != Role::leader) {
      // The timeout itself disproves leader liveness: drop the cached
      // leader so peers' prevotes are no longer refused by `have_leader`.
      leader_id_ = 0U;
      publish();
      start_prevote();
    }
  });
  heartbeat_timer_ = std::make_unique<Timer>(
      loop_,
      [this] -> void {
        if (role_ == Role::leader) {
          broadcast_append();
        }
      },
      true);
  reset_election_timer();
  return {};
}

auto State::stop() -> void { flush_waiters(Error::shutting_down); }

// ─── helpers ───────────────────────────────────────────────────────────────

auto State::quorum() const -> std::size_t {
  return (cfg_.peers.size() / 2U) + 1U;
}

auto State::send_msg(std::uint64_t peer_id, MsgType type, std::uint8_t flags,
                     std::uint64_t term, std::span<const std::byte> payload)
    -> bool {
  const auto frame = encode_frame(type, flags, term, payload);
  return xport_.send(peer_id, frame);
}

auto State::ack_leader(std::uint64_t src) -> void {
  role_ = Role::follower;
  leader_id_ = src;
  reset_election_timer();
  publish();
}

auto State::log_up_to_date(std::uint64_t term, std::uint64_t index) const
    -> bool {
  return term > log_.last_term() ||
         (term == log_.last_term() && index >= log_.last_index());
}

auto State::random_election_delay() -> std::chrono::milliseconds {
  std::uniform_int_distribution<std::int64_t> dist(
      cfg_.election_timeout_min.count(), cfg_.election_timeout_max.count());
  return std::chrono::milliseconds(dist(rng_));
}

auto State::reset_election_timer() -> void {
  election_timer_->arm(random_election_delay());
}

auto State::snapshot_path(std::uint64_t index) const -> std::string {
  return (std::filesystem::path(cfg_.snapshot_dir) /
          ("snapshot." + std::to_string(index) + ".bin"))
      .string();
}

void State::publish() {
  shared_.role.store(static_cast<unsigned>(role_), std::memory_order_relaxed);
  shared_.term.store(term_, std::memory_order_relaxed);
  shared_.leader.store(leader_id_, std::memory_order_relaxed);
  shared_.applied.store(applied_, std::memory_order_relaxed);
}

// ─── elections ─────────────────────────────────────────────────────────────

auto State::start_prevote() -> void {
  prevote_phase_ = true;
  probe_term_ = term_ + 1U;
  votes_ = 1U;  // ourselves
  vote_granted_.clear();
  const auto payload = build_request_vote(log_.last_term(), log_.last_index());
  for (const auto id : cluster_peers_) {
    send_msg(id, MsgType::request_vote, kFlagPreVote, probe_term_, payload);
  }
  reset_election_timer();
  logf(LogLevel::debug, "node {} prevote round for term {}", cfg_.node_id,
       probe_term_);
  publish();
  if (votes_ >= quorum()) {
    become_candidate();  // single-node cluster (or quorum of one)
  }
}

auto State::become_candidate() -> void {
  prevote_phase_ = false;
  role_ = Role::candidate;
  term_ = probe_term_;
  votes_ = 1U;
  vote_granted_.clear();
  if (auto r = log_.set_current_term(term_); !r) {
    logf(LogLevel::error, "term persist failed: {}",
         static_cast<int>(r.error()));
  }
  voted_for_ = cfg_.node_id;
  if (auto r = log_.set_voted_for(voted_for_); !r) {
    logf(LogLevel::error, "vote persist failed");
  }
  const auto payload = build_request_vote(log_.last_term(), log_.last_index());
  for (const auto id : cluster_peers_) {
    send_msg(id, MsgType::request_vote, 0U, term_, payload);
  }
  reset_election_timer();
  logf(LogLevel::info, "node {} starting election for term {}", cfg_.node_id,
       term_);
  publish();
  if (votes_ >= quorum()) {
    become_leader();  // self-vote already forms a quorum
  }
}

auto State::become_follower(std::uint64_t new_term, std::uint64_t new_leader)
    -> void {
  const bool stepped_up_term = new_term > term_;
  if (stepped_up_term) {
    term_ = new_term;
    voted_for_ = 0U;
    if (auto r = log_.set_current_term(term_); !r) {
      logf(LogLevel::error, "term persist failed");
    }
    if (auto r = log_.set_voted_for(voted_for_); !r) {
      logf(LogLevel::error, "vote persist failed");
    }
  }
  if (role_ == Role::leader) {
    flush_waiters(Error::not_leader);
    heartbeat_timer_->cancel();
  }
  role_ = Role::follower;
  if (new_leader != 0U) {
    leader_id_ = new_leader;
  } else if (stepped_up_term) {
    leader_id_ = 0U;
  }
  reset_election_timer();
  publish();
}

auto State::become_leader() -> void {
  role_ = Role::leader;
  leader_id_ = cfg_.node_id;
  tracks_.clear();
  for (const auto id : cluster_peers_) {
    auto& track = tracks_[id];
    track.next_index = log_.last_index() + 1U;
  }
  election_timer_->cancel();
  heartbeat_timer_->arm(cfg_.heartbeat_period);
  logf(LogLevel::info, "node {} became leader of term {}", cfg_.node_id, term_);

  // No-op entry commits immediately in the current term, which transitively
  // commits entries from older terms.
  std::vector<std::byte> noop_payload;
  LogEntry entry{
      .term = term_,
      .index = log_.last_index() + 1U,
      .data = noop_payload,
  };
  if (auto r = log_.append(entry); !r) {
    logf(LogLevel::error, "noop append failed");
  } else if (cfg_.fsync != FsyncMode::none) {
    if (auto r = log_.sync(); !r) {
      logf(LogLevel::error, "noop fsync failed: {}", to_string(r.error()));
    }
  }
  broadcast_append();
  advance_commit();
  publish();
}

// ─── votes ─────────────────────────────────────────────────────────────────

auto State::on_request_vote(std::uint64_t src, const WireMessage& msg) -> void {
  const auto& vote = std::get<RequestVoteMsg>(msg.body);
  const bool pre = (msg.flags & kFlagPreVote) != 0U;
  bool granted = false;

  if (pre) {
    // A prevote is granted only when the challenger could realistically win
    // AND our current authority is not intact. Leaders never grant: voting
    // for an unproven challenge lets one lagging follower dethrone a healthy
    // leader with the leader's own vote, which shows up as endless term
    // bouncing. Candidates grant only rounds strictly above their own probe.
    // Followers stay silent while a known leader is heartbeat-live.
    const bool up_to_date =
        log_up_to_date(vote.last_log_term, vote.last_log_index);
    if (msg.term > term_ && up_to_date) {
      switch (role_) {
        case Role::leader:
          granted = false;
          break;
        case Role::candidate:
          // Granting a prevote commits nothing, and refusing same-round
          // probes while campaigning can deadlock two nodes that timed out
          // together at the same term. Real votes still dedupe via
          // voted_for_, so this only risks an extra round, never lost
          // writes.
          granted = true;
          break;
        case Role::follower:
          granted =
              leader_id_ == 0U || !election_timer_ || !election_timer_->armed();
          break;
      }
    }
  } else {
    if (msg.term < term_) {
      // fallthrough with granted=false, reply carries our higher term
    } else {
      // Do not let a same-term candidate depose a leader we are actively
      // hearing from: only grant once our election deadline has expired.
      const bool live_leader_same_term =
          msg.term == term_ && leader_id_ != 0U && role_ == Role::follower &&
          election_timer_ != nullptr && election_timer_->armed();
      if (msg.term > term_) {
        become_follower(msg.term, 0U);
      }
      const bool ours = voted_for_ == 0U || voted_for_ == src;
      granted = !live_leader_same_term && ours && msg.term == term_ &&
                log_up_to_date(vote.last_log_term, vote.last_log_index);
      if (granted) {
        voted_for_ = src;
        if (auto r = log_.set_voted_for(voted_for_); !r) {
          logf(LogLevel::error, "vote persist failed");
        }
        reset_election_timer();
      }
    }
  }

  // A prevote reply echoes the probe term when granting (the responder's
  // own term stays untouched); a real-vote reply always carries the
  // responder's term so higher-term denials trigger a step-down.
  const std::uint64_t reply_term = pre && granted ? msg.term : term_;
  send_msg(src, MsgType::request_vote_resp, msg.flags, reply_term,
           build_vote_resp(granted));
}

auto State::on_vote_resp(std::uint64_t src, const WireMessage& msg) -> void {
  const auto& resp = std::get<RequestVoteRespMsg>(msg.body);
  const bool pre = (msg.flags & kFlagPreVote) != 0U;

  if (msg.term > (pre ? probe_term_ : term_)) {
    become_follower(msg.term, 0U);
    return;
  }

  if (pre) {
    if (!prevote_phase_ || msg.term != probe_term_) {
      return;
    }
    if (resp.vote_granted && vote_granted_.try_emplace(src, msg.term).second) {
      ++votes_;
    }
    if (votes_ >= quorum()) {
      become_candidate();
    }
    return;
  }

  if (role_ != Role::candidate || msg.term != term_) {
    return;
  }
  if (resp.vote_granted && vote_granted_.try_emplace(src, msg.term).second) {
    ++votes_;
  }
  if (votes_ >= quorum() && role_ == Role::candidate) {
    become_leader();
  }
}

// ─── replication ───────────────────────────────────────────────────────────

auto State::broadcast_append() -> void {
  for (const auto& id : cluster_peers_) {
    send_append_to(id, tracks_[id]);
  }
}

auto State::send_append_to(std::uint64_t peer_id, PeerTrack& track) -> void {
  if (track.tx.active) {
    return;  // snapshot stream owns this peer
  }
  if (track.inflight) {
    // A leader must never fall silent on a peer while a batch is stuck in
    // flight: its acknowledgement can vanish with a connection churn, and
    // the follower would time out and churn terms forever. Keep liveness
    // traffic going with an empty append anchored at the stuck batch's
    // expected tip — success proves the batch actually landed (the normal
    // response path absorbs it), failure yields a resync hint.
    const auto anchor = track.inflight_prev + track.inflight_count;
    if (anchor <= log_.last_included_index()) {
      return;  // snapshot boundary moved under the peer; install resumes it
    }
    const auto anchor_term = log_.term_at(anchor).value_or(0ULL);
    send_msg(peer_id, MsgType::append_entries, 0U, term_,
             build_append_entries(anchor, anchor_term, commit_,
                                  std::span<const LogEntry>{}));
    return;
  }
  if (track.next_index <= log_.last_included_index()) {
    start_install(peer_id, track);
    return;
  }
  const auto prev_index = track.next_index - 1U;
  auto prev_term_opt = log_.term_at(prev_index);
  if (!prev_term_opt && prev_index > 0U) {
    // prev was compacted away locally; ship our snapshot instead.
    start_install(peer_id, track);
    return;
  }
  const auto prev_term = prev_index == 0U ? 0ULL : prev_term_opt.value_or(0ULL);

  const auto budget =
      std::min<std::uint64_t>(cfg_.entry_batch_max_bytes, kAppendDataBudget);
  auto entries = log_.read(track.next_index, kMaxEntriesPerAppend, budget);
  if (!entries) {
    logf(LogLevel::error, "node {} log read failed", cfg_.node_id);
    return;
  }

  track.inflight_prev = prev_index;
  track.inflight_count = entries->size();
  const auto payload =
      build_append_entries(prev_index, prev_term, commit_, *entries);
  // Liveness traffic must go out even with nothing to replicate: followers
  // only reset their election deadline on a received append. The in-flight
  // gate is armed by real batches only; a dropped send leaves the peer on
  // the next event-driven retry rather than stranding it.
  if (send_msg(peer_id, MsgType::append_entries, 0U, term_, payload) &&
      !entries->empty()) {
    track.inflight = true;
  }
}

auto State::hint_for(std::uint64_t prev_term, std::uint64_t prev_index) const
    -> std::uint64_t {
  // Largest index <= min(prev_index, last_index) whose term matches
  // prev_term; falls back to the snapshot boundary (the leader then
  // switches to InstallSnapshot).
  const auto watermark = log_.last_included_index();
  auto index = std::min(prev_index, log_.last_index());
  while (index > watermark) {
    if (auto t = log_.term_at(index); t && *t == prev_term) {
      return index;
    }
    if (index == watermark + 1U) {
      break;  // Explicit guard: never underflow a uint64 loop counter.
    }
    --index;
  }
  return watermark;
}

auto State::on_append(std::uint64_t src, const WireMessage& msg) -> void {
  const auto& append = std::get<AppendEntriesMsg>(msg.body);
  if (msg.term < term_) {
    send_msg(src, MsgType::append_entries_resp, 0U, term_,
             build_append_resp(false, 0U));
    return;
  }

  const bool was_leader = role_ == Role::leader;
  if (msg.term > term_) {
    become_follower(msg.term, src);
    if (was_leader) {
      return;  // weird duplicate leader; we just stepped down
    }
  } else {
    // Same-term heartbeat: confirm the leader and restart the election
    // deadline without touching term/vote state (no timer-restart loop).
    ack_leader(src);
  }

  if (append.prev_log_index > 0U) {
    auto pt = log_.term_at(append.prev_log_index);
    const bool matches = append.prev_log_index == log_.last_included_index()
                             ? pt.value_or(0ULL) == append.prev_log_term
                             : pt == append.prev_log_term;
    if (!pt || !matches) {
      const auto hint = hint_for(append.prev_log_term, append.prev_log_index);
      send_msg(src, MsgType::append_entries_resp, 0U, term_,
               build_append_resp(false, hint));
      return;
    }
  }

  std::vector<LogEntry> new_entries;
  bool appended = false;
  for (std::size_t i = 0; i < append.entries.size(); ++i) {
    const auto& e = append.entries[i];
    if (e.index <= log_.last_included_index()) {
      continue;  // covered by snapshot
    }
    auto existing = log_.term_at(e.index);
    if (existing) {
      if (*existing == e.term) {
        continue;  // already stored (idempotent re-delivery)
      }
      if (auto r = log_.truncate_from(e.index); !r) {
        logf(LogLevel::error, "truncate failed");
        break;
      }
    }
    for (std::size_t k = i; k < append.entries.size(); ++k) {
      new_entries.push_back(LogEntry{
          .term = append.entries[k].term,
          .index = append.entries[k].index,
          .data =
              {
                  append.entries[k].data.begin(),
                  append.entries[k].data.end(),
              },
      });
    }
    appended = true;
    break;
  }

  if (appended) {
    if (auto r = log_.append(new_entries); !r) {
      logf(LogLevel::error, "node {} append failed on replication",
           cfg_.node_id);
      send_msg(src, MsgType::append_entries_resp, 0U, term_,
               build_append_resp(false, log_.last_index()));
      return;
    }
  }

  if (appended && cfg_.fsync != FsyncMode::none) {
    // Persist before acking (strict or batched); a silent fsync failure
    // would mean acking data that is not durable.
    if (auto r = log_.sync(); !r) {
      logf(LogLevel::error, "log fsync failed before ack: {}",
           to_string(r.error()));
    }
  }

  if (append.leader_commit > commit_) {
    commit_ = std::min(append.leader_commit, log_.last_index());
  }
  send_msg(src, MsgType::append_entries_resp, 0U, term_,
           build_append_resp(true, 0U));
  apply_upto();
}

auto State::on_append_resp(std::uint64_t src, const WireMessage& msg) -> void {
  if (role_ != Role::leader) {
    return;
  }
  if (msg.term > term_) {
    become_follower(msg.term, 0U);
    return;
  }
  if (msg.term != term_) {
    return;  // stale
  }
  const auto& resp = std::get<AppendEntriesRespMsg>(msg.body);
  auto& track = tracks_[src];
  track.inflight = false;

  if (resp.success) {
    const auto match = track.inflight_prev + track.inflight_count;
    if (match > track.match_index) {
      track.match_index = match;
      track.next_index = match + 1U;
    }
    advance_commit();
    // Pipeline the next batch only when one exists; otherwise the
    // heartbeat timer owns the connection (no empty-append ping-pong).
    if (track.next_index <= log_.last_index()) {
      send_append_to(src, track);
    }
  } else {
    // hint_index is the follower's best matching point; retry strictly
    // from it and only if that actually moves us backwards. If it does
    // not, the follower is at/before our snapshot boundary or the
    // rejection was I/O-driven: fall back to heartbeat-paced retries
    // (or InstallSnapshot via send_append_to's boundary gate).
    const auto retry = resp.hint_index + 1U;
    if (retry < track.next_index) {
      track.next_index = retry;
      send_append_to(src, track);
    }
  }
}

auto State::advance_commit() -> void {
  std::vector<std::uint64_t> matches;
  matches.reserve(cfg_.peers.size());
  matches.push_back(log_.last_index());  // ourselves
  for (const auto id : cluster_peers_) {
    matches.push_back(tracks_[id].match_index);
  }
  std::ranges::sort(matches, std::greater<>());
  const auto candidate = matches[quorum() - 1U];
  while (commit_ < candidate) {
    auto t = log_.term_at(commit_ + 1U);
    if (!t || *t != term_) {
      break;  // only commit entries of the current term directly
    }
    ++commit_;
  }
  flush_waiters_committed();
  apply_upto();
}

auto State::apply_upto() -> void {
  const auto target = std::min(commit_, log_.last_index());
  while (applied_ < target) {
    auto entries = log_.read(applied_ + 1U, 1U, kReadAll);
    if (!entries || entries->empty()) {
      break;
    }
    sm_.on_apply(entries->front());
    ++applied_;
  }
  publish();
}

auto State::flush_waiters_committed() -> void {
  while (!waiters_.empty() && waiters_.front().index <= commit_) {
    const auto index = waiters_.front().index;
    const auto t = log_.term_at(index);
    waiters_.front().completion(
        Node::CommitInfo{.index = index, .term = t.value_or(0ULL)});
    waiters_.pop_front();
  }
}

auto State::flush_waiters(Error error) -> void {
  auto pending = std::move(waiters_);
  waiters_.clear();
  for (auto& waiter : pending) {
    waiter.completion(std::unexpected(error));
  }
}

// ─── submit ────────────────────────────────────────────────────────────────

auto State::submit(std::vector<std::byte> data, Node::Completion completion)
    -> void {
  if (data.size() > kMaxEntryDataBytes) {
    completion(std::unexpected(Error::entry_too_large));
    return;
  }
  if (role_ != Role::leader) {
    completion(std::unexpected(Error::not_leader));
    return;
  }
  LogEntry entry{
      .term = term_,
      .index = log_.last_index() + 1U,
      .data = std::move(data),
  };
  if (auto r = log_.append(entry); !r) {
    completion(std::unexpected(r.error()));
    return;
  }
  if (cfg_.fsync == FsyncMode::strict) {
    if (auto r = log_.sync(); !r) {
      completion(std::unexpected(r.error()));
      return;
    }
  }
  waiters_.push_back(
      Waiter{.index = entry.index, .completion = std::move(completion)});
  broadcast_append();
  advance_commit();
}

// ─── snapshots ─────────────────────────────────────────────────────────────

auto State::snapshot() -> Result<std::uint64_t> {
  const auto index = applied_;
  if (index <= log_.last_included_index()) {
    return index;
  }
  auto term_opt = log_.term_at(index);
  if (!term_opt) {
    return std::unexpected(Error::io_failure);
  }
  const auto path = snapshot_path(index);
  if (auto r = write_atomically(
          path, path + ".tmp",
          [&](int fd) -> Result<void> { return sm_.take_snapshot(fd); });
      !r) {
    return std::unexpected(r.error());
  }

  const auto previous = log_.last_included_index();
  if (auto r = log_.compact_upto(index, *term_opt); !r) {
    return std::unexpected(r.error());
  }
  if (previous > 0U) {
    ::unlink(snapshot_path(previous).c_str());  // best effort
  }
  commit_ = std::max(commit_, log_.last_included_index());
  logf(LogLevel::info, "node {} snapshotted at index {}", cfg_.node_id, index);
  publish();
  return index;
}

auto State::start_install(std::uint64_t peer_id, PeerTrack& track) -> void {
  track.inflight = false;
  track.tx = Tx{};
  track.tx.active = true;
  track.tx.offer_pending = true;
  track.tx.index = log_.last_included_index();
  track.tx.term = log_.last_included_term();
  if (!send_msg(peer_id, MsgType::install_snapshot, kFlagOffer, term_,
                build_install_snapshot(track.tx.index, track.tx.term, 0U, {}))) {
    track.tx = Tx{};  // Nothing on the wire: retry the offer later.
    return;
  }
  logf(LogLevel::info, "node {} offering snapshot {} to peer {}", cfg_.node_id,
       track.tx.index, peer_id);
}

auto State::send_snapshot_chunk(std::uint64_t peer_id, PeerTrack& track)
    -> void {
  auto& tx = track.tx;
  std::array<std::byte, kSnapshotChunk> buf{};
  const auto got =
      ::pread(tx.fd, buf.data(), buf.size(), static_cast<off_t>(tx.offset));
  if (got < 0) {
    ::close(tx.fd);
    tx = Tx{};
    logf(LogLevel::error, "node {} snapshot read failed for peer {}",
         cfg_.node_id, peer_id);
    return;
  }
  const bool final = static_cast<std::size_t>(got) < buf.size();
  tx.last_len = static_cast<std::uint64_t>(got);
  tx.final_pending = final;
  send_msg(peer_id, MsgType::install_snapshot, final ? kFlagFinal : 0U, term_,
           build_install_snapshot(
               tx.index, tx.term, tx.offset,
               std::span<const std::byte>(
                   buf.data(), static_cast<std::size_t>(got))));
}

auto State::on_install(std::uint64_t src, const WireMessage& msg) -> void {
  const auto& install = std::get<InstallSnapshotMsg>(msg.body);
  if (msg.term < term_) {
    send_msg(src, MsgType::install_snapshot_resp, 0U, term_,
             build_install_resp(false));
    return;
  }
  become_follower(msg.term, src);

  const SnapshotInfo info{
      .last_included_index = install.last_included_index,
      .last_included_term = install.last_included_term,
  };

  const bool offer = (msg.flags & kFlagOffer) != 0U;
  const bool final = (msg.flags & kFlagFinal) != 0U;
  bool ok = false;

  if (offer) {
    auto restored = sm_.restore_snapshot(std::string_view{}, info, true);
    if (restored) {
      ok = static_cast<bool>(
          finish_adopt(info.last_included_index, info.last_included_term, {}));
    } else if (restored.error() == Error::restore_failed) {
      send_msg(src, MsgType::install_snapshot_resp, kFlagWantInline, term_,
               build_install_resp(false));
      return;
    }
  } else {
    const auto recv_path = snapshot_path(install.last_included_index) +
                           ".recv." + std::to_string(src) + ".tmp";
    if (rx_.index != install.last_included_index ||
        rx_.term != install.last_included_term || rx_.fd < 0) {
      if (rx_.fd >= 0) {
        ::close(rx_.fd);
      }
      rx_.fd = ::open(recv_path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
      rx_.term = install.last_included_term;
      rx_.index = install.last_included_index;
      rx_.written = 0U;
      if (rx_.fd < 0) {
        send_msg(src, MsgType::install_snapshot_resp, 0U, term_,
                 build_install_resp(false));
        return;
      }
    }
    if (install.offset != rx_.written ||
        ::pwrite(rx_.fd, install.data.data(), install.data.size(),
                 static_cast<off_t>(rx_.written)) !=
            static_cast<::ssize_t>(install.data.size())) {
      send_msg(src, MsgType::install_snapshot_resp, 0U, term_,
               build_install_resp(false));
      return;
    }
    rx_.written += install.data.size();
    if (final) {
      ok = fsync(rx_.fd) == 0;
      ::close(rx_.fd);
      rx_.fd = -1;
      if (ok) {
        auto restored = sm_.restore_snapshot(recv_path, info, false);
        if (restored) {
          ok = static_cast<bool>(finish_adopt(
              info.last_included_index, info.last_included_term, recv_path));
        }
        if (!ok) {
          ::unlink(recv_path.c_str());
        }
      }
    }
  }

  send_msg(src, MsgType::install_snapshot_resp, 0U, term_,
           build_install_resp(ok));
}

auto State::on_install_resp(std::uint64_t src, const WireMessage& msg) -> void {
  auto it = tracks_.find(src);
  if (it == tracks_.end() || !it->second.tx.active) {
    return;
  }
  auto& track = it->second;
  auto& tx = track.tx;
  const auto& resp = std::get<InstallSnapshotRespMsg>(msg.body);
  const bool want_inline = (msg.flags & kFlagWantInline) != 0U;

  if (want_inline && tx.offer_pending) {
    const auto path = snapshot_path(tx.index);
    tx.fd = ::open(path.c_str(), O_RDONLY);
    tx.offset = 0U;
    tx.offer_pending = false;
    if (tx.fd < 0) {
      tx = Tx{};
      return;
    }
    send_snapshot_chunk(src, track);
    return;
  }

  if (resp.ok) {
    if (tx.offer_pending || tx.final_pending) {
      const auto adopted_index = tx.index;
      if (tx.fd >= 0) {
        ::close(tx.fd);
      }
      tx = Tx{};
      track.match_index = adopted_index;
      track.next_index = adopted_index + 1U;
      logf(LogLevel::info, "node {} installed snapshot {} on peer {}",
           cfg_.node_id, adopted_index, src);
      send_append_to(src, track);
    } else {
      tx.offset += tx.last_len;
      send_snapshot_chunk(src, track);
    }
  } else {
    if (tx.fd >= 0) {
      ::close(tx.fd);
    }
    tx = Tx{};  // retry later via normal append flow
  }
}

// `adopted_path` non-empty: inline transfer — promote the received tmp file
// to the permanent snapshot path. Empty: out-of-band restore — materialize a
// local snapshot copy so this node can serve it later (deterministic state
// machine makes this equal to the original).
auto State::finish_adopt(std::uint64_t index, std::uint64_t term,
                         std::string_view adopted_path) -> Result<void> {
  const auto permanent = snapshot_path(index);
  if (!adopted_path.empty()) {
    if (::rename(std::string(adopted_path).c_str(), permanent.c_str()) != 0) {
      return std::unexpected(Error::io_failure);
    }
  } else {
    if (auto r = write_atomically(
            permanent, permanent + ".tmp",
            [&](int fd) -> Result<void> { return sm_.take_snapshot(fd); });
        !r) {
      return std::unexpected(r.error());
    }
  }

  if (index > log_.last_included_index()) {
    if (auto r = log_.adopt_snapshot(index, term); !r) {
      return std::unexpected(r.error());
    }
  }
  commit_ = std::max(commit_, index);
  applied_ = std::max(applied_, index);

  publish();
  return {};
}

auto State::on_message(std::uint64_t src, const WireMessage& msg) -> void {
  switch (msg.type) {
    case MsgType::handshake:
      return;  // consumed by transport
    case MsgType::request_vote: {
      on_request_vote(src, msg);
      return;
    }
    case MsgType::request_vote_resp: {
      on_vote_resp(src, msg);
      return;
    }
    case MsgType::append_entries: {
      on_append(src, msg);
      return;
    }
    case MsgType::append_entries_resp: {
      on_append_resp(src, msg);
      return;
    }
    case MsgType::install_snapshot: {
      on_install(src, msg);
      return;
    }
    case MsgType::install_snapshot_resp: {
      on_install_resp(src, msg);
      return;
    }
  }
}

auto State::on_peer_down(std::uint64_t peer) -> void {
  auto it = tracks_.find(peer);
  if (it == tracks_.end()) {
    return;  // No replication state for this peer (yet).
  }
  auto& track = it->second;
  track.inflight = false;
  if (track.tx.fd >= 0) {
    ::close(track.tx.fd);
  }
  track.tx = Tx{};
  if (role_ == Role::leader) {
    send_append_to(peer, track);
  }
}

}  // namespace bg::raft::detail
