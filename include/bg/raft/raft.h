#pragma once

// The complete public API of libbgraft. Internal machinery (wire protocol,
// event loop, transport, write-ahead log, replication driver) lives under
// src/ and is not installed.

// libbgraft is POSIX-only; symbols are exported with ELF/Mach-O visibility,
// never with dllexport-style attributes. Define BG_RAFT_STATIC when linking
// the static library.
#if defined(BG_RAFT_STATIC)
#define BG_RAFT_API
#else
#define BG_RAFT_API __attribute__((visibility("default")))
#endif

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace bg::raft {

// Library-wide error codes. Values are part of the ABI; never renumber
// existing entries.
enum class Error : std::int16_t {
  unknown = -1,
  not_leader = -2,
  io_failure = -3,
  invalid_config = -4,
  shutting_down = -5,
  timeout = -6,
  protocol_violation = -7,
  restore_failed = -8,
  snapshot_failed = -9,
  not_found = -10,
  busy = -11,
  entry_too_large = -12,
};

template <typename T>
using Result = std::expected<T, Error>;

auto to_string(Error error) -> std::string_view;

// Upper bound for a single log entry payload accepted by Node::submit().
// The wire format carries an append_entries batch inside one frame with a
// 16-bit payload length, so an entry larger than this could never be
// replicated; submitting one fails fast with Error::entry_too_large.
inline constexpr std::size_t kMaxEntryDataBytes = 65535U - 26U - 20U;

// A numeric or symbolic host with a TCP port. Symbolic hosts are resolved
// when a connection is dialled, not when the endpoint is constructed.
struct Endpoint {
  std::string host;
  std::uint16_t port{};

  auto operator<=>(const Endpoint&) const = default;
};

// How often the write-ahead log is flushed to stable storage.
enum class FsyncMode : std::uint8_t {
  none,     // never fsync (fast; a host crash may lose recent entries)
  batched,  // one fsync per processed batch of appends
  strict,   // fsync after every individual append
};

// One cluster member. The list in NodeConfig::peers describes the full
// membership (including this node); membership is static at phase 1.
struct PeerConfig {
  std::uint64_t id{};
  Endpoint endpoint;
};

// Transport encryption (mutual TLS over TCP). TLS is enabled by setting all
// three PEM paths — a partial set is rejected by NodeConfig::validate():
// this node's certificate (signed by a shared cluster CA), its private key,
// and the CA certificate used to verify every peer.
// When left empty the transport stays plain TCP — set up TLS for any
// deployment where peers cross an untrusted network.
struct TlsConfig {
  std::string cert_path;
  std::string key_path;
  std::string ca_path;

  auto enabled() const -> bool {
    return !cert_path.empty() || !key_path.empty() || !ca_path.empty();
  }
};

struct NodeConfig {
  std::uint64_t node_id{};
  std::uint64_t cluster_id{};  // peers from another cluster are rejected
  Endpoint listen;             // port 0 selects an ephemeral port at start()
  std::vector<PeerConfig> peers;

  std::chrono::milliseconds election_timeout_min{150};
  std::chrono::milliseconds election_timeout_max{300};
  std::chrono::milliseconds heartbeat_period{50};

  std::string log_path;      // write-ahead log file (created if missing)
  std::string snapshot_dir;  // directory holding snapshot files

  std::uint32_t entry_batch_max_bytes{16 * 1024};
  FsyncMode fsync{FsyncMode::strict};
  TlsConfig tls;

  auto validate() const -> Result<void>;
};

// One replicated command.
struct LogEntry {
  std::uint64_t term{};
  std::uint64_t index{};
  std::vector<std::byte> data;

  // A defaulted comparison must spell its bool return type; trailing form
  // is not permitted by the standard.
  // NOLINTNEXTLINE(modernize-use-trailing-return-type)
  bool operator==(const LogEntry&) const = default;
};

// Identifies the state machine prefix covered by a snapshot.
struct SnapshotInfo {
  std::uint64_t last_included_index{};
  std::uint64_t last_included_term{};
};

// User-implemented application state machine. All methods run on the node's
// event-loop thread and must not block it for long.
class StateMachine {
 public:
  virtual ~StateMachine() = default;

  // Apply one committed entry. Entries are delivered in strict index order.
  virtual auto on_apply(const LogEntry& entry) -> void = 0;

  // Serialize the current state into the already-open POSIX fd.
  virtual auto take_snapshot(int fd) -> Result<void> = 0;

  // Replace local state with the snapshot identified by `info`.
  //
  // Two transfer modes exist (see src/state.cc):
  //   - out_of_band == true:  `path` is empty. For very large state this is
  //     the recommended mode: fetch the snapshot data through the application
  //     channel (shared storage, rsync, object store, ...) and return success.
  //     Returning Error::restore_failed asks the leader to re-send the
  //     snapshot inline, chunked, over the Raft connection instead.
  //   - out_of_band == false: the library has already streamed the full
  //     snapshot to `path`; open and restore it, then return success.
  virtual auto restore_snapshot(std::string_view path, const SnapshotInfo& info,
                                bool out_of_band) -> Result<void> = 0;
};

enum class Role : std::uint8_t {
  follower = 0,
  candidate = 1,
  leader = 2,
};

// One Raft node. All public methods are safe to call from any thread.
// The node owns an event-loop thread; completion callbacks and state-machine
// invocations run on that thread.
class BG_RAFT_API Node {
 public:
  struct CommitInfo {
    std::uint64_t index;
    std::uint64_t term;
  };

  using Completion = std::function<void(Result<CommitInfo>)>;

  Node(NodeConfig config, StateMachine& state_machine);
  ~Node();

  Node(const Node&) = delete;
  auto operator=(const Node&) -> Node& = delete;
  Node(Node&&) noexcept = delete;
  auto operator=(Node&&) noexcept -> Node& = delete;

  // Opens the write-ahead log, binds the listen socket, dials peers and
  // starts the loop thread. Errors from those steps surface here.
  auto start() -> Result<void>;

  // Stops the loop thread and closes all descriptors. Idempotent.
  auto shutdown() -> void;

  // The actually-bound endpoint (resolves port 0). Valid after start().
  [[nodiscard]] auto bound_endpoint() const -> Endpoint;

  // Appends a command to the log when this node is the leader. The
  // completion fires on the loop thread once the entry commits, or with an
  // error (Error::not_leader, Error::shutting_down, ...) without committing.
  auto submit(std::span<const std::byte> data, Completion completion) -> void;

  // Convenience: blocks the calling thread until `completion` semantics end.
  auto submit(std::span<const std::byte> data) -> Result<CommitInfo>;

  [[nodiscard]] auto role() const -> Role;
  [[nodiscard]] auto current_term() const -> std::uint64_t;
  [[nodiscard]] auto leader_id() const -> std::optional<std::uint64_t>;
  [[nodiscard]] auto last_applied() const -> std::uint64_t;

  // Snapshots the state machine at last_applied and compacts the log up to
  // that index. Allowed on any node; returns the new last_included_index.
  auto make_snapshot() -> Result<std::uint64_t>;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace bg::raft
