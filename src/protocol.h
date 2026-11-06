#pragma once

#include <bg/raft/raft.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <span>
#include <variant>
#include <vector>

struct evbuffer;

// Compact fixed-width wire format. Header is exactly 12 bytes:
//
//   u8  type    (MsgType)
//   u8  flags   (kFlag* bits)
//   u16 payload_len        (max frame size, 65535 bytes payload)
//   u64 term               (0 for Handshake)
//
// All integers big-endian, no magic, no version field, no varints. Peer
// identity arrives once per connection in the Handshake message.

namespace bg::raft::detail {

inline constexpr std::size_t kHeaderSize = 12;
inline constexpr std::uint16_t kMaxPayload = 65535U;

enum class MsgType : std::uint8_t {
  handshake = 0,
  request_vote = 1,
  request_vote_resp = 2,
  append_entries = 3,
  append_entries_resp = 4,
  install_snapshot = 5,
  install_snapshot_resp = 6,
};

inline constexpr std::uint8_t kFlagPreVote = 1U << 0U;  // request_vote
inline constexpr std::uint8_t kFlagOffer = 1U << 1U;    // install_snapshot
inline constexpr std::uint8_t kFlagWantInline = 1U
                                                << 1U;  // install_snapshot_resp
inline constexpr std::uint8_t kFlagFinal = 1U << 2U;    // install_snapshot

struct HandshakeMsg {
  std::uint64_t cluster_id;
  std::uint64_t node_id;
};

// Request carries last-log data; flags distinguish prevote from real vote.
struct RequestVoteMsg {
  std::uint64_t last_log_term;
  std::uint64_t last_log_index;
};

struct RequestVoteRespMsg {
  bool vote_granted;
};

// Entry spans alias the FrameReader's current payload buffer and are valid
// only for the duration of the handler call.
struct EntryRef {
  std::uint64_t term;
  std::uint64_t index;
  std::span<const std::byte> data;
};

struct AppendEntriesMsg {
  std::uint64_t prev_log_index = 0;
  std::uint64_t prev_log_term = 0;
  std::uint64_t leader_commit = 0;
  std::vector<EntryRef> entries;
};

struct AppendEntriesRespMsg {
  bool success;
  std::uint64_t hint_index;
};

struct InstallSnapshotMsg {
  std::uint64_t last_included_index = 0;
  std::uint64_t last_included_term = 0;
  std::uint64_t offset = 0;
  std::span<const std::byte> data;  // empty for offers
};

struct InstallSnapshotRespMsg {
  bool ok;
};

using MsgBody = std::variant<HandshakeMsg, RequestVoteMsg, RequestVoteRespMsg,
                             AppendEntriesMsg, AppendEntriesRespMsg,
                             InstallSnapshotMsg, InstallSnapshotRespMsg>;

struct WireMessage {
  MsgType type{};
  std::uint8_t flags{};
  std::uint64_t term{};
  MsgBody body;
};

// Bounds-checked sequential reader over a payload span.
class Cursor {
 public:
  explicit Cursor(std::span<const std::byte> data) : data_(data) {}

  template <typename T>
  auto take() -> Result<T> {
    if (data_.size() - pos_ < sizeof(T)) {
      return std::unexpected(Error::protocol_violation);
    }
    std::uint64_t raw = 0;
    if constexpr (sizeof(T) == 1) {
      raw = static_cast<unsigned char>(data_[pos_]);
    } else {
      raw = read_be(sizeof(T));
    }
    pos_ += sizeof(T);
    return static_cast<T>(raw);
  }

  auto take_bytes(std::size_t n) -> Result<std::span<const std::byte>> {
    if (data_.size() - pos_ < n) {
      return std::unexpected(Error::protocol_violation);
    }
    const auto out = data_.subspan(pos_, n);
    pos_ += n;
    return out;
  }

  [[nodiscard]] auto remaining() const -> std::size_t {
    return data_.size() - pos_;
  }

 private:
  auto read_be(std::size_t n) const -> std::uint64_t;

  std::span<const std::byte> data_;
  std::size_t pos_{};
};

// Decode a complete payload for `type` (payload may alias reader storage).
auto decode_body(MsgType type, std::span<const std::byte> payload)
    -> Result<MsgBody>;

// Encode a full frame (header + payload) ready for one write.
auto encode_frame(MsgType type, std::uint8_t flags, std::uint64_t term,
                  std::span<const std::byte> payload) -> std::vector<std::byte>;

// Payload builders.
auto build_handshake(std::uint64_t cluster_id, std::uint64_t node_id)
    -> std::vector<std::byte>;
auto build_request_vote(std::uint64_t last_log_term,
                        std::uint64_t last_log_index) -> std::vector<std::byte>;
auto build_vote_resp(bool granted) -> std::vector<std::byte>;
auto build_append_entries(std::uint64_t prev_index, std::uint64_t prev_term,
                          std::uint64_t leader_commit,
                          std::span<const LogEntry> entries)
    -> std::vector<std::byte>;
auto build_append_resp(bool success, std::uint64_t hint_index)
    -> std::vector<std::byte>;
auto build_install_snapshot(std::uint64_t last_included_index,
                            std::uint64_t last_included_term,
                            std::uint64_t offset,
                            std::span<const std::byte> data)
    -> std::vector<std::byte>;
auto build_install_resp(bool ok) -> std::vector<std::byte>;

// Two-state streaming frame parser consumed in place from an evbuffer.
// Header: 12-byte scratch only while split across reads. Payload: one reused
// slab sized to the frame currently being parsed. Completed frames are
// dispatched synchronously; nothing is queued.
class FrameReader {
 public:
  // Handler returns an error to abort the stream (connection will be dropped).
  using Handler = std::function<Result<void>(const WireMessage& msg)>;

  auto feed(struct evbuffer* in, const Handler& handler) -> Result<void>;

 private:
  enum class State { header, payload };

  auto decode_header() -> Result<void>;
  auto dispatch(const Handler& handler) -> Result<void>;

  State state_{State::header};
  std::array<std::byte, kHeaderSize> header_{};
  std::size_t header_fill_{};
  std::vector<std::byte> payload_;
  std::size_t payload_fill_{};
  WireMessage pending_;
};

}  // namespace bg::raft::detail
