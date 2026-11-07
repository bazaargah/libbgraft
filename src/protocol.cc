#include "protocol.h"

#include <event2/buffer.h>

#include <algorithm>

#include "diag.h"

namespace bg::raft::detail {

auto decode_body(MsgType type, std::span<const std::byte> payload)
    -> Result<MsgBody> {
  Cursor c(payload);
  switch (type) {
    case MsgType::handshake: {
      auto cluster = c.take<std::uint64_t>();
      auto node = c.take<std::uint64_t>();
      if (!cluster || !node) {
        return std::unexpected(Error::protocol_violation);
      }
      return HandshakeMsg{.cluster_id = *cluster, .node_id = *node};
    }
    case MsgType::request_vote: {
      auto last_term = c.take<std::uint64_t>();
      auto last_index = c.take<std::uint64_t>();
      if (!last_term || !last_index) {
        return std::unexpected(Error::protocol_violation);
      }
      return RequestVoteMsg{
          .last_log_term = *last_term,
          .last_log_index = *last_index,
      };
    }
    case MsgType::request_vote_resp: {
      auto granted = c.take<std::uint8_t>();
      if (!granted) {
        return std::unexpected(Error::protocol_violation);
      }
      return RequestVoteRespMsg{*granted != 0U};
    }
    case MsgType::append_entries: {
      AppendEntriesMsg msg;
      auto prev_index = c.take<std::uint64_t>();
      auto prev_term = c.take<std::uint64_t>();
      auto commit = c.take<std::uint64_t>();
      auto count = c.take<std::uint16_t>();
      if (!prev_index || !prev_term || !commit || !count) {
        return std::unexpected(Error::protocol_violation);
      }
      msg.prev_log_index = *prev_index;
      msg.prev_log_term = *prev_term;
      msg.leader_commit = *commit;
      for (std::uint16_t i = 0; i < *count; ++i) {
        auto term = c.take<std::uint64_t>();
        auto index = c.take<std::uint64_t>();
        auto len = c.take<std::uint32_t>();
        if (!term || !index || !len) {
          return std::unexpected(Error::protocol_violation);
        }
        auto data = c.take_bytes(*len);
        if (!data) {
          return std::unexpected(Error::protocol_violation);
        }
        msg.entries.push_back(
            EntryRef{.term = *term, .index = *index, .data = *data});
      }
      if (c.remaining() != 0U) {
        return std::unexpected(Error::protocol_violation);
      }
      return msg;
    }
    case MsgType::append_entries_resp: {
      auto success = c.take<std::uint8_t>();
      auto hint = c.take<std::uint64_t>();
      auto sm_wm = c.take<std::uint64_t>();
      if (!success || !hint || !sm_wm) {
        return std::unexpected(Error::protocol_violation);
      }
      return AppendEntriesRespMsg{
          .success = *success != 0U,
          .hint_index = *hint,
          .sm_index = *sm_wm,
      };
    }
    case MsgType::install_snapshot: {
      InstallSnapshotMsg msg;
      auto last_index = c.take<std::uint64_t>();
      auto last_term = c.take<std::uint64_t>();
      auto offset = c.take<std::uint64_t>();
      if (!last_index || !last_term || !offset) {
        return std::unexpected(Error::protocol_violation);
      }
      msg.last_included_index = *last_index;
      msg.last_included_term = *last_term;
      msg.offset = *offset;
      msg.data = payload.subspan(payload.size() - c.remaining());
      return msg;
    }
    case MsgType::install_snapshot_resp: {
      auto ok = c.take<std::uint8_t>();
      if (!ok) {
        return std::unexpected(Error::protocol_violation);
      }
      return InstallSnapshotRespMsg{*ok != 0U};
    }
  }
  return std::unexpected(Error::protocol_violation);
}

auto encode_frame(MsgType type, std::uint8_t flags, std::uint64_t term,
                  std::span<const std::byte> payload)
    -> std::vector<std::byte> {
  if (payload.size() > kMaxPayload) {
    // Never emit a frame we cannot length-prefix honestly; the 16-bit
    // payload field would silently truncate. Transport::send drops an
    // empty result loudly.
    logf(LogLevel::error, "encode_frame: payload {} exceeds {}", payload.size(),
         kMaxPayload);
    return {};
  }
  std::vector<std::byte> out;
  out.reserve(kHeaderSize + payload.size());
  be_write(out, static_cast<std::uint8_t>(type));
  be_write(out, flags);
  be_write(out, static_cast<std::uint16_t>(payload.size()));
  be_write(out, term);
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

auto build_handshake(std::uint64_t cluster_id, std::uint64_t node_id)
    -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, cluster_id);
  be_write(p, node_id);
  return p;
}

auto build_request_vote(std::uint64_t last_log_term,
                        std::uint64_t last_log_index)
    -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, last_log_term);
  be_write(p, last_log_index);
  return p;
}

auto build_vote_resp(bool granted) -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, static_cast<std::uint8_t>(granted ? 1U : 0U));
  return p;
}

auto build_append_entries(std::uint64_t prev_index, std::uint64_t prev_term,
                          std::uint64_t leader_commit,
                          std::span<const LogEntry> entries)
    -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, prev_index);
  be_write(p, prev_term);
  be_write(p, leader_commit);
  be_write(p, static_cast<std::uint16_t>(entries.size()));
  for (const auto& e : entries) {
    be_write(p, e.term);
    be_write(p, e.index);
    be_write(p, static_cast<std::uint32_t>(e.data.size()));
    p.insert(p.end(), e.data.begin(), e.data.end());
  }
  return p;
}

auto build_append_resp(bool success, std::uint64_t hint_index,
                       std::uint64_t sm_index) -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, static_cast<std::uint8_t>(success ? 1U : 0U));
  be_write(p, hint_index);
  be_write(p, sm_index);
  return p;
}

auto build_install_snapshot(std::uint64_t last_included_index,
                            std::uint64_t last_included_term,
                            std::uint64_t offset,
                            std::span<const std::byte> data)
    -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, last_included_index);
  be_write(p, last_included_term);
  be_write(p, offset);
  p.insert(p.end(), data.begin(), data.end());
  return p;
}

auto build_install_resp(bool ok) -> std::vector<std::byte> {
  std::vector<std::byte> p;
  be_write(p, static_cast<std::uint8_t>(ok ? 1U : 0U));
  return p;
}

auto FrameReader::decode_header() -> Result<void> {
  Cursor c(header_);
  auto type = c.take<std::uint8_t>();
  auto flags = c.take<std::uint8_t>();
  auto len = c.take<std::uint16_t>();
  auto term = c.take<std::uint64_t>();
  if (!type || !flags || !len || !term) {
    return std::unexpected(Error::protocol_violation);
  }
  if (*type > static_cast<std::uint8_t>(MsgType::install_snapshot_resp)) {
    return std::unexpected(Error::protocol_violation);
  }
  pending_.type = static_cast<MsgType>(*type);
  pending_.flags = *flags;
  pending_.term = *term;
  if (*len == 0U) {
    auto body = decode_body(pending_.type, {});
    if (!body) {
      return std::unexpected(body.error());
    }
    pending_.body = std::move(*body);
    state_ = State::header;
  } else {
    payload_.resize(*len);
    payload_fill_ = 0U;
    state_ = State::payload;
  }
  return {};
}

auto FrameReader::dispatch(const Handler& handler) -> Result<void> {
  return handler(pending_);
}

auto FrameReader::feed(struct evbuffer* in, const Handler& handler)
    -> Result<void> {
  for (;;) {
    std::size_t avail = evbuffer_get_length(in);
    if (avail == 0U) {
      break;
    }
    if (state_ == State::header) {
      const std::size_t want = kHeaderSize - header_fill_;
      const std::size_t take = std::min(want, avail);
      evbuffer_remove(in, header_.data() + header_fill_, take);
      header_fill_ += take;
      if (header_fill_ < kHeaderSize) {
        break;
      }
      header_fill_ = 0U;
      if (auto r = decode_header(); !r) {
        return r;
      }
      // Empty-payload frames are complete immediately.
      if (state_ == State::header) {
        if (auto r = dispatch(handler); !r) {
          return r;
        }
      }
    } else {
      const std::size_t want = payload_.size() - payload_fill_;
      const std::size_t take = std::min(want, avail);
      evbuffer_remove(in, payload_.data() + payload_fill_, take);
      payload_fill_ += take;
      if (payload_fill_ < payload_.size()) {
        break;
      }
      state_ = State::header;
      auto body = decode_body(pending_.type, payload_);
      if (!body) {
        return std::unexpected(body.error());
      }
      pending_.body = std::move(*body);
      if (auto r = dispatch(handler); !r) {
        return r;
      }
    }
  }
  return {};
}

}  // namespace bg::raft::detail
