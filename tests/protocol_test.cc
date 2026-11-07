#include "protocol.h"

#include <event2/buffer.h>

#include <cstddef>
#include <cstring>
#include <random>
#include <vector>

#include "harness/check.h"

using namespace bg::raft;
using namespace bg::raft::detail;

namespace {

// Runs `fn` on the decoded message while the reader's payload buffer is
// still alive (decoded spans alias the reader, exactly like the library's
// synchronous dispatch contract).
template <typename Fn>
auto inspect(std::span<const std::byte> frame, const Fn& fn) -> void {
  auto* input = evbuffer_new();
  evbuffer_add(input, frame.data(), frame.size());
  FrameReader reader;
  std::size_t seen = 0;
  auto r = reader.feed(input, [&](const WireMessage& m) -> Result<void> {
    fn(m);
    ++seen;
    return {};
  });
  CHECK(r.has_value());
  CHECK_EQ(seen, 1U);
  evbuffer_free(input);
}

void feed_chunked(std::span<const std::byte> frame, std::size_t chunk,
                  std::size_t expect_messages) {
  auto* input = evbuffer_new();
  FrameReader reader;
  std::size_t received = 0;
  auto handler = [&](const WireMessage&) -> Result<void> {
    ++received;
    return {};
  };
  std::size_t offset = 0;
  while (offset < frame.size()) {
    const auto n = std::min(chunk, frame.size() - offset);
    evbuffer_add(input, frame.data() + offset, n);
    offset += n;
    auto r = reader.feed(input, handler);
    CHECK(r.has_value());
  }
  CHECK_EQ(received, expect_messages);
  CHECK(evbuffer_get_length(input) == 0);
  evbuffer_free(input);
}

void test_handshake() {
  const auto payload = build_handshake(0x0102030405060708ULL, 42ULL);
  const auto frame = encode_frame(MsgType::handshake, 0U, 0U, payload);
  inspect(frame, [](const WireMessage& got) -> void {
    CHECK(got.type == MsgType::handshake);
    const auto* p = std::get_if<HandshakeMsg>(&got.body);
    CHECK(p != nullptr);
    if (p == nullptr) {
      return;
    }
    CHECK_EQ(p->cluster_id, 0x0102030405060708ULL);
    CHECK_EQ(p->node_id, 42U);
  });
}

void test_request_vote() {
  const auto payload = build_request_vote(6ULL, 99ULL);
  const auto frame =
      encode_frame(MsgType::request_vote, kFlagPreVote, 7ULL, payload);
  inspect(frame, [](const WireMessage& got) -> void {
    CHECK(got.flags == kFlagPreVote);
    CHECK_EQ(got.term, 7U);
    const auto* p = std::get_if<RequestVoteMsg>(&got.body);
    CHECK(p != nullptr);
    if (p == nullptr) {
      return;
    }
    CHECK_EQ(p->last_log_term, 6U);
    CHECK_EQ(p->last_log_index, 99U);
  });

  const auto resp = build_vote_resp(true);
  inspect(encode_frame(MsgType::request_vote_resp, 0U, 7ULL, resp),
          [](const WireMessage& got) -> void {
            const auto* r = std::get_if<RequestVoteRespMsg>(&got.body);
            CHECK(r != nullptr);
            if (r == nullptr) {
              return;
            }
            CHECK(r->vote_granted);
          });
}

void test_append_entries() {
  std::vector<LogEntry> entries;
  entries.push_back(LogEntry{
      .term = 4U,
      .index = 11U,
      .data =
          {
              std::byte{1},
              std::byte{2},
              std::byte{3},
              std::byte{4},
              std::byte{5},
          },
  });
  entries.push_back(
      LogEntry{.term = 4U, .index = 12U, .data = {}});  // no-op style
  const auto payload = build_append_entries(10ULL, 3ULL, 8ULL, entries);
  const auto frame = encode_frame(MsgType::append_entries, 0U, 4ULL, payload);
  inspect(frame, [](const WireMessage& got) -> void {
    const auto* p = std::get_if<AppendEntriesMsg>(&got.body);
    CHECK(p != nullptr);
    if (p == nullptr) {
      return;
    }
    CHECK_EQ(p->entries.size(), 2U);
    CHECK_EQ(p->prev_log_index, 10U);
    CHECK_EQ(p->prev_log_term, 3U);
    CHECK_EQ(p->leader_commit, 8U);
    CHECK_EQ(p->entries[0].index, 11U);
    CHECK_EQ(p->entries[0].term, 4U);
    CHECK(p->entries[0].data.size() == 5);
    if (p->entries[0].data.size() == 5) {
      CHECK(std::memcmp(p->entries[0].data.data(), "\x01\x02\x03\x04\x05", 5) ==
            0);
    }
    CHECK(p->entries[1].data.empty());
  });

  const auto resp = build_append_resp(false, 7ULL, 41ULL);
  inspect(encode_frame(MsgType::append_entries_resp, 0U, 4ULL, resp),
          [](const WireMessage& got) -> void {
            const auto* ar = std::get_if<AppendEntriesRespMsg>(&got.body);
            CHECK(ar != nullptr);
            if (ar == nullptr) {
              return;
            }
            CHECK(!ar->success);
            CHECK_EQ(ar->hint_index, 7U);
            CHECK_EQ(ar->sm_index, 41U);
          });
  // Short AER payloads (pre-watermark shape) are a protocol violation.
  std::vector<std::byte> legacy;
  be_write(legacy, static_cast<std::uint8_t>(1U));
  be_write(legacy, std::uint64_t{9});
  {
    auto* input = evbuffer_new();
    const auto frame = encode_frame(MsgType::append_entries_resp, 0U, 4ULL,
                                    legacy);
    evbuffer_add(input, frame.data(), frame.size());
    FrameReader reader;
    bool called = false;
    auto r = reader.feed(input, [&](const WireMessage&) -> Result<void> {
      called = true;
      return {};
    });
    CHECK(!r.has_value());
    CHECK(r.error() == Error::protocol_violation);
    CHECK(!called);
    evbuffer_free(input);
  }
}

void test_install_snapshot() {
  const std::array<std::byte, 4> blob{
      std::byte{'a'},
      std::byte{'b'},
      std::byte{'c'},
      std::byte{'d'},
  };
  const auto payload = build_install_snapshot(100ULL, 5ULL, 0ULL, blob);
  inspect(encode_frame(MsgType::install_snapshot, kFlagFinal, 5ULL, payload),
          [](const WireMessage& got) -> void {
            const auto* i = std::get_if<InstallSnapshotMsg>(&got.body);
            CHECK(i != nullptr);
            if (i == nullptr) {
              return;
            }
            CHECK_EQ(i->last_included_index, 100U);
            CHECK_EQ(i->last_included_term, 5U);
            CHECK_EQ(i->offset, 0U);
            CHECK(i->data.size() == 4);
            CHECK(got.flags == kFlagFinal);
          });

  // Out-of-band offer: zero data length.
  inspect(encode_frame(MsgType::install_snapshot, kFlagOffer, 5ULL,
                       build_install_snapshot(100ULL, 5ULL, 0ULL, {})),
          [](const WireMessage& got) -> void {
            const auto* o = std::get_if<InstallSnapshotMsg>(&got.body);
            CHECK(o != nullptr);
            if (o == nullptr) {
              return;
            }
            CHECK(o->data.empty());
            CHECK(got.flags == kFlagOffer);
          });

  inspect(encode_frame(MsgType::install_snapshot_resp, kFlagWantInline, 5ULL,
                       build_install_resp(false)),
          [](const WireMessage& got) -> void {
            const auto* w = std::get_if<InstallSnapshotRespMsg>(&got.body);
            CHECK(w != nullptr);
            if (w == nullptr) {
              return;
            }
            CHECK(!w->ok);
            CHECK(got.flags == kFlagWantInline);
          });

  inspect(encode_frame(MsgType::install_snapshot_resp, 0U, 5ULL,
                       build_install_resp(true)),
          [](const WireMessage& got) -> void {
            const auto* k = std::get_if<InstallSnapshotRespMsg>(&got.body);
            CHECK(k != nullptr);
            if (k == nullptr) {
              return;
            }
            CHECK(k->ok);
          });
}

void test_split_reassembly() {
  std::vector<LogEntry> entries;
  entries.push_back(
      LogEntry{.term = 1U, .index = 1U, .data = {std::byte{0xFF}}});
  const auto frame =
      encode_frame(MsgType::append_entries, 0U, 1ULL,
                   build_append_entries(0ULL, 0ULL, 0ULL, entries));
  feed_chunked(frame, 1, 1);  // byte by byte
  feed_chunked(frame, 3, 1);  // odd splits
  feed_chunked(frame, kHeaderSize - 1, 1);
  feed_chunked(frame, kHeaderSize, 1);  // header-aligned split
}

void test_back_to_back() {
  std::vector<std::byte> stream;
  for (int i = 0; i < 3; ++i) {
    const auto frame =
        encode_frame(MsgType::handshake, 0U, 0ULL, build_handshake(7ULL, 7ULL));
    stream.insert(stream.end(), frame.begin(), frame.end());
  }
  feed_chunked(stream, 7, 3);

  auto* input = evbuffer_new();
  evbuffer_add(input, stream.data(), stream.size());
  FrameReader reader;
  std::size_t seen = 0;
  auto r = reader.feed(input, [&](const WireMessage&) -> Result<void> {
    ++seen;
    return {};
  });
  CHECK(r.has_value());
  CHECK_EQ(seen, 3U);
  evbuffer_free(input);
}

void test_protocol_violation() {
  // Unknown message type in the header.
  std::array<std::byte, kHeaderSize> head{};
  head[0] = std::byte{99};
  auto* input = evbuffer_new();
  evbuffer_add(input, head.data(), head.size());
  FrameReader reader;
  bool called = false;
  auto r = reader.feed(input, [&](const WireMessage&) -> Result<void> {
    called = true;
    return {};
  });
  CHECK(!r.has_value());
  CHECK(r.error() == Error::protocol_violation);
  CHECK(!called);
  evbuffer_free(input);
}

void test_partial_frame_buffers() {
  std::vector<LogEntry> entries;
  entries.push_back(LogEntry{
      .term = 2U,
      .index = 3U,
      .data = std::vector<std::byte>(64, std::byte{0x5A}),
  });
  const auto frame =
      encode_frame(MsgType::append_entries, 0U, 2ULL,
                   build_append_entries(2ULL, 1ULL, 2ULL, entries));

  auto* input = evbuffer_new();
  evbuffer_add(input, frame.data(), frame.size() - 1);  // one byte short
  FrameReader reader;
  std::size_t seen = 0;
  auto r = reader.feed(input, [&](const WireMessage&) -> Result<void> {
    ++seen;
    return {};
  });
  CHECK(r.has_value());
  CHECK_EQ(seen, 0U);
  evbuffer_add(input, frame.data() + frame.size() - 1, 1);
  r = reader.feed(input, [&](const WireMessage&) -> Result<void> {
    ++seen;
    return {};
  });
  CHECK(r.has_value());
  CHECK_EQ(seen, 1U);
  evbuffer_free(input);
}

// Random hostile input must either be rejected as a protocol violation
// or be legitimately awaiting the rest of its frame -- never crash.
// This suite runs under ASan+UBSan in the debug preset.
void test_garbage_feed() {
  std::mt19937_64 rng(977ULL);  // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
  std::size_t rejected = 0;
  std::size_t tolerated = 0;
  for (int round = 0; round < 4000; ++round) {
    const auto size = static_cast<std::size_t>(rng() % 400U);
    std::vector<std::byte> blob(size);
    for (auto& byte : blob) {
      byte = static_cast<std::byte>(rng() & 0xFFU);
    }
    auto* input = evbuffer_new();
    evbuffer_add(input, blob.data(), blob.size());
    FrameReader reader;
    auto r = reader.feed(input, [](const WireMessage&) -> Result<void> {
      return {};
    });
    if (r.has_value()) {
      ++tolerated;  // incomplete frame buffered for more data
    } else {
      CHECK(r.error() == Error::protocol_violation);
      ++rejected;
    }
    evbuffer_free(input);
  }
  CHECK(rejected > 0U);
  CHECK(tolerated > 0U);
}

}  // namespace

auto main() -> int {
  RUN_TEST(test_handshake);
  RUN_TEST(test_request_vote);
  RUN_TEST(test_append_entries);
  RUN_TEST(test_install_snapshot);
  RUN_TEST(test_split_reassembly);
  RUN_TEST(test_back_to_back);
  RUN_TEST(test_protocol_violation);
  RUN_TEST(test_partial_frame_buffers);
  RUN_TEST(test_garbage_feed);
  return bgraft_test::summary("protocol");
}
