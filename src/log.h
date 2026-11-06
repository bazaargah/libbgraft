#pragma once

#include <bg/raft/raft.h>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <vector>

// File-backed write-ahead log. One concrete implementation — no storage
// abstraction. Record layout (fixed widths, big-endian):
//
//   u32 crc | u32 len | term:u64 | index:u64 | data[len-16]
//
// crc covers the len field through the end of data. A torn or bad tail
// record is truncated away on open. Term/voted_for/snapshot watermark live
// in a fixed 36-byte sidecar ("<path>.meta", rewritten atomically).

namespace bg::raft::detail {

class Log {
 public:
  Log();
  ~Log();

  Log(const Log&) = delete;
  auto operator=(const Log&) -> Log& = delete;

  auto open(const std::string& path) -> Result<void>;
  auto close() -> void;

  // Durability. append() performs the write(); sync() fsyncs.
  auto append(const LogEntry& entry) -> Result<void>;
  auto append(std::span<const LogEntry> entries) -> Result<void>;
  auto sync() const -> Result<void>;

  [[nodiscard]] auto first_index() const -> std::uint64_t;
  [[nodiscard]] auto last_index() const -> std::uint64_t;
  [[nodiscard]] auto last_term() const -> std::uint64_t;
  [[nodiscard]] auto term_at(std::uint64_t index) const
      -> std::optional<std::uint64_t>;
  auto read(std::uint64_t from_index, std::uint32_t max_entries,
            std::uint64_t max_bytes) -> Result<std::vector<LogEntry>>;

  auto truncate_from(std::uint64_t index) -> Result<void>;

  // Drops all records with index <= `up_to` and raises the snapshot
  // watermark; rewrites the file atomically.
  auto compact_upto(std::uint64_t up_to, std::uint64_t up_to_term)
      -> Result<void>;

  // Meta accessors (persisted when setter is called).
  [[nodiscard]] auto current_term() const -> std::uint64_t;
  auto set_current_term(std::uint64_t term) -> Result<void>;
  [[nodiscard]] auto voted_for() const -> std::uint64_t;  // 0 = none
  auto set_voted_for(std::uint64_t peer_id) -> Result<void>;
  [[nodiscard]] auto last_included_index() const -> std::uint64_t;
  [[nodiscard]] auto last_included_term() const -> std::uint64_t;
  auto set_snapshot_meta(std::uint64_t index, std::uint64_t term)
      -> Result<void>;

  // Drops the in-memory view of the log to `index` (exclusive) after an
  // out-of-band snapshot restore: the file records are compacted lazily by
  // compact_upto(), but the watermark must move immediately.
  auto adopt_snapshot(std::uint64_t index, std::uint64_t term) -> Result<void>;

 private:
  struct Record {
    std::uint64_t index;
    std::uint64_t term;
    std::uint64_t offset;
    std::uint32_t data_len;
  };

  struct Meta {
    std::uint64_t term{};
    std::uint64_t voted_for{};  // 0 = none
    std::uint64_t last_included_index{};
    std::uint64_t last_included_term{};
  };

  static auto meta_path(const std::string& path) -> std::string;
  auto replay() -> Result<void>;
  auto load_meta() -> Result<void>;
  auto write_meta() const -> Result<void>;
  auto write_record(const LogEntry& entry) -> Result<void>;
  auto append_offset() const -> std::uint64_t;

  std::string path_;
  int fd_{-1};
  std::vector<Record> records_;
  std::uint64_t file_size_{};
  Meta meta_;
};

}  // namespace bg::raft::detail
