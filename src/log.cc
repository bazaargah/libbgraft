#include "log.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <utility>

#include "detail/be.h"
#include "detail/crc32.h"
#include "detail/fs.h"

namespace bg::raft::detail {

namespace {
constexpr std::size_t kRecordHeaderSize = 8;  // crc + len
constexpr std::size_t kRecordFixedBody = 16;  // term + index
constexpr std::size_t kMetaSize = 36;         // crc + 4 x u64
constexpr std::uint32_t kMaxRecordData = 4U << 20U;
}  // namespace

Log::Log() = default;

Log::~Log() { close(); }

auto Log::meta_path(const std::string& path) -> std::string {
  return path + ".meta";
}

auto Log::open(const std::string& path) -> Result<void> {
  path_ = path;
  fd_ = ::open(path.c_str(), O_RDWR | O_CREAT, 0600);
  if (fd_ < 0) {
    return std::unexpected(Error::io_failure);
  }
  struct stat st{};
  if (fstat(fd_, &st) != 0) {
    return std::unexpected(Error::io_failure);
  }
  file_size_ = static_cast<std::uint64_t>(st.st_size);
  if (auto r = load_meta(); !r) {
    return r;
  }
  return replay();
}

auto Log::close() -> void {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

auto Log::load_meta() -> Result<void> {
  const auto mp = meta_path(path_);
  const int mfd = ::open(mp.c_str(), O_RDONLY);
  if (mfd < 0) {
    meta_ = {};  // fresh node
    return write_meta();
  }
  std::array<std::byte, kMetaSize> buf{};
  const auto got = ::read(mfd, buf.data(), buf.size());
  ::close(mfd);
  if (std::cmp_not_equal(got, buf.size())) {
    return std::unexpected(Error::io_failure);
  }
  std::span<const std::byte> all(buf);
  const auto stored = be_read_u32(all.first(4));
  if (crc32(all.subspan(4)) != stored) {
    return std::unexpected(Error::io_failure);
  }
  auto body = all.subspan(4);
  meta_.term = be_read_u64(body.subspan(0));
  meta_.voted_for = be_read_u64(body.subspan(8));
  meta_.last_included_index = be_read_u64(body.subspan(16));
  meta_.last_included_term = be_read_u64(body.subspan(24));
  return {};
}

auto Log::write_meta() const -> Result<void> {
  std::array<std::byte, kMetaSize> buf{};
  auto body = std::span<std::byte>(buf.data() + 4, 32);
  auto put = [&](std::size_t at, std::uint64_t v) -> void {
    const auto n = std::byteswap(v);
    std::memcpy(body.data() + at, &n, sizeof(n));
  };
  put(0, meta_.term);
  put(8, meta_.voted_for);
  put(16, meta_.last_included_index);
  put(24, meta_.last_included_term);
  be_place(buf.data(), crc32(std::span<const std::byte>(body)));

  const auto target = meta_path(path_);
  return write_atomically(target, target + ".tmp", [&](int tfd) -> Result<void> {
    if (::write(tfd, buf.data(), buf.size()) !=
        static_cast<::ssize_t>(buf.size())) {
      return std::unexpected(Error::io_failure);
    }
    return {};
  });
}

auto Log::replay() -> Result<void> {
  records_.clear();
  std::uint64_t offset = 0U;
  std::uint64_t good_end = 0U;
  std::uint64_t expect = meta_.last_included_index + 1U;
  std::array<std::byte, kRecordHeaderSize> head{};
  while (offset + kRecordHeaderSize <= file_size_) {
    const auto got =
        ::pread(fd_, head.data(), head.size(), static_cast<off_t>(offset));
    if (std::cmp_not_equal(got, head.size())) {
      break;
    }
    const std::span<const std::byte> head_span(head);
    const auto crc = be_read_u32(head_span.first(4));
    const auto len = be_read_u32(head_span.subspan(4));
    if (len < kRecordFixedBody || len - kRecordFixedBody > kMaxRecordData ||
        offset + kRecordHeaderSize + len > file_size_) {
      break;
    }
    std::vector<std::byte> body(len);
    const auto body_got =
        ::pread(fd_, body.data(), body.size(),
                static_cast<off_t>(offset + kRecordHeaderSize));
    if (std::cmp_not_equal(body_got, body.size()) || crc32(body) != crc) {
      break;
    }
    const auto term = be_read_u64(std::span<const std::byte>(body).subspan(0));
    const auto index = be_read_u64(std::span<const std::byte>(body).subspan(8));
    const auto record_end = offset + kRecordHeaderSize + len;
    // Records at or below the watermark are redundant survivors of an
    // interrupted compaction; they are skipped and dropped by the next
    // full truncate/rewrite.
    if (index <= meta_.last_included_index) {
      offset = record_end;
      good_end = record_end;
      continue;
    }
    if (index > expect) {
      break;  // gap: stop, tail is truncated at good_end
    }
    records_.push_back(Record{
        .index = index,
        .term = term,
        .offset = offset,
        .data_len = len - static_cast<std::uint32_t>(kRecordFixedBody),
    });
    expect = index + 1U;
    offset = record_end;
    good_end = record_end;
  }
  // Drop any torn/invalid tail so future appends start from a clean end.
  if (good_end != file_size_) {
    if (ftruncate(fd_, static_cast<off_t>(good_end)) != 0) {
      return std::unexpected(Error::io_failure);
    }
    file_size_ = good_end;
  }
  return {};
}

auto Log::append_offset() const -> std::uint64_t {
  return records_.empty() ? file_size_
                          : records_.back().offset + kRecordHeaderSize +
                                kRecordFixedBody + records_.back().data_len;
}

auto Log::write_record(const LogEntry& entry) -> Result<void> {
  std::vector<std::byte> buf;
  buf.reserve(kRecordHeaderSize + kRecordFixedBody + entry.data.size());
  buf.resize(kRecordHeaderSize);  // crc/len placeholder
  be_write(buf, entry.term);
  be_write(buf, entry.index);
  buf.insert(buf.end(), entry.data.begin(), entry.data.end());
  const auto len = static_cast<std::uint32_t>(buf.size() - kRecordHeaderSize);
  const auto crc =
      crc32(std::span<const std::byte>(buf.data() + kRecordHeaderSize, len));
  be_place(buf.data(), crc);
  be_place(buf.data() + 4, len);

  const auto offset = append_offset();
  const auto written =
      ::pwrite(fd_, buf.data(), buf.size(), static_cast<off_t>(offset));
  if (std::cmp_not_equal(written, buf.size())) {
    return std::unexpected(Error::io_failure);
  }
  records_.push_back(Record{
      .index = entry.index,
      .term = entry.term,
      .offset = offset,
      .data_len = static_cast<std::uint32_t>(entry.data.size()),
  });
  file_size_ = offset + buf.size();
  return {};
}

auto Log::append(const LogEntry& entry) -> Result<void> {
  if (entry.index != last_index() + 1U) {
    return std::unexpected(Error::io_failure);
  }
  return write_record(entry);
}

auto Log::append(std::span<const LogEntry> entries) -> Result<void> {
  auto next = last_index() + 1U;
  for (const auto& entry : entries) {
    if (entry.index != next) {
      return std::unexpected(Error::io_failure);
    }
    if (auto r = write_record(entry); !r) {
      return r;
    }
    ++next;
  }
  return {};
}

auto Log::sync() const -> Result<void> {
  if (fsync(fd_) != 0) {
    return std::unexpected(Error::io_failure);
  }
  return {};
}

auto Log::first_index() const -> std::uint64_t {
  return meta_.last_included_index + 1U;
}

auto Log::last_index() const -> std::uint64_t {
  return records_.empty() ? meta_.last_included_index : records_.back().index;
}

auto Log::last_term() const -> std::uint64_t {
  return records_.empty() ? meta_.last_included_term : records_.back().term;
}

auto Log::term_at(std::uint64_t index) const -> std::optional<std::uint64_t> {
  if (index == meta_.last_included_index) {
    return meta_.last_included_term;
  }
  if (index < first_index() || index > last_index()) {
    return std::nullopt;
  }
  return records_[static_cast<std::size_t>(index - first_index())].term;
}

auto Log::read(std::uint64_t from_index, std::uint32_t max_entries,
               std::uint64_t max_bytes) -> Result<std::vector<LogEntry>> {
  std::vector<LogEntry> out;
  if (from_index < first_index() || from_index > last_index() + 1U) {
    return out;
  }
  auto start = static_cast<std::size_t>(from_index - first_index());
  std::uint64_t budget = 0U;
  for (std::size_t i = start; i < records_.size(); ++i) {
    const auto& rec = records_[i];
    // Always admit the first record even if it exceeds max_bytes: the
    // submit-time entry cap guarantees a single entry still fits one
    // frame, and refusing it here would stall replication forever.
    if (!out.empty() &&
        (out.size() >= max_entries || budget + rec.data_len > max_bytes)) {
      break;
    }
    std::vector<std::byte> buf(kRecordFixedBody + rec.data_len);
    const auto got =
        ::pread(fd_, buf.data(), buf.size(),
                static_cast<off_t>(rec.offset + kRecordHeaderSize));
    if (std::cmp_not_equal(got, buf.size())) {
      return std::unexpected(Error::io_failure);
    }
    LogEntry entry;
    entry.term = be_read_u64(std::span<const std::byte>(buf).subspan(0));
    entry.index = be_read_u64(std::span<const std::byte>(buf).subspan(8));
    entry.data.assign(
        buf.begin() + static_cast<std::ptrdiff_t>(kRecordFixedBody), buf.end());
    budget += rec.data_len;
    out.push_back(std::move(entry));
  }
  return out;
}

auto Log::truncate_from(std::uint64_t index) -> Result<void> {
  if (index <= meta_.last_included_index) {
    return std::unexpected(Error::io_failure);
  }
  if (index > last_index()) {
    return {};
  }
  const auto& rec = records_[static_cast<std::size_t>(index - first_index())];
  if (ftruncate(fd_, static_cast<off_t>(rec.offset)) != 0) {
    return std::unexpected(Error::io_failure);
  }
  file_size_ = rec.offset;
  records_.erase(
      records_.begin() + static_cast<std::ptrdiff_t>(index - first_index()),
      records_.end());
  return {};
}

auto Log::compact_upto(std::uint64_t up_to, std::uint64_t up_to_term)
    -> Result<void> {
  if (up_to < meta_.last_included_index || up_to > last_index()) {
    return std::unexpected(Error::io_failure);
  }
  if (up_to == meta_.last_included_index) {
    return {};
  }

  // 1. Move the watermark into meta first; a crash between steps 1 and 3
  //    merely leaves redundant (harmless) records in the data file.
  meta_.last_included_index = up_to;
  meta_.last_included_term = up_to_term;
  if (auto r = write_meta(); !r) {
    return r;
  }

  // 2. Rewrite the data file keeping only records above the watermark.
  const auto tmp = path_ + ".compact.tmp";
  if (auto r = write_atomically(path_, tmp, [&](int tfd) -> Result<void> {
        std::uint64_t written_total = 0U;
        for (const auto& rec : records_) {
          if (rec.index <= up_to) {
            continue;
          }
          const std::size_t size =
              kRecordHeaderSize + kRecordFixedBody + rec.data_len;
          std::vector<std::byte> buf(size);
          const auto got =
              ::pread(fd_, buf.data(), buf.size(),
                      static_cast<off_t>(rec.offset));
          if (std::cmp_not_equal(got, buf.size()) ||
              ::pwrite(tfd, buf.data(), buf.size(),
                       static_cast<off_t>(written_total)) !=
                  static_cast<::ssize_t>(buf.size())) {
            return std::unexpected(Error::io_failure);
          }
          written_total += size;
        }
        return {};
      });
      !r) {
    return r;
  }

  // 3. Reopen and replay the compacted file.
  close();
  return open(path_);
}

auto Log::adopt_snapshot(std::uint64_t index, std::uint64_t term)
    -> Result<void> {
  // Follower-side install: (index, term) becomes the new base. Every
  // physical record is dropped — entries above the adopted index are
  // uncommitted leftovers of an older era and must not survive.
  if (index < meta_.last_included_index) {
    return std::unexpected(Error::io_failure);
  }
  meta_.last_included_index = index;
  meta_.last_included_term = term;
  if (auto r = write_meta(); !r) {
    return r;
  }
  if (::ftruncate(fd_, 0) != 0) {
    return std::unexpected(Error::io_failure);
  }
  records_.clear();
  file_size_ = 0U;
  return {};
}

auto Log::current_term() const -> std::uint64_t { return meta_.term; }

auto Log::set_current_term(std::uint64_t term) -> Result<void> {
  meta_.term = term;
  return write_meta();
}

auto Log::voted_for() const -> std::uint64_t { return meta_.voted_for; }

auto Log::set_voted_for(std::uint64_t peer_id) -> Result<void> {
  meta_.voted_for = peer_id;
  return write_meta();
}

auto Log::last_included_index() const -> std::uint64_t {
  return meta_.last_included_index;
}

auto Log::last_included_term() const -> std::uint64_t {
  return meta_.last_included_term;
}

auto Log::set_snapshot_meta(std::uint64_t index, std::uint64_t term)
    -> Result<void> {
  meta_.last_included_index = index;
  meta_.last_included_term = term;
  // Physically dead records below the new watermark are dropped by the next
  // replay; keep the in-memory view consistent right now.
  records_.erase(std::ranges::remove_if(records_,
                                        [&](const Record& rec) -> bool {
                                          return rec.index <= index;
                                        })
                     .begin(),
                 records_.end());
  return write_meta();
}

}  // namespace bg::raft::detail
