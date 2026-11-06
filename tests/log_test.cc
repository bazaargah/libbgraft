#include "log.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include "harness/check.h"

using namespace bg::raft;
using namespace bg::raft::detail;

namespace {

struct TempDir {
  std::string path;
  TempDir() {
    std::string tmpl =
        (std::filesystem::temp_directory_path() / "bgraft_log_test_XXXXXX")
            .string();
    const char* dir = mkdtemp(tmpl.data());
    CHECK(dir != nullptr);
    path = std::string{dir != nullptr ? dir : "."} + "/";
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
  }
};

auto entry(std::uint64_t term, std::uint64_t index, std::string_view data)
    -> LogEntry {
  LogEntry e{.term = term, .index = index, .data = {}};
  for (const char c : data) {
    e.data.push_back(static_cast<std::byte>(c));
  }
  return e;
}

void test_append_read_reload() {
  const TempDir dir;
  const auto path = dir.path + "log.bin";
  {
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.last_index(), 0U);
    CHECK_EQ(log.first_index(), 1U);
    CHECK(!log.append(entry(1, 2, "x")).has_value());  // non-sequential
    for (std::uint64_t i = 1; i <= 5; ++i) {
      CHECK(log.append(entry((i % 2) + 1, i, "e" + std::to_string(i)))
                .has_value());
    }
    std::vector<LogEntry> append_batch{
        entry(2, 6, "six"),
        entry(2, 7, "seven"),
    };
    CHECK(log.append(append_batch).has_value());
    CHECK(log.sync().has_value());
    CHECK_EQ(log.last_index(), 7U);
    CHECK_EQ(log.last_term(), 2U);
    CHECK(log.term_at(4) == std::optional{1U});
    CHECK(!log.term_at(8).has_value());

    auto batch = log.read(2, 3, 1024);
    CHECK(batch.has_value());
    CHECK_EQ(batch->size(), 3U);
    CHECK_EQ((*batch)[0].index, 2U);
    CHECK_EQ((*batch)[0].term, 1U);  // term = index % 2 + 1
    CHECK(std::memcmp((*batch)[0].data.data(), "e2", 2) == 0);
    auto limited = log.read(1, 10, 4);  // byte budget: two 2-byte entries
    CHECK(limited.has_value());
    CHECK_EQ(limited->size(), 2U);
  }
  {  // reload from disk
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.last_index(), 7U);
    auto all = log.read(1, 100, 1U << 20U);
    CHECK_EQ(all->size(), 7U);
    CHECK_EQ(all->back().index, 7U);
    CHECK(all->back().data.size() == 5);
  }
}

void test_persistent_meta() {
  const TempDir dir;
  const auto path = dir.path + "log.bin";
  {
    Log log;
    CHECK(log.open(path).has_value());
    CHECK(log.set_current_term(9).has_value());
    CHECK(log.set_voted_for(3).has_value());
    CHECK(log.append(entry(9, 1, "a")).has_value());
    CHECK(log.sync().has_value());
  }
  {
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.current_term(), 9U);
    CHECK_EQ(log.voted_for(), 3U);
    CHECK_EQ(log.last_index(), 1U);
  }
}

void test_truncate() {
  const TempDir dir;
  const auto path = dir.path + "log.bin";
  Log log;
  CHECK(log.open(path).has_value());
  for (std::uint64_t i = 1; i <= 6; ++i) {
    CHECK(log.append(entry(i <= 3 ? 1 : 2, i, "d")).has_value());
  }
  CHECK(log.truncate_from(4).has_value());
  CHECK_EQ(log.last_index(), 3U);
  CHECK(!log.term_at(4).has_value());
  // Re-append a conflicting entry at the truncation point.
  CHECK(log.append(entry(3, 4, "conflict")).has_value());
  CHECK_EQ(log.last_index(), 4U);
  // Truncating below the start removes everything.
  CHECK(log.truncate_from(1).has_value());
  CHECK_EQ(log.last_index(), 0U);
  CHECK(log.append(entry(4, 1, "fresh")).has_value());
}

void test_compact_and_adopt() {
  const TempDir dir;
  const auto path = dir.path + "log.bin";
  {
    Log log;
    CHECK(log.open(path).has_value());
    for (std::uint64_t i = 1; i <= 10; ++i) {
      CHECK(log.append(entry(1, i, "payload" + std::to_string(i))).has_value());
    }
    CHECK(log.sync().has_value());
    CHECK(log.compact_upto(6, 1).has_value());
    CHECK_EQ(log.last_included_index(), 6U);
    CHECK_EQ(log.last_included_term(), 1U);
    CHECK_EQ(log.first_index(), 7U);
    CHECK(log.term_at(6) == std::optional{1U});  // watermark
    CHECK(!log.term_at(5).has_value());
    auto rest = log.read(log.first_index(), 100, 1U << 20U);
    CHECK_EQ(rest->size(), 4U);  // only 7..10 remain
    CHECK_EQ(rest->front().index, 7U);
  }
  {  // after reload the watermark survives
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.last_included_index(), 6U);
    CHECK_EQ(log.last_index(), 10U);
    CHECK(log.set_snapshot_meta(12, 2).has_value());
    CHECK_EQ(log.last_included_index(), 12U);
    CHECK_EQ(log.last_index(), 12U);  // truncated to watermark
    CHECK_EQ(log.last_term(), 2U);
  }
  {  // adopt a newer snapshot position
    Log log;
    CHECK(log.open(path).has_value());
    CHECK(log.append(entry(3, 13, "x")).has_value());
    CHECK(log.adopt_snapshot(14, 3).has_value());
    CHECK_EQ(log.last_included_index(), 14U);
    CHECK_EQ(log.first_index(), 15U);
    CHECK_EQ(log.last_index(), 14U);
    // Appending after an adopted snapshot continues seamlessly.
    CHECK(log.append(entry(3, 15, "y")).has_value());
    CHECK_EQ(log.last_index(), 15U);
  }
}

void test_corrupt_tail_dropped() {
  const TempDir dir;
  const auto path = dir.path + "log.bin";
  std::size_t second_record_start = 0;
  {
    Log log;
    CHECK(log.open(path).has_value());
    // header 8 + term/index 16 + 4-byte payload = 28 bytes per record
    CHECK(log.append(entry(1, 1, "aaaa")).has_value());
    second_record_start = 28;
    CHECK(log.append(entry(1, 2, "bbbb")).has_value());
    CHECK(log.sync().has_value());
  }
  {  // flip a byte inside the payload of the last record
    int fd = ::open(path.c_str(), O_RDWR);
    CHECK(fd >= 0);
    char junk = 0x7F;
    CHECK(::pwrite(fd, &junk, 1,
                   static_cast<off_t>(second_record_start + 8 + 16)) == 1);
    ::close(fd);
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.last_index(), 1U);  // corrupt tail dropped
    // A torn tail record is also dropped.
    CHECK(::truncate(path.c_str(),
                     static_cast<off_t>(second_record_start + 12)) == 0);
  }
  {
    Log log;
    CHECK(log.open(path).has_value());
    CHECK_EQ(log.last_index(), 1U);
    CHECK(log.append(entry(1, 2, "cdef")).has_value());  // can re-append
    CHECK_EQ(log.last_index(), 2U);
  }
}

}  // namespace

auto main() -> int {
  RUN_TEST(test_append_read_reload);
  RUN_TEST(test_persistent_meta);
  RUN_TEST(test_truncate);
  RUN_TEST(test_compact_and_adopt);
  RUN_TEST(test_corrupt_tail_dropped);
  return bgraft_test::summary("log");
}
