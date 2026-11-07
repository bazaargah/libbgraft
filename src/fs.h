#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <string>
#include <string_view>
#include <utility>

#include <bg/raft/raft.h>

namespace bg::raft::detail {

// Publish `final_path` atomically: `body` fills a fresh temp file (mode
// 0600) with fd-based writes; the file is then fsynced and renamed over
// the target. Any failure removes the temp file and propagates the error,
// leaving the previous contents of `final_path` intact.
template <typename Fn>
auto write_atomically(std::string_view final_path, std::string_view tmp_path,
                      Fn&& body) -> Result<void> {
  const int fd = ::open(std::string(tmp_path).c_str(),
                        O_RDWR | O_CREAT | O_TRUNC, 0600);
  if (fd < 0) {
    return std::unexpected(Error::io_failure);
  }
  auto result = body(fd);
  if (result && ::fsync(fd) != 0) {
    result = std::unexpected(Error::io_failure);
  }
  ::close(fd);
  if (!result) {
    ::unlink(std::string(tmp_path).c_str());
    return result;
  }
  if (::rename(std::string(tmp_path).c_str(),
               std::string(final_path).c_str()) != 0) {
    ::unlink(std::string(tmp_path).c_str());
    return std::unexpected(Error::io_failure);
  }
  return {};
}

}  // namespace bg::raft::detail
