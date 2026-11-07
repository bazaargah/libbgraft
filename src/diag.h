#pragma once

#include <array>
#include <cstdlib>
#include <format>
#include <print>
#include <string_view>

// Tiny level-gated diagnostics over std::print. Enabled with the
// BG_RAFT_LOG env var set to one of: debug, info, warn, error, off
// (default: warn).

namespace bg::raft::detail {

enum class LogLevel : int {
  debug = 0,
  info = 1,
  warn = 2,
  error = 3,
  off = 4,
};

inline auto log_level() -> LogLevel& {
  static LogLevel level = [] -> bg::raft::detail::LogLevel {
    // Read exactly once during first-use initialisation; the library never
    // calls setenv(), so getenv() is stable here despite the check.
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* env = std::getenv("BG_RAFT_LOG");
    const std::string_view name = env != nullptr ? env : "";
    if (name == "debug") return LogLevel::debug;
    if (name == "info") return LogLevel::info;
    if (name == "warn") return LogLevel::warn;
    if (name == "error") return LogLevel::error;
    if (name == "off") return LogLevel::off;
    return LogLevel::warn;
  }();
  return level;
}

template <typename... Args>
auto logf(LogLevel level, std::format_string<Args...> fmt, Args&&... args)
    -> void {
  if (level < log_level()) {
    return;
  }
  static constexpr std::array<std::string_view, 5> kTag = {
      "D", "I", "W", "E", "-",
  };
  std::println(stderr, "[bgraft {}] {}", kTag[static_cast<int>(level)],
               std::format(fmt, std::forward<Args>(args)...));
}

}  // namespace bg::raft::detail
