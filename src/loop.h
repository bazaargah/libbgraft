#pragma once

#include <bg/raft/raft.h>
#include <event2/event.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

struct event;
struct event_base;

// One libevent event_base plus a cross-thread task queue woken through a
// self-pipe (thread-safe with plain POSIX writes). All Raft logic runs on
// the loop thread; anything outside must go through post(). Timers are
// loop-thread objects.

namespace bg::raft::detail {

class Loop {
 public:
  Loop();
  ~Loop();

  Loop(const Loop&) = delete;
  auto operator=(const Loop&) -> Loop& = delete;

  [[nodiscard]] auto base() const -> struct event_base* { return base_; }

  auto spawn() -> void;
  auto quit() -> void;                            // thread-safe
  auto post(std::function<void()> task) -> void;  // thread-safe

  [[nodiscard]] auto on_loop_thread() const -> bool {
    return std::this_thread::get_id() == thread_id_.load();
  }

 private:
  static void on_wake(evutil_socket_t fd, short what, void* arg);
  auto drain() -> void;

  struct event_base* base_;
  struct event* wake_{nullptr};
  std::array<int, 2> fds_{-1, -1};
  std::thread thread_;
  std::atomic<std::thread::id> thread_id_;
  std::mutex mutex_;
  std::vector<std::function<void()>> queue_;
  std::atomic<bool> quit_{false};
};

class Timer {
 public:
  Timer(Loop& loop, std::function<void()> callback, bool repeating = false);
  ~Timer();

  Timer(const Timer&) = delete;
  auto operator=(const Timer&) -> Timer& = delete;

  // Loop thread only. Cancel is safe on an unarmed timer.
  auto arm(std::chrono::milliseconds delay) -> void;
  auto cancel() -> void;
  [[nodiscard]] auto armed() const -> bool { return armed_; }

 private:
  static void on_fire(evutil_socket_t fd, short what, void* arg);

  Loop& loop_;
  struct event* event_;
  std::function<void()> callback_;
  bool repeating_;
  std::chrono::milliseconds interval_{0};
  bool armed_{};
};

}  // namespace bg::raft::detail
