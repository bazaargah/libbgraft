#include "loop.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <utility>

namespace bg::raft::detail {

Loop::Loop() : base_(event_base_new()) {
  if (::pipe(fds_.data()) != 0) {
    fds_[0] = -1;
    fds_[1] = -1;
    return;
  }
  for (const int fd : fds_) {
    const auto flags = static_cast<unsigned>(::fcntl(fd, F_GETFL));
    ::fcntl(fd, F_SETFL, static_cast<int>(flags | O_NONBLOCK));
  }
  wake_ = event_new(base_, fds_[0], EV_READ | EV_PERSIST, &Loop::on_wake, this);
  event_add(wake_, nullptr);
}

Loop::~Loop() {
  quit();
  if (wake_ != nullptr) {
    event_del(wake_);
    event_free(wake_);
  }
  event_base_free(base_);
  if (fds_[0] >= 0) {
    ::close(fds_[0]);
  }
  if (fds_[1] >= 0) {
    ::close(fds_[1]);
  }
}

auto Loop::spawn() -> void {
  thread_ = std::thread([this] -> void {
    thread_id_.store(std::this_thread::get_id());
    event_base_dispatch(base_);
  });
}

auto Loop::quit() -> void {
  const bool was_quitting = quit_.exchange(true);
  if (thread_.joinable()) {
    if (!was_quitting) {
      post([this] -> void { event_base_loopbreak(base_); });
    }
    thread_.join();
  }
}

auto Loop::post(std::function<void()> task) -> void {
  {
    std::scoped_lock lock(mutex_);
    queue_.push_back(std::move(task));
  }
  const char ping = 'x';
  (void)!::write(fds_[1], &ping, 1);  // EAGAIN is fine: a wake is pending
}

auto Loop::drain() -> void {
  std::vector<std::function<void()>> tasks;
  do {
    tasks.clear();
    {
      std::scoped_lock lock(mutex_);
      tasks.swap(queue_);
    }
    for (auto& task : tasks) {
      task();
    }
  } while (!tasks.empty() && !quit_.load());
}

void Loop::on_wake(evutil_socket_t, short, void* arg) {
  auto* self = static_cast<Loop*>(arg);
  std::array<char, 64> scratch{};
  while (::read(self->fds_[0], scratch.data(), scratch.size()) > 0) {
  }
  self->drain();
}

Timer::Timer(Loop& loop, std::function<void()> callback, bool repeating)
    : loop_(loop), callback_(std::move(callback)), repeating_(repeating) {
  event_ = event_new(loop_.base(), -1, repeating_ ? EV_PERSIST : 0,
                     &Timer::on_fire, this);
}

Timer::~Timer() {
  cancel();
  event_free(event_);
}

auto Timer::arm(std::chrono::milliseconds delay) -> void {
  cancel();
  interval_ = delay;
  timeval tv{
      .tv_sec = static_cast<time_t>(delay.count() / 1000),
      .tv_usec = static_cast<suseconds_t>(delay.count() % 1000) * 1000,
  };
  event_add(event_, &tv);
  armed_ = true;
}

auto Timer::cancel() -> void {
  if (armed_) {
    event_del(event_);
    armed_ = false;
  }
}

void Timer::on_fire(evutil_socket_t, short, void* arg) {
  auto* self = static_cast<Timer*>(arg);
  if (!self->repeating_) {
    self->armed_ = false;
  }
  self->callback_();
}

}  // namespace bg::raft::detail
