#pragma once

// Minimal assertion harness — the test suite deliberately carries no
// third-party dependency, and prints through std::print like the library.

#include <print>

namespace bgraft_test {

inline auto failures() -> int& {
  static int count = 0;
  return count;
}

#define CHECK(cond)                                              \
  do {                                                           \
    if (!(cond)) {                                               \
      std::print("FAIL {}:{}: {}\n", __FILE__, __LINE__, #cond); \
      ++bgraft_test::failures();                                 \
    }                                                            \
  } while (false)

#define CHECK_EQ(lhs, rhs)                                                     \
  do {                                                                         \
    const auto check_lhs = (lhs);                                              \
    const auto check_rhs = (rhs);                                              \
    if (!(check_lhs == check_rhs)) {                                           \
      std::print("FAIL {}:{}: {} ({}) == {} ({})\n", __FILE__, __LINE__, #lhs, \
                 check_lhs, #rhs, check_rhs);                                  \
      ++bgraft_test::failures();                                               \
    }                                                                          \
  } while (false)

#define RUN_TEST(fn)            \
  do {                          \
    std::print("== {}\n", #fn); \
    fn();                       \
  } while (false)

inline auto summary(const char* suite) -> int {
  if (failures() == 0) {
    std::println("PASS {}", suite);
    return 0;
  }
  std::println("FAILED {}: {} check(s)", suite, failures());
  return 1;
}

}  // namespace bgraft_test
