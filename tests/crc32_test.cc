#include "detail/crc32.h"

#include <cstddef>
#include <cstdint>
#include <random>
#include <span>
#include <string_view>
#include <vector>

#include "harness/check.h"

using namespace bg::raft::detail;

namespace {

auto to_bytes(std::string_view text) -> std::vector<std::byte> {
  std::vector<std::byte> out(text.size());
  for (std::size_t i = 0; i < text.size(); ++i) {
    out[i] = static_cast<std::byte>(static_cast<unsigned char>(text[i]));
  }
  return out;
}

auto make_blob(std::mt19937_64& rng, std::size_t size)
    -> std::vector<std::byte> {
  std::vector<std::byte> out(size);
  for (auto& byte : out) {
    byte = static_cast<std::byte>(static_cast<unsigned char>(rng() & 0xFFU));
  }
  return out;
}

// CRC-32/ISO-HDLC check value: the standard "123456789" vector.
auto test_known_answer() -> void {
  const auto data = to_bytes("123456789");
  CHECK_EQ(crc32(std::span<const std::byte>(data)), 0xCBF43926U);
}

// Seeding with the running result must chain across record boundaries, the
// way log.cc stitches header and body CRCs together.
auto test_chaining() -> void {
  // Deterministic on purpose: fixed seeds make failures reproducible.
  std::mt19937_64 rng(0xC0FFEEULL);  // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
  for (int round = 0; round < 64; ++round) {
    const auto head = make_blob(rng, static_cast<std::size_t>(rng() % 17U));
    const auto tail = make_blob(rng, static_cast<std::size_t>(rng() % 65U));
    std::vector<std::byte> whole;
    whole.insert(whole.end(), head.begin(), head.end());
    whole.insert(whole.end(), tail.begin(), tail.end());

    const auto first = crc32(std::span<const std::byte>(head));
    const auto chained = crc32(std::span<const std::byte>(tail), first);
    const auto direct = crc32(std::span<const std::byte>(whole));
    CHECK_EQ(chained, direct);
  }
}

// The on-disk format is defined by the bytewise reference; the hardware
// path must agree for every record length and tail size it handles.
auto test_backends_agree() -> void {
#if defined(__ARM_FEATURE_CRC32)
  std::mt19937_64 rng(42ULL);  // NOLINT(bugprone-random-generator-seed,cert-msc32-c,cert-msc51-cpp)
  for (std::size_t size = 0; size <= 260; ++size) {
    const auto data = make_blob(rng, size);
    const auto seed = static_cast<std::uint32_t>(rng());
    CHECK_EQ(crc32(std::span<const std::byte>(data), seed),
             crc32_bytewise(std::span<const std::byte>(data), seed));
  }
#endif
}

}  // namespace

auto main() -> int {
  RUN_TEST(test_known_answer);
  RUN_TEST(test_chaining);
  RUN_TEST(test_backends_agree);
  return bgraft_test::summary("crc32");
}
