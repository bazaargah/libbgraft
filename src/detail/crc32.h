#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>

#if defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

// CRC-32 (IEEE 802.3, reflected) for write-ahead log records. Register
// convention matches zlib's crc32(): complement the seed going in,
// complement the state coming out. On AArch64 with the CRC extension the
// hardware instructions take the raw state directly, so both paths agree
// bit-for-bit for any seed; the bytewise table is the portable fallback.

namespace bg::raft::detail {

constexpr auto crc32_table() -> std::array<std::uint32_t, 256> {
  std::array<std::uint32_t, 256> table{};
  for (std::uint32_t i = 0; i < 256; ++i) {
    auto crc = i;
    for (int k = 0; k < 8; ++k) {
      crc = (crc & 1U) ? (0xEDB88320U ^ (crc >> 1U)) : (crc >> 1U);
    }
    table[i] = crc;
  }
  return table;
}

inline auto crc32_bytewise(std::span<const std::byte> data, std::uint32_t seed)
    -> std::uint32_t {
  static constexpr auto kTable = crc32_table();
  auto crc = ~seed;
  for (const auto byte : data) {
    crc =
        kTable[(crc ^ static_cast<unsigned char>(byte)) & 0xFFU] ^ (crc >> 8U);
  }
  return ~crc;
}

#if defined(__ARM_FEATURE_CRC32)

template <typename Word>
inline auto load_le(std::span<const std::byte> bytes) -> Word {
  auto word = Word{};
  std::memcpy(&word, bytes.data(), sizeof(Word));
  if constexpr (std::endian::native == std::endian::big) {
    word = std::byteswap(word);
  }
  return word;
}

inline auto crc32_hardware(std::span<const std::byte> data, std::uint32_t seed)
    -> std::uint32_t {
  auto state = static_cast<unsigned>(~seed);
  auto rest = data;
  for (; rest.size() >= 8U; rest = rest.subspan(8U)) {
    state = __crc32d(state, load_le<std::uint64_t>(rest.first<8>()));
  }
  if (rest.size() >= 4U) {
    state = __crc32w(state, load_le<std::uint32_t>(rest.first<4>()));
    rest = rest.subspan(4U);
  }
  if (rest.size() >= 2U) {
    state = __crc32h(state, load_le<std::uint16_t>(rest.first<2>()));
    rest = rest.subspan(2U);
  }
  for (const auto byte : rest) {
    state = __crc32b(state, static_cast<unsigned char>(byte));
  }
  return ~static_cast<std::uint32_t>(state);
}

#endif  // __ARM_FEATURE_CRC32

inline auto crc32(std::span<const std::byte> data, std::uint32_t seed = 0U)
    -> std::uint32_t {
#if defined(__ARM_FEATURE_CRC32)
  return crc32_hardware(data, seed);
#else
  return crc32_bytewise(data, seed);
#endif
}

}  // namespace bg::raft::detail
