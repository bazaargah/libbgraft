#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

// Big-endian fixed-width encode/decode. Callers must guarantee span sizes;
// the bounds-checked path lives in the Cursor of protocol.h.

namespace bg::raft::detail {

inline auto be_read_u16(std::span<const std::byte> b) -> std::uint16_t {
  std::uint16_t v{};
  std::memcpy(&v, b.data(), sizeof(v));
  return std::byteswap(v);
}

inline auto be_read_u32(std::span<const std::byte> b) -> std::uint32_t {
  std::uint32_t v{};
  std::memcpy(&v, b.data(), sizeof(v));
  return std::byteswap(v);
}

inline auto be_read_u64(std::span<const std::byte> b) -> std::uint64_t {
  std::uint64_t v{};
  std::memcpy(&v, b.data(), sizeof(v));
  return std::byteswap(v);
}

inline auto be_write(std::vector<std::byte>& out, std::uint16_t v) -> void {
  const auto n = std::byteswap(v);
  const auto* p = reinterpret_cast<const std::byte*>(&n);
  out.insert(out.end(), p, p + sizeof(v));
}

inline auto be_write(std::vector<std::byte>& out, std::uint32_t v) -> void {
  const auto n = std::byteswap(v);
  const auto* p = reinterpret_cast<const std::byte*>(&n);
  out.insert(out.end(), p, p + sizeof(v));
}

inline auto be_write(std::vector<std::byte>& out, std::uint64_t v) -> void {
  const auto n = std::byteswap(v);
  const auto* p = reinterpret_cast<const std::byte*>(&n);
  out.insert(out.end(), p, p + sizeof(v));
}

inline auto be_write(std::vector<std::byte>& out, std::uint8_t v) -> void {
  out.push_back(static_cast<std::byte>(v));
}

// Overwrite an already-reserved big-endian u32 slot (CRC placeholders).
inline auto be_place(std::byte* at, std::uint32_t v) -> void {
  const auto n = std::byteswap(v);
  std::memcpy(at, &n, sizeof(n));
}

}  // namespace bg::raft::detail
