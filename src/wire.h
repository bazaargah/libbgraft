#pragma once

// libbgraft-internal aliases for libbgwire (see DESIGN.md CR-4/D-23): the
// CRC-32, big-endian helpers, and the bounds-checked Cursor now live in the
// shared ecosystem library. The using-declarations keep the historical
// unqualified spelling at all call sites inside bg::raft::detail.

#include <bg/wire/wire.h>

namespace bg::raft::detail {

using bg::wire::be_place;
using bg::wire::be_read_u16;
using bg::wire::be_read_u32;
using bg::wire::be_read_u64;
using bg::wire::be_write;
using bg::wire::crc32;
using bg::wire::Cursor;

}  // namespace bg::raft::detail
