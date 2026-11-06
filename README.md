# libbgraft

A compact Raft consensus library for embedded, append-only database
workloads. Pure C++23 on top of libevent, with a single public header,
error values instead of exceptions, and optional mutual TLS.

libbgraft is part of **Bazaargah**, a distributed software system — the
`bg` prefix and `bg::` namespace identify that family.

**License:** MIT · **Status:** pre-1.0 · POSIX, static cluster membership.

## What it gives you

- `bg::raft::Node` — one object per Raft node; run several in one process
  or one per machine.
- You implement a `bg::raft::StateMachine` (apply, snapshot, restore);
  the library replicates log entries, elects a leader and keeps a
  crash-safe on-disk log.
- Submit commands to the leader; your completion fires once the entry is
  committed by a quorum.

## Requirements

| Tool | Version |
| --- | --- |
| CMake | 3.30+ |
| C++23 compiler + stdlib | Apple Clang 21+ / LLVM 19+ / GCC 14+ |
| libevent | 2+ (built against OpenSSL if you want TLS) |
| OpenSSL | 3+ (only with TLS enabled) |

## Build

```sh
cmake --preset relwithdebinfo   # or: debug (ASan+UBSan), release
cmake --build --preset relwithdebinfo
ctest --preset relwithdebinfo
```

A runnable three-node cluster is in `examples/` — after building, run
`./build/relwithdebinfo/examples/demo`.

## Usage

```cpp
#include <bg/raft/raft.h>

class MySm final : public bg::raft::StateMachine {
  auto on_apply(const bg::raft::LogEntry& e) -> void override { /* ... */ }
  auto take_snapshot(int fd) -> Result<void> override { /* write to fd */ }
  auto restore_snapshot(std::string_view path, const SnapshotInfo& info,
                        bool out_of_band) -> Result<void> override;
};

MySm sm;
bg::raft::NodeConfig cfg{
    .node_id = 1, .cluster_id = 0xC0FFEE,
    .listen = {.host = "127.0.0.1", .port = 7901},
    .peers = {{1, {"127.0.0.1", 7901}}, {2, {"127.0.0.1", 7902}},
              {3, {"127.0.0.1", 7903}}},
    .log_path = "/var/lib/mydb/raft.log",
    .snapshot_dir = "/var/lib/mydb/snaps",
};

bg::raft::Node node(cfg, sm);
if (auto r = node.start(); !r) { /* handle bg::raft::Error */ }

// Append a command; the completion fires once the entry is committed.
node.submit(payload_bytes, [](Result<CommitInfo> info) { /* ... */ });
auto blocking = node.submit(payload_bytes);   // Result<CommitInfo>

node.make_snapshot();      // explicit snapshot at the last applied index
node.shutdown();
```

All fallible calls return `std::expected<T, bg::raft::Error>` — no
exceptions. Members are configured up front: every node knows every other
node's endpoint, and `peers` must include the node itself.

Query the node at any time with `role()`, `current_term()`,
`leader_id()` and `last_applied()`.

### Logging

Set `BG_RAFT_LOG` to `debug`, `info`, `warn` (default) or `error`. One
line per event on stderr; leave it unset to stay quiet.

### Transport security

Point the node at PEM material to enable mutual TLS for all peer traffic
— a certificate and key signed by one cluster CA, plus the CA itself:

```cpp
cfg.tls = bg::raft::TlsConfig{
    .cert_path = "/etc/mydb/tls/node1.crt",
    .key_path = "/etc/mydb/tls/node1.key",
    .ca_path = "/etc/mydb/tls/ca.crt",
};
```

Peers without a certificate from that CA never reach the protocol. TLS
1.2 or newer, on both accepted and dialled connections. With `tls` empty
the transport stays plain TCP — fine on a private network.

One-off CA and node certificates with the `openssl` CLI:

```sh
openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt \
    -days 365 -subj /CN=my-cluster-ca
openssl req -new -newkey rsa:2048 -nodes -keyout node1.key \
    -out node1.csr -subj /CN=node1
openssl x509 -req -in node1.csr -CA ca.crt -CAkey ca.key \
    -CAcreateserial -days 365 -out node1.crt
```

Use a dedicated CA per cluster, and keep `cluster_id` identical across
its nodes.

### Security notes

- TLS protects the wire only. Entries are stored unencrypted in
  `log_path`/`snapshot_dir`; if you need confidentiality at rest, encrypt
  entry payloads in your application — the library treats them as opaque
  bytes.
- State files are created owner-only (`0600`).

## Consuming the library

`cmake --install` ships a CMake package:

```cmake
find_package(bgraft 0.1 CONFIG REQUIRED)
target_link_libraries(myapp PRIVATE BG::bgraft)
```

The package locates libevent and OpenSSL for you
(`-DCMAKE_PREFIX_PATH=...` helps when they live off the default search
paths). The library is static; define `BG_RAFT_STATIC` when compiling
against it.

## Repository layout

```
include/bg/raft/     public API (single header: raft.h)
src/                 implementation
tests/               test suites (run via ctest)
examples/            runnable three-node demo
```

## Contributing

Issues and pull requests are welcome; for anything beyond a typo, open an
issue first. Contributions are licensed under MIT.

## License

libbgraft is released under the MIT License. See
[LICENSE](LICENSE) for the full text. Report vulnerabilities per
[SECURITY.md](SECURITY.md).
