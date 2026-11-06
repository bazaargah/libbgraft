#pragma once

#include <bg/raft/raft.h>
#include <event2/util.h>

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <span>
#include <vector>

#include "loop.h"
#include "protocol.h"

struct bufferevent;
struct evconnlistener;
struct ssl_ctx_st;  // OpenSSL's SSL_CTX, kept out of this header

// Peer-to-peer TCP transport. Symmetric framing: both accepted and dialled
// connections speak the same protocol and hand-shake first, so a single dial
// between any two peers suffices. Reconnect uses capped exponential backoff.
// All methods run on the loop thread; teardown is always deferred via
// post() because bufferevents must not be freed inside their own callbacks.

namespace bg::raft::detail {

class Transport {
 public:
  // src_id is the peer identity learned from its handshake.
  using MessageHandler =
      std::function<void(std::uint64_t src_id, const WireMessage& msg)>;
  // Fires when a peer loses its last live connection, so the Raft layer
  // can clear per-peer in-flight state that would otherwise never get an
  // acknowledgement.
  using PeerDownHandler = std::function<void(std::uint64_t peer_id)>;

  explicit Transport(Loop& loop);
  ~Transport();

  Transport(const Transport&) = delete;
  auto operator=(const Transport&) -> Transport& = delete;

  // Enable mutual TLS for every connection (accepted and dialled). Must be
  // called before listen()/add_peer(). Loads the node certificate, private
  // key and CA bundle; peers presenting no or foreign certificates are
  // rejected inside the handshake, before any protocol bytes flow.
  auto set_tls(const TlsConfig& tls) -> Result<void>;

  auto set_handler(MessageHandler handler) -> void;
  auto set_peer_down(PeerDownHandler handler) -> void;

  // Bind the listener; returns the actually-bound endpoint (port 0 resolved).
  auto listen(const Endpoint& endpoint, std::uint64_t cluster_id,
              std::uint64_t self_id) -> Result<Endpoint>;

  // Register a persistent peer and start dialling it.
  auto add_peer(const PeerConfig& peer) -> void;

  // Returns false when no live connection exists and the frame was dropped
  // (fire-and-forget otherwise; Raft timers drive retries).
  auto send(std::uint64_t peer_id, std::span<const std::byte> frame) -> bool;

 private:
  struct Conn {
    Transport* owner{};
    std::uint64_t peer_id{~0ULL};  // valid once handshaked
    bufferevent* bev{nullptr};
    evutil_socket_t fd{-1};
    FrameReader reader;
    bool handshaked{};
    bool outgoing{};
    bool dead{};
  };

  struct Dial {
    PeerConfig peer;
    std::uint64_t backoff_ms{250};
    std::unique_ptr<Timer> timer;
  };

  static void on_read(struct bufferevent*, void* arg);
  static void on_event(struct bufferevent*, short events, void* arg);
  static void on_accept(struct evconnlistener*, evutil_socket_t fd,
                        struct sockaddr*, int, void* arg);

  auto attach(evutil_socket_t fd, bool outgoing) -> Conn*;
  // Feeds the connection's evbuffer; drops the conn on protocol violation.
  auto handle_readable(Conn* conn) -> void;
  auto drop(Conn* conn, const char* why) -> void;  // deferred close
  auto schedule_dial(std::uint64_t peer_id) -> void;
  auto dial_now(const PeerConfig& peer) -> void;
  auto find_conn(std::uint64_t peer_id) -> Conn*;
  auto is_known_peer(std::uint64_t id) const -> bool;

  Loop& loop_;
  struct event_base* base_;
  ssl_ctx_st* tls_ctx_{nullptr};  // owned; non-null once set_tls succeeds
  std::uint64_t cluster_id_{};
  std::uint64_t self_id_{};
  evconnlistener* listener_{nullptr};
  MessageHandler handler_;
  PeerDownHandler peer_down_;
  std::map<std::uint64_t, PeerConfig> peers_;
  std::map<std::uint64_t, Dial> dials_;
  std::map<evutil_socket_t, std::unique_ptr<Conn>> conns_;  // by fd
};

}  // namespace bg::raft::detail
