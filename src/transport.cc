#include "transport.h"

#include <event2/bufferevent.h>
#include <event2/bufferevent_ssl.h>
#include <event2/listener.h>
#include <netdb.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <sys/socket.h>

#include <array>
#include <cstring>
#include <string>
#include <utility>

#include "diag.h"

namespace bg::raft::detail {

Transport::Transport(Loop& loop) : loop_(loop), base_(loop.base()) {}

Transport::~Transport() {
  if (listener_ != nullptr) {
    evconnlistener_free(listener_);
  }
  for (auto& [fd, conn] : conns_) {
    bufferevent_free(conn->bev);
    conn->bev = nullptr;
  }
  conns_.clear();
  if (tls_ctx_ != nullptr) {
    SSL_CTX_free(tls_ctx_);
  }
}

auto Transport::set_handler(MessageHandler handler) -> void {
  handler_ = std::move(handler);
}

auto Transport::set_peer_down(PeerDownHandler handler) -> void {
  peer_down_ = std::move(handler);
}

auto Transport::set_tls(const TlsConfig& tls) -> Result<void> {
  if (tls_ctx_ != nullptr) {
    return std::unexpected(Error::invalid_config);  // already initialised
  }
  SSL_CTX* ctx = SSL_CTX_new(TLS_method());
  if (ctx == nullptr) {
    return std::unexpected(Error::io_failure);
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_options(ctx, SSL_OP_NO_RENEGOTIATION);
  if (SSL_CTX_use_certificate_file(ctx, tls.cert_path.c_str(),
                                   SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_use_PrivateKey_file(ctx, tls.key_path.c_str(),
                                  SSL_FILETYPE_PEM) != 1 ||
      SSL_CTX_check_private_key(ctx) != 1 ||
      SSL_CTX_load_verify_locations(ctx, tls.ca_path.c_str(), nullptr) != 1) {
    logf(LogLevel::error, "TLS initialisation failed: {}",
         ERR_reason_error_string(ERR_peek_last_error()));
    SSL_CTX_free(ctx);
    return std::unexpected(Error::invalid_config);
  }
  // Enforce a certificate from our CA in both directions: peers presenting
  // nothing, or a foreign certificate, are cut off in the handshake.
  SSL_CTX_set_verify(ctx,
                     SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
                     nullptr);
  tls_ctx_ = ctx;
  logf(LogLevel::info, "TLS enabled (peer verification against cluster CA)");
  return {};
}

auto Transport::is_known_peer(std::uint64_t id) const -> bool {
  return peers_.contains(id);
}

auto Transport::listen(const Endpoint& endpoint, std::uint64_t cluster_id,
                       std::uint64_t self_id) -> Result<Endpoint> {
  cluster_id_ = cluster_id;
  self_id_ = self_id;

  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE | AI_NUMERICSERV;
  addrinfo* res = nullptr;
  const auto service = std::to_string(endpoint.port);
  if (getaddrinfo(endpoint.host.c_str(), service.c_str(), &hints, &res) != 0) {
    return std::unexpected(Error::invalid_config);
  }
  listener_ =
      evconnlistener_new_bind(base_, &Transport::on_accept, this,
                              LEV_OPT_CLOSE_ON_FREE | LEV_OPT_REUSEABLE, 128,
                              res->ai_addr, static_cast<int>(res->ai_addrlen));
  freeaddrinfo(res);
  if (listener_ == nullptr) {
    logf(LogLevel::error, "cannot bind {}:{}", endpoint.host, endpoint.port);
    return std::unexpected(Error::io_failure);
  }

  Endpoint bound;
  bound.host = endpoint.host;
  bound.port = endpoint.port;
  if (endpoint.port == 0U) {
    sockaddr_storage ss{};
    socklen_t len = sizeof(ss);
    if (getsockname(evconnlistener_get_fd(listener_),
                    reinterpret_cast<sockaddr*>(&ss), &len) == 0) {
      bound.port = ntohs(reinterpret_cast<sockaddr_in*>(&ss)->sin_port);
    }
  }
  return bound;
}

auto Transport::add_peer(const PeerConfig& peer) -> void {
  if (peer.id == self_id_) {
    return;  // never dial ourselves
  }
  peers_[peer.id] = peer;
  dial_now(peer);
}

void Transport::on_accept(struct evconnlistener*, evutil_socket_t fd,
                          struct sockaddr*, int, void* arg) {
  auto* self = static_cast<Transport*>(arg);
  auto* conn = self->attach(fd, false);
  if (conn == nullptr) {
    evutil_closesocket(fd);
    return;
  }
  auto frame = encode_frame(MsgType::handshake, 0U, 0U,
                            build_handshake(self->cluster_id_, self->self_id_));
  bufferevent_write(conn->bev, frame.data(), frame.size());
}

auto Transport::attach(evutil_socket_t fd, bool outgoing) -> Conn* {
  auto conn = std::make_unique<Conn>();
  conn->owner = this;
  conn->fd = fd;
  conn->outgoing = outgoing;
#ifdef SO_NOSIGPIPE
  // A TLS peer that rejects the handshake closes while writes are queued;
  // without this, send() would raise SIGPIPE and kill the process.
  const int nosigpipe = 1;
  ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif
  if (tls_ctx_ != nullptr) {
    SSL* ssl = SSL_new(tls_ctx_);
    if (ssl == nullptr) {
      return nullptr;
    }
    conn->bev = bufferevent_openssl_socket_new(
        base_, fd, ssl, outgoing ? BUFFEREVENT_SSL_CONNECTING
                                  : BUFFEREVENT_SSL_ACCEPTING,
        BEV_OPT_CLOSE_ON_FREE);
    if (conn->bev == nullptr) {
      SSL_free(ssl);
      return nullptr;
    }
  } else {
    conn->bev = bufferevent_socket_new(base_, fd, BEV_OPT_CLOSE_ON_FREE);
    if (conn->bev == nullptr) {
      return nullptr;
    }
  }
  auto* raw = conn.get();
  conns_[fd] = std::move(conn);
  bufferevent_setcb(raw->bev, &Transport::on_read, nullptr,
                    &Transport::on_event, raw);
  bufferevent_enable(raw->bev, EV_READ | EV_WRITE);
  return raw;
}

auto Transport::dial_now(const PeerConfig& peer) -> void {
  addrinfo hints{};
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;
  addrinfo* res = nullptr;
  const auto service = std::to_string(peer.endpoint.port);
  if (getaddrinfo(peer.endpoint.host.c_str(), service.c_str(), &hints, &res) !=
      0) {
    logf(LogLevel::warn, "cannot resolve peer {} host", peer.id);
    return;
  }
  evutil_socket_t fd = socket(res->ai_family, res->ai_socktype, 0);
  if (fd < 0) {
    freeaddrinfo(res);
    return;
  }
  evutil_make_socket_nonblocking(fd);
  auto* conn = attach(fd, true);
  if (conn == nullptr) {
    evutil_closesocket(fd);
    freeaddrinfo(res);
    return;
  }
  conn->peer_id = peer.id;  // known a priori for outgoing dials
  if (bufferevent_socket_connect(conn->bev, res->ai_addr,
                                 static_cast<int>(res->ai_addrlen)) != 0) {
    freeaddrinfo(res);
    drop(conn, "connect failed");
    return;
  }
  freeaddrinfo(res);
  auto frame = encode_frame(MsgType::handshake, 0U, 0U,
                            build_handshake(cluster_id_, self_id_));
  bufferevent_write(conn->bev, frame.data(), frame.size());
}

auto Transport::find_conn(std::uint64_t peer_id) -> Conn* {
  Conn* fallback = nullptr;
  for (auto& [fd, conn] : conns_) {
    if (conn->dead || !conn->handshaked || conn->peer_id != peer_id) {
      continue;
    }
    if (conn->outgoing) {
      return conn.get();
    }
    fallback = conn.get();
  }
  return fallback;
}

auto Transport::send(std::uint64_t peer_id, std::span<const std::byte> frame)
    -> bool {
  if (frame.empty()) {
    logf(LogLevel::error, "n{} send dropped: empty frame to peer {}", self_id_,
         peer_id);
    return false;
  }
  auto* conn = find_conn(peer_id);
  if (conn == nullptr) {
    logf(LogLevel::debug, "n{} send dropped: no handshaked conn to peer {}",
         self_id_, peer_id);
    return false;
  }
  bufferevent_write(conn->bev, frame.data(), frame.size());
  return true;
}

void Transport::on_read(struct bufferevent*, void* arg) {
  auto* conn = static_cast<Conn*>(arg);
  conn->owner->handle_readable(conn);
}

auto Transport::handle_readable(Conn* conn) -> void {
  auto* input = bufferevent_get_input(conn->bev);
  auto fed =
      conn->reader.feed(input, [&](const WireMessage& msg) -> Result<void> {
        if (!conn->handshaked) {
          if (msg.type != MsgType::handshake) {
            return std::unexpected(Error::protocol_violation);
          }
          const auto& hs = std::get<HandshakeMsg>(msg.body);
          if (hs.cluster_id != cluster_id_ || hs.node_id == self_id_ ||
              !is_known_peer(hs.node_id)) {
            return std::unexpected(Error::protocol_violation);
          }
          if (conn->peer_id != ~0ULL && conn->peer_id != hs.node_id) {
            return std::unexpected(Error::protocol_violation);
          }
          conn->peer_id = hs.node_id;
          conn->handshaked = true;
          logf(LogLevel::debug, "n{} handshake done fd={} peer={} outgoing={}",
               self_id_, conn->fd, conn->peer_id, conn->outgoing);
          return {};
        }
        if (msg.type == MsgType::handshake) {
          return std::unexpected(Error::protocol_violation);
        }
        if (handler_ != nullptr) {
          if (msg.type != MsgType::append_entries ||
              !std::get<AppendEntriesMsg>(msg.body).entries.empty()) {
            logf(LogLevel::debug, "n{} recv peer={} type={} term={}", self_id_,
                 conn->peer_id, static_cast<int>(msg.type), msg.term);
          }
          handler_(conn->peer_id, msg);
        }
        return {};
      });
  if (!fed) {
    drop(conn, "protocol violation");
  }
}

namespace {
// Renders OpenSSL's last error into a stable string. Safe: only the loop
// thread calls this, and the result is logged at once.
auto tls_error_reason() -> const char* {
  static thread_local std::string storage;
  const unsigned long err = ERR_peek_last_error();
  if (err != 0) {
    std::array<char, 256> buf{};
    ERR_error_string_n(err, buf.data(), buf.size());
    storage = std::string("TLS error: ") + buf.data();
  } else {
    storage = "TLS/IO error";
  }
  ERR_clear_error();
  return storage.c_str();
}
}  // namespace

void Transport::on_event(struct bufferevent* bev, short events, void* arg) {
  auto* conn = static_cast<Conn*>(arg);
  // libevent's event flags are plain signed ints; widen once.
  constexpr auto kClose = static_cast<unsigned>(BEV_EVENT_EOF) |
                          static_cast<unsigned>(BEV_EVENT_ERROR);
  const auto mask = static_cast<unsigned>(events);
  if ((mask & BEV_EVENT_CONNECTED) != 0) {
    return;
  }
  if ((mask & kClose) != 0 && !conn->dead) {
    const char* why = "connection closed";
    if ((mask & BEV_EVENT_ERROR) != 0 && conn->owner->tls_ctx_ != nullptr) {
      // Likely a failed TLS handshake (foreign or missing peer certificate);
      // surface OpenSSL's reason string for operators.
      why = tls_error_reason();
    }
    conn->owner->drop(conn, why);
  }
  (void)bev;
}

auto Transport::schedule_dial(std::uint64_t peer_id) -> void {
  auto it = dials_.find(peer_id);
  if (it == dials_.end()) {
    if (!peers_.contains(peer_id)) {
      return;
    }
    it = dials_
             .emplace(peer_id,
                      Dial{
                          .peer = peers_[peer_id],
                          .backoff_ms = 250U,
                          .timer = nullptr,
                      })
             .first;
    it->second.timer = std::make_unique<Timer>(
        loop_, [this, peer_id] -> void { dial_now(peers_[peer_id]); });
  }
  const auto delay = std::min<std::uint64_t>(it->second.backoff_ms, 2000U);
  it->second.backoff_ms = delay * 2U;
  it->second.timer->arm(std::chrono::milliseconds(delay));
}

// Deferred from any callback context; frees the bufferevent on the loop.
auto Transport::drop(Conn* conn, const char* why) -> void {
  if (conn->dead) {
    return;
  }
  conn->dead = true;
  const std::uint64_t peer_id = conn->peer_id;
  const bool outgoing = conn->outgoing;
  const bool was_handshaked = conn->handshaked;
  const evutil_socket_t fd = conn->fd;
  logf(LogLevel::debug, "n{} drop conn fd={} peer={} ({})", self_id_, fd,
       peer_id, why);
  loop_.post([this, fd, peer_id, outgoing, was_handshaked] -> void {
    auto it = conns_.find(fd);
    if (it != conns_.end()) {
      auto* dead = it->second.get();
      if (dead->bev != nullptr) {
        bufferevent_free(dead->bev);
        dead->bev = nullptr;
      }
      conns_.erase(it);
    }
    // Notify only when the peer has no other live connection: the Raft
    // layer must retire in-flight expectations that can never be acked.
    if (was_handshaked && peer_id != ~0ULL && peers_.contains(peer_id)) {
      bool other_alive = false;
      for (const auto& [fd2, conn2] : conns_) {
        if (!conn2->dead && conn2->handshaked && conn2->peer_id == peer_id) {
          other_alive = true;
          break;
        }
      }
      if (!other_alive && peer_down_ != nullptr) {
        peer_down_(peer_id);
      }
    }
    if (outgoing && peer_id != ~0ULL && peers_.contains(peer_id)) {
      schedule_dial(peer_id);
    }
  });
}

}  // namespace bg::raft::detail
