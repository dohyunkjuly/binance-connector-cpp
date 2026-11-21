#include "websocket.hpp"

#include <binance/version.hpp>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <simdjson.h>

#include <algorithm>
#include <deque>
#include <random>
#include <tuple>

namespace binance::detail {

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
using tcp = asio::ip::tcp;
using namespace std::chrono_literals;

namespace {

constexpr auto kAwait = asio::as_tuple(asio::use_awaitable);

using WsSocket = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

Error to_error(const boost::system::error_code& ec, std::string_view what) {
    const bool timeout = ec == beast::error::timeout || ec == asio::error::timed_out;
    return Error{timeout ? ErrorCode::Timeout : ErrorCode::Network,
                 std::string(what) + ": " + ec.message()};
}

std::string describe_close(const boost::system::error_code& ec, const WsSocket& ws) {
    if (ec == websocket::error::closed) {
        const auto& r = ws.reason();
        std::string s = "closed by server (code " + std::to_string(r.code);
        if (!r.reason.empty()) s += ", " + std::string(r.reason.data(), r.reason.size());
        return s + ")";
    }
    if (ec == beast::error::timeout) return "connection silent (idle timeout)";
    return ec.message();
}

std::string_view first_key(std::string_view m) {
    std::size_t i = 0;
    auto skip_ws = [&] {
        while (i < m.size() && (m[i] == ' ' || m[i] == '\n' || m[i] == '\r' || m[i] == '\t')) ++i;
    };
    skip_ws();
    if (i >= m.size() || m[i] != '{') return {};
    ++i;
    skip_ws();
    if (i >= m.size() || m[i] != '"') return {};
    ++i;
    const auto end = m.find('"', i);
    if (end == std::string_view::npos) return {};
    return m.substr(i, end - i);
}

bool valid_stream_name(std::string_view s) {
    if (s.empty() || s.size() > 128) return false;
    return std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
               c == '@' || c == '_' || c == '.' || c == '-' || c == '!';
    });
}

std::chrono::milliseconds jittered(std::chrono::milliseconds d) {
    thread_local std::minstd_rand rng{std::random_device{}()};
    std::uniform_int_distribution<std::int64_t> dist(d.count() / 2, d.count());
    return std::chrono::milliseconds{dist(rng)};
}

simdjson::ondemand::parser& json_parser() {
    thread_local simdjson::ondemand::parser parser;
    return parser;
}

asio::awaitable<Result<void>> open_ws(WsSocket& ws, const WsEndpoint& ep, const WsOptions& opt,
                                      const bool& stopped) {
    auto ex = co_await asio::this_coro::executor;
    const auto aborted = [&]() -> Result<void> {
        return Error{ErrorCode::Cancelled, "stopped while connecting"};
    };

    tcp::resolver resolver(ex);
    auto [rec, endpoints] = co_await resolver.async_resolve(ep.host, ep.port, kAwait);
    if (stopped) co_return aborted();
    if (rec) co_return to_error(rec, "resolve " + ep.host);

    auto& tcp_stream = beast::get_lowest_layer(ws);
    tcp_stream.expires_after(opt.connect_timeout);
    auto [cec, connected] = co_await tcp_stream.async_connect(endpoints, kAwait);
    if (stopped) co_return aborted();
    if (cec) co_return to_error(cec, "connect " + ep.host);

    if (!SSL_set_tlsext_host_name(ws.next_layer().native_handle(), ep.host.c_str())) {
        co_return Error{ErrorCode::Network, "failed to set TLS SNI for " + ep.host};
    }
    ws.next_layer().set_verify_callback(ssl::host_name_verification(ep.host));
    auto [hec] = co_await ws.next_layer().async_handshake(ssl::stream_base::client, kAwait);
    if (stopped) co_return aborted();
    if (hec) co_return to_error(hec, "TLS handshake with " + ep.host);

    // From here on the WebSocket layer handles timeouts (handshake, idle + keep-alive pings).
    tcp_stream.expires_never();
    websocket::stream_base::timeout to{};
    to.handshake_timeout = opt.connect_timeout;
    to.idle_timeout = opt.idle_timeout;
    to.keep_alive_pings = true;
    ws.set_option(to);
    ws.set_option(websocket::stream_base::decorator([](websocket::request_type& req) {
        req.set(beast::http::field::user_agent, "binance-connector-cpp/" + std::string(version()));
    }));
    ws.read_message_max(64 * 1024 * 1024);

    const std::string host_header = ep.port == "443" ? ep.host : ep.host + ":" + ep.port;
    auto [wec] = co_await ws.async_handshake(host_header, ep.target, kAwait);
    if (stopped) co_return aborted();
    if (wec) co_return to_error(wec, "WebSocket handshake with " + ep.host + ep.target);
    ws.text(true);
    co_return Result<void>{};
}

}  // namespace

struct WsLink {
    WsLink(asio::any_io_executor ex, ssl::context& tls) : ws(ex, tls), wake(ex), lifetime(ex) {}

    WsSocket ws;
    beast::flat_buffer buffer;
    asio::steady_timer wake;      ///< cancelled to wake the writer
    asio::steady_timer lifetime;  ///< max_lifetime watchdog
    std::deque<std::string> outbox;
    std::chrono::steady_clock::time_point opened_at;
    bool open = false;     ///< handshake complete
    bool closing = false;  ///< close requested; writer sends the close frame
    bool planned = false;  ///< closed on purpose: reconnect without backoff
    bool dead = false;     ///< reader finished

    void send(std::string msg) {
        outbox.push_back(std::move(msg));
        wake.cancel();
    }

    /// Graceful close if open, otherwise abort whatever handshake is in progress.
    void close(bool is_planned) {
        if (dead) return;
        planned = planned || is_planned;
        if (!open) {
            beast::get_lowest_layer(ws).cancel();
            return;
        }
        closing = true;
        wake.cancel();
    }

    /// Next message, with kMessagePadding readable bytes after it. Valid until consume().
    std::pair<std::string_view, std::size_t> message() {
        buffer.prepare(kMessagePadding);
        const auto data = buffer.data();
        return {std::string_view{static_cast<const char*>(data.data()), data.size()},
                data.size() + kMessagePadding};
    }
    void consume() { buffer.consume(buffer.size()); }

    void finish() {
        dead = true;
        wake.cancel();
        lifetime.cancel();
    }
};

namespace {

/// One write at a time (Beast allows one outstanding write), at most one per `spacing`.
asio::awaitable<void> link_writer(std::shared_ptr<WsLink> link, std::chrono::milliseconds spacing) {
    asio::steady_timer pace(link->ws.get_executor());
    for (;;) {
        if (link->dead) co_return;
        if (link->closing) {
            co_await link->ws.async_close(websocket::close_code::normal, kAwait);
            co_return;  // the reader sees the close and finishes
        }
        if (link->outbox.empty()) {
            link->wake.expires_at(asio::steady_timer::time_point::max());
            co_await link->wake.async_wait(kAwait);
            continue;
        }
        std::string msg = std::move(link->outbox.front());
        link->outbox.pop_front();
        auto [ec, n] = co_await link->ws.async_write(asio::buffer(msg), kAwait);
        if (ec) {
            beast::get_lowest_layer(link->ws).cancel();  // make sure the reader notices
            co_return;
        }
        if (spacing.count() > 0) {
            pace.expires_after(spacing);
            co_await pace.async_wait(kAwait);
        }
    }
}

asio::awaitable<void> link_lifetime(std::shared_ptr<WsLink> link, std::chrono::milliseconds max) {
    link->lifetime.expires_after(max);
    auto [ec] = co_await link->lifetime.async_wait(kAwait);
    if (!ec && !link->dead) link->close(true);
}

}  // namespace

std::shared_ptr<WsStream> WsStream::create(asio::any_io_executor ex, ssl::context& tls,
                                           WsEndpoint endpoint, WsOptions options,
                                           MessageHandler on_message, WsStateHandler on_state) {
    return std::shared_ptr<WsStream>(new WsStream(std::move(ex), tls, std::move(endpoint),
                                                  std::move(options), std::move(on_message),
                                                  std::move(on_state)));
}

WsStream::WsStream(asio::any_io_executor ex, ssl::context& tls, WsEndpoint endpoint,
                   WsOptions options, MessageHandler on_message, WsStateHandler on_state)
    : strand_(asio::make_strand(ex)),
      tls_(tls),
      endpoint_(std::move(endpoint)),
      options_(std::move(options)),
      on_message_(std::move(on_message)),
      on_state_(std::move(on_state)),
      backoff_timer_(strand_) {}

void WsStream::start() {
    asio::post(strand_, [self = shared_from_this()] {
        if (self->started_ || self->stopped_) return;
        self->started_ = true;
        asio::co_spawn(self->strand_, run(self), asio::detached);
    });
}

void WsStream::stop() {
    asio::post(strand_, [self = shared_from_this()] {
        if (self->stopped_) return;
        self->stopped_ = true;
        self->backoff_timer_.cancel();
        if (self->link_) {
            self->link_->close(true);
        } else if (!self->started_) {
            // run() never started: finish here.
            for (auto& q : self->queued_) {
                if (q.done) q.done(Error{ErrorCode::Cancelled, "stream stopped"});
            }
            self->queued_.clear();
            self->set_state(WsState::Stopped, "stopped");
        }
    });
}

void WsStream::subscribe(std::vector<std::string> streams, DoneHandler done) {
    asio::post(strand_, [self = shared_from_this(), streams = std::move(streams),
                         done = std::move(done)]() mutable {
        auto fail = [&](Error e) {
            if (done) done(std::move(e));
        };
        if (self->stopped_) return fail({ErrorCode::Cancelled, "stream stopped"});

        std::vector<std::string> fresh;
        for (auto& s : streams) {
            if (!valid_stream_name(s)) {
                return fail({ErrorCode::InvalidArgument, "invalid stream name '" + s + "'"});
            }
            if (!self->streams_.contains(s) &&
                std::find(fresh.begin(), fresh.end(), s) == fresh.end()) {
                fresh.push_back(std::move(s));
            }
        }
        if (self->streams_.size() + fresh.size() > self->options_.max_streams) {
            return fail(
                {ErrorCode::InvalidArgument, "too many streams on one connection (max " +
                                                 std::to_string(self->options_.max_streams) + ")"});
        }

        if (fresh.empty()) {
            // Already wanted. Done now if confirmed, otherwise when the pending ack arrives.
            for (auto& [id, p] : self->pending_acks_) {
                if (p.subscribe && !p.initial) {
                    for (auto& name : streams) {
                        if (std::find(p.streams.begin(), p.streams.end(), name) !=
                            p.streams.end()) {
                            auto prev = std::move(p.done);
                            p.done = [prev = std::move(prev),
                                      done = std::move(done)](Result<void> r) {
                                if (prev) prev(r);
                                if (done) done(std::move(r));
                            };
                            return;
                        }
                    }
                }
            }
            if (done) done(Result<void>{});
            return;
        }

        self->streams_.insert(fresh.begin(), fresh.end());
        self->stream_count_ = self->streams_.size();
        if (self->link_ && self->link_->open && !self->link_->closing) {
            self->send_control(true, std::move(fresh), std::move(done), false);
        } else {
            self->queued_.push_back({std::move(fresh), std::move(done)});
        }
    });
}

void WsStream::unsubscribe(std::vector<std::string> streams, DoneHandler done) {
    asio::post(strand_, [self = shared_from_this(), streams = std::move(streams),
                         done = std::move(done)]() mutable {
        std::vector<std::string> removed;
        for (auto& s : streams) {
            if (self->streams_.erase(s)) {
                self->confirmed_.erase(s);
                removed.push_back(std::move(s));
            }
        }
        for (auto& q : self->queued_) {
            std::erase_if(q.streams, [&](const std::string& s) {
                return std::find(removed.begin(), removed.end(), s) != removed.end();
            });
        }
        self->stream_count_ = self->streams_.size();

        if (!removed.empty() && self->link_ && self->link_->open && !self->link_->closing) {
            self->send_control(false, std::move(removed), std::move(done), false);
        } else if (done) {
            done(Result<void>{});
        }
    });
}

void WsStream::set_target(std::string target) {
    asio::post(strand_, [self = shared_from_this(), target = std::move(target)]() mutable {
        self->endpoint_.target = std::move(target);
        self->close_current(true);
    });
}

void WsStream::reconnect() {
    asio::post(strand_, [self = shared_from_this()] { self->close_current(true); });
}

void WsStream::send(std::string text) {
    asio::post(strand_, [self = shared_from_this(), text = std::move(text)]() mutable {
        if (self->link_ && self->state_ == WsState::Open) self->link_->send(std::move(text));
    });
}

void WsStream::close_current(bool planned) {
    if (link_) {
        link_->close(planned);
    } else {
        backoff_timer_.cancel();  // reconnect now instead of after the backoff
    }
}

void WsStream::set_state(WsState s, const std::string& reason) {
    if (s == state_ && s != WsState::Open) return;
    state_ = s;
    if (on_state_) on_state_(s, reason);
}

void WsStream::send_control(bool subscribe, std::vector<std::string> streams, DoneHandler done,
                            bool initial) {
    const std::uint64_t id = next_id_++;
    std::string msg =
        subscribe ? R"({"method":"SUBSCRIBE","params":[)" : R"({"method":"UNSUBSCRIBE","params":[)";
    for (std::size_t i = 0; i < streams.size(); ++i) {
        if (i) msg += ',';
        msg += '"';
        msg += streams[i];  // validated: no characters needing escapes
        msg += '"';
    }
    msg += R"(],"id":)" + std::to_string(id) + "}";
    pending_acks_[id] = PendingAck{std::move(streams), subscribe, initial, std::move(done)};
    link_->send(std::move(msg));
}

void WsStream::on_connected() {
    // Restore what the server had confirmed before, in one message. Open once it is acked.
    if (!confirmed_.empty()) {
        send_control(true, {confirmed_.begin(), confirmed_.end()}, {}, true);
    } else {
        set_state(WsState::Open, "");
    }
    // New subscriptions made while disconnected go separately, so a bad name cannot spoil the
    // restore of the confirmed ones.
    auto queued = std::move(queued_);
    queued_.clear();
    for (auto& q : queued) {
        if (q.streams.empty()) {
            if (q.done) q.done(Result<void>{});
        } else {
            send_control(true, std::move(q.streams), std::move(q.done), false);
        }
    }
}

void WsStream::on_disconnected() {
    // Unanswered requests: subscribes are retried on the next connection, unsubscribes are
    // already effective (the next connection does not resubscribe them).
    for (auto& [id, p] : pending_acks_) {
        if (p.initial) continue;
        if (p.subscribe) {
            std::erase_if(p.streams, [&](const std::string& s) { return !streams_.contains(s); });
            queued_.push_back({std::move(p.streams), std::move(p.done)});
        } else if (p.done) {
            p.done(Result<void>{});
        }
    }
    pending_acks_.clear();
}

void WsStream::handle_ack(std::uint64_t id, Result<void> result) {
    auto it = pending_acks_.find(id);
    if (it == pending_acks_.end()) return;
    PendingAck p = std::move(it->second);
    pending_acks_.erase(it);

    if (p.subscribe) {
        if (result) {
            for (auto& s : p.streams) {
                if (streams_.contains(s)) confirmed_.insert(s);
            }
        } else if (!p.initial) {
            // Rejected: forget these so they are not retried on every reconnect.
            for (auto& s : p.streams) {
                if (!confirmed_.contains(s)) streams_.erase(s);
            }
            stream_count_ = streams_.size();
        }
    }
    if (p.initial) {
        set_state(WsState::Open,
                  result ? "" : "restoring subscriptions failed: " + result.error().message);
    }
    if (p.done) p.done(std::move(result));
}

void WsStream::handle_message(std::string_view msg, std::size_t capacity) {
    const auto key = first_key(msg);
    if (options_.raw || (key != "result" && key != "id" && key != "error")) {
        if (on_message_) {
            try {
                on_message_(msg);
            } catch (...) {
                // A throwing handler must not kill the connection.
            }
        }
        return;
    }

    // Control response: {"result":null,"id":1} or {"error":{"code":2,"msg":"..."},"id":1}
    simdjson::ondemand::document doc;
    if (json_parser().iterate(msg.data(), msg.size(), capacity).get(doc)) return;
    std::uint64_t id = 0;
    if (doc["id"].get_uint64().get(id)) return;
    simdjson::ondemand::object err;
    if (!doc["error"].get_object().get(err)) {
        std::int64_t code = 0;
        std::string_view text;
        std::ignore = err["code"].get_int64().get(code);
        std::ignore = err["msg"].get_string().get(text);
        handle_ack(id, Error{ErrorCode::Exchange, std::string(text), static_cast<int>(code)});
    } else {
        handle_ack(id, Result<void>{});
    }
}

asio::awaitable<void> WsStream::run(std::shared_ptr<WsStream> self) {
    auto backoff = self->options_.backoff_initial;
    const auto spacing =
        self->options_.max_messages_per_second > 0
            ? std::chrono::milliseconds{1000 / self->options_.max_messages_per_second}
            : 0ms;

    while (!self->stopped_) {
        auto link = std::make_shared<WsLink>(self->strand_, self->tls_);
        self->link_ = link;

        auto opened = co_await open_ws(link->ws, self->endpoint_, self->options_, self->stopped_);
        if (self->stopped_) break;
        std::string reason;
        bool planned = link->planned;
        if (opened) {
            ++self->connections_opened_;
            link->open = true;
            link->opened_at = std::chrono::steady_clock::now();
            asio::co_spawn(self->strand_, link_writer(link, spacing), asio::detached);
            asio::co_spawn(self->strand_, link_lifetime(link, self->options_.max_lifetime),
                           asio::detached);
            self->on_connected();

            for (;;) {
                auto [ec, n] = co_await link->ws.async_read(link->buffer, kAwait);
                if (ec) {
                    reason = describe_close(ec, link->ws);
                    break;
                }
                if (self->stopped_) break;
                auto [msg, capacity] = link->message();
                self->handle_message(msg, capacity);
                link->consume();
            }
            planned = link->planned;
            if (planned) reason = "reconnecting (planned)";
            const bool healthy =
                std::chrono::steady_clock::now() - link->opened_at >= self->options_.healthy_after;
            if (healthy) backoff = self->options_.backoff_initial;
            planned = planned || healthy;
        } else {
            reason = opened.error().message;
        }

        link->finish();
        self->link_.reset();
        self->on_disconnected();
        if (self->stopped_) break;
        self->set_state(WsState::Connecting, reason);

        if (!planned) {
            self->backoff_timer_.expires_after(jittered(backoff));
            co_await self->backoff_timer_.async_wait(kAwait);
            backoff = std::min(backoff * 2, self->options_.backoff_max);
        }
    }

    if (self->link_) {
        self->link_->finish();
        self->link_.reset();
    }
    self->on_disconnected();
    for (auto& q : self->queued_) {
        if (q.done) q.done(Error{ErrorCode::Cancelled, "stream stopped"});
    }
    self->queued_.clear();
    self->set_state(WsState::Stopped, "stopped");
}

struct WsApi::Session : WsLink {
    using WsLink::WsLink;
    bool draining = false;  ///< replaced by a newer session; close once pending is empty
    std::unordered_map<std::uint64_t, std::shared_ptr<Pending>> pending;
};

struct WsApi::Pending {
    explicit Pending(asio::any_io_executor ex) : timer(ex) {}
    asio::steady_timer timer;  ///< expires = timeout; cancelled = result is set
    std::optional<Result<WsApiResponse>> result;
};

std::shared_ptr<WsApi> WsApi::create(asio::any_io_executor ex, ssl::context& tls,
                                     WsEndpoint endpoint, WsOptions options, RateLimiter& limiter,
                                     std::string limit_key,
                                     std::chrono::milliseconds request_timeout,
                                     WsStateHandler on_state) {
    return std::shared_ptr<WsApi>(new WsApi(std::move(ex), tls, std::move(endpoint),
                                            std::move(options), limiter, std::move(limit_key),
                                            request_timeout, std::move(on_state)));
}

WsApi::WsApi(asio::any_io_executor ex, ssl::context& tls, WsEndpoint endpoint, WsOptions options,
             RateLimiter& limiter, std::string limit_key, std::chrono::milliseconds request_timeout,
             WsStateHandler on_state)
    : strand_(asio::make_strand(ex)),
      tls_(tls),
      endpoint_(std::move(endpoint)),
      options_(std::move(options)),
      limiter_(limiter),
      limit_key_(std::move(limit_key)),
      request_timeout_(request_timeout),
      on_state_(std::move(on_state)),
      maintain_timer_(strand_) {}

void WsApi::start() {
    asio::post(strand_, [self = shared_from_this()] {
        if (self->started_ || self->stopped_) return;
        self->started_ = true;
        asio::co_spawn(self->strand_, maintain(self), asio::detached);
    });
}

void WsApi::stop() {
    asio::post(strand_, [self = shared_from_this()] {
        if (self->stopped_) return;
        self->stopped_ = true;
        self->open_ = false;
        self->maintain_timer_.cancel();
        if (self->connecting_) self->connecting_->close(true);
        for (auto& s : self->live_) s->close(true);
        if (!self->started_) self->set_state(WsState::Stopped, "stopped");
    });
}

void WsApi::set_state(WsState st, const std::string& reason) {
    if (st == state_) return;
    state_ = st;
    if (on_state_) on_state_(st, reason);
}

void WsApi::request(WsApiCall call, Callback cb) {
    asio::co_spawn(strand_, run_with_callback(shared_from_this(), std::move(call), std::move(cb)),
                   asio::detached);
}

asio::awaitable<void> WsApi::run_with_callback(std::shared_ptr<WsApi> self, WsApiCall call,
                                               Callback cb) {
    auto r = co_await guarded(self, std::move(call));
    if (cb) cb(std::move(*r));
}

asio::awaitable<std::optional<Result<WsApiResponse>>> WsApi::guarded(std::shared_ptr<WsApi> self,
                                                                     WsApiCall call) {
    try {
        co_return co_await do_request(std::move(self), std::move(call));
    } catch (const std::exception& e) {
        co_return Result<WsApiResponse>{Error{ErrorCode::Internal, e.what()}};
    }
}

asio::awaitable<Result<WsApiResponse>> WsApi::co_request(WsApiCall call) {
    // Runs on our strand, resumes the caller on its executor. optional<>: co_spawn needs a
    // default-constructible result type.
    auto r = co_await asio::co_spawn(strand_, guarded(shared_from_this(), std::move(call)),
                                     asio::use_awaitable);
    co_return std::move(*r);
}

asio::awaitable<Result<WsApiResponse>> WsApi::do_request(std::shared_ptr<WsApi> self,
                                                         WsApiCall call) {
    auto not_connected = [&]() -> Result<WsApiResponse> {
        if (self->stopped_) return Error{ErrorCode::Cancelled, "WebSocket API stopped"};
        return Error{ErrorCode::Network, "WebSocket API not connected"};
    };
    if (!self->current_) co_return not_connected();

    for (;;) {
        auto wait =
            self->limiter_.reserve(self->limit_key_, call.weight, call.is_order,
                                   std::chrono::system_clock::now(), self->request_timeout_);
        if (!wait) co_return wait.error();
        if (wait->count() == 0) break;
        asio::steady_timer timer(self->strand_, *wait);
        co_await timer.async_wait(kAwait);
        if (!self->current_) co_return not_connected();
    }

    auto s = self->current_;
    const std::uint64_t id = self->next_id_++;
    std::string msg = R"({"id":)" + std::to_string(id) + R"(,"method":")" + call.method + '"';
    if (!call.params_json.empty()) msg += R"(,"params":)" + call.params_json;
    msg += '}';

    auto pending = std::make_shared<Pending>(self->strand_);
    pending->timer.expires_after(self->request_timeout_);
    s->pending.emplace(id, pending);
    s->send(std::move(msg));

    co_await pending->timer.async_wait(kAwait);
    if (pending->result) co_return std::move(*pending->result);

    s->pending.erase(id);
    self->close_if_drained(*s);
    co_return Error{ErrorCode::Timeout, "no response to " + call.method + " within " +
                                            std::to_string(self->request_timeout_.count()) +
                                            " ms; outcome unknown"};
}

void WsApi::close_if_drained(Session& s) {
    if (s.draining && s.pending.empty()) s.close(true);
}

void WsApi::handle_message(Session& s, std::string_view msg, std::size_t capacity) {
    // {"id":1,"status":200,"result":{...},"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT",
    //  "interval":"MINUTE","intervalNum":1,"limit":2400,"count":5}]}
    simdjson::ondemand::document doc;
    if (json_parser().iterate(msg.data(), msg.size(), capacity).get(doc)) return;
    std::uint64_t id = 0;
    if (doc["id"].get_uint64().get(id)) return;
    auto it = s.pending.find(id);
    if (it == s.pending.end()) return;  // answer to a request that already timed out
    std::int64_t status = 0;
    std::ignore = doc["status"].get_int64().get(status);

    // Feed rate-limit usage back to the limiter in the X-MBX header format it understands.
    std::vector<std::pair<std::string, std::string>> headers;
    simdjson::ondemand::array limits;
    if (!doc["rateLimits"].get_array().get(limits)) {
        for (auto item : limits) {
            simdjson::ondemand::object o;
            if (item.get_object().get(o)) continue;
            std::string_view type, interval;
            std::int64_t num = 0, count = 0;
            if (o["rateLimitType"].get_string().get(type) ||
                o["interval"].get_string().get(interval) || o["intervalNum"].get_int64().get(num) ||
                o["count"].get_int64().get(count)) {
                continue;
            }
            if (count < 0) continue;  // futures order calls report weight as limit/count -1
            const char unit = interval == "SECOND"   ? 's'
                              : interval == "MINUTE" ? 'm'
                              : interval == "HOUR"   ? 'h'
                              : interval == "DAY"    ? 'd'
                                                     : '\0';
            if (!unit) continue;
            std::string name = type == "ORDERS" ? "x-mbx-order-count-" : "x-mbx-used-weight-";
            if (type != "ORDERS" && type != "REQUEST_WEIGHT") continue;
            headers.emplace_back(name + std::to_string(num) + unit, std::to_string(count));
        }
    }
    if (status == 429 || status == 418) {
        // error.data.retryAfter is an epoch-ms timestamp.
        std::uint64_t retry_at = 0;
        if (!doc.find_field_unordered("error")["data"]["retryAfter"].get_uint64().get(retry_at)) {
            const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                    std::chrono::system_clock::now().time_since_epoch())
                                    .count();
            const auto secs = (static_cast<std::int64_t>(retry_at) - now_ms + 999) / 1000;
            headers.emplace_back("retry-after", std::to_string(std::max<std::int64_t>(1, secs)));
        }
    }
    limiter_.on_response(limit_key_, static_cast<unsigned>(status), headers,
                         std::chrono::system_clock::now());

    auto pending = std::move(it->second);
    s.pending.erase(it);
    if (status == 429) {
        pending->result = Error{ErrorCode::RateLimited, "WebSocket API 429: " + std::string(msg)};
    } else if (status == 418) {
        pending->result = Error{ErrorCode::IpBanned, "WebSocket API 418: " + std::string(msg)};
    } else {
        pending->result = WsApiResponse{static_cast<int>(status), std::string(msg)};
    }
    pending->timer.cancel();
    close_if_drained(s);
}

asio::awaitable<void> WsApi::read_loop(std::shared_ptr<WsApi> self, std::shared_ptr<Session> s) {
    std::string reason;
    for (;;) {
        auto [ec, n] = co_await s->ws.async_read(s->buffer, kAwait);
        if (ec) {
            reason = describe_close(ec, s->ws);
            break;
        }
        auto [msg, capacity] = s->message();
        self->handle_message(*s, msg, capacity);
        s->consume();
    }

    s->finish();
    for (auto& [id, p] : s->pending) {
        p->result = self->stopped_
                        ? Error{ErrorCode::Cancelled, "WebSocket API stopped"}
                        : Error{ErrorCode::Timeout, "connection lost before the response (" +
                                                        reason + "); outcome unknown"};
        p->timer.cancel();
    }
    s->pending.clear();
    std::erase(self->live_, s);

    if (self->current_ == s) {
        self->current_.reset();
        self->open_ = false;
        if (!self->stopped_) self->set_state(WsState::Connecting, reason);
        self->maintain_timer_.cancel();  // reconnect
    }
}

asio::awaitable<void> WsApi::maintain(std::shared_ptr<WsApi> self) {
    auto backoff = self->options_.backoff_initial;
    auto wait = [&](std::chrono::milliseconds d) -> asio::awaitable<void> {
        self->maintain_timer_.expires_after(d);
        co_await self->maintain_timer_.async_wait(kAwait);
    };

    while (!self->stopped_) {
        auto s = std::make_shared<Session>(self->strand_, self->tls_);
        self->connecting_ = s;
        auto opened = co_await open_ws(s->ws, self->endpoint_, self->options_, self->stopped_);
        self->connecting_.reset();
        if (self->stopped_) break;

        if (!opened) {
            // During a rotation the old session keeps serving; just try again later.
            if (!self->current_) self->set_state(WsState::Connecting, opened.error().message);
            co_await wait(jittered(backoff));
            backoff = std::min(backoff * 2, self->options_.backoff_max);
            continue;
        }

        ++self->connections_opened_;
        s->open = true;
        s->opened_at = std::chrono::steady_clock::now();
        asio::co_spawn(self->strand_, link_writer(s, 0ms), asio::detached);
        asio::co_spawn(self->strand_, read_loop(self, s), asio::detached);
        self->live_.push_back(s);

        // Make before break: new requests go to the new session; the old one finishes the
        // requests it already sent, then closes.
        if (auto old = std::exchange(self->current_, s)) {
            old->draining = true;
            self->close_if_drained(*old);
        }
        self->open_ = true;
        self->set_state(WsState::Open, "");

        co_await wait(self->options_.max_lifetime);  // cancelled early if the session is lost
        if (self->stopped_) break;
        if (self->current_ != s) {
            const bool healthy =
                std::chrono::steady_clock::now() - s->opened_at >= self->options_.healthy_after;
            if (healthy) {
                backoff = self->options_.backoff_initial;
            } else {
                co_await wait(jittered(backoff));
                backoff = std::min(backoff * 2, self->options_.backoff_max);
            }
        }
        // else: lifetime reached; loop opens the replacement while s keeps serving.
    }

    self->open_ = false;
    for (auto& s : self->live_) s->close(true);
    self->set_state(WsState::Stopped, "stopped");
}

}  // namespace binance::detail
