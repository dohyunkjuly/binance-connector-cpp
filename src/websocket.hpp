#pragma once

// WebSocket transport: WsStream (subscription streams) and WsApi (request/response).
// Each object runs on its own strand (callbacks serialized, on an IO thread);
// public methods may be called from any thread.

#include "http.hpp"

#include <binance/types.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace binance::detail {

/// Callback messages have this many readable bytes past their end, so simdjson can parse in place.
inline constexpr std::size_t kMessagePadding = 64;

struct WsEndpoint {
    std::string host;
    std::string port = "443";
    std::string target;
};

struct WsOptions {
    std::chrono::milliseconds connect_timeout{10'000};  ///< TCP + TLS + WS handshake
    /// No frame at all for half of this -> we ping; for all of it -> connection is dead.
    std::chrono::milliseconds idle_timeout{30'000};
    /// Binance drops connections at 24h; reconnect on our own terms before that.
    std::chrono::milliseconds max_lifetime{std::chrono::hours{23}};
    std::chrono::milliseconds backoff_initial{500};
    std::chrono::milliseconds backoff_max{30'000};
    /// A connection that lived at least this long reconnects at once instead of backing off.
    std::chrono::milliseconds healthy_after{10'000};
    /// WsStream only. Binance: futures 10/s, spot 5/s, including pongs. 0 = unlimited.
    int max_messages_per_second = 4;
    /// Streams per connection, WsStream only (Binance: futures 200, spot 1024).
    std::size_t max_streams = 200;
    /// WsStream only: no SUBSCRIBE handling; all messages go to on_message, caller uses send().
    bool raw = false;
};

enum class WsState {
    Connecting,  ///< also while reconnecting after a lost connection
    Open,        ///< WsStream: once earlier subscriptions are restored
    Stopped,     ///< stop() called; last state callback
};

using WsStateHandler = std::function<void(WsState, const std::string& reason)>;

struct WsLink;

class WsStream : public std::enable_shared_from_this<WsStream> {
public:
    using MessageHandler = std::function<void(std::string_view msg)>;  ///< see kMessagePadding
    using DoneHandler = std::function<void(Result<void>)>;

    static std::shared_ptr<WsStream> create(asio::any_io_executor ex, ssl::context& tls,
                                            WsEndpoint endpoint, WsOptions options,
                                            MessageHandler on_message, WsStateHandler on_state);

    void start();
    /// Close and stop reconnecting. Fires WsState::Stopped once; no callbacks after that.
    void stop();

    /// done fires once the server confirms (after reconnecting if down). Subscriptions persist
    /// across reconnects. Exceeding max_streams -> InvalidArgument.
    void subscribe(std::vector<std::string> streams, DoneHandler done = {});
    void unsubscribe(std::vector<std::string> streams, DoneHandler done = {});

    /// Reconnects with the new target (e.g. a new listenKey).
    void set_target(std::string target);
    void reconnect();
    /// Dropped if not connected. For raw streams.
    void send(std::string text);

    std::size_t stream_count() const noexcept { return stream_count_; }
    std::uint64_t connections_opened() const noexcept { return connections_opened_; }

private:
    WsStream(asio::any_io_executor ex, ssl::context& tls, WsEndpoint endpoint, WsOptions options,
             MessageHandler on_message, WsStateHandler on_state);

    struct Queued {
        std::vector<std::string> streams;
        DoneHandler done;
    };
    struct PendingAck {
        std::vector<std::string> streams;
        bool subscribe = false;
        bool initial = false;  ///< the resubscribe after connecting; gates WsState::Open
        DoneHandler done;
    };

    static asio::awaitable<void> run(std::shared_ptr<WsStream> self);
    void on_connected();
    void on_disconnected();
    void handle_message(std::string_view msg, std::size_t capacity);
    void handle_ack(std::uint64_t id, Result<void> result);
    void send_control(bool subscribe, std::vector<std::string> streams, DoneHandler done,
                      bool initial);
    void close_current(bool planned);
    void set_state(WsState s, const std::string& reason);

    // Everything below is touched only on strand_.
    asio::strand<asio::any_io_executor> strand_;
    ssl::context& tls_;
    WsEndpoint endpoint_;
    WsOptions options_;
    MessageHandler on_message_;
    WsStateHandler on_state_;

    bool started_ = false;
    bool stopped_ = false;
    WsState state_ = WsState::Connecting;
    std::shared_ptr<WsLink> link_;
    asio::steady_timer backoff_timer_;

    std::set<std::string> streams_;    ///< everything wanted
    std::set<std::string> confirmed_;  ///< acked by the server at least once
    std::vector<Queued> queued_;       ///< subscribe() calls waiting for a connection
    std::unordered_map<std::uint64_t, PendingAck> pending_acks_;
    std::uint64_t next_id_ = 1;

    std::atomic<std::size_t> stream_count_{0};
    std::atomic<std::uint64_t> connections_opened_{0};
};

struct WsApiResponse {
    int status = 0;    ///< HTTP-like status Binance puts in every response
    std::string body;  ///< the whole response message
};

struct WsApiCall {
    std::string method;
    std::string params_json;  ///< JSON object, signed if needed; empty = no params
    int weight = 1;
    bool is_order = false;
};

class WsApi : public std::enable_shared_from_this<WsApi> {
public:
    using Callback = std::function<void(Result<WsApiResponse>)>;

    /// Requests reserve capacity in limiter under limit_key; response rateLimits feed back.
    static std::shared_ptr<WsApi> create(asio::any_io_executor ex, ssl::context& tls,
                                         WsEndpoint endpoint, WsOptions options,
                                         RateLimiter& limiter, std::string limit_key,
                                         std::chrono::milliseconds request_timeout,
                                         WsStateHandler on_state);

    void start();
    void stop();

    bool is_open() const noexcept { return open_; }

    /// Send a request; cb runs on an IO thread. Error semantics matter for orders:
    ///   Network   - NOT sent (not connected). Safe to retry over REST.
    ///   Timeout   - sent, but no answer (timed out or connection lost). Outcome unknown:
    ///               query the order before retrying.
    ///   RateLimited / IpBanned - refused before sending, or HTTP-like 429 / 418 from Binance.
    ///   Cancelled - stop() was called.
    /// Any other status Binance answers with (including 4xx) is returned as a response.
    void request(WsApiCall call, Callback cb);
    /// Coroutine form of request(); may be awaited from any executor.
    asio::awaitable<Result<WsApiResponse>> co_request(WsApiCall call);

    std::uint64_t connections_opened() const noexcept { return connections_opened_; }

    struct Session;
    struct Pending;

private:
    WsApi(asio::any_io_executor ex, ssl::context& tls, WsEndpoint endpoint, WsOptions options,
          RateLimiter& limiter, std::string limit_key, std::chrono::milliseconds request_timeout,
          WsStateHandler on_state);

    static asio::awaitable<void> maintain(std::shared_ptr<WsApi> self);
    static asio::awaitable<void> read_loop(std::shared_ptr<WsApi> self, std::shared_ptr<Session> s);
    static asio::awaitable<Result<WsApiResponse>> do_request(std::shared_ptr<WsApi> self,
                                                             WsApiCall call);
    static asio::awaitable<std::optional<Result<WsApiResponse>>> guarded(
        std::shared_ptr<WsApi> self, WsApiCall call);
    static asio::awaitable<void> run_with_callback(std::shared_ptr<WsApi> self, WsApiCall call,
                                                   Callback cb);
    void handle_message(Session& s, std::string_view msg, std::size_t capacity);
    void close_if_drained(Session& s);
    void set_state(WsState st, const std::string& reason);

    // Everything below is touched only on strand_.
    asio::strand<asio::any_io_executor> strand_;
    ssl::context& tls_;
    WsEndpoint endpoint_;
    WsOptions options_;
    RateLimiter& limiter_;
    std::string limit_key_;
    std::chrono::milliseconds request_timeout_;
    WsStateHandler on_state_;

    bool started_ = false;
    bool stopped_ = false;
    WsState state_ = WsState::Connecting;
    std::shared_ptr<Session> current_;            ///< receives new requests
    std::shared_ptr<Session> connecting_;         ///< being opened (initial, reconnect or rotation)
    std::vector<std::shared_ptr<Session>> live_;  ///< current + draining sessions
    asio::steady_timer maintain_timer_;  ///< wakes maintain(): lifetime, lost connection, stop
    std::uint64_t next_id_ = 1;

    std::atomic<bool> open_{false};
    std::atomic<std::uint64_t> connections_opened_{0};
};

}  // namespace binance::detail
