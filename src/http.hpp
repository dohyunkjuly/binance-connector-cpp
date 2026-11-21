#pragma once

// REST transport: pooled keep-alive HTTPS connections, timeouts, rate limiting.
// Knows nothing about Binance endpoints or signing (that lives in client.cpp).

#include <binance/types.hpp>

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/beast/http/verb.hpp>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

namespace binance::detail {

namespace asio = boost::asio;
namespace ssl = boost::asio::ssl;
namespace http = boost::beast::http;

/// TLS client context that verifies servers against the OS trust store.
ssl::context make_tls_context();

/// Fails on corrupt input or output larger than max_size.
Result<std::string> gunzip(std::string_view in, std::size_t max_size);

/// One Binance limit, e.g. 2400 weight per minute, or 300 orders per 10 seconds.
struct RateLimit {
    std::chrono::milliseconds interval;
    int max = 0;
    bool counts_orders = false;  ///< false = request weight, true = order count
};

/// Tracks usage per host in fixed windows aligned to the clock (as Binance does), corrected by the
/// X-MBX-* response headers. Thread-safe.
class RateLimiter {
public:
    using TimePoint = std::chrono::system_clock::time_point;

    /// safety: use at most this fraction of each limit, leaving room for clock skew.
    explicit RateLimiter(double safety = 0.9) : safety_(safety) {}

    void set_limits(const std::string& host, std::vector<RateLimit> limits);

    /// 0ms = reserved, go now; >0 = nothing reserved, call again after waiting that long;
    /// error = would have to wait longer than max_wait (RateLimited) or IP is banned (IpBanned).
    Result<std::chrono::milliseconds> reserve(const std::string& host, int weight, bool is_order,
                                              TimePoint now, std::chrono::milliseconds max_wait);

    /// Header names must be lower-case.
    void on_response(const std::string& host, unsigned status,
                     const std::vector<std::pair<std::string, std::string>>& headers,
                     TimePoint now);

    /// 0 if the window is not configured.
    int used(const std::string& host, std::chrono::milliseconds interval, bool orders,
             TimePoint now) const;

private:
    struct Window {
        RateLimit limit;
        std::int64_t index = -1;  ///< which clock-aligned window `used` belongs to
        int used = 0;
    };
    struct HostState {
        std::vector<Window> windows;
        TimePoint blocked_until{};
        bool banned = false;
    };

    double safety_;
    mutable std::mutex mu_;
    std::unordered_map<std::string, HostState> hosts_;
};

struct HttpRequest {
    http::verb method = http::verb::get;
    std::string host;
    std::string target;  ///< path + query
    std::string body;    ///< sent as application/x-www-form-urlencoded when non-empty
    std::vector<std::pair<std::string, std::string>> headers;
    int weight = 1;          ///< Binance request weight of this endpoint
    bool is_order = false;   ///< counts towards order-rate limits
    bool idempotent = true;  ///< false = never resend once it may have reached the server
};

struct HttpResponse {
    unsigned status = 0;
    std::string body;
    std::vector<std::pair<std::string, std::string>> headers;  ///< x-mbx-* and retry-after only
};

class HttpClient : public std::enable_shared_from_this<HttpClient> {
public:
    using Callback = std::function<void(Result<HttpResponse>)>;

    static std::shared_ptr<HttpClient> create(asio::any_io_executor ex, ssl::context& tls,
                                              RateLimiter& limiter,
                                              std::chrono::milliseconds timeout);

    /// Send a request; cb runs on an IO thread. HTTP error statuses (4xx/5xx) are returned as
    /// responses for the caller to interpret, except 429 -> RateLimited and 418 -> IpBanned.
    void request(HttpRequest req, Callback cb);

    asio::awaitable<Result<HttpResponse>> co_request(HttpRequest req);

    void close_idle();

    std::uint64_t connections_opened() const noexcept { return connections_opened_; }

    struct Connection;

private:
    HttpClient(asio::any_io_executor ex, ssl::context& tls, RateLimiter& limiter,
               std::chrono::milliseconds timeout);

    static asio::awaitable<void> run_with_callback(std::shared_ptr<HttpClient> self,
                                                   HttpRequest req, Callback cb);
    asio::awaitable<Result<std::unique_ptr<Connection>>> connect(const std::string& host);
    std::unique_ptr<Connection> take_idle(const std::string& host);
    void return_idle(const std::string& host, std::unique_ptr<Connection> conn);

    asio::any_io_executor ex_;
    ssl::context& tls_;
    RateLimiter& limiter_;
    std::chrono::milliseconds timeout_;

    std::mutex pool_mu_;
    std::unordered_map<std::string, std::vector<std::unique_ptr<Connection>>> idle_;
    std::atomic<std::uint64_t> connections_opened_{0};
};

}  // namespace binance::detail
