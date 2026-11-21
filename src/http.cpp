#include "http.hpp"

#include <binance/version.hpp>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/zlib.hpp>

#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstdlib>
#include <filesystem>

#ifdef _WIN32
#include <wincrypt.h>
// wincrypt.h defines macros that collide with OpenSSL names.
#undef X509_NAME
#undef X509_EXTENSIONS
#undef PKCS7_SIGNER_INFO
#undef OCSP_REQUEST
#undef OCSP_RESPONSE
#endif

namespace binance::detail {

namespace beast = boost::beast;
using tcp = asio::ip::tcp;
using namespace std::chrono_literals;

constexpr auto kAwait = asio::as_tuple(asio::use_awaitable);
constexpr std::size_t kMaxBodyBytes = 64 * 1024 * 1024;  // spot exchangeInfo is >10 MB
constexpr auto kMaxIdleAge = 30s;  // don't reuse connections the server may have closed

namespace {

#ifdef _WIN32
// OpenSSL cannot read the Windows certificate store; without this every TLS connection fails.
void add_windows_root_certs(ssl::context& ctx) {
    HCERTSTORE store = CertOpenSystemStoreW(0, L"ROOT");
    if (!store) return;
    X509_STORE* x509_store = SSL_CTX_get_cert_store(ctx.native_handle());
    PCCERT_CONTEXT cert = nullptr;
    while ((cert = CertEnumCertificatesInStore(store, cert)) != nullptr) {
        const unsigned char* p = cert->pbCertEncoded;
        if (X509* x = d2i_X509(nullptr, &p, static_cast<long>(cert->cbCertEncoded))) {
            X509_STORE_add_cert(x509_store, x);
            X509_free(x);
        }
    }
    CertCloseStore(store, 0);
}
#endif

}  // namespace

ssl::context make_tls_context() {
    ssl::context ctx{ssl::context::tls_client};
    ctx.set_options(ssl::context::default_workarounds | ssl::context::no_sslv2 |
                    ssl::context::no_sslv3 | ssl::context::no_tlsv1 | ssl::context::no_tlsv1_1);
    ctx.set_verify_mode(ssl::verify_peer);

    boost::system::error_code ec;
    ctx.set_default_verify_paths(ec);

#ifdef _WIN32
    add_windows_root_certs(ctx);
#else
    // vcpkg's OpenSSL looks for CA certificates in its own, empty, install tree.
    std::vector<std::string> candidates;
    if (const char* env = std::getenv("SSL_CERT_FILE")) candidates.emplace_back(env);
    for (const char* p : {"/etc/ssl/cert.pem",                   // macOS, Alpine
                          "/etc/ssl/certs/ca-certificates.crt",  // Debian, Ubuntu
                          "/etc/pki/tls/certs/ca-bundle.crt",    // RHEL, Fedora
                          "/etc/ssl/ca-bundle.pem"}) {           // openSUSE
        candidates.emplace_back(p);
    }
    for (const auto& path : candidates) {
        if (std::filesystem::exists(path)) {
            ctx.load_verify_file(path, ec);
            if (!ec) break;
        }
    }
#endif
    return ctx;
}

namespace {

std::int64_t epoch_ms(RateLimiter::TimePoint t) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(t.time_since_epoch()).count();
}

/// Binance writes minutes as "1m" (spot) or "1M" (futures), so the unit is case-insensitive.
std::optional<std::chrono::milliseconds> parse_header_interval(std::string_view s) {
    if (s.size() < 2) return std::nullopt;
    std::int64_t n = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size() - 1, n);
    if (ec != std::errc{} || ptr != s.data() + s.size() - 1 || n <= 0) return std::nullopt;
    switch (std::tolower(static_cast<unsigned char>(s.back()))) {
        case 's': return std::chrono::seconds{n};
        case 'm': return std::chrono::minutes{n};
        case 'h': return std::chrono::hours{n};
        case 'd': return std::chrono::days{n};
        default: return std::nullopt;
    }
}

std::optional<int> parse_int(std::string_view s) {
    int v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc{} || ptr != s.data() + s.size()) return std::nullopt;
    return v;
}

}  // namespace

void RateLimiter::set_limits(const std::string& host, std::vector<RateLimit> limits) {
    std::lock_guard lock(mu_);
    auto& h = hosts_[host];
    h.windows.clear();
    for (auto& l : limits) h.windows.push_back(Window{l});
}

Result<std::chrono::milliseconds> RateLimiter::reserve(const std::string& host, int weight,
                                                       bool is_order, TimePoint now,
                                                       std::chrono::milliseconds max_wait) {
    std::lock_guard lock(mu_);
    auto& h = hosts_[host];

    if (now < h.blocked_until) {
        const auto wait = std::chrono::ceil<std::chrono::milliseconds>(h.blocked_until - now);
        if (h.banned) {
            return Error{ErrorCode::IpBanned, "IP banned by Binance for another " +
                                                  std::to_string(wait.count()) + " ms"};
        }
        if (wait > max_wait) {
            return Error{ErrorCode::RateLimited, "rate limited by Binance for another " +
                                                     std::to_string(wait.count()) + " ms"};
        }
        return wait;
    }
    h.banned = false;

    const std::int64_t now_ms = epoch_ms(now);
    std::int64_t wait_ms = 0;
    for (auto& w : h.windows) {
        if (w.limit.counts_orders && !is_order) continue;
        const std::int64_t len = w.limit.interval.count();
        const std::int64_t idx = now_ms / len;
        if (idx != w.index) {
            w.index = idx;
            w.used = 0;
        }
        const int cost = w.limit.counts_orders ? 1 : weight;
        const int cap = std::max(1, static_cast<int>(w.limit.max * safety_));
        // A request heavier than the whole cap may still go when the window is empty.
        if (w.used > 0 && w.used + cost > cap) {
            wait_ms = std::max(wait_ms, (idx + 1) * len - now_ms);
        }
    }

    if (wait_ms > 0) {
        if (std::chrono::milliseconds{wait_ms} > max_wait) {
            return Error{ErrorCode::RateLimited, "local rate limit reached; next window in " +
                                                     std::to_string(wait_ms) + " ms"};
        }
        return std::chrono::milliseconds{wait_ms};
    }

    for (auto& w : h.windows) {
        if (w.limit.counts_orders && !is_order) continue;
        w.used += w.limit.counts_orders ? 1 : weight;
    }
    return std::chrono::milliseconds{0};
}

void RateLimiter::on_response(const std::string& host, unsigned status,
                              const std::vector<std::pair<std::string, std::string>>& headers,
                              TimePoint now) {
    std::lock_guard lock(mu_);
    auto& h = hosts_[host];
    const std::int64_t now_ms = epoch_ms(now);

    std::optional<int> retry_after;
    for (const auto& [name, value] : headers) {
        if (name == "retry-after") {
            retry_after = parse_int(value);
            continue;
        }
        bool orders = false;
        std::string_view suffix;
        constexpr std::string_view kWeight = "x-mbx-used-weight-";
        constexpr std::string_view kOrders = "x-mbx-order-count-";
        if (name.starts_with(kWeight)) {
            suffix = std::string_view{name}.substr(kWeight.size());
        } else if (name.starts_with(kOrders)) {
            suffix = std::string_view{name}.substr(kOrders.size());
            orders = true;
        } else {
            continue;
        }
        const auto interval = parse_header_interval(suffix);
        const auto used = parse_int(value);
        if (!interval || !used) continue;

        for (auto& w : h.windows) {
            if (w.limit.counts_orders != orders || w.limit.interval != *interval) continue;
            const std::int64_t idx = now_ms / w.limit.interval.count();
            if (idx == w.index) {
                w.used = std::max(w.used, *used);  // ours may include requests still in flight
            } else {
                w.index = idx;
                w.used = *used;
            }
        }
    }

    if (status == 429 || status == 418) {
        h.blocked_until = now + std::chrono::seconds{retry_after.value_or(60)};
        h.banned = status == 418;
    }
}

int RateLimiter::used(const std::string& host, std::chrono::milliseconds interval, bool orders,
                      TimePoint now) const {
    std::lock_guard lock(mu_);
    auto it = hosts_.find(host);
    if (it == hosts_.end()) return 0;
    for (const auto& w : it->second.windows) {
        if (w.limit.counts_orders == orders && w.limit.interval == interval) {
            return w.index == epoch_ms(now) / interval.count() ? w.used : 0;
        }
    }
    return 0;
}

struct HttpClient::Connection {
    Connection(asio::any_io_executor ex, ssl::context& tls) : stream(ex, tls) {}
    beast::ssl_stream<beast::tcp_stream> stream;
    beast::flat_buffer buffer;
    std::chrono::steady_clock::time_point idle_since;
};

namespace {

Error to_error(const boost::system::error_code& ec, std::string_view what) {
    const bool timeout = ec == beast::error::timeout || ec == asio::error::timed_out;
    return Error{timeout ? ErrorCode::Timeout : ErrorCode::Network,
                 std::string(what) + ": " + ec.message()};
}

bool is_interesting_header(std::string_view lower_name) {
    return lower_name.starts_with("x-mbx-") || lower_name == "retry-after";
}

}  // namespace

Result<std::string> gunzip(std::string_view in, std::size_t max_size) {
    // Header (RFC 1952): magic 1f 8b, method 8 (deflate), flags, mtime, xfl, os, optional fields.
    auto bad = [](std::string_view why) -> Result<std::string> {
        return Error{ErrorCode::Network, "corrupt gzip body: " + std::string(why)};
    };
    const auto* p = reinterpret_cast<const unsigned char*>(in.data());
    const std::size_t n = in.size();
    if (n < 18 || p[0] != 0x1f || p[1] != 0x8b || p[2] != 8) return bad("header");
    const unsigned flags = p[3];
    std::size_t pos = 10;
    if (flags & 0x04) {  // FEXTRA
        if (pos + 2 > n) return bad("extra");
        pos += 2 + (std::size_t{p[pos]} | (std::size_t{p[pos + 1]} << 8));
    }
    for (unsigned flag : {0x08u, 0x10u}) {  // FNAME, FCOMMENT: zero-terminated
        if (flags & flag) {
            while (pos < n && p[pos] != 0) ++pos;
            ++pos;
        }
    }
    if (flags & 0x02) pos += 2;  // FHCRC
    if (pos + 8 > n) return bad("truncated");

    // Trailer: CRC32, then uncompressed size mod 2^32. TLS already guarantees integrity, so the
    // CRC is not checked; the size is a good first guess for the output buffer.
    const std::size_t isize = std::size_t{p[n - 4]} | (std::size_t{p[n - 3]} << 8) |
                              (std::size_t{p[n - 2]} << 16) | (std::size_t{p[n - 1]} << 24);
    std::string out;
    out.resize(std::min(std::max(isize, (n - pos) * 4), max_size));

    beast::zlib::inflate_stream inflater;  // raw deflate, 32 KB window
    beast::zlib::z_params zs;
    zs.next_in = p + pos;
    // The 8 trailer bytes are passed too: Beast's inflater may need to read a little past the
    // final deflate block before it reports the end of the stream (seen on small bodies).
    zs.avail_in = n - pos;
    std::size_t produced = 0;
    for (;;) {
        zs.next_out = out.data() + produced;
        zs.avail_out = out.size() - produced;
        const std::size_t in_before = zs.avail_in;
        const std::size_t out_before = produced;
        boost::system::error_code ec;
        inflater.write(zs, beast::zlib::Flush::sync, ec);
        produced = out.size() - zs.avail_out;
        if (ec == beast::zlib::error::end_of_stream) break;
        if (ec && ec != beast::zlib::error::need_buffers) return bad(ec.message());
        if (zs.avail_out == 0) {
            if (out.size() >= max_size) return bad("larger than the body limit");
            out.resize(std::min(out.size() * 2, max_size));
        } else if (zs.avail_in == in_before && produced == out_before) {
            return bad("truncated");
        }
    }
    if ((produced & 0xFFFFFFFFu) != isize) return bad("size does not match the trailer");
    out.resize(produced);
    return out;
}

std::shared_ptr<HttpClient> HttpClient::create(asio::any_io_executor ex, ssl::context& tls,
                                               RateLimiter& limiter,
                                               std::chrono::milliseconds timeout) {
    return std::shared_ptr<HttpClient>(new HttpClient(std::move(ex), tls, limiter, timeout));
}

HttpClient::HttpClient(asio::any_io_executor ex, ssl::context& tls, RateLimiter& limiter,
                       std::chrono::milliseconds timeout)
    : ex_(std::move(ex)), tls_(tls), limiter_(limiter), timeout_(timeout) {}

void HttpClient::request(HttpRequest req, Callback cb) {
    asio::co_spawn(ex_, run_with_callback(shared_from_this(), std::move(req), std::move(cb)),
                   asio::detached);
}

asio::awaitable<void> HttpClient::run_with_callback(std::shared_ptr<HttpClient> self,
                                                    HttpRequest req, Callback cb) {
    Result<HttpResponse> result = Error{ErrorCode::Internal, "request did not complete"};
    try {
        result = co_await self->co_request(std::move(req));
    } catch (const std::exception& e) {
        result = Error{ErrorCode::Internal, e.what()};
    }
    cb(std::move(result));
}

std::unique_ptr<HttpClient::Connection> HttpClient::take_idle(const std::string& host) {
    std::lock_guard lock(pool_mu_);
    auto& pool = idle_[host];
    const auto now = std::chrono::steady_clock::now();
    while (!pool.empty()) {
        auto conn = std::move(pool.back());
        pool.pop_back();
        if (now - conn->idle_since < kMaxIdleAge) return conn;
    }
    return nullptr;
}

void HttpClient::return_idle(const std::string& host, std::unique_ptr<Connection> conn) {
    conn->idle_since = std::chrono::steady_clock::now();
    std::lock_guard lock(pool_mu_);
    idle_[host].push_back(std::move(conn));
}

void HttpClient::close_idle() {
    std::lock_guard lock(pool_mu_);
    idle_.clear();
}

asio::awaitable<Result<std::unique_ptr<HttpClient::Connection>>> HttpClient::connect(
    const std::string& host) {
    auto conn = std::make_unique<Connection>(ex_, tls_);
    auto& tcp_stream = beast::get_lowest_layer(conn->stream);

    tcp::resolver resolver(ex_);
    auto [rec, endpoints] = co_await resolver.async_resolve(host, "443", kAwait);
    if (rec) co_return to_error(rec, "resolve " + host);

    tcp_stream.expires_after(timeout_);
    auto [cec, ep] = co_await tcp_stream.async_connect(endpoints, kAwait);
    if (cec) co_return to_error(cec, "connect " + host);

    if (!SSL_set_tlsext_host_name(conn->stream.native_handle(), host.c_str())) {
        co_return Error{ErrorCode::Network, "failed to set TLS SNI for " + host};
    }
    conn->stream.set_verify_callback(ssl::host_name_verification(host));

    auto [hec] = co_await conn->stream.async_handshake(ssl::stream_base::client, kAwait);
    if (hec) co_return to_error(hec, "TLS handshake with " + host);

    ++connections_opened_;
    co_return std::move(conn);
}

asio::awaitable<Result<HttpResponse>> HttpClient::co_request(HttpRequest req) {
    for (;;) {
        auto wait = limiter_.reserve(req.host, req.weight, req.is_order,
                                     std::chrono::system_clock::now(), timeout_);
        if (!wait) co_return wait.error();
        if (wait->count() == 0) break;
        asio::steady_timer timer(ex_, *wait);
        co_await timer.async_wait(kAwait);
    }

    http::request<http::string_body> breq{req.method, req.target, 11};
    breq.set(http::field::host, req.host);
    breq.set(http::field::user_agent, "binance-connector-cpp/" + std::string(version()));
    breq.set(http::field::accept_encoding, "gzip");  // 18 MB spot exchangeInfo -> 0.3 MB
    breq.keep_alive(true);
    for (const auto& [name, value] : req.headers) breq.set(name, value);
    if (!req.body.empty()) {
        breq.set(http::field::content_type, "application/x-www-form-urlencoded");
        breq.body() = req.body;
    }
    breq.prepare_payload();

    // A pooled connection may have been closed by the server while idle; then retry once on
    // a fresh connection, but only when that cannot duplicate the request.
    for (int attempt = 0;; ++attempt) {
        std::unique_ptr<Connection> conn = take_idle(req.host);
        const bool reused = conn != nullptr;
        if (!conn) {
            auto c = co_await connect(req.host);
            if (!c) co_return c.error();
            conn = std::move(c).value();
        }

        auto& tcp_stream = beast::get_lowest_layer(conn->stream);
        tcp_stream.expires_after(timeout_);

        auto [wec, wn] = co_await http::async_write(conn->stream, breq, kAwait);
        if (wec) {
            // Write failed: the server did not get a complete request, so a retry is safe.
            if (reused && attempt == 0) continue;
            co_return to_error(wec, "send to " + req.host);
        }

        http::response_parser<http::string_body> parser;
        parser.body_limit(kMaxBodyBytes);
        auto [rec, rn] = co_await http::async_read(conn->stream, conn->buffer, parser, kAwait);
        if (rec) {
            // The server may have processed the request; only resend if that is harmless.
            if (reused && attempt == 0 && req.idempotent && rec != beast::error::timeout) continue;
            co_return to_error(rec, "receive from " + req.host);
        }

        auto res = parser.release();
        HttpResponse out;
        out.status = res.result_int();
        if (res[http::field::content_encoding] == "gzip") {
            auto plain = gunzip(res.body(), kMaxBodyBytes);
            if (!plain) {
                co_return plain.error();  // connection is dropped: state unknown
            }
            out.body = std::move(plain).value();
        } else {
            out.body = std::move(res.body());
        }
        for (const auto& field : res) {
            std::string name(field.name_string());
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
            if (is_interesting_header(name)) out.headers.emplace_back(name, field.value());
        }

        limiter_.on_response(req.host, out.status, out.headers, std::chrono::system_clock::now());

        if (res.keep_alive()) return_idle(req.host, std::move(conn));

        if (out.status == 429) {
            co_return Error{ErrorCode::RateLimited, "HTTP 429 from " + req.host + ": " + out.body};
        }
        if (out.status == 418) {
            co_return Error{ErrorCode::IpBanned, "HTTP 418 from " + req.host + ": " + out.body};
        }
        co_return out;
    }
}

}  // namespace binance::detail
