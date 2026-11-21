#include "http.hpp"

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/beast/zlib.hpp>

#include <cstdlib>
#include <optional>

using namespace binance;
using namespace binance::detail;
using namespace std::chrono_literals;

namespace {

const std::string kHost = "fapi.binance.com";

/// 2024-01-01 00:00:00 UTC + offset; aligned to minute boundaries.
RateLimiter::TimePoint at(std::chrono::milliseconds offset) {
    return RateLimiter::TimePoint{std::chrono::milliseconds{1'704'067'200'000}} + offset;
}

}  // namespace

// --- RateLimiter ----------------------------------------------------------------------------

TEST(RateLimiter, NoLimitsConfiguredAlwaysGo) {
    RateLimiter lim;
    auto r = lim.reserve(kHost, 1000, false, at(0ms), 10s);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(*r, 0ms);
}

TEST(RateLimiter, CountsWeightAndWaitsForNextWindow) {
    RateLimiter lim(1.0);
    lim.set_limits(kHost, {{1min, 100, false}});

    EXPECT_EQ(*lim.reserve(kHost, 60, false, at(0ms), 1min), 0ms);
    EXPECT_EQ(*lim.reserve(kHost, 40, false, at(10s), 1min), 0ms);
    EXPECT_EQ(lim.used(kHost, 1min, false, at(10s)), 100);

    // Full: must wait until the minute ends (50 s from t=10s).
    auto r = lim.reserve(kHost, 1, false, at(10s), 1min);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(*r, 50s);
    EXPECT_EQ(lim.used(kHost, 1min, false, at(10s)), 100);  // nothing reserved

    EXPECT_EQ(*lim.reserve(kHost, 1, false, at(60s), 1min), 0ms);
    EXPECT_EQ(lim.used(kHost, 1min, false, at(60s)), 1);
}

TEST(RateLimiter, SafetyMargin) {
    RateLimiter lim(0.9);
    lim.set_limits(kHost, {{1min, 100, false}});
    EXPECT_EQ(*lim.reserve(kHost, 90, false, at(0ms), 1min), 0ms);
    EXPECT_GT(*lim.reserve(kHost, 1, false, at(0ms), 1min), 0ms);
}

TEST(RateLimiter, WaitLongerThanMaxIsAnError) {
    RateLimiter lim(1.0);
    lim.set_limits(kHost, {{1min, 10, false}});
    ASSERT_EQ(*lim.reserve(kHost, 10, false, at(0ms), 1min), 0ms);
    auto r = lim.reserve(kHost, 1, false, at(1s), 5s);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::RateLimited);
}

TEST(RateLimiter, OrderLimitsOnlyCountOrders) {
    RateLimiter lim(1.0);
    lim.set_limits(kHost, {{1min, 1000, false}, {10s, 2, true}});
    EXPECT_EQ(*lim.reserve(kHost, 1, true, at(0ms), 1min), 0ms);
    EXPECT_EQ(*lim.reserve(kHost, 1, true, at(1s), 1min), 0ms);
    EXPECT_EQ(*lim.reserve(kHost, 5, false, at(2s), 1min), 0ms);  // not an order: unaffected
    EXPECT_EQ(*lim.reserve(kHost, 1, true, at(2s), 1min), 8s);    // third order waits for t=10s
    EXPECT_EQ(lim.used(kHost, 1min, false, at(2s)), 7);
}

TEST(RateLimiter, HeadersCorrectUsage) {
    RateLimiter lim(1.0);
    lim.set_limits(kHost, {{1min, 2400, false}, {10s, 300, true}});
    lim.on_response(kHost, 200, {{"x-mbx-used-weight-1m", "1500"}, {"x-mbx-order-count-10s", "12"}},
                    at(5s));
    EXPECT_EQ(lim.used(kHost, 1min, false, at(5s)), 1500);
    EXPECT_EQ(lim.used(kHost, 10s, true, at(5s)), 12);
    // Header for an unconfigured window is ignored.
    lim.on_response(kHost, 200, {{"x-mbx-used-weight-1d", "5"}}, at(5s));
    EXPECT_EQ(lim.used(kHost, 1min, false, at(5s)), 1500);
    // Header never lowers our own count within the same window (requests still in flight).
    lim.on_response(kHost, 200, {{"x-mbx-used-weight-1m", "10"}}, at(6s));
    EXPECT_EQ(lim.used(kHost, 1min, false, at(6s)), 1500);
}

TEST(RateLimiter, Http429BlocksUntilRetryAfter) {
    RateLimiter lim;
    lim.on_response(kHost, 429, {{"retry-after", "30"}}, at(0ms));
    auto r = lim.reserve(kHost, 1, false, at(10s), 1min);
    ASSERT_TRUE(r.ok());
    EXPECT_EQ(*r, 20s);
    EXPECT_EQ(*lim.reserve(kHost, 1, false, at(30s), 1min), 0ms);
}

TEST(RateLimiter, Http418RefusesUntilBanEnds) {
    RateLimiter lim;
    lim.on_response(kHost, 418, {{"retry-after", "120"}}, at(0ms));
    auto r = lim.reserve(kHost, 1, false, at(10s), 10min);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::IpBanned);
    EXPECT_EQ(*lim.reserve(kHost, 1, false, at(120s), 1min), 0ms);
}

// --- gzip ---------------------------------------------------------------------------------

namespace {

/// gzip = 10-byte header + raw deflate + CRC32 + size. CRC left 0 (gunzip does not check it).
std::string gzip(std::string_view plain, bool with_name = false, bool true_size = true) {
    std::string out = {'\x1f', '\x8b', 8, with_name ? '\x08' : '\0', 0, 0, 0, 0, 0, 3};
    if (with_name) out += std::string("data.json") + '\0';
    boost::beast::zlib::deflate_stream deflater;
    deflater.reset(6, 15, 8, boost::beast::zlib::Strategy::normal);
    std::string body(plain.size() + 1024, '\0');
    boost::beast::zlib::z_params zs;
    zs.next_in = plain.data();
    zs.avail_in = plain.size();
    zs.next_out = body.data();
    zs.avail_out = body.size();
    boost::system::error_code ec;
    deflater.write(zs, boost::beast::zlib::Flush::finish, ec);
    body.resize(body.size() - zs.avail_out);
    out += body;
    const auto n = static_cast<std::uint32_t>(true_size ? plain.size() : 0);
    out += std::string(4, '\0');
    for (int i = 0; i < 4; ++i) out += static_cast<char>((n >> (8 * i)) & 0xFF);
    return out;
}

std::string sample_json(std::size_t symbols) {
    std::string s = R"({"symbols":[)";
    for (std::size_t i = 0; i < symbols; ++i) {
        if (i) s += ',';
        s += R"({"symbol":"SYM)" + std::to_string(i) + R"(USDT","status":"TRADING"})";
    }
    return s + "]}";
}

}  // namespace

TEST(Gunzip, RoundTrip) {
    const auto plain = sample_json(20000);  // ~0.9 MB, compresses ~30x
    const auto gz = gzip(plain, true);
    EXPECT_LT(gz.size(), plain.size() / 10);
    auto r = gunzip(gz, 64 << 20);
    ASSERT_TRUE(r.ok()) << r.error().message;
    EXPECT_EQ(*r, plain);
}

TEST(Gunzip, SmallBodies) {
    for (std::size_t n : {0u, 1u, 2u, 3u, 10u}) {
        const auto plain = sample_json(n);
        auto r = gunzip(gzip(plain), 64 << 20);
        ASSERT_TRUE(r.ok()) << n << ": " << r.error().message;
        EXPECT_EQ(*r, plain);
    }
    const std::string book =
        R"({"symbol":"BTCUSDT","bidPrice":"61657.10","bidQty":"2.149","askPrice":"61657.20",)"
        R"("askQty":"5.123","time":1795177098025,"lastUpdateId":12178674684067})";
    auto r = gunzip(gzip(book), 64 << 20);
    ASSERT_TRUE(r.ok()) << r.error().message;
    EXPECT_EQ(*r, book);
}

TEST(Gunzip, RejectsWrongSizeField) {
    // The trailer's size field must match: catches a damaged or mis-decoded body.
    auto r = gunzip(gzip(sample_json(5000), false, false), 64 << 20);
    ASSERT_FALSE(r.ok());
    EXPECT_NE(r.error().message.find("size"), std::string::npos);
}

TEST(Gunzip, RejectsBadInput) {
    const auto gz = gzip(sample_json(100));
    EXPECT_FALSE(gunzip("not gzip at all, really not", 1 << 20).ok());
    EXPECT_FALSE(gunzip(gz.substr(0, gz.size() / 2), 1 << 20).ok());
    auto big = gunzip(gzip(sample_json(5000)), 1000);
    ASSERT_FALSE(big.ok());
    EXPECT_NE(big.error().message.find("limit"), std::string::npos);
}

// --- Live (network) -------------------------------------------------------------------------
// Run with BINANCE_LIVE_TESTS=1. Uses only public endpoints, no API key.

class LiveHttp : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::getenv("BINANCE_LIVE_TESTS")) GTEST_SKIP() << "set BINANCE_LIVE_TESTS=1";
    }
};

TEST_F(LiveHttp, FuturesAndSpotServerTimeWithKeepAlive) {
    boost::asio::io_context io;
    auto tls = make_tls_context();
    RateLimiter limiter;
    limiter.set_limits("fapi.binance.com", {{1min, 2400, false}});
    limiter.set_limits("api.binance.com", {{1min, 6000, false}});
    auto client = HttpClient::create(io.get_executor(), tls, limiter, 10s);

    std::optional<Result<HttpResponse>> futures1, futures2, spot;
    client->request({.host = "fapi.binance.com", .target = "/fapi/v1/time"},
                    [&](Result<HttpResponse> r) {
                        futures1 = std::move(r);
                        // Second request after the first finished: should reuse the connection.
                        client->request({.host = "fapi.binance.com", .target = "/fapi/v1/time"},
                                        [&](Result<HttpResponse> r2) { futures2 = std::move(r2); });
                    });
    client->request({.host = "api.binance.com", .target = "/api/v3/time"},
                    [&](Result<HttpResponse> r) { spot = std::move(r); });
    io.run();

    for (auto* r : {&futures1, &futures2, &spot}) {
        ASSERT_TRUE(r->has_value());
        ASSERT_TRUE((*r)->ok()) << (*r)->error().message;
        EXPECT_EQ((**r)->status, 200u);
        EXPECT_NE((**r)->body.find("serverTime"), std::string::npos) << (**r)->body;
    }
    EXPECT_EQ(client->connections_opened(), 2u);  // one per host, futures one reused
    EXPECT_GT(limiter.used("fapi.binance.com", 1min, false, std::chrono::system_clock::now()), 0);
    EXPECT_GT(limiter.used("api.binance.com", 1min, false, std::chrono::system_clock::now()), 0);
}

TEST_F(LiveHttp, ErrorStatusIsReturnedAsResponse) {
    boost::asio::io_context io;
    auto tls = make_tls_context();
    RateLimiter limiter;
    auto client = HttpClient::create(io.get_executor(), tls, limiter, 10s);

    std::optional<Result<HttpResponse>> res;
    client->request(
        {.host = "fapi.binance.com", .target = "/fapi/v1/klines?symbol=NOT_A_SYMBOL&interval=1m"},
        [&](Result<HttpResponse> r) { res = std::move(r); });
    io.run();

    ASSERT_TRUE(res.has_value());
    ASSERT_TRUE(res->ok()) << res->error().message;
    EXPECT_EQ((*res)->status, 400u);
    EXPECT_NE((*res)->body.find("\"code\""), std::string::npos) << (*res)->body;
}

TEST_F(LiveHttp, LargeBodyArrivesGzipped) {
    boost::asio::io_context io;
    auto tls = make_tls_context();
    RateLimiter limiter;
    auto client = HttpClient::create(io.get_executor(), tls, limiter, 10s);
    std::optional<Result<HttpResponse>> res;
    const auto t0 = std::chrono::steady_clock::now();
    client->request({.host = "api.binance.com", .target = "/api/v3/exchangeInfo", .weight = 20},
                    [&](Result<HttpResponse> r) { res = std::move(r); });
    io.run();
    ASSERT_TRUE(res && res->ok()) << (res ? res->error().message : "no result");
    EXPECT_EQ((*res)->status, 200u);
    EXPECT_GT((*res)->body.size(), 10'000'000u);  // ~18 MB once decompressed
    EXPECT_EQ((*res)->body.front(), '{');
    EXPECT_EQ((*res)->body.back(), '}');
    std::cerr << "spot exchangeInfo: " << (*res)->body.size() << " bytes in "
              << std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::steady_clock::now() - t0)
                     .count()
              << " ms\n";
}
