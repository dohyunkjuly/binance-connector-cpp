#include "websocket.hpp"

#include <gtest/gtest.h>

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket.hpp>
#include <boost/beast/websocket/ssl.hpp>

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <charconv>
#include <cstdlib>
#include <optional>
#include <string>
#include <vector>

using namespace binance;
using namespace binance::detail;
using namespace std::chrono_literals;

namespace beast = boost::beast;
namespace websocket = boost::beast::websocket;
using tcp = boost::asio::ip::tcp;

namespace {

constexpr auto kAwait = asio::as_tuple(asio::use_awaitable);
using ServerWs = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

// --- Self-signed certificate for "localhost", generated in memory ------------------------------

struct TestCert {
    EVP_PKEY* key = nullptr;
    X509* cert = nullptr;

    TestCert() {
        key = EVP_EC_gen("P-256");
        cert = X509_new();
        X509_set_version(cert, 2);
        ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
        X509_gmtime_adj(X509_getm_notBefore(cert), -3600);
        X509_gmtime_adj(X509_getm_notAfter(cert), 86400);
        X509_set_pubkey(cert, key);
        X509_NAME* name = X509_get_subject_name(cert);
        X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                   reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
        X509_set_issuer_name(cert, name);
        X509V3_CTX ctx;
        X509V3_set_ctx_nodb(&ctx);
        X509V3_set_ctx(&ctx, cert, cert, nullptr, nullptr, 0);
        X509_EXTENSION* ext =
            X509V3_EXT_conf_nid(nullptr, &ctx, NID_subject_alt_name, "DNS:localhost,IP:127.0.0.1");
        X509_add_ext(cert, ext, -1);
        X509_EXTENSION_free(ext);
        X509_sign(cert, key, EVP_sha256());
    }
    ~TestCert() {
        X509_free(cert);
        EVP_PKEY_free(key);
    }
};

const TestCert& test_cert() {
    static TestCert c;
    return c;
}

// --- Local WSS server -------------------------------------------------------------------------

/// Accepts WSS connections on 127.0.0.1 and runs `handler(ws, connection_index)` for each.
/// When the handler returns, the TCP connection is dropped abruptly.
class TestServer {
public:
    using Handler = std::function<asio::awaitable<void>(ServerWs&, int)>;

    TestServer(asio::io_context& io, Handler handler)
        : io_(io),
          acceptor_(io, tcp::endpoint(asio::ip::make_address("127.0.0.1"), 0)),
          handler_(std::move(handler)),
          server_tls_(ssl::context::tls_server),
          client_tls_(ssl::context::tls_client) {
        SSL_CTX_use_certificate(server_tls_.native_handle(), test_cert().cert);
        SSL_CTX_use_PrivateKey(server_tls_.native_handle(), test_cert().key);
        client_tls_.set_verify_mode(ssl::verify_peer);
        X509_STORE_add_cert(SSL_CTX_get_cert_store(client_tls_.native_handle()), test_cert().cert);
        asio::co_spawn(io_, accept_loop(), asio::detached);
    }

    WsEndpoint endpoint(std::string target = "/stream") const {
        return {"localhost", std::to_string(acceptor_.local_endpoint().port()), std::move(target)};
    }
    ssl::context& client_tls() { return client_tls_; }
    void stop() { acceptor_.close(); }
    int connections() const { return next_index_; }

private:
    asio::awaitable<void> accept_loop() {
        for (;;) {
            auto [ec, sock] = co_await acceptor_.async_accept(kAwait);
            if (ec) co_return;
            asio::co_spawn(io_, session(std::move(sock), next_index_++), asio::detached);
        }
    }

    asio::awaitable<void> session(tcp::socket sock, int index) {
        ServerWs ws(std::move(sock), server_tls_);
        auto [hec] = co_await ws.next_layer().async_handshake(ssl::stream_base::server, kAwait);
        if (hec) co_return;
        auto [aec] = co_await ws.async_accept(kAwait);
        if (aec) co_return;
        co_await handler_(ws, index);
        boost::system::error_code ignored;
        beast::get_lowest_layer(ws).socket().close(ignored);
    }

    asio::io_context& io_;
    tcp::acceptor acceptor_;
    Handler handler_;
    ssl::context server_tls_;
    ssl::context client_tls_;
    int next_index_ = 0;
};

asio::awaitable<std::optional<std::string>> read_text(ServerWs& ws) {
    beast::flat_buffer buf;
    auto [ec, n] = co_await ws.async_read(buf, kAwait);
    if (ec) co_return std::nullopt;
    co_return beast::buffers_to_string(buf.data());
}

asio::awaitable<void> write_text(ServerWs& ws, std::string s) {
    ws.text(true);
    co_await ws.async_write(asio::buffer(s), kAwait);
}

asio::awaitable<void> sleep_for(ServerWs& ws, std::chrono::milliseconds d) {
    asio::steady_timer t(ws.get_executor(), d);
    co_await t.async_wait(kAwait);
}

std::uint64_t id_of(const std::string& msg) {
    const auto pos = msg.find("\"id\":");
    std::uint64_t id = 0;
    if (pos != std::string::npos) {
        std::from_chars(msg.data() + pos + 5, msg.data() + msg.size(), id);
    }
    return id;
}

std::string ack(const std::string& request) {
    return R"({"result":null,"id":)" + std::to_string(id_of(request)) + "}";
}

/// Acks every SUBSCRIBE/UNSUBSCRIBE until the client goes away; records what it received.
asio::awaitable<void> ack_everything(ServerWs& ws, std::vector<std::string>* received = nullptr) {
    while (auto msg = co_await read_text(ws)) {
        if (received) received->push_back(*msg);
        co_await write_text(ws, ack(*msg));
    }
}

WsOptions fast_options() {
    WsOptions o;
    o.connect_timeout = 2s;
    o.backoff_initial = 10ms;
    o.backoff_max = 50ms;
    o.max_messages_per_second = 0;
    return o;
}

struct StateLog {
    std::vector<WsState> states;
    std::vector<std::string> reasons;
    WsStateHandler handler() {
        return [this](WsState s, const std::string& r) {
            states.push_back(s);
            reasons.push_back(r);
        };
    }
};

}  // namespace

// --- WsStream --------------------------------------------------------------------------------

TEST(WsStream, SubscribeDataAndUnsubscribe) {
    asio::io_context io;
    std::vector<std::string> server_got;
    TestServer server(io, [&](ServerWs& ws, int) -> asio::awaitable<void> {
        auto sub = co_await read_text(ws);
        if (!sub) co_return;
        server_got.push_back(*sub);
        co_await write_text(ws, ack(*sub));
        co_await write_text(ws, R"({"stream":"btcusdt@aggTrade","data":{"p":"1.5"}})");
        co_await ack_everything(ws, &server_got);
    });

    StateLog log;
    std::vector<std::string> messages;
    std::optional<Result<void>> sub_done, unsub_done;
    std::shared_ptr<WsStream> stream;
    stream = WsStream::create(
        io.get_executor(), server.client_tls(), server.endpoint(), fast_options(),
        [&](std::string_view m) {
            messages.emplace_back(m);
            stream->unsubscribe({"btcusdt@aggTrade"}, [&](Result<void> r) {
                unsub_done = r;
                stream->stop();
                server.stop();
            });
        },
        log.handler());

    stream->subscribe({"btcusdt@aggTrade", "btcusdt@aggTrade"},  // duplicate collapses
                      [&](Result<void> r) { sub_done = r; });
    stream->start();
    io.run_for(5s);

    ASSERT_TRUE(sub_done.has_value());
    EXPECT_TRUE(sub_done->ok());
    ASSERT_TRUE(unsub_done.has_value());
    EXPECT_TRUE(unsub_done->ok());
    ASSERT_EQ(messages.size(), 1u);  // acks are not forwarded
    EXPECT_EQ(messages[0], R"({"stream":"btcusdt@aggTrade","data":{"p":"1.5"}})");
    ASSERT_EQ(server_got.size(), 2u);
    EXPECT_EQ(server_got[0], R"({"method":"SUBSCRIBE","params":["btcusdt@aggTrade"],"id":1})");
    EXPECT_EQ(server_got[1], R"({"method":"UNSUBSCRIBE","params":["btcusdt@aggTrade"],"id":2})");
    EXPECT_EQ(stream->stream_count(), 0u);
    EXPECT_EQ(log.states, (std::vector{WsState::Open, WsState::Stopped}));
}

TEST(WsStream, ReconnectRestoresConfirmedSubscriptions) {
    asio::io_context io;
    std::vector<std::string> second_conn_got;
    std::shared_ptr<WsStream> stream;
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index == 0) {
            auto sub = co_await read_text(ws);
            co_await write_text(ws, ack(*sub));
            stream->subscribe({"b@trade"});  // sent while connected...
            auto sub2 = co_await read_text(ws);
            co_await write_text(ws, ack(*sub2));
            co_return;  // ...then the connection drops
        }
        auto restore = co_await read_text(ws);
        second_conn_got.push_back(*restore);
        co_await write_text(ws, ack(*restore));
        co_await write_text(ws, R"({"stream":"a@trade","data":{}})");
        co_await ack_everything(ws);
    });

    StateLog log;
    int messages = 0;
    stream = WsStream::create(
        io.get_executor(), server.client_tls(), server.endpoint(), fast_options(),
        [&](std::string_view) {
            ++messages;
            stream->stop();
            server.stop();
        },
        log.handler());
    stream->subscribe({"a@trade"});
    stream->start();
    io.run_for(5s);

    EXPECT_EQ(messages, 1);
    EXPECT_EQ(stream->connections_opened(), 2u);
    ASSERT_EQ(second_conn_got.size(), 1u);
    EXPECT_EQ(second_conn_got[0],
              R"({"method":"SUBSCRIBE","params":["a@trade","b@trade"],"id":3})");
    EXPECT_EQ(log.states,
              (std::vector{WsState::Open, WsState::Connecting, WsState::Open, WsState::Stopped}));
}

TEST(WsStream, RawModeForwardsEverythingAndSendsOnlyWhatItIsTold) {
    // Raw mode (spot user data via the WebSocket API): every message reaches on_message, also
    // {"id":...} answers; nothing is sent on its own, not even after a reconnect.
    asio::io_context io;
    std::vector<std::vector<std::string>> server_got(2);
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index > 1) co_return;
        auto msg = co_await read_text(ws);
        if (!msg) co_return;
        server_got[index].push_back(*msg);
        co_await write_text(ws, R"({"id":"uds","status":200,"result":{"subscriptionId":0}})");
        co_await write_text(ws, R"({"subscriptionId":0,"event":{"e":"outboundAccountPosition"}})");
        if (index == 0) co_return;  // drop the first connection
        while (auto more = co_await read_text(ws)) server_got[index].push_back(*more);
    });

    WsOptions opts = fast_options();
    opts.raw = true;
    std::vector<std::string> messages;
    StateLog log;
    std::shared_ptr<WsStream> stream;
    int opens = 0;
    stream = WsStream::create(
        io.get_executor(), server.client_tls(), server.endpoint(), opts,
        [&](std::string_view m) {
            messages.emplace_back(m);
            if (messages.size() == 4) {
                stream->stop();
                server.stop();
            }
        },
        [&](WsState st, const std::string& reason) {
            log.handler()(st, reason);
            if (st == WsState::Open) stream->send("subscribe #" + std::to_string(++opens));
        });
    stream->send("before start");  // not connected: dropped
    stream->start();
    io.run_for(5s);

    ASSERT_EQ(messages.size(), 4u);
    EXPECT_EQ(messages[0], R"({"id":"uds","status":200,"result":{"subscriptionId":0}})");
    EXPECT_EQ(messages[1], R"({"subscriptionId":0,"event":{"e":"outboundAccountPosition"}})");
    EXPECT_EQ(messages[2], messages[0]);
    EXPECT_EQ(server_got[0], std::vector<std::string>{"subscribe #1"});
    EXPECT_EQ(server_got[1], std::vector<std::string>{"subscribe #2"});
    EXPECT_EQ(stream->connections_opened(), 2u);
}

TEST(WsStream, RejectedSubscriptionIsReportedAndForgotten) {
    asio::io_context io;
    TestServer server(io, [&](ServerWs& ws, int) -> asio::awaitable<void> {
        auto sub = co_await read_text(ws);
        co_await write_text(ws,
                            R"({"error":{"code":2,"msg":"Invalid request: unknown stream"},"id":)" +
                                std::to_string(id_of(*sub)) + "}");
        co_await ack_everything(ws);
    });

    std::optional<Result<void>> done;
    std::shared_ptr<WsStream> stream = WsStream::create(io.get_executor(), server.client_tls(),
                                                        server.endpoint(), fast_options(), {}, {});
    stream->subscribe({"nope@nothing"}, [&](Result<void> r) {
        done = r;
        stream->stop();
        server.stop();
    });
    stream->start();
    io.run_for(5s);

    ASSERT_TRUE(done.has_value());
    ASSERT_FALSE(done->ok());
    EXPECT_EQ(done->error().code, ErrorCode::Exchange);
    EXPECT_EQ(done->error().exchange_code, 2);
    EXPECT_EQ(done->error().message, "Invalid request: unknown stream");
    EXPECT_EQ(stream->stream_count(), 0u);
}

TEST(WsStream, LocalValidation) {
    asio::io_context io;
    auto opts = fast_options();
    opts.max_streams = 2;
    ssl::context tls(ssl::context::tls_client);
    auto stream = WsStream::create(io.get_executor(), tls, {"localhost", "1", "/"}, opts, {}, {});

    std::vector<Result<void>> results;
    auto collect = [&](Result<void> r) { results.push_back(std::move(r)); };
    stream->subscribe({"bad name"}, collect);
    stream->subscribe({"a@trade", "b@trade", "c@trade"}, collect);
    stream->stop();
    stream->subscribe({"a@trade"}, collect);
    io.run_for(1s);

    ASSERT_EQ(results.size(), 3u);
    EXPECT_EQ(results[0].error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(results[1].error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(results[2].error().code, ErrorCode::Cancelled);
}

TEST(WsStream, SilentConnectionIsReplaced) {
    asio::io_context io;
    std::shared_ptr<WsStream> stream;
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index == 0) {
            co_await sleep_for(ws, 1500ms);  // not reading: no pongs, no data
            co_return;
        }
        co_await write_text(ws, R"({"e":"hello"})");
        co_await ack_everything(ws);
    });

    StateLog log;
    auto opts = fast_options();
    opts.idle_timeout = 400ms;
    stream = WsStream::create(
        io.get_executor(), server.client_tls(), server.endpoint("/ws/key"), opts,
        [&](std::string_view) {
            stream->stop();
            server.stop();
        },
        log.handler());
    stream->start();
    io.run_for(5s);

    EXPECT_EQ(stream->connections_opened(), 2u);
    ASSERT_GE(log.states.size(), 2u);
    EXPECT_EQ(log.states[1], WsState::Connecting);
    EXPECT_NE(log.reasons[1].find("idle"), std::string::npos) << log.reasons[1];
}

TEST(WsStream, MaxLifetimeReconnects) {
    asio::io_context io;
    std::shared_ptr<WsStream> stream;
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index == 1) {
            stream->stop();
            server.stop();
        }
        co_await ack_everything(ws);
    });

    StateLog log;
    auto opts = fast_options();
    opts.max_lifetime = 200ms;
    opts.backoff_initial = 5s;  // planned reconnects must not wait for the backoff
    stream = WsStream::create(io.get_executor(), server.client_tls(), server.endpoint(), opts, {},
                              log.handler());
    const auto t0 = std::chrono::steady_clock::now();
    stream->start();
    io.run_for(5s);

    EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);
    EXPECT_EQ(server.connections(), 2);
    ASSERT_GE(log.reasons.size(), 2u);
    EXPECT_EQ(log.reasons[1], "reconnecting (planned)");
}

// --- WsApi -----------------------------------------------------------------------------------

namespace {

std::string api_response(const std::string& request, int status, const std::string& result,
                         int weight_used = 0) {
    std::string s = R"({"id":)" + std::to_string(id_of(request)) + R"(,"status":)" +
                    std::to_string(status) + "," + result;
    if (weight_used) {
        s += R"(,"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE",)"
             R"("intervalNum":1,"limit":2400,"count":)" +
             std::to_string(weight_used) + "}]";
    }
    return s + "}";
}

const std::string kKey = "ws-fapi.test";

}  // namespace

TEST(WsApi, ResponsesAreRoutedById) {
    asio::io_context io;
    std::vector<std::string> requests;
    TestServer server(io, [&](ServerWs& ws, int) -> asio::awaitable<void> {
        auto a = co_await read_text(ws);
        auto b = co_await read_text(ws);
        requests = {*a, *b};
        // Answer out of order.
        co_await write_text(ws, api_response(*b, 200, R"("result":{"who":"b"})", 77));
        co_await write_text(ws, api_response(*a, 400, R"("error":{"code":-1121,"msg":"bad"})"));
        co_await ack_everything(ws);
    });

    RateLimiter limiter(1.0);
    limiter.set_limits(kKey, {{1min, 2400, false}});
    std::optional<Result<WsApiResponse>> ra, rb;
    std::shared_ptr<WsApi> api;
    auto finish = [&] {
        if (ra && rb) {
            api->stop();
            server.stop();
        }
    };
    api = WsApi::create(io.get_executor(), server.client_tls(), server.endpoint("/ws-fapi/v1"),
                        fast_options(), limiter, kKey, 2s, [&](WsState s, const std::string&) {
                            if (s != WsState::Open) return;
                            api->request({.method = "order.place",
                                          .params_json = R"({"symbol":"BTCUSDT"})",
                                          .is_order = true},
                                         [&](Result<WsApiResponse> r) {
                                             ra = r;
                                             finish();
                                         });
                            api->request({.method = "ticker.price", .weight = 2},
                                         [&](Result<WsApiResponse> r) {
                                             rb = r;
                                             finish();
                                         });
                        });
    api->start();
    io.run_for(5s);

    ASSERT_EQ(requests.size(), 2u);
    EXPECT_EQ(requests[0], R"({"id":1,"method":"order.place","params":{"symbol":"BTCUSDT"}})");
    EXPECT_EQ(requests[1], R"({"id":2,"method":"ticker.price"})");
    ASSERT_TRUE(ra && ra->ok());
    EXPECT_EQ((*ra)->status, 400);
    EXPECT_NE((*ra)->body.find("-1121"), std::string::npos);
    ASSERT_TRUE(rb && rb->ok());
    EXPECT_EQ((*rb)->status, 200);
    EXPECT_NE((*rb)->body.find(R"("who":"b")"), std::string::npos);
    EXPECT_EQ(limiter.used(kKey, 1min, false, std::chrono::system_clock::now()), 77);
}

TEST(WsApi, NotConnectedIsNetworkError) {
    asio::io_context io;
    ssl::context tls(ssl::context::tls_client);
    RateLimiter limiter;
    auto api = WsApi::create(io.get_executor(), tls, {"localhost", "1", "/"}, fast_options(),
                             limiter, kKey, 1s, {});
    std::optional<Result<WsApiResponse>> r;
    api->request({.method = "order.place"}, [&](Result<WsApiResponse> x) { r = x; });
    io.run_for(1s);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->error().code, ErrorCode::Network);
}

TEST(WsApi, NoAnswerIsTimeout) {
    asio::io_context io;
    TestServer server(io, [&](ServerWs& ws, int) -> asio::awaitable<void> {
        while (co_await read_text(ws)) {
        }  // read, never answer
    });
    RateLimiter limiter;
    std::optional<Result<WsApiResponse>> r;
    std::shared_ptr<WsApi> api;
    api = WsApi::create(io.get_executor(), server.client_tls(), server.endpoint("/"),
                        fast_options(), limiter, kKey, 200ms, [&](WsState s, const std::string&) {
                            if (s != WsState::Open) return;
                            api->request({.method = "order.place"}, [&](Result<WsApiResponse> x) {
                                r = x;
                                api->stop();
                                server.stop();
                            });
                        });
    api->start();
    io.run_for(5s);
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->ok());
    EXPECT_EQ(r->error().code, ErrorCode::Timeout);
}

TEST(WsApi, ConnectionLostWithRequestInFlightIsTimeout) {
    asio::io_context io;
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index == 0) {
            co_await read_text(ws);  // take the request, then drop the connection
            co_return;
        }
        co_await ack_everything(ws);
    });
    RateLimiter limiter;
    StateLog log;
    std::optional<Result<WsApiResponse>> r;
    std::shared_ptr<WsApi> api;
    api = WsApi::create(io.get_executor(), server.client_tls(), server.endpoint("/"),
                        fast_options(), limiter, kKey, 5s, [&](WsState s, const std::string& why) {
                            log.handler()(s, why);
                            if (s == WsState::Open && !r) {
                                api->request({.method = "order.place"},
                                             [&](Result<WsApiResponse> x) { r = x; });
                            }
                            if (s == WsState::Open && r) {  // reconnected
                                api->stop();
                                server.stop();
                            }
                        });
    api->start();
    io.run_for(5s);
    ASSERT_TRUE(r.has_value());
    ASSERT_FALSE(r->ok());
    EXPECT_EQ(r->error().code, ErrorCode::Timeout);
    EXPECT_NE(r->error().message.find("connection lost"), std::string::npos);
    EXPECT_EQ(log.states,
              (std::vector{WsState::Open, WsState::Connecting, WsState::Open, WsState::Stopped}));
}

TEST(WsApi, Status429IsRateLimitedAndBlocksTheLimiter) {
    asio::io_context io;
    TestServer server(io, [&](ServerWs& ws, int) -> asio::awaitable<void> {
        auto req = co_await read_text(ws);
        const auto retry_at = std::chrono::duration_cast<std::chrono::milliseconds>(
                                  std::chrono::system_clock::now().time_since_epoch())
                                  .count() +
                              30'000;
        co_await write_text(ws, api_response(*req, 429,
                                             R"("error":{"code":-1003,"msg":"Too many requests",)"
                                             R"("data":{"retryAfter":)" +
                                                 std::to_string(retry_at) + "}}"));
        co_await ack_everything(ws);
    });
    RateLimiter limiter;
    std::optional<Result<WsApiResponse>> r;
    std::shared_ptr<WsApi> api;
    api = WsApi::create(io.get_executor(), server.client_tls(), server.endpoint("/"),
                        fast_options(), limiter, kKey, 2s, [&](WsState s, const std::string&) {
                            if (s != WsState::Open) return;
                            api->request({.method = "order.place"}, [&](Result<WsApiResponse> x) {
                                r = x;
                                api->stop();
                                server.stop();
                            });
                        });
    api->start();
    io.run_for(5s);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->error().code, ErrorCode::RateLimited);
    auto wait = limiter.reserve(kKey, 1, false, std::chrono::system_clock::now(), 1h);
    ASSERT_TRUE(wait.ok());
    EXPECT_GT(*wait, 25s);
}

TEST(WsApi, RotationKeepsServingInFlightRequests) {
    asio::io_context io;
    bool old_session_closed_gracefully = false;
    int conn_of_b = -1;
    TestServer server(io, [&](ServerWs& ws, int index) -> asio::awaitable<void> {
        if (index == 0) {
            auto a = co_await read_text(ws);
            co_await sleep_for(ws, 600ms);  // the rotation happens meanwhile
            co_await write_text(ws, api_response(*a, 200, R"("result":"a")"));
            beast::flat_buffer buf;
            auto [ec, n] = co_await ws.async_read(buf, kAwait);
            old_session_closed_gracefully = ec == websocket::error::closed;
            co_return;
        }
        // Later sessions (the rotation repeats every 300 ms) may get nothing before closing.
        auto b = co_await read_text(ws);
        if (!b) co_return;
        conn_of_b = index;
        co_await write_text(ws, api_response(*b, 200, R"("result":"b")"));
        co_await ack_everything(ws);
    });

    RateLimiter limiter;
    auto opts = fast_options();
    opts.max_lifetime = 300ms;
    std::optional<Result<WsApiResponse>> ra, rb;
    std::shared_ptr<WsApi> api;
    asio::steady_timer later(io);
    api = WsApi::create(io.get_executor(), server.client_tls(), server.endpoint("/"), opts, limiter,
                        kKey, 3s, [&](WsState s, const std::string&) {
                            if (s != WsState::Open || ra) return;
                            api->request({.method = "a"}, [&](Result<WsApiResponse> x) { ra = x; });
                        });
    later.expires_after(450ms);  // after the rotation, before "a" is answered
    later.async_wait(
        [&](auto) { api->request({.method = "b"}, [&](Result<WsApiResponse> x) { rb = x; }); });
    asio::steady_timer done(io, 1500ms);
    done.async_wait([&](auto) {
        api->stop();
        server.stop();
    });
    api->start();
    io.run_for(5s);

    ASSERT_TRUE(ra && ra->ok()) << (ra ? ra->error().message : "no result");
    EXPECT_NE((*ra)->body.find(R"("result":"a")"), std::string::npos);
    ASSERT_TRUE(rb && rb->ok()) << (rb ? rb->error().message : "no result");
    EXPECT_EQ(conn_of_b, 1);
    EXPECT_TRUE(old_session_closed_gracefully);
}

// --- Live (network) --------------------------------------------------------------------------
// Run with BINANCE_LIVE_TESTS=1. Public data only, no API key.

class LiveWs : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::getenv("BINANCE_LIVE_TESTS")) GTEST_SKIP() << "set BINANCE_LIVE_TESTS=1";
    }
};

TEST_F(LiveWs, FuturesAndSpotBookTickerStreams) {
    asio::io_context io;
    auto tls = make_tls_context();
    std::string futures_msg, spot_msg;
    std::shared_ptr<WsStream> futures, spot;
    auto maybe_stop = [&] {
        if (!futures_msg.empty() && !spot_msg.empty()) {
            futures->stop();
            spot->stop();
        }
    };
    futures =
        WsStream::create(io.get_executor(), tls, {"fstream.binance.com", "443", "/stream"}, {},
                         [&](std::string_view m) {
                             if (futures_msg.empty()) futures_msg = m;
                             maybe_stop();
                         },
                         {});
    auto spot_opts = WsOptions{};
    spot_opts.max_streams = 1024;
    spot = WsStream::create(io.get_executor(), tls, {"stream.binance.com", "443", "/stream"},
                            spot_opts,
                            [&](std::string_view m) {
                                if (spot_msg.empty()) spot_msg = m;
                                maybe_stop();
                            },
                            {});
    std::optional<Result<void>> fdone, sdone;
    futures->subscribe({"btcusdt@bookTicker"}, [&](Result<void> r) { fdone = r; });
    spot->subscribe({"btcusdt@bookTicker"}, [&](Result<void> r) { sdone = r; });
    futures->start();
    spot->start();
    io.run_for(15s);

    ASSERT_TRUE(fdone && fdone->ok());
    ASSERT_TRUE(sdone && sdone->ok());
    EXPECT_NE(futures_msg.find(R"("stream":"btcusdt@bookTicker")"), std::string::npos)
        << futures_msg;
    EXPECT_NE(spot_msg.find(R"("stream":"btcusdt@bookTicker")"), std::string::npos) << spot_msg;
}

TEST_F(LiveWs, FuturesWebSocketApiTickerPrice) {
    asio::io_context io;
    auto tls = make_tls_context();
    RateLimiter limiter;
    limiter.set_limits("ws-fapi.binance.com", {{1min, 2400, false}});
    std::optional<Result<WsApiResponse>> r;
    std::shared_ptr<WsApi> api;
    api = WsApi::create(
        io.get_executor(), tls, {"ws-fapi.binance.com", "443", "/ws-fapi/v1"}, {}, limiter,
        "ws-fapi.binance.com", 10s, [&](WsState s, const std::string& why) {
            if (s == WsState::Connecting) std::cerr << "connecting: " << why << "\n";
            if (s != WsState::Open) return;
            api->request({.method = "ticker.price", .params_json = R"({"symbol":"BTCUSDT"})"},
                         [&](Result<WsApiResponse> x) {
                             r = x;
                             api->stop();
                         });
        });
    api->start();
    io.run_for(15s);

    ASSERT_TRUE(r.has_value());
    ASSERT_TRUE(r->ok()) << r->error().message;
    EXPECT_EQ((*r)->status, 200) << (*r)->body;
    EXPECT_NE((*r)->body.find("\"price\""), std::string::npos) << (*r)->body;
    EXPECT_GT(limiter.used("ws-fapi.binance.com", 1min, false, std::chrono::system_clock::now()),
              0);
}
