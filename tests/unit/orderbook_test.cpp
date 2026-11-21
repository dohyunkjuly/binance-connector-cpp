// Samples captured from fstream / stream / demo-fapi on 2026-11-21 (levels trimmed).

#include <binance/client.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

using namespace binance;
using namespace std::chrono_literals;

// Internals of client.cpp under test. DepthEvent must match client.cpp exactly.
namespace binance::detail {

struct DepthEvent {
    std::int64_t first_id = 0;
    std::int64_t last_id = 0;
    std::int64_t prev_id = -1;
    Timestamp time{};
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
};

Result<DepthEvent> parse_depth_snapshot(std::string_view json);
Result<DepthEvent> parse_depth_event(std::string_view json);
Result<OrderBook> replay_depth(Market market, const DepthEvent& snapshot,
                               const std::vector<DepthEvent>& events, std::size_t levels);
Result<std::vector<Result<Order>>> parse_batch_orders(std::string_view json);

struct TestHook {
    static void drop_account_events(Client& c, Market market, bool drop);
    static void reconnect_account(Client& c, Market market);
    static void drop_next_book_update(Client& c, Market market, const std::string& symbol);
    static std::string book_status(Client& c, Market market, const std::string& symbol);
};

}  // namespace binance::detail

namespace {

Decimal d(std::string_view s) { return *Decimal::parse(s); }

detail::DepthEvent event(std::int64_t first, std::int64_t last, std::int64_t prev,
                         std::vector<PriceLevel> bids, std::vector<PriceLevel> asks) {
    detail::DepthEvent e;
    e.first_id = first;
    e.last_id = last;
    e.prev_id = prev;
    e.bids = std::move(bids);
    e.asks = std::move(asks);
    return e;
}

detail::DepthEvent snapshot(std::int64_t id) {
    // bids 100 / 99 / 98, asks 101 / 102 / 103
    return event(0, id, -1, {{d("100"), d("1")}, {d("99"), d("2")}, {d("98"), d("3")}},
                 {{d("101"), d("1")}, {d("102"), d("2")}, {d("103"), d("3")}});
}

}  // namespace

// --- Parsing ---------------------------------------------------------------------------------

TEST(DepthParse, FuturesDiffAndPartial) {
    auto diff = detail::parse_depth_event(
        R"({"e":"depthUpdate","E":1795266173473,"T":1795266173472,"s":"BTCUSDT","ps":"BTCUSDT","U":12192733149308,"u":12192733155047,"pu":12192733149264,"b":[["5894.60","0.000"],["8445.70","0.690"]],"a":[["58945.80","0.752"],["58956.30","0.086"]],"st":1})");
    ASSERT_TRUE(diff.ok()) << diff.error().message;
    EXPECT_EQ(diff->first_id, 12192733149308);
    EXPECT_EQ(diff->last_id, 12192733155047);
    EXPECT_EQ(diff->prev_id, 12192733149264);
    EXPECT_EQ(to_epoch_ms(diff->time), 1795266173473);
    ASSERT_EQ(diff->bids.size(), 2u);
    EXPECT_EQ(diff->bids[0].price, d("5894.6"));
    EXPECT_TRUE(diff->bids[0].qty.is_zero());  // 0 = remove the level
    EXPECT_EQ(diff->asks[1].qty, d("0.086"));

    // Top-N stream: same format on futures.
    auto partial = detail::parse_depth_event(
        R"({"e":"depthUpdate","E":1795266173699,"T":1795266173697,"s":"BTCUSDT","ps":"BTCUSDT","U":12192733164767,"u":12192733170001,"pu":12192733164457,"b":[["58945.70","2.820"],["58945.60","0.008"]],"a":[["58945.80","0.746"],["58945.90","0.003"]],"st":1})");
    ASSERT_TRUE(partial.ok());
    EXPECT_EQ(partial->bids[0].price, d("58945.7"));
}

TEST(DepthParse, SpotDiffAndPartial) {
    auto diff = detail::parse_depth_event(
        R"({"e":"depthUpdate","E":1795266173414,"s":"BTCUSDT","U":103380179965,"u":103380179979,"b":[["58977.15000000","3.38417000"],["58917.00000000","0.00700000"]],"a":[["58977.16000000","0.31684000"],["58984.21000000","0.00009000"]]})");
    ASSERT_TRUE(diff.ok()) << diff.error().message;
    EXPECT_EQ(diff->first_id, 103380179965);
    EXPECT_EQ(diff->last_id, 103380179979);
    EXPECT_EQ(diff->prev_id, -1);  // spot sends no pu
    EXPECT_EQ(diff->bids[0].qty, d("3.38417"));

    // Spot's top-N stream is a different shape: no event fields at all.
    auto partial = detail::parse_depth_event(
        R"({"lastUpdateId":103380179979,"bids":[["58977.15000000","3.38417000"],["58976.25000000","0.45723000"]],"asks":[["58977.16000000","0.31684000"],["58977.17000000","0.00072000"]]})");
    ASSERT_TRUE(partial.ok()) << partial.error().message;
    EXPECT_EQ(partial->last_id, 103380179979);
    ASSERT_EQ(partial->asks.size(), 2u);
    EXPECT_EQ(partial->asks[0].price, d("58977.16"));
}

TEST(DepthParse, RestSnapshots) {
    auto f = detail::parse_depth_snapshot(
        R"({"lastUpdateId":12192733199516,"E":1795266174109,"T":1795266174103,"bids":[["58945.70","3.074"],["58945.60","0.004"]],"asks":[["58945.80","0.813"],["58945.90","0.003"]]})");
    ASSERT_TRUE(f.ok()) << f.error().message;
    EXPECT_EQ(f->last_id, 12192733199516);
    EXPECT_EQ(to_epoch_ms(f->time), 1795266174109);
    auto s = detail::parse_depth_snapshot(
        R"({"lastUpdateId":103380180753,"bids":[["58980.23000000","6.14333000"],["58980.22000000","0.01254000"]],"asks":[["58980.24000000","0.00064000"],["58980.25000000","0.00036000"]]})");
    ASSERT_TRUE(s.ok()) << s.error().message;
    EXPECT_EQ(s->bids[1].price, d("58980.22"));

    EXPECT_FALSE(detail::parse_depth_snapshot(R"({"code":-1121,"msg":"Invalid symbol."})").ok());
    EXPECT_FALSE(
        detail::parse_depth_event(R"({"e":"depthUpdate","U":1,"u":2,"b":[["1"]],"a":[]})").ok());
}

// --- Local book: Binance's sequence rules ----------------------------------------------------

TEST(LocalBook, FuturesSequence) {
    const auto F = Market::Futures;
    // Snapshot 100: u=95 is old; u=105 bridges it (U <= 100 <= u); then pu == previous u.
    auto book = detail::replay_depth(
        F, snapshot(100),
        {event(90, 95, 89, {{d("100"), d("50")}}, {}),                    // old: ignored
         event(98, 105, 97, {{d("100"), d("5")}}, {{d("101"), d("0")}}),  // bridge; remove 101
         event(106, 110, 105, {{d("100.5"), d("7")}}, {})},               // pu == 105
        10);
    ASSERT_TRUE(book.ok()) << book.error().message;
    ASSERT_EQ(book->bids.size(), 4u);
    EXPECT_EQ(book->bids[0].price, d("100.5"));  // best first
    EXPECT_EQ(book->bids[1].qty, d("5"));        // 100: updated, not the old event's 50
    EXPECT_EQ(book->asks[0].price, d("102"));    // 101 removed
    EXPECT_EQ(book->last_update_id, 110);

    // A missed message: pu does not match the previous u.
    EXPECT_FALSE(detail::replay_depth(F, snapshot(100),
                                      {event(98, 105, 97, {}, {}), event(107, 110, 106, {}, {})}, 5)
                     .ok());
    // The first event starts after the snapshot: events between them are missing.
    EXPECT_FALSE(detail::replay_depth(F, snapshot(100), {event(102, 110, 101, {}, {})}, 5).ok());
    // Levels: only the top n come back.
    auto top1 = detail::replay_depth(F, snapshot(100), {}, 1);
    ASSERT_EQ(top1->bids.size(), 1u);
    EXPECT_EQ(top1->bids[0].price, d("100"));
}

TEST(LocalBook, SpotSequence) {
    const auto S = Market::Spot;
    // Snapshot 100. u <= 100 is old; the first applied has U <= 101 <= u; then U = previous u + 1.
    auto book =
        detail::replay_depth(S, snapshot(100),
                             {event(95, 100, -1, {{d("100"), d("50")}}, {}),  // old
                              event(99, 105, -1, {{d("97"), d("1")}}, {}),    // bridge
                              event(106, 108, -1, {{d("99"), d("0")}}, {})},  // next; remove 99
                             10);
    ASSERT_TRUE(book.ok()) << book.error().message;
    ASSERT_EQ(book->bids.size(), 3u);
    EXPECT_EQ(book->bids[1].price, d("98"));
    EXPECT_EQ(book->bids[2].price, d("97"));
    EXPECT_FALSE(detail::replay_depth(S, snapshot(100),
                                      {event(99, 105, -1, {}, {}), event(107, 110, -1, {}, {})}, 5)
                     .ok());
    EXPECT_FALSE(detail::replay_depth(S, snapshot(100), {event(102, 110, -1, {}, {})}, 5).ok());
}

// --- Batch answers ---------------------------------------------------------------------------

TEST(BatchParse, MixedResults) {
    auto r = detail::parse_batch_orders(
        R"([{"orderId":28689757066,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"bprobe-1","price":"35366.00","origQty":"0.0030","executedQty":"0.0000","cumQty":"0.0000","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0.00","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"EXPIRE_MAKER","goodTillDate":0,"updateTime":1795266175000},{"code":-4014,"msg":"Price not increased by tick size."},{"code":-4120,"msg":"Order type not supported for this endpoint. Please use the Algo Order API endpoints instead."}])");
    ASSERT_TRUE(r.ok()) << r.error().message;
    ASSERT_EQ(r->size(), 3u);
    ASSERT_TRUE((*r)[0].ok());
    EXPECT_EQ((*r)[0]->client_order_id, "bprobe-1");
    EXPECT_EQ((*r)[0]->price, d("35366"));
    ASSERT_FALSE((*r)[1].ok());
    EXPECT_EQ((*r)[1].error().exchange_code, -4014);
    EXPECT_EQ((*r)[1].error().code, ErrorCode::Exchange);
    EXPECT_EQ((*r)[2].error().exchange_code, -4120);
    EXPECT_FALSE(detail::parse_batch_orders(R"({"code":-1102})").ok());
}

// --- Live ------------------------------------------------------------------------------------
// BINANCE_LIVE_TESTS=1; the batch test also needs BINANCE_DEMO_API_KEY and _SECRET
// (places BTCUSDT limit orders far from the market and cancels them).

namespace {

class Quiet : public Logger {
public:
    void log(LogLevel level, std::string_view msg) override {
        if (level >= LogLevel::Warn) std::cerr << "[binance] " << msg << "\n";
    }
};

Config quiet() {
    Config c;
    c.logger = std::make_shared<Quiet>();
    return c;
}

void expect_sane(const OrderBook& b, std::size_t levels) {
    ASSERT_EQ(b.bids.size(), levels);
    ASSERT_EQ(b.asks.size(), levels);
    EXPECT_LT(b.bids[0].price, b.asks[0].price);
    for (std::size_t i = 1; i < levels; ++i) {
        EXPECT_GT(b.bids[i - 1].price, b.bids[i].price);
        EXPECT_LT(b.asks[i - 1].price, b.asks[i].price);
        EXPECT_TRUE(b.bids[i].qty.is_positive());
    }
}

}  // namespace

class LiveOrderBook : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::getenv("BINANCE_LIVE_TESTS")) GTEST_SKIP() << "set BINANCE_LIVE_TESTS=1";
    }
};

TEST_F(LiveOrderBook, FullAndTopOfBook) {
    Client client(quiet());
    for (Market m : {Market::Futures, Market::Spot}) {
        SCOPED_TRACE(std::string(to_string(m)));
        auto first = client.watch_order_book(m, "BTCUSDT", 100);
        ASSERT_TRUE(first.ok()) << first.error().message;
        auto eth = client.watch_order_book(m, "ETHUSDT", 5);
        ASSERT_TRUE(eth.ok()) << eth.error().message;
        expect_sane(*eth, 5);
        std::this_thread::sleep_for(3s);  // a few dozen diffs on top of the snapshot

        auto full = client.watch_order_book(m, "BTCUSDT", 100);
        ASSERT_TRUE(full.ok()) << full.error().message;
        expect_sane(*full, 100);
        EXPECT_GT(full->last_update_id, first->last_update_id);
        auto top = client.watch_order_book(m, "BTCUSDT", 3);  // the same full book, 3 levels
        ASSERT_TRUE(top.ok()) << top.error().message;
        ASSERT_EQ(top->bids.size(), 3u);

        // One order-book watch per symbol.
        EXPECT_EQ(client.watch_order_book(m, "BTCUSDT", 20).error().code,
                  ErrorCode::InvalidArgument);
        client.unwatch_order_book(m, "BTCUSDT");

        auto rest = client.get_order_book_rest_api(m, "BTCUSDT", 10);
        ASSERT_TRUE(rest.ok()) << rest.error().message;
        expect_sane(*rest, 10);
    }
}

TEST_F(LiveOrderBook, GapRebuildsTheBook) {
    Client client(quiet());
    constexpr auto F = Market::Futures;
    ASSERT_TRUE(client.watch_order_book(F, "BTCUSDT", 50).ok());
    std::this_thread::sleep_for(1s);
    EXPECT_EQ(detail::TestHook::book_status(client, F, "BTCUSDT"), "live");

    // Lose one diff: the next one does not continue the sequence -> resyncing -> rebuilt -> live.
    detail::TestHook::drop_next_book_update(client, F, "BTCUSDT");
    bool saw_resync = false;
    const auto end = std::chrono::steady_clock::now() + 15s;
    for (;;) {
        const auto st = detail::TestHook::book_status(client, F, "BTCUSDT");
        if (st == "resyncing") saw_resync = true;
        if (saw_resync && st == "live") break;
        ASSERT_LT(std::chrono::steady_clock::now(), end) << "no rebuild (status " << st << ")";
        std::this_thread::sleep_for(5ms);
    }
    auto book = client.watch_order_book(F, "BTCUSDT", 50);
    ASSERT_TRUE(book.ok()) << book.error().message;
    expect_sane(*book, 50);
}

TEST_F(LiveOrderBook, BatchOrdersOnDemo) {
    const char* key = std::getenv("BINANCE_DEMO_API_KEY");
    const char* secret = std::getenv("BINANCE_DEMO_API_SECRET");
    if (!key || !secret) GTEST_SKIP() << "set BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET";
    Config cfg = quiet();
    cfg.environment = Environment::Demo;
    cfg.credentials = {key, secret, ""};
    Client client(cfg);
    constexpr auto F = Market::Futures;
    ASSERT_TRUE(client.load_instruments(F).ok());
    auto mark_price = client.get_mark_price_rest_api("BTCUSDT");
    ASSERT_TRUE(mark_price.ok()) << mark_price.error().message;
    const Decimal mark = mark_price->mark_price;
    const Decimal far =
        *client.round_price(F, "BTCUSDT", *Decimal::mul(mark, d("0.6")), Rounding::Down);

    auto limit = [&](Decimal price) {
        OrderRequest r;
        r.symbol = "BTCUSDT";
        r.type = OrderType::Limit;
        r.side = Side::Buy;
        r.qty = d("0.003");
        r.price = price;
        return r;
    };
    std::vector<OrderRequest> reqs;
    for (int i = 0; i < 6; ++i) reqs.push_back(limit(far - Decimal::from_int(i)));  // 2 requests
    reqs.push_back(limit(far + d("0.05")));                                         // off tick
    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopMarket;
    stop.qty = d("0.003");
    stop.stop_price = far;
    reqs.push_back(stop);  // algo: placed alone

    auto placed = client.create_orders(F, reqs);
    ASSERT_EQ(placed.size(), reqs.size());
    std::vector<OrderRef> refs;
    for (int i = 0; i < 6; ++i) {
        ASSERT_TRUE(placed[i].ok()) << i << ": " << placed[i].error().message;
        EXPECT_EQ(placed[i]->price, far - Decimal::from_int(i));  // results in request order
        refs.push_back({"BTCUSDT", placed[i]->id});
    }
    ASSERT_FALSE(placed[6].ok());
    EXPECT_EQ(placed[6].error().code, ErrorCode::InvalidArgument);  // caught before sending
    ASSERT_TRUE(placed[7].ok()) << placed[7].error().message;
    EXPECT_TRUE(placed[7]->is_algo);

    OrderRef algo;
    algo.order_id = placed[7]->id;
    algo.is_algo = true;
    refs.push_back(algo);
    refs.push_back({"BTCUSDT", 1});  // unknown order
    auto cancelled = client.cancel_orders(F, refs);
    ASSERT_EQ(cancelled.size(), refs.size());
    for (std::size_t i = 0; i + 1 < refs.size(); ++i) {
        ASSERT_TRUE(cancelled[i].ok()) << i << ": " << cancelled[i].error().message;
        EXPECT_EQ(cancelled[i]->status, OrderStatus::Canceled);
    }
    ASSERT_FALSE(cancelled.back().ok());
    EXPECT_EQ(cancelled.back().error().exchange_code, -2011);
}
