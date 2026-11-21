
#include <binance/client.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

using namespace binance;
using namespace std::chrono_literals;

namespace binance::detail {
std::string hmac_sha256_hex(std::string_view key, std::string_view data);
std::optional<Error> validate_futures_order(const OrderRequest& r, const Instrument& ins,
                                            std::optional<bool> hedge_mode);
std::vector<std::pair<std::string, std::string>> futures_order_params(
    const OrderRequest& r, const std::string& client_order_id);
std::string ws_signed_params(std::vector<std::pair<std::string, std::string>> params,
                             std::string_view api_key, std::string_view secret,
                             std::int64_t timestamp, std::int64_t recv_window);
std::optional<Error> validate_spot_order(const OrderRequest& r, const Instrument& ins);
std::vector<std::pair<std::string, std::string>> spot_order_params(
    const OrderRequest& r, const std::string& client_order_id);
Result<Order> parse_spot_order(std::string_view json);
}  // namespace binance::detail

namespace {

Decimal d(std::string_view s) { return *Decimal::parse(s); }

/// BTCUSDT's futures rules (2026-11).
Instrument btc() {
    Instrument i;
    i.symbol = "BTCUSDT";
    i.trading = true;
    i.tick_size = d("0.1");
    i.min_price = d("261.1");
    i.max_price = d("809484");
    i.step_size = d("0.001");
    i.min_qty = d("0.001");
    i.max_qty = d("1000");
    i.market_step_size = d("0.001");
    i.market_min_qty = d("0.001");
    i.market_max_qty = d("120");
    i.min_notional = d("100");
    return i;
}

OrderRequest limit(std::string_view price, std::string_view qty) {
    OrderRequest r;
    r.symbol = "BTCUSDT";
    r.side = Side::Buy;
    r.type = OrderType::Limit;
    r.price = d(price);
    r.qty = d(qty);
    return r;
}

/// The validation error message, or "" if valid.
std::string check(const OrderRequest& r, std::optional<bool> hedge = std::nullopt) {
    auto e = detail::validate_futures_order(r, btc(), hedge);
    if (!e) return "";
    EXPECT_EQ(e->code, ErrorCode::InvalidArgument);
    return e->message;
}

bool contains(const std::string& s, std::string_view part) {
    return s.find(part) != std::string::npos;
}

/// BTCUSDT's spot rules (2026-11).
Instrument btc_spot() {
    Instrument i;
    i.market = Market::Spot;
    i.symbol = "BTCUSDT";
    i.trading = true;
    i.tick_size = d("0.01");
    i.min_price = d("0.01");
    i.max_price = d("1000000");
    i.step_size = d("0.00001");
    i.min_qty = d("0.00001");
    i.max_qty = d("9000");
    i.min_notional = d("5");
    return i;
}

OrderRequest spot_limit(std::string_view price, std::string_view qty) {
    OrderRequest r;
    r.symbol = "BTCUSDT";
    r.side = Side::Buy;
    r.type = OrderType::Limit;
    r.price = d(price);
    r.qty = d(qty);
    return r;
}

std::string check_spot(const OrderRequest& r) {
    auto e = detail::validate_spot_order(r, btc_spot());
    return e ? e->message : "";
}

std::string param(const std::vector<std::pair<std::string, std::string>>& p, std::string_view key) {
    for (const auto& [k, v] : p) {
        if (k == key) return v;
    }
    return "<none>";
}

}  // namespace

// --- Validation ------------------------------------------------------------------------------

TEST(OrderValidation, PricesAndQuantities) {
    EXPECT_EQ(check(limit("60000.1", "0.002")), "");
    EXPECT_TRUE(contains(check(limit("60000.17", "0.002")), "tick size 0.1"));
    EXPECT_TRUE(contains(check(limit("60000", "0.0025")), "step size 0.001"));
    EXPECT_TRUE(contains(check(limit("60000", "0")), "qty must be set and positive"));
    EXPECT_TRUE(contains(check(limit("100", "1")), "below the minimum 261.1"));
    EXPECT_TRUE(contains(check(limit("900000", "1")), "above the maximum"));
    EXPECT_TRUE(contains(check(limit("60000", "1001")), "above the maximum 1000"));
    // 60000 x 0.001 = 60 < 100
    EXPECT_TRUE(contains(check(limit("60000", "0.001")), "minimum notional 100"));
    auto reduce = limit("60000", "0.001");
    reduce.reduce_only = true;  // closing orders are exempt
    EXPECT_EQ(check(reduce), "");

    OrderRequest market;
    market.symbol = "BTCUSDT";
    market.type = OrderType::Market;
    market.qty = d("0.001");  // no notional check: Binance prices it
    EXPECT_EQ(check(market), "");
    market.qty = d("121");
    EXPECT_TRUE(contains(check(market), "above the maximum 120"));  // MARKET_LOT_SIZE
    market.qty = d("0.001");
    market.price = d("60000");
    EXPECT_TRUE(contains(check(market), "price is only for"));
}

TEST(OrderValidation, OrderTypes) {
    auto no_price = limit("1", "0.002");
    no_price.price.reset();
    EXPECT_TRUE(contains(check(no_price), "needs a price"));

    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopMarket;
    stop.qty = d("0.002");
    EXPECT_TRUE(contains(check(stop), "needs stop_price"));
    stop.stop_price = d("55000");
    EXPECT_EQ(check(stop), "");
    stop.close_position = true;
    EXPECT_TRUE(contains(check(stop), "leave qty unset"));
    stop.qty.reset();
    EXPECT_EQ(check(stop), "");
    stop.type = OrderType::Market;
    stop.stop_price.reset();
    EXPECT_TRUE(contains(check(stop), "close_position is only for"));

    OrderRequest stop_limit = limit("54000", "0.002");
    stop_limit.type = OrderType::StopLimit;
    stop_limit.stop_price = d("54000.05");
    EXPECT_TRUE(contains(check(stop_limit), "stop_price 54000.05 is not on the tick size"));

    OrderRequest trailing;
    trailing.symbol = "BTCUSDT";
    trailing.type = OrderType::TrailingStopMarket;
    trailing.qty = d("0.002");
    EXPECT_TRUE(contains(check(trailing), "needs callback_rate"));
    trailing.callback_rate = d("25");
    EXPECT_TRUE(contains(check(trailing), "between 0.1 and 20"));
    trailing.callback_rate = d("1");
    EXPECT_EQ(check(trailing), "");

    auto maker = limit("60000", "0.002");
    maker.type = OrderType::LimitMaker;
    EXPECT_TRUE(contains(check(maker), "GTX"));
    auto gtd = limit("60000", "0.002");
    gtd.time_in_force = TimeInForce::GTD;
    EXPECT_TRUE(contains(check(gtd), "needs good_till"));
    OrderRequest market_tif;
    market_tif.symbol = "BTCUSDT";
    market_tif.type = OrderType::Market;
    market_tif.qty = d("0.002");
    market_tif.time_in_force = TimeInForce::IOC;
    EXPECT_TRUE(contains(check(market_tif), "time_in_force is only for limit"));
}

TEST(OrderValidation, PositionMode) {
    auto plain = limit("60000", "0.002");
    EXPECT_EQ(check(plain, false), "");
    EXPECT_TRUE(contains(check(plain, true), "hedge mode: set position_side"));
    auto hedged = plain;
    hedged.position_side = PositionSide::Long;
    EXPECT_EQ(check(hedged, true), "");
    EXPECT_TRUE(contains(check(hedged, false), "one-way mode"));
    EXPECT_EQ(check(hedged, std::nullopt), "");  // mode unknown: Binance decides
    hedged.reduce_only = true;
    EXPECT_TRUE(contains(check(hedged, true), "no reduce_only"));

    Instrument halted = btc();
    halted.trading = false;
    EXPECT_TRUE(detail::validate_futures_order(plain, halted, std::nullopt).has_value());
}

// --- Request building and signing ------------------------------------------------------------

TEST(OrderParams, LimitAndAlgo) {
    using P = std::vector<std::pair<std::string, std::string>>;
    auto l = limit("60000.1", "0.002");
    EXPECT_EQ(detail::futures_order_params(l, "my-id"), (P{{"symbol", "BTCUSDT"},
                                                           {"side", "BUY"},
                                                           {"type", "LIMIT"},
                                                           {"quantity", "0.002"},
                                                           {"price", "60000.1"},
                                                           {"timeInForce", "GTC"},
                                                           {"newClientOrderId", "my-id"}}));

    OrderRequest s;
    s.symbol = "BTCUSDT";
    s.side = Side::Sell;
    s.type = OrderType::StopMarket;
    s.stop_price = d("55000");
    s.close_position = true;
    s.working_type = WorkingType::MarkPrice;
    s.position_side = PositionSide::Long;
    EXPECT_EQ(detail::futures_order_params(s, "algo-1"), (P{{"algoType", "CONDITIONAL"},
                                                            {"symbol", "BTCUSDT"},
                                                            {"side", "SELL"},
                                                            {"type", "STOP_MARKET"},
                                                            {"positionSide", "LONG"},
                                                            {"triggerPrice", "55000"},
                                                            {"closePosition", "true"},
                                                            {"workingType", "MARK_PRICE"},
                                                            {"clientAlgoId", "algo-1"}}));

    OrderRequest tp = limit("70000", "0.002");
    tp.side = Side::Sell;
    tp.type = OrderType::TakeProfitLimit;
    tp.stop_price = d("69900");
    tp.reduce_only = true;
    auto p = detail::futures_order_params(tp, "x");
    EXPECT_NE(
        std::find(p.begin(), p.end(), std::pair<std::string, std::string>{"type", "TAKE_PROFIT"}),
        p.end());
    EXPECT_NE(
        std::find(p.begin(), p.end(), std::pair<std::string, std::string>{"reduceOnly", "true"}),
        p.end());
    EXPECT_NE(
        std::find(p.begin(), p.end(), std::pair<std::string, std::string>{"timeInForce", "GTC"}),
        p.end());
}

TEST(OrderParams, WebSocketSignature) {
    const std::string json = detail::ws_signed_params(
        {{"symbol", "BTCUSDT"}, {"side", "BUY"}, {"quantity", "0.002"}, {"orderId", "42"}}, "KEY",
        "SECRET", 1700000000000, 5000);
    // Signed over every param sorted by name.
    const std::string payload =
        "apiKey=KEY&orderId=42&quantity=0.002&recvWindow=5000&side=BUY&symbol=BTCUSDT&"
        "timestamp=1700000000000";
    const std::string expected =
        R"({"apiKey":"KEY","orderId":42,"quantity":"0.002","recvWindow":5000,"side":"BUY",)"
        R"("symbol":"BTCUSDT","timestamp":1700000000000,"signature":")" +
        detail::hmac_sha256_hex("SECRET", payload) + R"("})";
    EXPECT_EQ(json, expected);
}

TEST(Trading, LocalErrors) {
    struct Quiet : Logger {
        void log(LogLevel, std::string_view) override {}
    };
    Config cfg;
    cfg.logger = std::make_shared<Quiet>();
    Client client(cfg);
    OrderRequest r = limit("60000", "0.002");
    EXPECT_EQ(client.create_order(Market::Futures, r).error().code,
              ErrorCode::InvalidArgument);  // no keys
    EXPECT_EQ(client.create_order(Market::Spot, r).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.set_leverage("BTCUSDT", 0).error().code, ErrorCode::InvalidArgument);

    cfg.credentials = {"key", "secret", ""};
    Client keyed(cfg);
    OrderRef nothing;
    nothing.symbol = "BTCUSDT";
    EXPECT_EQ(keyed.cancel_order(Market::Futures, nothing).error().code,
              ErrorCode::InvalidArgument);
    r.client_order_id = "has spaces";
    EXPECT_EQ(keyed.create_order(Market::Futures, r).error().code, ErrorCode::InvalidArgument);
}

// --- Spot ------------------------------------------------------------------------------------

TEST(SpotOrderValidation, Valid) {
    EXPECT_EQ(check_spot(spot_limit("36000.01", "0.0002")), "");
    OrderRequest maker = spot_limit("36000", "0.0002");
    maker.type = OrderType::LimitMaker;
    EXPECT_EQ(check_spot(maker), "");
    OrderRequest by_quote;
    by_quote.symbol = "BTCUSDT";
    by_quote.type = OrderType::Market;
    by_quote.quote_qty = d("10");
    EXPECT_EQ(check_spot(by_quote), "");
    OrderRequest stop = spot_limit("56000", "0.0002");
    stop.side = Side::Sell;
    stop.type = OrderType::StopLimit;
    stop.stop_price = d("57000");
    EXPECT_EQ(check_spot(stop), "");
}

TEST(SpotOrderValidation, RulesAndFields) {
    EXPECT_TRUE(contains(check_spot(spot_limit("36000.005", "0.0002")), "tick size 0.01"));
    EXPECT_TRUE(contains(check_spot(spot_limit("36000", "0.000025")), "step size 0.00001"));
    EXPECT_TRUE(contains(check_spot(spot_limit("36000", "0.0001")), "notional"));  // 3.6 < 5

    OrderRequest both;
    both.symbol = "BTCUSDT";
    both.type = OrderType::Market;
    both.qty = d("0.0002");
    both.quote_qty = d("10");
    EXPECT_TRUE(contains(check_spot(both), "not both"));
    OrderRequest small = both;
    small.qty.reset();
    small.quote_qty = d("1");
    EXPECT_TRUE(contains(check_spot(small), "quote_qty 1 is below the minimum notional 5"));
    OrderRequest quote_limit = spot_limit("36000", "0.0002");
    quote_limit.qty.reset();
    quote_limit.quote_qty = d("10");
    EXPECT_TRUE(contains(check_spot(quote_limit), "only for Market"));

    OrderRequest maker_tif = spot_limit("36000", "0.0002");
    maker_tif.type = OrderType::LimitMaker;
    maker_tif.time_in_force = TimeInForce::GTC;
    EXPECT_TRUE(contains(check_spot(maker_tif), "time_in_force"));
    OrderRequest no_stop = spot_limit("36000", "0.0002");
    no_stop.type = OrderType::StopLimit;
    EXPECT_TRUE(contains(check_spot(no_stop), "stop_price"));
}

TEST(SpotOrderValidation, FuturesOnlyThingsAreRefused) {
    OrderRequest trailing;
    trailing.symbol = "BTCUSDT";
    trailing.type = OrderType::TrailingStopMarket;
    trailing.qty = d("0.0002");
    EXPECT_TRUE(contains(check_spot(trailing), "futures only"));
    OrderRequest gtx = spot_limit("36000", "0.0002");
    gtx.time_in_force = TimeInForce::GTX;
    EXPECT_TRUE(contains(check_spot(gtx), "LimitMaker"));
    OrderRequest reduce = spot_limit("36000", "0.0002");
    reduce.reduce_only = true;
    EXPECT_TRUE(contains(check_spot(reduce), "futures only"));
    OrderRequest side = spot_limit("36000", "0.0002");
    side.position_side = PositionSide::Long;
    EXPECT_TRUE(contains(check_spot(side), "futures only"));
}

TEST(SpotOrderParams, BinanceNames) {
    OrderRequest stop = spot_limit("56000", "0.0002");
    stop.side = Side::Sell;
    stop.type = OrderType::StopLimit;
    stop.stop_price = d("57000");
    auto p = detail::spot_order_params(stop, "my-id");
    EXPECT_EQ(param(p, "type"), "STOP_LOSS_LIMIT");
    EXPECT_EQ(param(p, "side"), "SELL");
    EXPECT_EQ(param(p, "timeInForce"), "GTC");
    EXPECT_EQ(param(p, "stopPrice"), "57000");
    EXPECT_EQ(param(p, "newClientOrderId"), "my-id");
    EXPECT_EQ(param(p, "newOrderRespType"), "RESULT");

    OrderRequest by_quote;
    by_quote.symbol = "BTCUSDT";
    by_quote.type = OrderType::Market;
    by_quote.quote_qty = d("10");
    p = detail::spot_order_params(by_quote, "x");
    EXPECT_EQ(param(p, "type"), "MARKET");
    EXPECT_EQ(param(p, "quoteOrderQty"), "10");
    EXPECT_EQ(param(p, "quantity"), "<none>");
    EXPECT_EQ(param(p, "timeInForce"), "<none>");

    OrderRequest maker = spot_limit("36000", "0.0002");
    maker.type = OrderType::LimitMaker;
    p = detail::spot_order_params(maker, "x");
    EXPECT_EQ(param(p, "type"), "LIMIT_MAKER");
    EXPECT_EQ(param(p, "timeInForce"), "<none>");
}

TEST(SpotOrderAnswers, PlaceAndCancel) {
    // Placing (captured on spot Demo): "transactTime", no "time" / "updateTime".
    auto placed = detail::parse_spot_order(
        R"({"symbol":"BTCUSDT","orderId":85170551903,"orderListId":-1,)"
        R"("clientOrderId":"sprobe-lim","transactTime":1795283944842,"price":"36034.86000000",)"
        R"("origQty":"0.00020000","executedQty":"0.00000000","origQuoteOrderQty":"0.00000000",)"
        R"("cummulativeQuoteQty":"0.00000000","status":"NEW","timeInForce":"GTC","type":"LIMIT",)"
        R"("side":"BUY","workingTime":1795283944842,"fills":[],)"
        R"("selfTradePreventionMode":"EXPIRE_MAKER"})");
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    EXPECT_EQ(placed->market, Market::Spot);
    EXPECT_EQ(placed->status, OrderStatus::New);
    EXPECT_EQ(placed->client_order_id, "sprobe-lim");
    EXPECT_EQ(to_epoch_ms(placed->update_time), 1795283944842);
    EXPECT_EQ(to_epoch_ms(placed->create_time), 1795283944842);

    // Cancelling: "clientOrderId" is the cancel request's id, the order's is "origClientOrderId".
    auto cancelled = detail::parse_spot_order(
        R"({"symbol":"BTCUSDT","origClientOrderId":"sprobe-stop","orderId":85170640000,)"
        R"("orderListId":-1,"clientOrderId":"soxGV9zwwvwWffHo8thbqh",)"
        R"("transactTime":1795283976244,"price":"56469.61000000","origQty":"0.00020000",)"
        R"("executedQty":"0.00000000","origQuoteOrderQty":"0.00000000",)"
        R"("cummulativeQuoteQty":"0.00000000","status":"CANCELED","timeInForce":"GTC",)"
        R"("type":"STOP_LOSS_LIMIT","side":"SELL","stopPrice":"57070.35000000",)"
        R"("selfTradePreventionMode":"EXPIRE_MAKER"})");
    ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
    EXPECT_EQ(cancelled->client_order_id, "sprobe-stop");
    EXPECT_EQ(cancelled->status, OrderStatus::Canceled);
    EXPECT_EQ(cancelled->type, OrderType::StopLimit);
    EXPECT_EQ(cancelled->stop_price, d("57070.35"));
}

TEST(SpotTrading, FuturesOnlyCallsAreRefused) {
    Config cfg;
    cfg.credentials = {"key", "secret", ""};
    Client client(cfg);
    constexpr auto S = Market::Spot;
    EXPECT_EQ(client.amend_order(S, {"BTCUSDT", 1, std::nullopt, Side::Buy, d("0.001"), d("1")})
                  .error()
                  .code,
              ErrorCode::InvalidArgument);
    auto placed = client.create_orders(S, {spot_limit("36000", "0.0002")});
    ASSERT_EQ(placed.size(), 1u);
    EXPECT_TRUE(contains(placed[0].error().message, "futures only"));
    auto cancelled = client.cancel_orders(S, {{"BTCUSDT", 1}});
    EXPECT_EQ(cancelled[0].error().code, ErrorCode::InvalidArgument);
    OrderRef algo{"BTCUSDT", 1};
    algo.is_algo = true;
    EXPECT_EQ(client.cancel_order(S, algo).error().code, ErrorCode::InvalidArgument);
}

// --- Live, on Demo trading -------------------------------------------------------------------
// Needs BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY and BINANCE_DEMO_API_SECRET.
// Uses small BTCUSDT orders and a 0.003 BTC position; leaves the account as it found it.

namespace {

class Recording : public Logger {
public:
    void log(LogLevel level, std::string_view msg) override {
        if (level >= LogLevel::Warn) std::cerr << "[binance] " << msg << "\n";
        std::lock_guard lock(mu_);
        lines_.emplace_back(msg);
    }
    bool saw(std::string_view part) { return count(part) > 0; }
    int count(std::string_view part) {
        std::lock_guard lock(mu_);
        int n = 0;
        for (const auto& l : lines_) {
            if (contains(l, part)) ++n;
        }
        return n;
    }

private:
    std::mutex mu_;
    std::vector<std::string> lines_;
};

}  // namespace

class LiveTrading : public ::testing::Test {
protected:
    void SetUp() override {
        const char* key = std::getenv("BINANCE_DEMO_API_KEY");
        const char* secret = std::getenv("BINANCE_DEMO_API_SECRET");
        if (!std::getenv("BINANCE_LIVE_TESTS") || !key || !secret) {
            GTEST_SKIP()
                << "set BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET";
        }
        logger = std::make_shared<Recording>();
        Config cfg;
        cfg.environment = Environment::Demo;
        cfg.credentials = {key, secret, ""};
        cfg.logger = logger;
        client = std::make_unique<Client>(cfg);
        ASSERT_TRUE(client->watch_open_orders(F, "BTCUSDT").ok());
        ASSERT_TRUE(client->watch_algo_orders("BTCUSDT").ok());
        ASSERT_TRUE(client->watch_positions().ok());
        cleanup();
        ASSERT_TRUE(client->load_instruments(F).ok());
        // A BUY price 40% under the market: rests without filling.
        auto mark_price = client->get_mark_price_rest_api("BTCUSDT");
        ASSERT_TRUE(mark_price.ok()) << mark_price.error().message;
        const Decimal mark = mark_price->mark_price;
        far = *client->round_price(F, "BTCUSDT", *Decimal::mul(mark, d("0.6")), Rounding::Down);
    }

    void TearDown() override {
        if (!client) return;
        cleanup();
        (void)client->set_position_mode(false);
    }

    /// No BTCUSDT orders, no position.
    void cleanup() {
        ASSERT_TRUE(client->cancel_all_orders(F, "BTCUSDT").ok());
        std::this_thread::sleep_for(500ms);
        // Keep the Result alive: a range-for over a member of a temporary would dangle.
        const auto positions = btc_position();
        for (const auto& [side, p] : positions) {
            OrderRequest close;
            close.symbol = "BTCUSDT";
            close.type = OrderType::Market;
            close.side = p.amount.is_negative() ? Side::Buy : Side::Sell;
            close.qty = p.amount.is_negative() ? -p.amount : p.amount;
            if (p.side == PositionSide::Both) {
                close.reduce_only = true;
            } else {
                close.position_side = p.side;
            }
            auto r = client->create_order(F, close);
            EXPECT_TRUE(r.ok()) << r.error().message;
        }
        wait_until([&] { return btc_position().empty(); });
    }

    std::map<PositionSide, Position> btc_position() {
        auto all = client->watch_positions();
        EXPECT_TRUE(all.ok()) << all.error().message;
        if (!all) return {};
        auto it = all->find("BTCUSDT");
        return it == all->end() ? std::map<PositionSide, Position>{} : it->second;
    }

    template <class Pred>
    bool wait_until(Pred pred, std::chrono::seconds timeout = 15s) {
        const auto end = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < end) {
            if (pred()) return true;
            std::this_thread::sleep_for(100ms);
        }
        return pred();
    }

    OrderRequest far_limit(std::string_view qty = "0.003") {
        OrderRequest r;
        r.symbol = "BTCUSDT";
        r.side = Side::Buy;
        r.type = OrderType::Limit;
        r.price = far;
        r.qty = d(qty);
        return r;
    }

    static constexpr auto F = Market::Futures;
    std::shared_ptr<Recording> logger;
    std::unique_ptr<Client> client;
    Decimal far;
};

TEST_F(LiveTrading, PlaceAmendCancelOverRestAndWebSocket) {
    // The first order goes over REST while the WebSocket API session starts; later ones over it.
    for (int round = 0; round < 2; ++round) {
        SCOPED_TRACE(round == 0 ? "first order" : "WebSocket API");
        if (round == 1)
            ASSERT_TRUE(wait_until([&] { return logger->saw("trading session connected"); }));

        auto placed = client->create_order(F, far_limit());
        ASSERT_TRUE(placed.ok()) << placed.error().message;
        EXPECT_EQ(placed->status, OrderStatus::New);
        EXPECT_EQ(placed->price, far);
        EXPECT_TRUE(placed->client_order_id.starts_with("bcpp-"));
        EXPECT_FALSE(placed->is_algo);
        ASSERT_TRUE(
            wait_until([&] { return client->watch_open_orders(F, "BTCUSDT")->size() == 1; }));

        const Decimal lower = far - d("100");
        auto amended = client->amend_order(
            F, {"BTCUSDT", placed->id, std::nullopt, Side::Buy, d("0.004"), lower});
        ASSERT_TRUE(amended.ok()) << amended.error().message;
        EXPECT_EQ(amended->price, lower);
        EXPECT_EQ(amended->qty, d("0.004"));
        EXPECT_EQ(amended->id, placed->id);

        auto fetched =
            client->get_order_rest_api(F, {"BTCUSDT", std::nullopt, placed->client_order_id});
        ASSERT_TRUE(fetched.ok()) << fetched.error().message;
        EXPECT_EQ(fetched->id, placed->id);

        auto cancelled = client->cancel_order(F, {"BTCUSDT", placed->id});
        ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
        EXPECT_EQ(cancelled->status, OrderStatus::Canceled);
        // Cancelling again: the order already ended; its final state comes back.
        auto again = client->cancel_order(F, {"BTCUSDT", placed->id});
        ASSERT_TRUE(again.ok()) << again.error().message;
        EXPECT_EQ(again->status, OrderStatus::Canceled);
        ASSERT_TRUE(wait_until([&] { return client->watch_open_orders(F, "BTCUSDT")->empty(); }));
    }
    EXPECT_TRUE(logger->saw("order.place over the WebSocket API"));
    EXPECT_TRUE(logger->saw("order.modify over the WebSocket API"));
    EXPECT_TRUE(logger->saw("order.cancel over the WebSocket API"));
}

TEST_F(LiveTrading, RestApiTwinsNeverUseTheWebSocket) {
    // Bring the WebSocket API session up first, then use only the *_rest_api forms.
    ASSERT_TRUE(client->create_order(F, far_limit()).ok());
    ASSERT_TRUE(wait_until([&] { return logger->saw("trading session connected"); }));
    ASSERT_TRUE(client->cancel_all_orders(F, "BTCUSDT").ok());
    const int over_ws = logger->count("over the WebSocket API");

    auto placed = client->create_order_rest_api(F, far_limit());
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    EXPECT_EQ(placed->status, OrderStatus::New);
    const Decimal lower = far - d("100");
    auto amended = client->amend_order_rest_api(
        F, {"BTCUSDT", placed->id, std::nullopt, Side::Buy, d("0.004"), lower});
    ASSERT_TRUE(amended.ok()) << amended.error().message;
    EXPECT_EQ(amended->price, lower);
    auto cancelled = client->cancel_order_rest_api(F, {"BTCUSDT", placed->id});
    ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
    EXPECT_EQ(cancelled->status, OrderStatus::Canceled);

    EXPECT_EQ(logger->count("over the WebSocket API"), over_ws);
    // Amend is futures only.
    EXPECT_EQ(client
                  ->amend_order_rest_api(
                      Market::Spot, {"BTCUSDT", 1, std::nullopt, Side::Buy, d("0.001"), d("1")})
                  .error()
                  .code,
              ErrorCode::InvalidArgument);
}

TEST_F(LiveTrading, AlgoOrdersAndCancelAll) {
    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopMarket;
    stop.stop_price = far;  // a sell stop far below the market: does not trigger
    stop.qty = d("0.003");
    stop.client_order_id =
        "t-stop-" +
        std::to_string(std::chrono::system_clock::now().time_since_epoch().count() % 1000000000);
    auto placed = client->create_order(F, stop);
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    EXPECT_TRUE(placed->is_algo);
    EXPECT_EQ(placed->type, OrderType::StopMarket);
    EXPECT_EQ(placed->stop_price, far);
    EXPECT_EQ(placed->client_order_id, *stop.client_order_id);

    OrderRef ref;
    ref.client_order_id = stop.client_order_id;
    ref.is_algo = true;
    auto cancelled = client->cancel_order(F, ref);
    ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
    EXPECT_EQ(cancelled->status, OrderStatus::Canceled);

    ASSERT_TRUE(client->create_order(F, far_limit()).ok());
    stop.client_order_id.reset();
    ASSERT_TRUE(client->create_order(F, stop).ok());
    ASSERT_TRUE(wait_until([&] {
        return client->watch_open_orders(F, "BTCUSDT")->size() == 1 &&
               client->watch_algo_orders("BTCUSDT")->size() == 1;
    }));
    ASSERT_TRUE(client->cancel_all_orders(F, "BTCUSDT").ok());
    EXPECT_TRUE(wait_until([&] {
        return client->watch_open_orders(F, "BTCUSDT")->empty() &&
               client->watch_algo_orders("BTCUSDT")->empty();
    }));
}

TEST_F(LiveTrading, ValidationRejectsLocally) {
    auto off_tick = far_limit();
    off_tick.price = far + d("0.05");
    auto r = client->create_order(F, off_tick);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::InvalidArgument);
    EXPECT_TRUE(contains(r.error().message, "tick size"));
    auto unknown = far_limit();
    unknown.symbol = "NOTACOINUSDT";
    EXPECT_EQ(client->create_order(F, unknown).error().code, ErrorCode::InvalidArgument);
    std::this_thread::sleep_for(500ms);
    EXPECT_TRUE(client->watch_open_orders(F, "BTCUSDT")->empty());
}

TEST_F(LiveTrading, MarketRoundTrip) {
    OrderRequest buy;
    buy.symbol = "BTCUSDT";
    buy.side = Side::Buy;
    buy.type = OrderType::Market;
    buy.qty = d("0.003");
    auto opened = client->create_order(F, buy);
    ASSERT_TRUE(opened.ok()) << opened.error().message;
    ASSERT_TRUE(wait_until([&] {
        auto p = btc_position();
        return p.size() == 1 && p.begin()->second.amount == d("0.003");
    }));

    OrderRequest sell = buy;
    sell.side = Side::Sell;
    sell.reduce_only = true;
    auto closed = client->create_order(F, sell);
    ASSERT_TRUE(closed.ok()) << closed.error().message;
    EXPECT_TRUE(closed->reduce_only);
    EXPECT_TRUE(wait_until([&] { return btc_position().empty(); }));
}

TEST_F(LiveTrading, AccountSettings) {
    // Leverage: a watched position follows the change (ACCOUNT_CONFIG_UPDATE).
    OrderRequest buy;
    buy.symbol = "BTCUSDT";
    buy.side = Side::Buy;
    buy.type = OrderType::Market;
    buy.qty = d("0.003");
    ASSERT_TRUE(client->create_order(F, buy).ok());
    auto leverage = [&] {
        auto p = btc_position();
        return !p.empty() ? p.begin()->second.leverage : 0;
    };
    ASSERT_TRUE(wait_until([&] { return leverage() > 0; }));
    const int original = leverage();
    const int other = original == 10 ? 11 : 10;
    ASSERT_TRUE(client->set_leverage("BTCUSDT", other).ok());
    EXPECT_TRUE(wait_until([&] { return leverage() == other; }));
    ASSERT_TRUE(client->set_leverage("BTCUSDT", original).ok());
    EXPECT_TRUE(wait_until([&] { return leverage() == original; }));
    cleanup();  // position mode can only change without positions

    EXPECT_TRUE(client->set_margin_type("BTCUSDT", MarginType::Cross).ok());  // already

    // Hedge mode: the account reloads, and orders then need a position side.
    ASSERT_TRUE(client->set_position_mode(true).ok());
    // Checked locally once the reload has the new mode (before that Binance refuses it: -4061).
    ASSERT_TRUE(wait_until([&] {
        auto r = client->create_order(F, far_limit());
        return !r.ok() && contains(r.error().message, "hedge mode: set position_side");
    }));
    auto with_side = far_limit();
    with_side.position_side = PositionSide::Long;
    auto placed = client->create_order(F, with_side);
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    EXPECT_EQ(placed->position_side, PositionSide::Long);
    ASSERT_TRUE(client->cancel_all_orders(F, "BTCUSDT").ok());
    std::this_thread::sleep_for(500ms);

    ASSERT_TRUE(client->set_position_mode(false).ok());
    ASSERT_TRUE(wait_until([&] { return client->create_order(F, far_limit()).ok(); }));
    ASSERT_TRUE(client->cancel_all_orders(F, "BTCUSDT").ok());
    EXPECT_TRUE(client->set_position_mode(false).ok());  // already

    auto fee = client->get_commission_rate_rest_api("BTCUSDT");
    ASSERT_TRUE(fee.ok()) << fee.error().message;
    EXPECT_TRUE(fee->taker.is_positive());
    EXPECT_GE(fee->taker, fee->maker);
}

// --- Live, on spot Demo trading (same keys) --------------------------------------------------
// Buys and sells back about 10 USDT of BTC (fees in BNB); other orders are cancelled.

class LiveSpotTrading : public ::testing::Test {
protected:
    void SetUp() override {
        const char* key = std::getenv("BINANCE_DEMO_API_KEY");
        const char* secret = std::getenv("BINANCE_DEMO_API_SECRET");
        if (!std::getenv("BINANCE_LIVE_TESTS") || !key || !secret) {
            GTEST_SKIP()
                << "set BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET";
        }
        logger = std::make_shared<Recording>();
        Config cfg;
        cfg.environment = Environment::Demo;
        cfg.credentials = {key, secret, ""};
        cfg.logger = logger;
        client = std::make_unique<Client>(cfg);
        ASSERT_TRUE(client->cancel_all_orders(S, "BTCUSDT").ok());
        ASSERT_TRUE(client->load_instruments(S).ok());
        auto ticker = client->get_book_ticker_rest_api(S, "BTCUSDT");
        ASSERT_TRUE(ticker.ok()) << ticker.error().message;
        bid = ticker->bid_price;
        // 40% under the market: a BUY rests without filling.
        far = *client->round_price(S, "BTCUSDT", *Decimal::mul(bid, d("0.6")), Rounding::Down);
    }
    void TearDown() override {
        if (client) (void)client->cancel_all_orders(S, "BTCUSDT");
    }

    template <class Pred>
    bool eventually(Pred pred, std::chrono::seconds timeout = 15s) {
        const auto end = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < end) {
            if (pred()) return true;
            std::this_thread::sleep_for(100ms);
        }
        return pred();
    }

    std::shared_ptr<Recording> logger;
    std::unique_ptr<Client> client;
    Decimal bid, far;
    static constexpr auto S = Market::Spot;
};

TEST_F(LiveSpotTrading, PlaceAndCancelOverWebSocketAndRest) {
    // The first order goes over REST while the WebSocket API session starts; then over it.
    ASSERT_TRUE(client->watch_open_orders(S, "BTCUSDT").ok());
    for (int round = 0; round < 2; ++round) {
        SCOPED_TRACE(round == 0 ? "first order" : "WebSocket API");
        if (round == 1) {
            ASSERT_TRUE(
                eventually([&] { return logger->count("spot trading session connected"); }));
        }
        auto placed = client->create_order(S, spot_limit(far.to_string(), "0.0002"));
        ASSERT_TRUE(placed.ok()) << placed.error().message;
        EXPECT_EQ(placed->market, Market::Spot);
        EXPECT_EQ(placed->status, OrderStatus::New);
        EXPECT_EQ(placed->price, far);
        EXPECT_TRUE(placed->client_order_id.starts_with("bcpp-"));
        ASSERT_TRUE(
            eventually([&] { return client->watch_open_orders(S, "BTCUSDT")->size() == 1; }));

        auto cancelled = client->cancel_order(S, {"BTCUSDT", placed->id});
        ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
        EXPECT_EQ(cancelled->status, OrderStatus::Canceled);
        EXPECT_EQ(cancelled->client_order_id, placed->client_order_id);  // not the cancel's id
        // Again: the order already ended; its final state comes back.
        auto again = client->cancel_order(S, {"BTCUSDT", placed->id});
        ASSERT_TRUE(again.ok()) << again.error().message;
        EXPECT_EQ(again->status, OrderStatus::Canceled);
        ASSERT_TRUE(eventually([&] { return client->watch_open_orders(S, "BTCUSDT")->empty(); }));
    }
    EXPECT_GT(logger->count("order.place over the WebSocket API"), 0);
    EXPECT_GT(logger->count("order.cancel over the WebSocket API"), 0);

    // The *_rest_api twins never use the WebSocket API.
    const int over_ws = logger->count("over the WebSocket API");
    auto placed = client->create_order_rest_api(S, spot_limit(far.to_string(), "0.0002"));
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    auto cancelled =
        client->cancel_order_rest_api(S, {"BTCUSDT", std::nullopt, placed->client_order_id});
    ASSERT_TRUE(cancelled.ok()) << cancelled.error().message;
    EXPECT_EQ(cancelled->status, OrderStatus::Canceled);
    EXPECT_EQ(logger->count("over the WebSocket API"), over_ws);
}

TEST_F(LiveSpotTrading, MarketByQuoteAmountAndBack) {
    OrderRequest buy;
    buy.symbol = "BTCUSDT";
    buy.side = Side::Buy;
    buy.type = OrderType::Market;
    buy.quote_qty = d("10");
    auto bought = client->create_order(S, buy);
    ASSERT_TRUE(bought.ok()) << bought.error().message;
    EXPECT_EQ(bought->status, OrderStatus::Filled);
    EXPECT_TRUE(bought->filled_qty.is_positive());
    EXPECT_TRUE(bought->avg_price.is_positive());
    EXPECT_LE(bought->filled_quote_qty, d("10"));

    OrderRequest sell;
    sell.symbol = "BTCUSDT";
    sell.side = Side::Sell;
    sell.type = OrderType::Market;
    sell.qty = bought->filled_qty;
    auto sold = client->create_order(S, sell);
    ASSERT_TRUE(sold.ok()) << sold.error().message;
    EXPECT_EQ(sold->status, OrderStatus::Filled);
    EXPECT_EQ(sold->filled_qty, bought->filled_qty);
}

TEST_F(LiveSpotTrading, StopOrderValidationAndCancelAll) {
    // A sell stop-loss 5% under the market (the price band refuses farther ones).
    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopLimit;
    stop.qty = d("0.0002");
    stop.stop_price = *client->round_price(S, "BTCUSDT", *Decimal::mul(bid, d("0.95")));
    stop.price = *client->round_price(S, "BTCUSDT", *Decimal::mul(bid, d("0.94")));
    auto placed = client->create_order(S, stop);
    ASSERT_TRUE(placed.ok()) << placed.error().message;
    EXPECT_EQ(placed->type, OrderType::StopLimit);
    EXPECT_EQ(placed->stop_price, *stop.stop_price);
    ASSERT_TRUE(client->create_order(S, spot_limit(far.to_string(), "0.0002")).ok());
    EXPECT_EQ(client->get_open_orders_rest_api(S, "BTCUSDT")->size(), 2u);

    // Off the tick size: refused locally, nothing sent.
    auto off_tick = client->create_order(S, spot_limit((far + d("0.005")).to_string(), "0.0002"));
    ASSERT_FALSE(off_tick.ok());
    EXPECT_EQ(off_tick.error().code, ErrorCode::InvalidArgument);

    ASSERT_TRUE(client->cancel_all_orders(S, "BTCUSDT").ok());
    EXPECT_TRUE(client->get_open_orders_rest_api(S, "BTCUSDT")->empty());
    EXPECT_TRUE(client->cancel_all_orders(S, "BTCUSDT").ok());  // nothing left: still fine
}
