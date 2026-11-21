// Event and REST samples below were captured from demo-fapi / demo-fstream on 2026-11-20.

#include <binance/client.hpp>

#include "http.hpp"

#include <gtest/gtest.h>

#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>

#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <future>
#include <iostream>
#include <map>
#include <mutex>
#include <string>
#include <thread>

using namespace binance;
using namespace std::chrono_literals;

// Internals of client.cpp under test. The structs below must match client.cpp exactly.
namespace binance::detail {

struct AccountSettings {
    bool hedge_mode = false;
    bool multi_assets_margin = false;
};

struct SymbolSettings {
    std::string symbol;
    int leverage = 0;
    MarginType margin_type = MarginType::Cross;
};

struct UserEvent {
    enum class Kind {
        Order,
        Account,
        Config,
        MarginCall,
        ListenKeyExpired,
        StreamTerminated,
        Other
    };
    Kind kind = Kind::Other;
    std::int64_t time = 0;
    std::optional<Order> order;
    std::vector<Position> positions;
    std::vector<Balance> balances;
    std::string symbol;
    int leverage = 0;
    std::optional<bool> multi_assets;
};

struct TestHook {
    static void drop_account_events(Client& c, Market market, bool drop);
    static void reconnect_account(Client& c, Market market);
    static void drop_next_book_update(Client& c, Market market, const std::string& symbol);
    static std::string book_status(Client& c, Market market, const std::string& symbol);
};

std::string hmac_sha256_hex(std::string_view key, std::string_view data);
Result<UserEvent> parse_user_event(std::string_view json);
Result<std::vector<Order>> parse_open_orders(std::string_view json);
Result<std::vector<Order>> parse_open_algo_orders(std::string_view json);
Result<Order> parse_order(std::string_view json);
Result<Order> parse_algo_order(std::string_view json);
Result<std::vector<Position>> parse_position_risk(std::string_view json);
Result<std::vector<Balance>> parse_futures_balances(std::string_view json);
Result<std::vector<SymbolSettings>> parse_symbol_config(std::string_view json);
Result<AccountSettings> parse_account_config(std::string_view json);
Result<UserEvent> parse_spot_user_event(std::string_view json);
Result<std::vector<Balance>> parse_spot_account(std::string_view json);
Result<std::vector<Order>> parse_spot_open_orders(std::string_view json);
Result<Order> parse_spot_order(std::string_view json);
std::pair<bool, std::vector<std::string>> plan_orders_for_test(
    bool all, bool all_loaded, const std::map<std::string, bool>& symbols, bool full,
    Market market);
std::pair<bool, bool> order_scope_for_test(bool all, bool all_loaded,
                                           const std::map<std::string, bool>& symbols,
                                           const std::string& symbol);

}  // namespace binance::detail

namespace {

using Kind = detail::UserEvent::Kind;

Decimal d(std::string_view s) { return *Decimal::parse(s); }

detail::UserEvent event(std::string_view json) {
    auto r = detail::parse_user_event(json);
    EXPECT_TRUE(r.ok()) << r.error().message;
    return r.ok() ? *r : detail::UserEvent{};
}

}  // namespace

// --- Stream events ---------------------------------------------------------------------------

TEST(AccountParse, OrderNew) {
    auto ev = event(
        R"({"e":"ORDER_TRADE_UPDATE","T":1795208024163,"E":1795208024180,"o":{"s":"BTCUSDT","c":"probe-limit-1","S":"BUY","o":"LIMIT","f":"GTC","q":"0.003","p":"35058.7","ap":"0","sp":"0","x":"NEW","X":"NEW","i":28688769289,"l":"0","z":"0","L":"0","n":"0","N":"USDT","T":1795208024163,"t":0,"b":"105.1761","a":"0","m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"BOTH","cp":false,"rp":"0","pP":false,"si":0,"ss":0,"V":"EXPIRE_MAKER","pm":"NONE","gtd":0,"er":"0"}})");
    ASSERT_EQ(ev.kind, Kind::Order);
    EXPECT_EQ(ev.time, 1795208024180);
    ASSERT_TRUE(ev.order.has_value());
    const Order& o = *ev.order;
    EXPECT_FALSE(o.is_algo);
    EXPECT_EQ(o.id, 28688769289);
    EXPECT_EQ(o.client_order_id, "probe-limit-1");
    EXPECT_EQ(o.symbol, "BTCUSDT");
    EXPECT_EQ(o.side, Side::Buy);
    EXPECT_EQ(o.type, OrderType::Limit);
    EXPECT_EQ(o.time_in_force, TimeInForce::GTC);
    EXPECT_EQ(o.status, OrderStatus::New);
    EXPECT_EQ(o.price, d("35058.7"));
    EXPECT_EQ(o.qty, d("0.003"));
    EXPECT_TRUE(o.filled_qty.is_zero());
    EXPECT_EQ(o.position_side, PositionSide::Both);
    EXPECT_EQ(to_epoch_ms(o.update_time), 1795208024163);
}

TEST(AccountParse, OrderFill) {
    auto ev = event(
        R"({"e":"ORDER_TRADE_UPDATE","T":1795208035527,"E":1795208035556,"o":{"s":"BTCUSDT","c":"probe-mkt-1","S":"BUY","o":"MARKET","f":"GTC","q":"0.003","p":"0","ap":"58457.1","sp":"0","x":"TRADE","X":"FILLED","i":28688769407,"l":"0.003","z":"0.003","L":"58457.1","n":"0.07014852","N":"USDT","T":1795208035527,"t":559723763,"b":"0","a":"0","m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"MARKET","ps":"BOTH","cp":false,"rp":"0","pP":false,"si":0,"ss":0,"V":"EXPIRE_MAKER","pm":"NONE","gtd":0,"er":"0"}})");
    ASSERT_TRUE(ev.order.has_value());
    EXPECT_EQ(ev.order->status, OrderStatus::Filled);
    EXPECT_EQ(ev.order->type, OrderType::Market);
    EXPECT_EQ(ev.order->avg_price, d("58457.1"));
    EXPECT_EQ(ev.order->filled_qty, d("0.003"));
    EXPECT_EQ(ev.order->filled_quote_qty, d("175.3713"));

    auto close = event(
        R"({"e":"ORDER_TRADE_UPDATE","T":1795208038031,"E":1795208038036,"o":{"s":"BTCUSDT","c":"probe-mkt-2","S":"SELL","o":"MARKET","f":"GTC","q":"0.003","p":"0","ap":"58455.6","sp":"0","x":"TRADE","X":"FILLED","i":28688769446,"l":"0.003","z":"0.003","L":"58455.6","n":"0.07014672","N":"USDT","T":1795208038031,"t":559723768,"b":"0","a":"0","m":false,"R":true,"wt":"CONTRACT_PRICE","ot":"MARKET","ps":"BOTH","cp":false,"rp":"-0.0045","pP":false,"si":0,"ss":0,"V":"EXPIRE_MAKER","pm":"NONE","gtd":0,"er":"0"}})");
    EXPECT_TRUE(close.order->reduce_only);
}

TEST(AccountParse, AlgoOrder) {
    auto ev = event(
        R"({"e":"ALGO_UPDATE","T":1795208028888,"E":1795208028888,"o":{"caid":"probe-algo-1","aid":1000000295570420,"at":"CONDITIONAL","o":"STOP_MARKET","s":"BTCUSDT","S":"SELL","ps":"BOTH","f":"GTC","q":"0.003","X":"NEW","ai":"","tp":"35058.7","p":"0","V":"EXPIRE_MAKER","wt":"CONTRACT_PRICE","pm":"NONE","cp":false,"pP":false,"R":false,"tt":0,"gtd":0,"ia":false}})");
    ASSERT_EQ(ev.kind, Kind::Order);
    ASSERT_TRUE(ev.order.has_value());
    const Order& o = *ev.order;
    EXPECT_TRUE(o.is_algo);
    EXPECT_EQ(o.id, 1000000295570420);
    EXPECT_EQ(o.client_order_id, "probe-algo-1");
    EXPECT_EQ(o.type, OrderType::StopMarket);
    EXPECT_EQ(o.side, Side::Sell);
    EXPECT_EQ(o.status, OrderStatus::New);
    EXPECT_EQ(o.stop_price, d("35058.7"));
    EXPECT_EQ(o.triggered_order_id, 0);
    EXPECT_EQ(to_epoch_ms(o.update_time), 1795208028888);

    auto cancelled = event(
        R"({"e":"ALGO_UPDATE","T":1795208031240,"E":1795208031244,"o":{"caid":"probe-algo-1","aid":1000000295570420,"at":"CONDITIONAL","o":"STOP_MARKET","s":"BTCUSDT","S":"SELL","ps":"BOTH","f":"GTC","q":"0.003","X":"CANCELED","ai":"","tp":"35058.7","p":"0","V":"EXPIRE_MAKER","wt":"CONTRACT_PRICE","pm":"NONE","cp":false,"pP":false,"R":false,"tt":0,"gtd":0,"ia":false}})");
    EXPECT_EQ(cancelled.order->status, OrderStatus::Canceled);

    // Triggered: the real order's id comes as a string.
    auto triggered = event(
        R"({"e":"ALGO_UPDATE","T":5,"E":6,"o":{"caid":"x","aid":7,"at":"CONDITIONAL","o":"TAKE_PROFIT","s":"BTCUSDT","S":"SELL","ps":"LONG","f":"GTE_GTC","q":"0.01","X":"TRIGGERED","ai":"28688769999","tp":"70000","p":"70100","wt":"MARK_PRICE","cp":true,"R":false,"ap":"0","aq":"0"}})");
    EXPECT_EQ(triggered.order->status, OrderStatus::Triggered);
    EXPECT_EQ(triggered.order->triggered_order_id, 28688769999);
    EXPECT_EQ(triggered.order->type, OrderType::TakeProfitLimit);
    EXPECT_EQ(triggered.order->time_in_force, TimeInForce::GTC);
    EXPECT_EQ(triggered.order->working_type, WorkingType::MarkPrice);
    EXPECT_EQ(triggered.order->position_side, PositionSide::Long);
    EXPECT_TRUE(triggered.order->close_position);
}

TEST(AccountParse, AccountUpdate) {
    auto ev = event(
        R"({"e":"ACCOUNT_UPDATE","T":1795208035555,"E":1795208035556,"a":{"B":[{"a":"USDT","wb":"5017.40511412","cw":"5017.40511412","bc":"0"}],"P":[{"s":"BTCUSDT","pa":"0.003","ep":"58457.1","cr":"28.27239999","up":"0.00152123","mt":"cross","iw":"0","ps":"BOTH","ma":"USDT","bep":"58480.48284"}],"m":"ORDER"}})");
    ASSERT_EQ(ev.kind, Kind::Account);
    ASSERT_EQ(ev.balances.size(), 1u);
    EXPECT_EQ(ev.balances[0].asset, "USDT");
    EXPECT_EQ(ev.balances[0].total, d("5017.40511412"));
    ASSERT_EQ(ev.positions.size(), 1u);
    const Position& p = ev.positions[0];
    EXPECT_EQ(p.symbol, "BTCUSDT");
    EXPECT_EQ(p.side, PositionSide::Both);
    EXPECT_EQ(p.amount, d("0.003"));
    EXPECT_EQ(p.entry_price, d("58457.1"));
    EXPECT_EQ(p.break_even_price, d("58480.48284"));
    EXPECT_EQ(p.unrealized_pnl, d("0.00152123"));
    EXPECT_EQ(p.margin_type, MarginType::Cross);
    EXPECT_EQ(to_epoch_ms(p.update_time), 1795208035556);

    // Funding fee on a cross position: balances only. Short in hedge mode, isolated.
    auto funding = event(
        R"({"e":"ACCOUNT_UPDATE","T":1,"E":2,"a":{"m":"FUNDING_FEE","S":"ETHUSDT","B":[{"a":"USDT","wb":"100.5","cw":"100.5","bc":"0"}],"P":[]}})");
    EXPECT_TRUE(funding.positions.empty());
    auto shrt = event(
        R"({"e":"ACCOUNT_UPDATE","T":1,"E":2,"a":{"m":"ORDER","B":[],"P":[{"s":"ETHUSDT","pa":"-1.5","ep":"3000","cr":"0","up":"-2","mt":"isolated","iw":"150.25","ps":"SHORT","ma":"USDT","bep":"3001"}]}})");
    ASSERT_EQ(shrt.positions.size(), 1u);
    EXPECT_EQ(shrt.positions[0].amount, d("-1.5"));
    EXPECT_EQ(shrt.positions[0].side, PositionSide::Short);
    EXPECT_EQ(shrt.positions[0].margin_type, MarginType::Isolated);
    EXPECT_EQ(shrt.positions[0].isolated_wallet, d("150.25"));
}

TEST(AccountParse, ConfigMarginCallAndExpiry) {
    auto lev = event(
        R"({"e":"ACCOUNT_CONFIG_UPDATE","T":1795208033009,"E":1795208033009,"ac":{"s":"BTCUSDT","l":10}})");
    ASSERT_EQ(lev.kind, Kind::Config);
    EXPECT_EQ(lev.symbol, "BTCUSDT");
    EXPECT_EQ(lev.leverage, 10);
    EXPECT_FALSE(lev.multi_assets.has_value());

    auto multi = event(R"({"e":"ACCOUNT_CONFIG_UPDATE","T":1,"E":2,"ai":{"j":true}})");
    ASSERT_EQ(multi.kind, Kind::Config);
    EXPECT_TRUE(multi.symbol.empty());
    EXPECT_EQ(multi.multi_assets, std::optional<bool>(true));

    auto call = event(
        R"({"e":"MARGIN_CALL","E":1587727187525,"cw":"3.16812045","p":[{"s":"ETHUSDT","ps":"LONG","pa":"1.327","mt":"CROSSED","iw":"0","mp":"187.17127","up":"-1.166074","mm":"1.614445"}]})");
    ASSERT_EQ(call.kind, Kind::MarginCall);
    ASSERT_EQ(call.positions.size(), 1u);
    EXPECT_EQ(call.positions[0].unrealized_pnl, d("-1.166074"));
    EXPECT_EQ(call.positions[0].side, PositionSide::Long);

    // As sent by demo-fstream: E is a string, spaces after the colons.
    auto expired =
        event(R"({"e": "listenKeyExpired","E": "1795208042302","listenKey": "redacted"})");
    EXPECT_EQ(expired.kind, Kind::ListenKeyExpired);
    EXPECT_EQ(expired.time, 1795208042302);

    // TRADE_LITE duplicates ORDER_TRADE_UPDATE with fewer fields: ignored.
    auto lite = event(
        R"({"e":"TRADE_LITE","E":1795208035555,"T":1795208035527,"s":"BTCUSDT","q":"0.0030","p":"0.00","m":false,"c":"probe-mkt-1","S":"BUY","L":"58457.10","l":"0.0030","t":559723763,"i":28688769407})");
    EXPECT_EQ(lite.kind, Kind::Other);
}

TEST(AccountParse, RejectsGarbage) {
    EXPECT_FALSE(detail::parse_user_event("not json").ok());
    EXPECT_FALSE(
        detail::parse_user_event(R"({"e":"ORDER_TRADE_UPDATE","E":1,"o":{"s":"BTCUSDT"}})").ok());
    EXPECT_FALSE(detail::parse_user_event(R"({"e":"ORDER_TRADE_UPDATE","E":1})").ok());
    EXPECT_EQ(detail::parse_user_event(R"({"E":1})").error().code, ErrorCode::Parse);
}

// --- REST snapshots --------------------------------------------------------------------------

TEST(AccountParse, RestOrders) {
    auto open = detail::parse_open_orders(
        R"([{"orderId":28688769289,"symbol":"BTCUSDT","status":"NEW","clientOrderId":"probe-limit-1","price":"35058.70","avgPrice":"0","origQty":"0.0030","executedQty":"0","cumQuote":"0.000000","timeInForce":"GTC","type":"LIMIT","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"LIMIT","priceMatch":"NONE","selfTradePreventionMode":"EXPIRE_MAKER","goodTillDate":0,"time":1795208024156,"strategyType":"","updateTime":1795208024163}])");
    ASSERT_TRUE(open.ok()) << open.error().message;
    ASSERT_EQ(open->size(), 1u);
    const Order& o = (*open)[0];
    EXPECT_EQ(o.id, 28688769289);
    EXPECT_EQ(o.price, d("35058.7"));
    EXPECT_EQ(o.status, OrderStatus::New);
    EXPECT_EQ(to_epoch_ms(o.create_time), 1795208024156);
    EXPECT_EQ(to_epoch_ms(o.update_time), 1795208024163);
    EXPECT_FALSE(o.is_algo);
    EXPECT_TRUE(detail::parse_open_orders("[]")->empty());

    auto filled = detail::parse_order(
        R"({"orderId":28688769407,"symbol":"BTCUSDT","status":"FILLED","clientOrderId":"probe-mkt-1","price":"0.00","avgPrice":"58457.100000","origQty":"0.0030","executedQty":"0.0030","cumQuote":"175.371300","timeInForce":"GTC","type":"MARKET","reduceOnly":false,"closePosition":false,"side":"BUY","positionSide":"BOTH","stopPrice":"0.00","workingType":"CONTRACT_PRICE","priceProtect":false,"origType":"MARKET","priceMatch":"NONE","selfTradePreventionMode":"EXPIRE_MAKER","goodTillDate":0,"time":1795208035520,"updateTime":1795208035527})");
    ASSERT_TRUE(filled.ok()) << filled.error().message;
    EXPECT_EQ(filled->status, OrderStatus::Filled);
    EXPECT_EQ(filled->filled_quote_qty, d("175.3713"));

    auto algo = detail::parse_open_algo_orders(
        R"([{"algoId":1000000295570420,"clientAlgoId":"probe-algo-1","algoType":"CONDITIONAL","orderType":"STOP_MARKET","symbol":"BTCUSDT","side":"SELL","positionSide":"BOTH","timeInForce":"GTC","quantity":"0.003","algoStatus":"NEW","actualOrderId":"","actualQty":"0.0","triggerPrice":"35058.7","price":"0.0","icebergQuantity":null,"selfTradePreventionMode":"EXPIRE_MAKER","workingType":"CONTRACT_PRICE","priceMatch":"NONE","closePosition":false,"priceProtect":false,"reduceOnly":false,"createTime":1795208028888,"updateTime":1795208028888,"triggerTime":0,"goodTillDate":0,"isActivated":false}])");
    ASSERT_TRUE(algo.ok()) << algo.error().message;
    ASSERT_EQ(algo->size(), 1u);
    EXPECT_TRUE((*algo)[0].is_algo);
    EXPECT_EQ((*algo)[0].id, 1000000295570420);
    EXPECT_EQ((*algo)[0].stop_price, d("35058.7"));
    EXPECT_EQ((*algo)[0].type, OrderType::StopMarket);
    EXPECT_EQ((*algo)[0].status, OrderStatus::New);
}

TEST(AccountParse, RestPositionsBalancesSettings) {
    auto pos = detail::parse_position_risk(
        R"([{"symbol":"BTCUSDT","positionSide":"BOTH","positionAmt":"0.0030","entryPrice":"58457.1","breakEvenPrice":"58480.48284","markPrice":"58455.60000000","unRealizedProfit":"-0.00450000","liquidationPrice":"0","isolatedMargin":"0","notional":"175.36680000","marginAsset":"USDT","isolatedWallet":"0","initialMargin":"8.76834001","maintMargin":"0.70146720","positionInitialMargin":"8.76834001","openOrderInitialMargin":"0","adl":0,"bidNotional":"0","askNotional":"0","updateTime":1795208035555}])");
    ASSERT_TRUE(pos.ok()) << pos.error().message;
    ASSERT_EQ(pos->size(), 1u);
    EXPECT_EQ((*pos)[0].amount, d("0.003"));
    EXPECT_EQ((*pos)[0].unrealized_pnl, d("-0.0045"));
    EXPECT_EQ((*pos)[0].break_even_price, d("58480.48284"));

    auto bal = detail::parse_futures_balances(
        R"([{"accountAlias":"uX","asset":"USDT","balance":"5017.47526264","crossWalletBalance":"5017.47526264","crossUnPnl":"0.00000000","availableBalance":"5000.00000000","maxWithdrawAmount":"5017.47526264","marginAvailable":true,"updateTime":1795208000000},{"accountAlias":"uX","asset":"FDUSD","balance":"0.00000000","crossWalletBalance":"0.00000000","crossUnPnl":"0.00000000","availableBalance":"0.00000000","maxWithdrawAmount":"0.00000000","marginAvailable":true,"updateTime":0}])");
    ASSERT_TRUE(bal.ok()) << bal.error().message;
    ASSERT_EQ(bal->size(), 2u);
    EXPECT_EQ((*bal)[0].total, d("5017.47526264"));
    EXPECT_EQ((*bal)[0].available, d("5000"));
    EXPECT_EQ((*bal)[0].locked, d("17.47526264"));

    auto sym = detail::parse_symbol_config(
        R"([{"symbol":"BTCUSDT","marginType":"CROSSED","isAutoAddMargin":false,"leverage":20,"maxNotionalValue":"20000000"},{"symbol":"ETHUSDT","marginType":"ISOLATED","isAutoAddMargin":false,"leverage":5,"maxNotionalValue":"1000000"}])");
    ASSERT_TRUE(sym.ok()) << sym.error().message;
    ASSERT_EQ(sym->size(), 2u);
    EXPECT_EQ((*sym)[0].leverage, 20);
    EXPECT_EQ((*sym)[0].margin_type, MarginType::Cross);
    EXPECT_EQ((*sym)[1].margin_type, MarginType::Isolated);

    auto cfg = detail::parse_account_config(
        R"({"feeTier":0,"canTrade":true,"canDeposit":true,"canWithdraw":true,"dualSidePosition":false,"updateTime":1794468580004,"multiAssetsMargin":false,"tradeGroupId":-1})");
    ASSERT_TRUE(cfg.ok()) << cfg.error().message;
    EXPECT_FALSE(cfg->hedge_mode);
    EXPECT_FALSE(cfg->multi_assets_margin);

    EXPECT_FALSE(detail::parse_position_risk(R"({"code":-2015})").ok());
}

// --- Spot (captured on spot Demo via the WebSocket API user data subscription) ---------------

namespace {

detail::UserEvent spot_event(std::string_view json) {
    auto r = detail::parse_spot_user_event(json);
    EXPECT_TRUE(r.ok()) << r.error().message;
    return r.ok() ? *r : detail::UserEvent{};
}

}  // namespace

TEST(SpotParse, ExecutionReportNewAndFill) {
    auto ev = spot_event(
        R"({"subscriptionId":0,"event":{"e":"executionReport","E":1795283944842,"s":"BTCUSDT",)"
        R"("c":"sprobe-lim","S":"BUY","o":"LIMIT","f":"GTC","q":"0.00020000","p":"36034.86000000",)"
        R"("P":"0.00000000","F":"0.00000000","g":-1,"C":"","x":"NEW","X":"NEW","r":"NONE",)"
        R"("i":85170551903,"l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,)"
        R"("T":1795283944842,"t":-1,"I":171247281377,"w":true,"m":false,"M":false,)"
        R"("O":1795283944842,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000",)"
        R"("W":1795283944842,"V":"EXPIRE_MAKER"}})");
    ASSERT_EQ(ev.kind, Kind::Order);
    const Order& o = *ev.order;
    EXPECT_EQ(o.market, Market::Spot);
    EXPECT_EQ(o.symbol, "BTCUSDT");
    EXPECT_EQ(o.client_order_id, "sprobe-lim");
    EXPECT_EQ(o.id, 85170551903);
    EXPECT_EQ(o.type, OrderType::Limit);
    EXPECT_EQ(o.status, OrderStatus::New);
    EXPECT_EQ(o.price, d("36034.86"));
    EXPECT_EQ(o.qty, d("0.0002"));

    auto fill = spot_event(
        R"({"subscriptionId":0,"event":{"e":"executionReport","E":1795283950694,"s":"BTCUSDT",)"
        R"("c":"sprobe-mkt","S":"BUY","o":"MARKET","f":"GTC","q":"0.00016000","p":"0.00000000",)"
        R"("P":"0.00000000","F":"0.00000000","g":-1,"C":"","x":"TRADE","X":"FILLED","r":"NONE",)"
        R"("i":85170569545,"l":"0.00016000","z":"0.00016000","L":"60054.95000000",)"
        R"("n":"0.00001315","N":"BNB","T":1795283950694,"t":351800475,"I":171247316703,)"
        R"("w":false,"m":false,"M":true,"O":1795283950694,"Z":"9.60879200","Y":"9.60879200",)"
        R"("Q":"10.00000000","W":1795283950694,"V":"EXPIRE_MAKER"}})");
    ASSERT_EQ(fill.kind, Kind::Order);
    EXPECT_EQ(fill.order->status, OrderStatus::Filled);
    EXPECT_EQ(fill.order->type, OrderType::Market);
    EXPECT_EQ(fill.order->filled_quote_qty, d("9.608792"));
    EXPECT_EQ(fill.order->avg_price, d("60054.95"));  // 9.608792 / 0.00016
}

TEST(SpotParse, CancelKeepsTheOrdersClientId) {
    // On a cancel "c" is the cancel request's own id; the order's is in "C".
    auto ev = spot_event(
        R"({"subscriptionId":0,"event":{"e":"executionReport","E":1795283976244,"s":"BTCUSDT",)"
        R"("c":"soxGV9zwwvwWffHo8thbqh","S":"SELL","o":"STOP_LOSS_LIMIT","f":"GTC",)"
        R"("q":"0.00020000","p":"56469.61000000","P":"57070.35000000","F":"0.00000000","g":-1,)"
        R"("C":"sprobe-stop","x":"CANCELED","X":"CANCELED","r":"NONE","i":85170640000,)"
        R"("l":"0.00000000","z":"0.00000000","L":"0.00000000","n":"0","N":null,)"
        R"("T":1795283976244,"t":-1,"I":171247470177,"w":false,"m":false,"M":false,)"
        R"("O":1795283974221,"Z":"0.00000000","Y":"0.00000000","Q":"0.00000000",)"
        R"("V":"EXPIRE_MAKER"}})");
    ASSERT_EQ(ev.kind, Kind::Order);
    EXPECT_EQ(ev.order->client_order_id, "sprobe-stop");
    EXPECT_EQ(ev.order->status, OrderStatus::Canceled);
    EXPECT_EQ(ev.order->type, OrderType::StopLimit);  // spot STOP_LOSS_LIMIT
    EXPECT_EQ(ev.order->stop_price, d("57070.35"));
    EXPECT_EQ(to_epoch_ms(ev.order->create_time), 1795283974221);
    EXPECT_EQ(to_epoch_ms(ev.order->update_time), 1795283976244);
}

TEST(SpotParse, BalancesAndStreamEnd) {
    auto ev = spot_event(
        R"({"subscriptionId":0,"event":{"e":"outboundAccountPosition","E":1795283944842,)"
        R"("u":1795283944842,"B":[{"a":"BTC","f":"0.05380000","l":"0.00000000"},)"
        R"({"a":"USDT","f":"4665.61273260","l":"7.20697200"}]}})");
    ASSERT_EQ(ev.kind, Kind::Account);
    ASSERT_EQ(ev.balances.size(), 2u);
    EXPECT_EQ(ev.balances[1].asset, "USDT");
    EXPECT_EQ(ev.balances[1].available, d("4665.6127326"));
    EXPECT_EQ(ev.balances[1].locked, d("7.206972"));
    EXPECT_EQ(ev.balances[1].total, d("4672.8197046"));

    EXPECT_EQ(spot_event(R"({"subscriptionId":0,"event":{"e":"eventStreamTerminated",)"
                         R"("E":1795283956682}})")
                  .kind,
              Kind::StreamTerminated);
    EXPECT_EQ(spot_event(R"({"subscriptionId":0,"event":{"e":"balanceUpdate","E":1,)"
                         R"("a":"USDT","d":"10","T":1}})")
                  .kind,
              Kind::Other);
    EXPECT_FALSE(detail::parse_spot_user_event(R"({"subscriptionId":0,"event":{"E":1}})").ok());
}

TEST(SpotParse, RestAccountAndOrders) {
    auto balances = detail::parse_spot_account(
        R"({"makerCommission":10,"takerCommission":10,"canTrade":true,"accountType":"SPOT",)"
        R"("balances":[{"asset":"BTC","free":"0.05380000","locked":"0.00000000"},)"
        R"({"asset":"USDT","free":"4672.81970460","locked":"1.00000000"}],)"
        R"("permissions":["SPOT"],"uid":1})");
    ASSERT_TRUE(balances.ok()) << balances.error().message;
    ASSERT_EQ(balances->size(), 2u);
    EXPECT_EQ((*balances)[1].total, d("4673.8197046"));
    EXPECT_EQ((*balances)[1].available, d("4672.8197046"));

    auto open = detail::parse_spot_open_orders(
        R"([{"symbol":"BTCUSDT","orderId":85170640000,"orderListId":-1,)"
        R"("clientOrderId":"sprobe-stop","price":"56469.61000000","origQty":"0.00020000",)"
        R"("executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"NEW",)"
        R"("timeInForce":"GTC","type":"STOP_LOSS_LIMIT","side":"SELL",)"
        R"("stopPrice":"57070.35000000","icebergQty":"0.00000000","time":1795283974221,)"
        R"("updateTime":1795283974221,"isWorking":false,"workingTime":-1,)"
        R"("origQuoteOrderQty":"0.00000000","selfTradePreventionMode":"EXPIRE_MAKER"}])");
    ASSERT_TRUE(open.ok()) << open.error().message;
    ASSERT_EQ(open->size(), 1u);
    EXPECT_EQ((*open)[0].market, Market::Spot);
    EXPECT_EQ((*open)[0].type, OrderType::StopLimit);
    EXPECT_EQ((*open)[0].stop_price, d("57070.35"));
    EXPECT_EQ((*open)[0].side, Side::Sell);

    auto one = detail::parse_spot_order(
        R"({"symbol":"BTCUSDT","orderId":85170551903,"orderListId":-1,)"
        R"("clientOrderId":"sprobe-lim","price":"36034.86000000","origQty":"0.00020000",)"
        R"("executedQty":"0.00000000","cummulativeQuoteQty":"0.00000000","status":"CANCELED",)"
        R"("timeInForce":"GTC","type":"LIMIT","side":"BUY","stopPrice":"0.00000000",)"
        R"("icebergQty":"0.00000000","time":1795283944842,"updateTime":1795283954292,)"
        R"("isWorking":true,"workingTime":1795283944842,"origQuoteOrderQty":"0.00000000",)"
        R"("selfTradePreventionMode":"EXPIRE_MAKER"})");
    ASSERT_TRUE(one.ok()) << one.error().message;
    EXPECT_EQ(one->status, OrderStatus::Canceled);
    EXPECT_EQ(one->client_order_id, "sprobe-lim");
    EXPECT_EQ(to_epoch_ms(one->create_time), 1795283944842);
}

// --- Open orders: what each load fetches -----------------------------------------------------

namespace {

std::map<std::string, bool> unloaded_symbols(int n) {
    std::map<std::string, bool> out;
    for (int i = 0; i < n; ++i) out["SYM" + std::to_string(i) + "USDT"] = false;
    return out;
}

}  // namespace

TEST(OpenOrdersPlan, LoadsOnlyWhatIsNew) {
    const std::map<std::string, bool> watched = {{"BTCUSDT", true}, {"ETHUSDT", false}};
    // A new symbol next to a loaded one: only the new one.
    auto plan = detail::plan_orders_for_test(false, false, watched, false, Market::Futures);
    EXPECT_FALSE(plan.first);
    EXPECT_EQ(plan.second, std::vector<std::string>{"ETHUSDT"});
    // After a reconnect: everything watched.
    plan = detail::plan_orders_for_test(false, false, watched, true, Market::Futures);
    EXPECT_EQ(plan.second, (std::vector<std::string>{"BTCUSDT", "ETHUSDT"}));
    // Nothing watched: nothing.
    plan = detail::plan_orders_for_test(false, false, {}, true, Market::Spot);
    EXPECT_FALSE(plan.first);
    EXPECT_TRUE(plan.second.empty());
}

TEST(OpenOrdersPlan, AllSymbols) {
    // All symbols watched and loaded: a symbol watched on top needs no request.
    auto plan =
        detail::plan_orders_for_test(true, true, {{"XRPUSDT", true}}, false, Market::Futures);
    EXPECT_FALSE(plan.first);
    EXPECT_TRUE(plan.second.empty());
    // Not loaded yet, or reloading: one request for all symbols.
    plan = detail::plan_orders_for_test(true, false, {}, false, Market::Futures);
    EXPECT_TRUE(plan.first);
    plan = detail::plan_orders_for_test(true, true, {{"XRPUSDT", true}}, true, Market::Spot);
    EXPECT_TRUE(plan.first);
    EXPECT_TRUE(plan.second.empty());
}

TEST(OpenOrdersPlan, ManySymbolsUseOneRequest) {
    // Futures: 40 x weight 1 per symbol = weight 40 for all at once.
    auto plan =
        detail::plan_orders_for_test(false, false, unloaded_symbols(39), false, Market::Futures);
    EXPECT_FALSE(plan.first);
    EXPECT_EQ(plan.second.size(), 39u);
    plan = detail::plan_orders_for_test(false, false, unloaded_symbols(40), false, Market::Futures);
    EXPECT_TRUE(plan.first);
    EXPECT_TRUE(plan.second.empty());
    // Spot: weight 6 per symbol, 80 for all: from 14 symbols on.
    plan = detail::plan_orders_for_test(false, false, unloaded_symbols(13), false, Market::Spot);
    EXPECT_FALSE(plan.first);
    plan = detail::plan_orders_for_test(false, false, unloaded_symbols(14), false, Market::Spot);
    EXPECT_TRUE(plan.first);
}

TEST(OpenOrdersPlan, WhatAWatchCoversAndHasLoaded) {
    const std::map<std::string, bool> watched = {{"BTCUSDT", true}, {"ETHUSDT", false}};
    using P = std::pair<bool, bool>;  // {covers, loaded}
    EXPECT_EQ(detail::order_scope_for_test(false, false, watched, "BTCUSDT"), P(true, true));
    EXPECT_EQ(detail::order_scope_for_test(false, false, watched, "ETHUSDT"), P(true, false));
    EXPECT_EQ(detail::order_scope_for_test(false, false, watched, "XRPUSDT"), P(false, false));
    EXPECT_EQ(detail::order_scope_for_test(false, false, watched, ""), P(false, false));
    // All symbols, loaded: covers and has every symbol.
    EXPECT_EQ(detail::order_scope_for_test(true, true, {}, "XRPUSDT"), P(true, true));
    EXPECT_EQ(detail::order_scope_for_test(true, true, {}, ""), P(false, true));
    EXPECT_EQ(detail::order_scope_for_test(true, false, {}, "XRPUSDT"), P(true, false));
}

// --- Without network -------------------------------------------------------------------------

TEST(Account, LocalErrors) {
    Config cfg;
    struct Quiet : Logger {
        void log(LogLevel, std::string_view) override {}
    };
    cfg.logger = std::make_shared<Quiet>();
    Client client(cfg);
    constexpr auto F = Market::Futures;

    // No API key: refused before any network I/O.
    auto no_keys = client.watch_balance(F);
    ASSERT_FALSE(no_keys.ok());
    EXPECT_EQ(no_keys.error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_positions().error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_open_orders(F, "BTCUSDT").error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_balance_rest_api(F).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_open_orders_rest_api(F).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_positions_rest_api().error().code, ErrorCode::InvalidArgument);

    EXPECT_EQ(client.watch_balance(Market::Spot).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_balance_rest_api(Market::Spot).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_open_orders(F, "bad symbol!").error().code, ErrorCode::InvalidArgument);
    client.unwatch_balance(F);  // not watched: no-op
    client.unwatch_open_orders(F, "BTCUSDT");
    client.unwatch_algo_orders();

    cfg.credentials = {"key", "secret", ""};
    Client keyed(cfg);
    EXPECT_EQ(keyed.watch_order(F, {"BTCUSDT"}).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.watch_order(F, {"", 1}).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.get_order_rest_api(F, {"BTCUSDT"}).error().code, ErrorCode::InvalidArgument);

    // Spot: no algo orders; bad symbols refused before sending.
    constexpr auto S = Market::Spot;
    OrderRef algo{"BTCUSDT", 1};
    algo.is_algo = true;
    EXPECT_EQ(keyed.watch_order(S, algo).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.get_order_rest_api(S, algo).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.watch_order(S, {"BTCUSDT"}).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.watch_open_orders(S, "bad symbol!").error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(keyed.get_open_orders_rest_api(S, "bad symbol!").error().code,
              ErrorCode::InvalidArgument);
    keyed.unwatch_open_orders(S, "BTCUSDT");
    keyed.unwatch_balance(S);
}

// --- Live, on Demo trading -------------------------------------------------------------------
// Needs BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY and BINANCE_DEMO_API_SECRET.
// Uses small BTCUSDT orders and a 0.003 BTC position; leaves none behind.

namespace {

class Quiet : public Logger {
public:
    void log(LogLevel level, std::string_view msg) override {
        if (level >= LogLevel::Warn) std::cerr << "[binance] " << msg << "\n";
    }
};

/// Minimal signed REST client, to act on the account outside Client.
class DemoRest {
public:
    DemoRest(std::string key, std::string secret, std::string host = "demo-fapi.binance.com")
        : host_(std::move(host)),
          key_(std::move(key)),
          secret_(std::move(secret)),
          tls_(detail::make_tls_context()),
          work_(io_.get_executor()),
          http_(detail::HttpClient::create(io_.get_executor(), tls_, limiter_, 10s)),
          thread_([this] { io_.run(); }) {}

    ~DemoRest() {
        work_.reset();
        http_.reset();
        io_.stop();
        thread_.join();
    }

    Result<std::string> call(detail::http::verb method, const std::string& path, std::string params,
                             bool sign = true) {
        if (sign) {
            if (!params.empty()) params += '&';
            params += "recvWindow=10000&timestamp=" +
                      std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                                         std::chrono::system_clock::now().time_since_epoch())
                                         .count());
            params += "&signature=" + detail::hmac_sha256_hex(secret_, params);
        }
        std::promise<Result<detail::HttpResponse>> p;
        auto f = p.get_future();
        http_->request({.method = method,
                        .host = host_,
                        .target = path + (params.empty() ? "" : "?" + params),
                        .headers = {{"X-MBX-APIKEY", key_}},
                        .idempotent = false},
                       [&p](Result<detail::HttpResponse> r) { p.set_value(std::move(r)); });
        auto r = f.get();
        if (!r) return r.error();
        if (r->status != 200) {
            return Error{ErrorCode::Exchange,
                         path + ": HTTP " + std::to_string(r->status) + " " + r->body};
        }
        return r->body;
    }

    std::string post(const std::string& path, const std::string& params) {
        auto r = call(detail::http::verb::post, path, params);
        EXPECT_TRUE(r.ok()) << r.error().message;
        return r.ok() ? *r : std::string{};
    }

    /// Cancel every BTCUSDT order and close any BTCUSDT position.
    void cleanup() {
        (void)call(detail::http::verb::delete_, "/fapi/v1/allOpenOrders", "symbol=BTCUSDT");
        (void)call(detail::http::verb::delete_, "/fapi/v1/algoOpenOrders", "symbol=BTCUSDT");
        auto risk = call(detail::http::verb::get, "/fapi/v3/positionRisk", "symbol=BTCUSDT");
        if (!risk) return;
        auto pos = detail::parse_position_risk(*risk);
        if (!pos) return;
        for (const auto& p : *pos) {
            if (p.amount.is_zero()) continue;
            const Decimal qty = p.amount.is_negative() ? -p.amount : p.amount;
            post("/fapi/v1/order", "symbol=BTCUSDT&type=MARKET&reduceOnly=true&side=" +
                                       std::string(p.amount.is_negative() ? "BUY" : "SELL") +
                                       "&quantity=" + qty.to_string());
        }
    }

    /// A BUY limit price 40% below the mark price: rests without filling.
    std::string far_price() {
        auto r = call(detail::http::verb::get, "/fapi/v1/premiumIndex", "symbol=BTCUSDT", false);
        EXPECT_TRUE(r.ok());
        const auto at = r->find("\"markPrice\":\"");
        const double mark = std::stod(r->substr(at + 13));
        return Decimal::parse(std::to_string(mark * 0.6))
            ->round_to(*Decimal::parse("0.1"), Rounding::Down)
            .to_string();
    }

private:
    std::string host_;
    std::string key_, secret_;
    boost::asio::io_context io_;
    boost::asio::ssl::context tls_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> work_;
    detail::RateLimiter limiter_;
    std::shared_ptr<detail::HttpClient> http_;
    std::thread thread_;
};

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 15s) {
    const auto end = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < end) {
        if (pred()) return true;
        std::this_thread::sleep_for(100ms);
    }
    return pred();
}

bool has_order(const Result<std::vector<Order>>& orders, const std::string& client_id) {
    if (!orders) return false;
    for (const auto& o : *orders) {
        if (o.client_order_id == client_id) return true;
    }
    return false;
}

Decimal btc_amount(const Result<Positions>& positions) {
    if (!positions) return Decimal{};
    auto it = positions->find("BTCUSDT");
    if (it == positions->end()) return Decimal{};
    auto side = it->second.find(PositionSide::Both);
    return side == it->second.end() ? Decimal{} : side->second.amount;
}

std::string unique_id(std::string_view prefix) {
    return std::string(prefix) + "-" +
           std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch())
                              .count());
}

std::int64_t order_id_of(const std::string& json) {
    auto o = detail::parse_order(json);
    EXPECT_TRUE(o.ok()) << json;
    return o.ok() ? o->id : 0;
}

}  // namespace

class LiveAccount : public ::testing::Test {
protected:
    void SetUp() override {
        const char* key = std::getenv("BINANCE_DEMO_API_KEY");
        const char* secret = std::getenv("BINANCE_DEMO_API_SECRET");
        if (!std::getenv("BINANCE_LIVE_TESTS") || !key || !secret) {
            GTEST_SKIP()
                << "set BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET";
        }
        config.environment = Environment::Demo;
        config.credentials = {key, secret, ""};
        config.logger = std::make_shared<Quiet>();
        rest = std::make_unique<DemoRest>(key, secret);
        rest->cleanup();
    }
    void TearDown() override {
        if (rest) rest->cleanup();
    }

    Config config;
    std::unique_ptr<DemoRest> rest;
    static constexpr auto F = Market::Futures;
};

TEST_F(LiveAccount, WatchesLoadThenFollowTheStream) {
    Client client(config);
    // Four watches, one user data stream. The first call of each loads over REST.
    auto balances = client.watch_balance(F);
    ASSERT_TRUE(balances.ok()) << balances.error().message;
    ASSERT_TRUE(balances->count("USDT"));
    EXPECT_TRUE(balances->at("USDT").total.is_positive());
    EXPECT_TRUE(balances->at("USDT").available.is_positive());
    auto positions = client.watch_positions();
    ASSERT_TRUE(positions.ok()) << positions.error().message;
    EXPECT_FALSE(positions->count("BTCUSDT"));
    auto open = client.watch_open_orders(F, "BTCUSDT");
    ASSERT_TRUE(open.ok()) << open.error().message;
    EXPECT_TRUE(open->empty());
    auto algos = client.watch_algo_orders("BTCUSDT");
    ASSERT_TRUE(algos.ok()) << algos.error().message;
    EXPECT_TRUE(algos->empty());

    // A resting limit order: open orders only.
    const std::string price = rest->far_price();
    const std::string limit_id = unique_id("t-lim");
    const auto limit = order_id_of(rest->post(
        "/fapi/v1/order",
        "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.003&price=" + price +
            "&newClientOrderId=" + limit_id));
    ASSERT_TRUE(
        eventually([&] { return has_order(client.watch_open_orders(F, "BTCUSDT"), limit_id); }));
    open = client.watch_open_orders(F, "BTCUSDT");
    ASSERT_EQ(open->size(), 1u);
    EXPECT_EQ((*open)[0].id, limit);
    EXPECT_EQ((*open)[0].price, *Decimal::parse(price));
    EXPECT_TRUE(client.watch_algo_orders("BTCUSDT")->empty());
    auto rest_open = client.get_open_orders_rest_api(F, "BTCUSDT");
    ASSERT_TRUE(rest_open.ok()) << rest_open.error().message;
    ASSERT_EQ(rest_open->size(), 1u);
    EXPECT_EQ((*rest_open)[0].id, limit);
    (void)rest->call(detail::http::verb::delete_, "/fapi/v1/order",
                     "symbol=BTCUSDT&orderId=" + std::to_string(limit));
    ASSERT_TRUE(eventually([&] { return client.watch_open_orders(F, "BTCUSDT")->empty(); }));

    // A conditional order: algo orders only.
    const std::string algo_id = unique_id("t-algo");
    rest->post("/fapi/v1/algoOrder",
               "algoType=CONDITIONAL&symbol=BTCUSDT&side=SELL&type=STOP_MARKET"
               "&quantity=0.003&triggerPrice=" +
                   price + "&clientAlgoId=" + algo_id);
    ASSERT_TRUE(
        eventually([&] { return has_order(client.watch_algo_orders("BTCUSDT"), algo_id); }));
    algos = client.watch_algo_orders("BTCUSDT");
    ASSERT_EQ(algos->size(), 1u);
    EXPECT_TRUE((*algos)[0].is_algo);
    EXPECT_EQ((*algos)[0].type, OrderType::StopMarket);
    EXPECT_TRUE(client.watch_open_orders(F, "BTCUSDT")->empty());
    EXPECT_EQ(client.get_algo_orders_rest_api("BTCUSDT")->size(), 1u);
    (void)rest->call(detail::http::verb::delete_, "/fapi/v1/algoOrder", "clientAlgoId=" + algo_id);
    ASSERT_TRUE(eventually([&] { return client.watch_algo_orders("BTCUSDT")->empty(); }));

    const Decimal before = client.watch_balance(F)->at("USDT").total;
    order_id_of(rest->post("/fapi/v1/order", "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.003"));
    ASSERT_TRUE(eventually([&] { return btc_amount(client.watch_positions()) == d("0.003"); }));
    const Position p = client.watch_positions()->at("BTCUSDT").at(PositionSide::Both);
    EXPECT_GT(p.leverage, 0);
    EXPECT_TRUE(p.entry_price.is_positive());
    EXPECT_EQ(btc_amount(client.get_positions_rest_api()), d("0.003"));

    order_id_of(rest->post("/fapi/v1/order",
                           "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.003&reduceOnly=true"));
    ASSERT_TRUE(eventually([&] { return !client.watch_positions()->count("BTCUSDT"); }));
    EXPECT_TRUE(eventually([&] {  // the fees changed USDT
        return client.watch_balance(F)->at("USDT").total != before;
    }));
    auto rest_balance = client.get_balance_rest_api(F);
    ASSERT_TRUE(rest_balance.ok()) << rest_balance.error().message;
    EXPECT_TRUE(rest_balance->count("USDT"));

    client.unwatch_balance(F);
    EXPECT_TRUE(client.watch_open_orders(F, "BTCUSDT").ok());
}

TEST_F(LiveAccount, OpenOrdersPerSymbol) {
    Client client(config);
    const std::string id = unique_id("t-sym");
    order_id_of(
        rest->post("/fapi/v1/order",
                   "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.003&price=" +
                       rest->far_price() + "&newClientOrderId=" + id));

    // Loaded per symbol: ETHUSDT's watch does not see the BTCUSDT order.
    auto eth = client.watch_open_orders(F, "ETHUSDT");
    ASSERT_TRUE(eth.ok()) << eth.error().message;
    EXPECT_FALSE(has_order(eth, id));
    auto btc = client.watch_open_orders(F, "BTCUSDT");
    ASSERT_TRUE(btc.ok()) << btc.error().message;
    EXPECT_TRUE(has_order(btc, id));
    EXPECT_FALSE(has_order(client.watch_open_orders(F, "ETHUSDT"), id));

    // Binance rejects the symbol: only that watch fails.
    auto bad = client.watch_open_orders(F, "NOTACOINUSDT");
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.error().code, ErrorCode::Exchange);
    EXPECT_TRUE(has_order(client.watch_open_orders(F, "BTCUSDT"), id));

    auto all = client.watch_open_orders(F);
    ASSERT_TRUE(all.ok()) << all.error().message;
    EXPECT_TRUE(has_order(all, id));

    // Unwatching BTCUSDT while all symbols are watched keeps its orders.
    client.unwatch_open_orders(F, "BTCUSDT");
    EXPECT_TRUE(has_order(client.watch_open_orders(F), id));
}

TEST_F(LiveAccount, WatchOrder) {
    Client client(config);
    const std::string id = unique_id("t-one");
    const auto placed = order_id_of(
        rest->post("/fapi/v1/order",
                   "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.003&price=" +
                       rest->far_price() + "&newClientOrderId=" + id));

    const OrderRef ref{"BTCUSDT", std::nullopt, id};
    auto o = client.watch_order(F, ref);  // watches BTCUSDT's open orders, answers from them
    ASSERT_TRUE(o.ok()) << o.error().message;
    EXPECT_EQ(o->status, OrderStatus::New);
    EXPECT_EQ(o->id, placed);
    EXPECT_EQ(client.watch_order(F, {"BTCUSDT", placed})->client_order_id, id);

    (void)rest->call(detail::http::verb::delete_, "/fapi/v1/order",
                     "symbol=BTCUSDT&origClientOrderId=" + id);
    // No longer open: read over REST.
    ASSERT_TRUE(eventually([&] {
        auto now = client.watch_order(F, ref);
        return now && now->status == OrderStatus::Canceled;
    }));
    EXPECT_EQ(client.get_order_rest_api(F, ref)->status, OrderStatus::Canceled);

    auto unknown = client.watch_order(F, {"BTCUSDT", std::nullopt, "t-no-such-order"});
    ASSERT_FALSE(unknown.ok());
    EXPECT_EQ(unknown.error().code, ErrorCode::NotFound);
}

TEST_F(LiveAccount, ReconnectReloads) {
    Client client(config);
    ASSERT_TRUE(client.watch_open_orders(F, "BTCUSDT").ok());
    ASSERT_TRUE(client.watch_positions().ok());

    // While "disconnected": a limit order rests, and a position opens.
    detail::TestHook::drop_account_events(client, F, true);
    const std::string limit_id = unique_id("t-rlim");
    const auto limit = order_id_of(
        rest->post("/fapi/v1/order",
                   "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.003&price=" +
                       rest->far_price() + "&newClientOrderId=" + limit_id));
    order_id_of(rest->post("/fapi/v1/order", "symbol=BTCUSDT&side=BUY&type=MARKET&quantity=0.003"));
    std::this_thread::sleep_for(2s);
    EXPECT_TRUE(client.watch_open_orders(F, "BTCUSDT")->empty());  // missed

    detail::TestHook::drop_account_events(client, F, false);
    detail::TestHook::reconnect_account(client, F);
    ASSERT_TRUE(eventually([&] {
        return has_order(client.watch_open_orders(F, "BTCUSDT"), limit_id) &&
               btc_amount(client.watch_positions()) == d("0.003");
    }));

    // While "disconnected" again: the order is cancelled, the position closed.
    detail::TestHook::drop_account_events(client, F, true);
    (void)rest->call(detail::http::verb::delete_, "/fapi/v1/order",
                     "symbol=BTCUSDT&orderId=" + std::to_string(limit));
    order_id_of(rest->post("/fapi/v1/order",
                           "symbol=BTCUSDT&side=SELL&type=MARKET&quantity=0.003&reduceOnly=true"));
    std::this_thread::sleep_for(2s);

    detail::TestHook::drop_account_events(client, F, false);
    detail::TestHook::reconnect_account(client, F);
    ASSERT_TRUE(eventually([&] {
        return client.watch_open_orders(F, "BTCUSDT")->empty() &&
               !client.watch_positions()->count("BTCUSDT");
    }));
}

TEST_F(LiveAccount, InvalidKeyFailsTheWatch) {
    Config bad = config;
    bad.credentials.api_key = "invalidinvalidinvalid";
    Client client(bad);
    auto r = client.watch_balance(F);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::Exchange);
    EXPECT_EQ(client.get_balance_rest_api(F).error().code, ErrorCode::Exchange);
}

// --- Live spot account, on spot Demo trading (same keys, leaves no orders) -------------------

class LiveSpotAccount : public ::testing::Test {
protected:
    void SetUp() override {
        const char* key = std::getenv("BINANCE_DEMO_API_KEY");
        const char* secret = std::getenv("BINANCE_DEMO_API_SECRET");
        if (!std::getenv("BINANCE_LIVE_TESTS") || !key || !secret) {
            GTEST_SKIP()
                << "set BINANCE_LIVE_TESTS=1, BINANCE_DEMO_API_KEY, BINANCE_DEMO_API_SECRET";
        }
        config.environment = Environment::Demo;
        config.credentials = {key, secret, ""};
        config.logger = std::make_shared<Quiet>();
        rest = std::make_unique<DemoRest>(key, secret, "demo-api.binance.com");
        cleanup();
    }
    void TearDown() override {
        if (rest) cleanup();
    }
    void cleanup() {
        (void)rest->call(detail::http::verb::delete_, "/api/v3/openOrders", "symbol=BTCUSDT");
    }

    /// A resting BUY limit order 40% under the market; returns its order id.
    std::int64_t place_far_buy(const std::string& client_id) {
        auto ticker =
            rest->call(detail::http::verb::get, "/api/v3/ticker/price", "symbol=BTCUSDT", false);
        EXPECT_TRUE(ticker.ok());
        const auto at = ticker->find("\"price\":\"");
        const double px = std::stod(ticker->substr(at + 9));
        const std::string price = Decimal::parse(std::to_string(px * 0.6))
                                      ->round_to(d("0.01"), Rounding::Down)
                                      .to_string();
        const std::string body = rest->post(
            "/api/v3/order",
            "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.0002&price=" + price +
                "&newClientOrderId=" + client_id);
        auto o = detail::parse_spot_order(body);
        if (o) return o->id;
        // The placing answer (ACK/RESULT) may lack fields: take the id only.
        const auto id_at = body.find("\"orderId\":");
        return id_at == std::string::npos ? 0 : std::stoll(body.substr(id_at + 10));
    }

    Config config;
    std::unique_ptr<DemoRest> rest;
    static constexpr auto S = Market::Spot;
};

TEST_F(LiveSpotAccount, BalancesAndOpenOrdersFollowTheStream) {
    Client client(config);
    auto balances = client.watch_balance(S);
    ASSERT_TRUE(balances.ok()) << balances.error().message;
    ASSERT_TRUE(balances->count("USDT"));
    const Balance usdt = balances->at("USDT");
    EXPECT_TRUE(usdt.total.is_positive());
    EXPECT_EQ(usdt.total, usdt.available + usdt.locked);
    auto open = client.watch_open_orders(S, "BTCUSDT");
    ASSERT_TRUE(open.ok()) << open.error().message;
    EXPECT_TRUE(open->empty());

    // Spot and futures are separate accounts.
    auto rest_balance = client.get_balance_rest_api(S);
    ASSERT_TRUE(rest_balance.ok()) << rest_balance.error().message;
    EXPECT_EQ(rest_balance->at("USDT").total, usdt.total);

    const std::string id = unique_id("ts-lim");
    const auto placed = place_far_buy(id);
    ASSERT_GT(placed, 0);
    ASSERT_TRUE(eventually([&] { return has_order(client.watch_open_orders(S, "BTCUSDT"), id); }));
    open = client.watch_open_orders(S, "BTCUSDT");
    ASSERT_EQ(open->size(), 1u);
    EXPECT_EQ((*open)[0].id, placed);
    EXPECT_EQ((*open)[0].market, Market::Spot);
    EXPECT_EQ(client.get_open_orders_rest_api(S, "BTCUSDT")->size(), 1u);
    // The order locks USDT: the stream carries free and locked.
    EXPECT_TRUE(
        eventually([&] { return client.watch_balance(S)->at("USDT").locked.is_positive(); }));

    auto one = client.watch_order(S, {"BTCUSDT", std::nullopt, id});
    ASSERT_TRUE(one.ok()) << one.error().message;
    EXPECT_EQ(one->status, OrderStatus::New);
    EXPECT_EQ(one->id, placed);

    (void)rest->call(detail::http::verb::delete_, "/api/v3/order",
                     "symbol=BTCUSDT&origClientOrderId=" + id);
    ASSERT_TRUE(eventually([&] { return client.watch_open_orders(S, "BTCUSDT")->empty(); }));
    auto ended = client.watch_order(S, {"BTCUSDT", placed});
    ASSERT_TRUE(ended.ok()) << ended.error().message;
    EXPECT_EQ(ended->status, OrderStatus::Canceled);
    EXPECT_EQ(ended->client_order_id, id);
    EXPECT_EQ(client.get_order_rest_api(S, {"BTCUSDT", placed})->status, OrderStatus::Canceled);

    auto unknown = client.watch_order(S, {"BTCUSDT", std::nullopt, "ts-no-such-order"});
    ASSERT_FALSE(unknown.ok());
    EXPECT_EQ(unknown.error().code, ErrorCode::NotFound);
    auto bad = client.watch_open_orders(S, "NOTACOINUSDT");
    ASSERT_FALSE(bad.ok());
    EXPECT_EQ(bad.error().code, ErrorCode::Exchange);
}

TEST_F(LiveSpotAccount, ReconnectReloads) {
    Client client(config);
    ASSERT_TRUE(client.watch_open_orders(S, "BTCUSDT").ok());

    // While "disconnected": an order rests. After reconnecting (subscribe again) it is there.
    detail::TestHook::drop_account_events(client, S, true);
    const std::string id = unique_id("ts-rlim");
    ASSERT_GT(place_far_buy(id), 0);
    std::this_thread::sleep_for(2s);
    EXPECT_TRUE(client.watch_open_orders(S, "BTCUSDT")->empty());  // missed

    detail::TestHook::drop_account_events(client, S, false);
    detail::TestHook::reconnect_account(client, S);
    ASSERT_TRUE(eventually([&] { return has_order(client.watch_open_orders(S, "BTCUSDT"), id); }));

    // Cancelled while "disconnected" again.
    detail::TestHook::drop_account_events(client, S, true);
    (void)rest->call(detail::http::verb::delete_, "/api/v3/order",
                     "symbol=BTCUSDT&origClientOrderId=" + id);
    std::this_thread::sleep_for(2s);
    detail::TestHook::drop_account_events(client, S, false);
    detail::TestHook::reconnect_account(client, S);
    ASSERT_TRUE(eventually([&] { return client.watch_open_orders(S, "BTCUSDT")->empty(); }));
}

TEST_F(LiveSpotAccount, FuturesAndSpotSideBySide) {
    Client client(config);
    DemoRest futures_rest(config.credentials.api_key, config.credentials.hmac_secret);
    futures_rest.cleanup();
    ASSERT_TRUE(client.watch_balance(Market::Futures).ok());
    ASSERT_TRUE(client.watch_balance(S).ok());
    ASSERT_TRUE(client.watch_open_orders(Market::Futures, "BTCUSDT").ok());
    ASSERT_TRUE(client.watch_open_orders(S, "BTCUSDT").ok());

    const std::string spot_id = unique_id("ts-both");
    ASSERT_GT(place_far_buy(spot_id), 0);
    const std::string futures_id = unique_id("t-both");
    order_id_of(futures_rest.post(
        "/fapi/v1/order",
        "symbol=BTCUSDT&side=BUY&type=LIMIT&timeInForce=GTC&quantity=0.003&price=" +
            futures_rest.far_price() + "&newClientOrderId=" + futures_id));

    ASSERT_TRUE(eventually([&] {
        return has_order(client.watch_open_orders(S, "BTCUSDT"), spot_id) &&
               has_order(client.watch_open_orders(Market::Futures, "BTCUSDT"), futures_id);
    }));
    EXPECT_FALSE(has_order(client.watch_open_orders(S, "BTCUSDT"), futures_id));
    EXPECT_FALSE(has_order(client.watch_open_orders(Market::Futures, "BTCUSDT"), spot_id));
    EXPECT_EQ(client.watch_open_orders(S, "BTCUSDT")->size(), 1u);
    EXPECT_EQ(client.watch_open_orders(Market::Futures, "BTCUSDT")->size(), 1u);
    // The futures order locks no spot USDT, and the spot order no futures margin.
    EXPECT_EQ(client.get_balance_rest_api(S)->at("USDT").locked,
              client.watch_balance(S)->at("USDT").locked);
    futures_rest.cleanup();
}

TEST_F(LiveSpotAccount, InvalidKeyFailsTheWatch) {
    Config bad = config;
    bad.credentials.api_key = "invalidinvalidinvalid";
    Client client(bad);
    auto r = client.watch_balance(S);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::Exchange);
}
