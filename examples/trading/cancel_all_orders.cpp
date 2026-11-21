// Cancel every open order of a symbol, regular and conditional: cancel_all_orders.
// Futures, Demo trading.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./cancel_all_orders

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());
    constexpr auto F = Market::Futures;

    // Check the price first (one REST request).
    auto ticker = client.get_book_ticker_rest_api(F, "BTCUSDT");
    std::cout << ticker->bid_price << "\n";
    // 60409

    // Our price: 40% under the best bid, so the order rests without filling. Prices must be on
    // the symbol's tick size: load the trading rules once, then round.
    if (!client.load_instruments(F)) return 1;
    const Decimal price = *client.round_price(
        F, "BTCUSDT", *Decimal::mul(ticker->bid_price, *Decimal::parse("0.6")), Rounding::Down);

    OrderRequest req;
    req.symbol = "BTCUSDT";
    req.side = Side::Buy;
    req.type = OrderType::Limit;
    req.qty = *Decimal::parse("0.003");
    req.price = price;
    if (!client.create_order(F, req)) return 1;
    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopMarket;
    stop.qty = *Decimal::parse("0.003");
    stop.stop_price = price;
    if (!client.create_order(F, stop)) return 1;

    auto done = client.cancel_all_orders(F, "BTCUSDT");
    std::cout << done << "\n";
    // ok

    auto regular = client.get_open_orders_rest_api(F, "BTCUSDT");
    auto algo = client.get_algo_orders_rest_api("BTCUSDT");
    std::cout << regular << "\n";
    // []
    std::cout << algo << "\n";
    // []

    // Spot: the same calls with Market::Spot (its own rules: tick 0.01, step 0.00001).
    constexpr auto S = Market::Spot;
    auto spot_ticker = client.get_book_ticker_rest_api(S, "BTCUSDT");
    if (!spot_ticker || !client.load_instruments(S)) return 1;
    OrderRequest spot_req;
    spot_req.symbol = "BTCUSDT";
    spot_req.side = Side::Buy;
    spot_req.type = OrderType::Limit;
    spot_req.qty = *Decimal::parse("0.0002");
    spot_req.price = *client.round_price(
        S, "BTCUSDT", *Decimal::mul(spot_ticker->bid_price, *Decimal::parse("0.6")),
        Rounding::Down);
    if (!client.create_order(S, spot_req)) return 1;
    auto spot_done = client.cancel_all_orders(S, "BTCUSDT");
    std::cout << spot_done << "\n";
    // ok
    auto spot_open = client.get_open_orders_rest_api(S, "BTCUSDT");
    std::cout << spot_open << "\n";
    // []
}
