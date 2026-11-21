// Cancel one order: cancel_order. Futures, Demo trading.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./cancel_order

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
    auto placed = client.create_order(F, req);
    if (!placed) return 1;

    // By order id, or by client order id: {"BTCUSDT", std::nullopt, "my-id"}.
    auto cancelled = client.cancel_order(F, {"BTCUSDT", placed->id});
    std::cout << cancelled << "\n";
    // {
    //   market: futures,
    //   symbol: "BTCUSDT",
    //   id: 28690176522,
    //   client_order_id: "bcpp-mwqv7pt5-743pwd0",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: CANCELED,
    //   price: 36245.4,
    //   stop_price: 0,
    //   avg_price: 0,
    //   qty: 0.003,
    //   filled_qty: 0,
    //   filled_quote_qty: 0,
    //   activation_price: 0,
    //   callback_rate: 0,
    //   reduce_only: false,
    //   close_position: false,
    //   working_type: CONTRACT_PRICE,
    //   create_time: 2026-11-21 20:33:14.299,
    //   update_time: 2026-11-21 20:33:14.299
    // }

    // Already ended: its final state, not an error.
    auto again = client.cancel_order(F, {"BTCUSDT", placed->id});
    std::cout << again->status << "\n";
    // CANCELED

    // Binance does not know it.
    auto unknown = client.cancel_order(F, {"BTCUSDT", 1});
    std::cout << unknown << "\n";
    // {
    //   code: Exchange,
    //   message: "Unknown order sent.",
    //   exchange_code: -2011
    // }

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
    auto spot_placed = client.create_order(S, spot_req);
    if (!spot_placed) return 1;
    auto spot_cancelled = client.cancel_order(S, {"BTCUSDT", spot_placed->id});
    std::cout << spot_cancelled << "\n";
    // {
    //   market: spot,
    //   symbol: "BTCUSDT",
    //   id: 85197743857,
    //   client_order_id: "bcpp-mwqv7t6l-pglemt1",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: CANCELED,
    //   price: 36284.37,
    //   stop_price: 0,
    //   avg_price: 0,
    //   qty: 0.0002,
    //   filled_qty: 0,
    //   filled_quote_qty: 0,
    //   activation_price: 0,
    //   callback_rate: 0,
    //   reduce_only: false,
    //   close_position: false,
    //   working_type: CONTRACT_PRICE,
    //   create_time: 2026-11-21 20:33:18.427,
    //   update_time: 2026-11-21 20:33:18.427
    // }
}
