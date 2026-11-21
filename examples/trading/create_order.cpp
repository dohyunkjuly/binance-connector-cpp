// Place an order: create_order (WebSocket API, REST while it is down) and
// create_order_rest_api (REST only). Futures, Demo trading. Places resting BUY limit orders 40%
// under the market and cancels them.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./create_order

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

    // Checked against the symbol's rules before sending (tick size, step size, min notional).
    OrderRequest req;
    req.symbol = "BTCUSDT";
    req.side = Side::Buy;
    req.type = OrderType::Limit;
    req.qty = *Decimal::parse("0.003");
    req.price = price;
    auto placed = client.create_order(F, req);
    std::cout << placed << "\n";
    // {
    //   market: futures,
    //   symbol: "BTCUSDT",
    //   id: 28690176402,
    //   client_order_id: "bcpp-mwqv7kak-vsh8kd0",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: NEW,
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
    //   create_time: 2026-11-21 20:33:06.899,
    //   update_time: 2026-11-21 20:33:06.899
    // }

    // A price off the tick size: refused locally, nothing is sent.
    OrderRequest bad = req;
    bad.price = price + *Decimal::parse("0.05");
    auto refused = client.create_order(F, bad);
    std::cout << refused << "\n";
    // {
    //   code: InvalidArgument,
    //   message: "BTCUSDT: price 36245.45 is not on the tick size 0.1; use round_price()",
    //   exchange_code: 0
    // }

    // The same over REST only (create_order uses the WebSocket API, REST while it is down).
    OrderRequest lower = req;
    lower.price = price - *Decimal::parse("10");
    auto over_rest = client.create_order_rest_api(F, lower);
    std::cout << over_rest->status << "\n";
    // NEW

    (void)client.cancel_all_orders(F, "BTCUSDT");

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
    std::cout << spot_placed << "\n";
    // {
    //   market: spot,
    //   symbol: "BTCUSDT",
    //   id: 85197720163,
    //   client_order_id: "bcpp-mwqv7nbr-9ayk0c2",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: NEW,
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
    //   create_time: 2026-11-21 20:33:10.545,
    //   update_time: 2026-11-21 20:33:10.545
    // }

    // A Market order can spend a quote amount instead of a quantity.
    OrderRequest spend;
    spend.symbol = "BTCUSDT";
    spend.side = Side::Buy;
    spend.type = OrderType::Market;
    spend.quote_qty = *Decimal::parse("10");  // 10 USDT of BTC
    auto bought = client.create_order(S, spend);
    std::cout << bought->filled_qty << "\n";
    // 0.00016
    OrderRequest sell_back = spend;  // and sell it again
    sell_back.side = Side::Sell;
    sell_back.quote_qty.reset();
    sell_back.qty = bought->filled_qty;
    (void)client.create_order(S, sell_back);

    (void)client.cancel_all_orders(S, "BTCUSDT");
}
