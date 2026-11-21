// Change price and quantity of a resting limit order: amend_order. Futures only (spot has no
// amend: cancel and place a new order). Demo trading.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./amend_order

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());
    constexpr auto F = Market::Futures;

    // Check the price first (one REST request).
    auto ticker = client.get_book_ticker_rest_api(F, "BTCUSDT");
    std::cout << ticker->bid_price << "\n";
    // 60394.6

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

    // Same order id, new price and quantity (it loses its place in the queue unless only the
    // quantity goes down).
    AmendRequest amend;
    amend.symbol = "BTCUSDT";
    amend.order_id = placed->id;
    amend.side = Side::Buy;
    amend.qty = *Decimal::parse("0.004");
    amend.price = price - *Decimal::parse("100");
    auto amended = client.amend_order(F, amend);
    std::cout << amended << "\n";
    // {
    //   market: futures,
    //   symbol: "BTCUSDT",
    //   id: 28690157637,
    //   client_order_id: "bcpp-mwqufw4u-xxj1ty0",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: NEW,
    //   price: 36136.7,
    //   stop_price: 0,
    //   avg_price: 0,
    //   qty: 0.004,
    //   filled_qty: 0,
    //   filled_quote_qty: 0,
    //   activation_price: 0,
    //   callback_rate: 0,
    //   reduce_only: false,
    //   close_position: false,
    //   working_type: CONTRACT_PRICE,
    //   create_time: 2026-11-21 20:11:36.190,
    //   update_time: 2026-11-21 20:11:36.190
    // }

    (void)client.cancel_order(F, {"BTCUSDT", placed->id});
}
