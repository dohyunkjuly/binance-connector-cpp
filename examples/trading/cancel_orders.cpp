// Cancel several orders at once: cancel_orders. Futures, Demo trading.
// One result per order, in the same order; 10 per request.
// Futures only: Binance has no batch orders for spot.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./cancel_orders

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
    OrderRequest lower = req;
    lower.price = price - *Decimal::parse("10");
    auto a = client.create_order(F, req);
    auto b = client.create_order(F, lower);
    if (!a || !b) return 1;

    // The last one is an order Binance does not know.
    auto results =
        client.cancel_orders(F, {{"BTCUSDT", a->id}, {"BTCUSDT", b->id}, {"BTCUSDT", 1}});
    std::cout << results[0]->status << "\n";
    // CANCELED
    std::cout << results[1]->status << "\n";
    // CANCELED
    std::cout << results[2] << "\n";
    // {
    //   code: Exchange,
    //   message: "Unknown order sent.",
    //   exchange_code: -2011
    // }
}
