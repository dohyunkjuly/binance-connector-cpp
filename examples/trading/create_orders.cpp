// Place several orders at once: create_orders. Futures, Demo trading.
// One result per request, in the same order; regular orders go 5 per request.
// Futures only: Binance has no batch orders for spot.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./create_orders

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
    OrderRequest bad = req;
    bad.price = price + *Decimal::parse("0.05");  // off the tick size: refused locally

    auto results = client.create_orders(F, {req, lower, bad});
    std::cout << results[0]->status << "\n";
    // NEW
    std::cout << results[1]->status << "\n";
    // NEW
    std::cout << results[2] << "\n";
    // {
    //   code: InvalidArgument,
    //   message: "BTCUSDT: price 36236.75 is not on the tick size 0.1; use round_price()",
    //   exchange_code: 0
    // }

    (void)client.cancel_all_orders(F, "BTCUSDT");
}
