// One-way or hedge mode, for the whole futures account: set_position_mode. Demo trading.
// Switches to hedge mode and back (needs no open orders or positions).
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./position_mode

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

    auto hedge = client.set_position_mode(true);
    std::cout << hedge << "\n";
    // ok

    // In hedge mode every order needs a position side: Binance refuses it without one.
    OrderRequest req;
    req.symbol = "BTCUSDT";
    req.side = Side::Buy;
    req.type = OrderType::Limit;
    req.qty = *Decimal::parse("0.003");
    req.price = price;
    auto no_side = client.create_order(F, req);
    std::cout << no_side << "\n";
    // {
    //   code: Exchange,
    //   message: "Order's position side does not match user's setting.",
    //   exchange_code: -4061
    // }

    req.position_side = PositionSide::Long;
    auto placed = client.create_order(F, req);
    std::cout << placed->position_side << "\n";
    // LONG
    (void)client.cancel_order(F, {"BTCUSDT", placed->id});

    auto one_way = client.set_position_mode(false);
    std::cout << one_way << "\n";
    // ok
}
