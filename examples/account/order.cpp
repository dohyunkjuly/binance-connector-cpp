// One order: watch_order, get_order_rest_api. Demo trading.
// Places a resting futures BUY limit order (40% under the market), follows it, cancels it.
// Spot works the same with Market::Spot.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./order

#include "../common.hpp"

#include <chrono>
#include <string>
#include <thread>

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

    // Your own id, to find the order again (unique per open order).
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const std::string my_id =
        "example-" + std::to_string(std::chrono::duration_cast<std::chrono::seconds>(now).count());
    OrderRequest req;
    req.symbol = "BTCUSDT";
    req.side = Side::Buy;
    req.type = OrderType::Limit;
    req.qty = *Decimal::parse("0.003");
    req.price = price;
    req.client_order_id = my_id;
    auto placed = client.create_order(F, req);
    if (!placed) return 1;

    // By client order id (or order id). Watches the symbol's open orders and answers from
    // them; reads the order over REST when it is no longer open there, or was last read over
    // REST more than 15 minutes ago.
    const OrderRef ref{"BTCUSDT", std::nullopt, my_id};
    auto order = client.watch_order(F, ref);
    std::cout << order << "\n";
    // {
    //   market: futures,
    //   symbol: "BTCUSDT",
    //   id: 28690157561,
    //   client_order_id: "example-1795291888",
    //   is_algo: false,
    //   triggered_order_id: 0,
    //   side: BUY,
    //   position_side: BOTH,
    //   type: LIMIT,
    //   time_in_force: GTC,
    //   status: NEW,
    //   price: 36236.7,
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
    //   create_time: 2026-11-21 20:11:28.575,
    //   update_time: 2026-11-21 20:11:28.599
    // }

    if (!client.cancel_order(F, {"BTCUSDT", placed->id})) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto cancelled = client.watch_order(F, ref);
    std::cout << cancelled->status << "\n";
    // CANCELED

    // One REST request.
    auto rest = client.get_order_rest_api(F, {"BTCUSDT", placed->id});
    std::cout << rest->status << "\n";
    // CANCELED

    // Binance does not know it.
    auto unknown = client.watch_order(F, {"BTCUSDT", std::nullopt, "no-such-order"});
    std::cout << unknown << "\n";
    // {
    //   code: NotFound,
    //   message: "Order does not exist.",
    //   exchange_code: -2013
    // }
}
