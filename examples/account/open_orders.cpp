// Open orders: get_open_orders_rest_api, watch_open_orders. Demo trading.
// Places one resting futures BUY limit order (40% under the market) and cancels it.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./open_orders

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(demo_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // Check the price first (one REST request).
    auto ticker = client.get_book_ticker_rest_api(F, "BTCUSDT");
    std::cout << ticker->bid_price << "\n";
    // 60394.6

    // Our price: 40% under the best bid, so the order rests without filling. Prices must be on
    // the symbol's tick size: load the trading rules once, then round.
    if (!client.load_instruments(F)) return 1;
    const Decimal price = *client.round_price(
        F, "BTCUSDT", *Decimal::mul(ticker->bid_price, *Decimal::parse("0.6")), Rounding::Down);

    // First call: loads over REST and opens the account stream. With a symbol only that
    // symbol's orders are loaded and kept (cheaper); without one, every symbol's.
    auto open = client.watch_open_orders(F, "BTCUSDT");
    std::cout << open << "\n";
    // []

    OrderRequest req;
    req.symbol = "BTCUSDT";
    req.side = Side::Buy;
    req.type = OrderType::Limit;
    req.qty = *Decimal::parse("0.003");
    req.price = price;
    auto placed = client.create_order(F, req);
    if (!placed) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Later calls: from memory, updated by the stream.
    auto with_order = client.watch_open_orders(F, "BTCUSDT");
    std::cout << with_order << "\n";
    // [
    //   {
    //     market: futures,
    //     symbol: "BTCUSDT",
    //     id: 28690157355,
    //     client_order_id: "bcpp-mwqufeul-fbq2wc0",
    //     is_algo: false,
    //     triggered_order_id: 0,
    //     side: BUY,
    //     position_side: BOTH,
    //     type: LIMIT,
    //     time_in_force: GTC,
    //     status: NEW,
    //     price: 36236.7,
    //     stop_price: 0,
    //     avg_price: 0,
    //     qty: 0.003,
    //     filled_qty: 0,
    //     filled_quote_qty: 0,
    //     activation_price: 0,
    //     callback_rate: 0,
    //     reduce_only: false,
    //     close_position: false,
    //     working_type: CONTRACT_PRICE,
    //     create_time: 2026-11-21 20:11:13.223,
    //     update_time: 2026-11-21 20:11:13.223
    //   }
    // ]

    // One REST request.
    auto rest = client.get_open_orders_rest_api(F, "BTCUSDT");
    std::cout << rest->size() << "\n";
    // 1

    if (!client.cancel_order(F, {"BTCUSDT", placed->id})) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto after = client.watch_open_orders(F, "BTCUSDT");
    std::cout << after << "\n";
    // []

    // Spot: the same calls with Market::Spot (spot stop / take-profit orders are here too).
    auto spot = client.watch_open_orders(S, "BTCUSDT");
    std::cout << spot << "\n";
    // []
}
