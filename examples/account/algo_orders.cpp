// Futures conditional (algo) orders: get_algo_orders_rest_api, watch_algo_orders.
// Demo trading. Places a STOP_MARKET sell far under the market and cancels it.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./algo_orders

#include "../common.hpp"

#include <chrono>
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

    // First call: loads over REST and opens the account stream.
    auto algos = client.watch_algo_orders("BTCUSDT");
    std::cout << algos << "\n";
    // []

    // Stop / take-profit / trailing orders go to Binance's algo service (is_algo: true).
    OrderRequest stop;
    stop.symbol = "BTCUSDT";
    stop.side = Side::Sell;
    stop.type = OrderType::StopMarket;
    stop.qty = *Decimal::parse("0.003");
    stop.stop_price = price;  // a sell stop far under the market: does not trigger
    auto placed = client.create_order(F, stop);
    if (!placed) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));

    // Later calls: from memory, updated by the stream.
    auto with_stop = client.watch_algo_orders("BTCUSDT");
    std::cout << with_stop << "\n";
    // [
    //   {
    //     market: futures,
    //     symbol: "BTCUSDT",
    //     id: 1000000296944186,
    //     client_order_id: "bcpp-mwqufmjh-to7bxp0",
    //     is_algo: true,
    //     triggered_order_id: 0,
    //     side: SELL,
    //     position_side: BOTH,
    //     type: STOP_MARKET,
    //     time_in_force: GTC,
    //     status: NEW,
    //     price: 0,
    //     stop_price: 36236.7,
    //     avg_price: 0,
    //     qty: 0.003,
    //     filled_qty: 0,
    //     filled_quote_qty: 0,
    //     activation_price: 0,
    //     callback_rate: 0,
    //     reduce_only: false,
    //     close_position: false,
    //     working_type: CONTRACT_PRICE,
    //     create_time: 2026-11-21 20:11:23.162,
    //     update_time: 2026-11-21 20:11:23.162
    //   }
    // ]

    // One REST request.
    auto rest = client.get_algo_orders_rest_api("BTCUSDT");
    std::cout << rest->size() << "\n";
    // 1

    OrderRef ref;
    ref.order_id = placed->id;
    ref.is_algo = true;
    if (!client.cancel_order(F, ref)) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto after = client.watch_algo_orders("BTCUSDT");
    std::cout << after << "\n";
    // []
}
