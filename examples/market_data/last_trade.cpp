// Last trade: get_last_trade_rest_api, watch_last_trade.
// Mainnet market data, no API key needed.
//
//   ./last_trade

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request.
    auto rest = client.get_last_trade_rest_api(F, "BTCUSDT");
    std::cout << rest << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   id: 3540906804,
    //   price: 60214,
    //   qty: 0.014,
    //   time: 2026-11-21 19:56:56.274,
    //   buyer_is_maker: true
    // }

    // First call: loads over REST and subscribes to the aggTrade stream.
    auto trade = client.watch_last_trade(F, "BTCUSDT");
    std::cout << trade << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   id: 3540906959,
    //   price: 60228,
    //   qty: 0.002,
    //   time: 2026-11-21 19:56:58.352,
    //   buyer_is_maker: false
    // }

    // Later calls: the newest trade, from memory.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto later = client.watch_last_trade(F, "BTCUSDT");
    std::cout << later << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   id: 3540906962,
    //   price: 60228,
    //   qty: 0.003,
    //   time: 2026-11-21 19:56:59.279,
    //   buyer_is_maker: false
    // }

    // Spot: the same calls with Market::Spot.
    auto spot = client.watch_last_trade(S, "BTCUSDT");
    std::cout << spot << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   id: 4127263418,
    //   price: 60262.04,
    //   qty: 0.014,
    //   time: 2026-11-21 19:56:57.724,
    //   buyer_is_maker: false
    // }

    client.unwatch_last_trade(F, "BTCUSDT");
    client.unwatch_last_trade(S, "BTCUSDT");
}
