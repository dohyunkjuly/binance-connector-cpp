// Rolling 24 h statistics: get_ticker_24h_rest_api, watch_ticker_24h.
// Mainnet market data, no API key needed.
//
//   ./ticker_24h

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request.
    auto rest = client.get_ticker_24h_rest_api(F, "BTCUSDT");
    std::cout << rest << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   last_price: 60228,
    //   open_price: 58631.3,
    //   high_price: 60978.3,
    //   low_price: 55126.7,
    //   volume: 397608,
    //   quote_volume: 2.31199e+10,
    //   price_change: 1596.7,
    //   price_change_percent: 2.723,
    //   trade_count: 9243146,
    //   open_time: 2026-11-20 19:57:00.000,
    //   close_time: 2026-11-21 19:57:01.586
    // }

    // First call: loads over REST and subscribes (about one update per second).
    auto ticker = client.watch_ticker_24h(F, "BTCUSDT");
    std::cout << ticker << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   last_price: 60228,
    //   open_price: 58631.3,
    //   high_price: 60978.3,
    //   low_price: 55126.7,
    //   volume: 397608,
    //   quote_volume: 2.31199e+10,
    //   price_change: 1596.7,
    //   price_change_percent: 2.723,
    //   trade_count: 9243168,
    //   open_time: 2026-11-20 19:57:00.000,
    //   close_time: 2026-11-21 19:57:03.703
    // }

    // Later calls: from memory.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto later = client.watch_ticker_24h(F, "BTCUSDT");
    std::cout << later << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   last_price: 60214.8,
    //   open_price: 58631.3,
    //   high_price: 60978.3,
    //   low_price: 55126.7,
    //   volume: 397618,
    //   quote_volume: 2.31205e+10,
    //   price_change: 1583.5,
    //   price_change_percent: 2.701,
    //   trade_count: 9243408,
    //   open_time: 2026-11-20 19:57:00.000,
    //   close_time: 2026-11-21 19:57:09.610
    // }

    // Spot: the same calls with Market::Spot.
    auto spot = client.watch_ticker_24h(S, "BTCUSDT");
    std::cout << spot << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   last_price: 60253.79,
    //   open_price: 58664.1,
    //   high_price: 60996,
    //   low_price: 55130,
    //   volume: 52494.6,
    //   quote_volume: 3.04877e+09,
    //   price_change: 1589.69,
    //   price_change_percent: 2.71,
    //   trade_count: 5735264,
    //   open_time: 2026-11-20 19:57:12.007,
    //   close_time: 2026-11-21 19:57:12.007
    // }

    client.unwatch_ticker_24h(F, "BTCUSDT");
    client.unwatch_ticker_24h(S, "BTCUSDT");
}
