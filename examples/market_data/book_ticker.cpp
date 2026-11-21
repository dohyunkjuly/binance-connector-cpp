// Best bid / ask: get_book_ticker_rest_api, watch_book_ticker.
// Mainnet market data, no API key needed.
//
//   ./book_ticker

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request.
    auto rest = client.get_book_ticker_rest_api(F, "BTCUSDT");
    std::cout << rest << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bid_price: 60227.9,
    //   bid_qty: 1.863,
    //   ask_price: 60228,
    //   ask_qty: 3.157,
    //   update_id: 12195406381612,
    //   time: 2026-11-21 19:57:02.480
    // }

    // First call: loads over REST and subscribes to the bookTicker stream (every change).
    auto ticker = client.watch_book_ticker(F, "BTCUSDT");
    std::cout << ticker << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bid_price: 60227.9,
    //   bid_qty: 1.925,
    //   ask_price: 60228,
    //   ask_qty: 2.742,
    //   update_id: 12195406485294,
    //   time: 2026-11-21 19:57:04.093
    // }

    // Later calls: from memory.
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto later = client.watch_book_ticker(F, "BTCUSDT");
    std::cout << later << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bid_price: 60227.9,
    //   bid_qty: 2.749,
    //   ask_price: 60228,
    //   ask_qty: 1.777,
    //   update_id: 12195406541370,
    //   time: 2026-11-21 19:57:05.026
    // }

    // Spot: the same calls with Market::Spot (spot has no time).
    auto spot = client.watch_book_ticker(S, "BTCUSDT");
    std::cout << spot << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bid_price: 60262.03,
    //   bid_qty: 0.45274,
    //   ask_price: 60262.04,
    //   ask_qty: 1.26096,
    //   update_id: 103400204872,
    //   time: 0
    // }

    client.unwatch_book_ticker(F, "BTCUSDT");
    client.unwatch_book_ticker(S, "BTCUSDT");
}
