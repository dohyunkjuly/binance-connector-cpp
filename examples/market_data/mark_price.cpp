// Mark price, index price and funding (futures only): get_mark_price_rest_api,
// watch_mark_price. Mainnet market data, no API key needed.
//
//   ./mark_price

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());

    // One REST request.
    auto rest = client.get_mark_price_rest_api("BTCUSDT");
    std::cout << rest << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   mark_price: 60222.02551449,
    //   index_price: 60250.21456522,
    //   funding_rate: -0.00002323,
    //   next_funding_time: 2026-11-22 00:00:00.000,
    //   time: 2026-11-21 19:57:13.002
    // }

    // First call: loads over REST and subscribes (one update per second).
    auto mark = client.watch_mark_price("BTCUSDT");
    std::cout << mark << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   mark_price: 60226.17869565,
    //   index_price: 60253.70369565,
    //   funding_rate: -0.00002323,
    //   next_funding_time: 2026-11-22 00:00:00.000,
    //   time: 2026-11-21 19:57:15.000
    // }

    // Later calls: from memory.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto later = client.watch_mark_price("BTCUSDT");
    std::cout << later << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   mark_price: 60232,
    //   index_price: 60254.93413043,
    //   funding_rate: -0.00002323,
    //   next_funding_time: 2026-11-22 00:00:00.000,
    //   time: 2026-11-21 19:57:17.000
    // }

    client.unwatch_mark_price("BTCUSDT");
}
