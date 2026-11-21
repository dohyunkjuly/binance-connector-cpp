// Order book: get_order_book_rest_api, watch_order_book.
// Mainnet market data, no API key needed.
//
//   ./order_book

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request: the best 5 levels per side.
    auto rest = client.get_order_book_rest_api(F, "BTCUSDT", 5);
    std::cout << rest << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bids: [
    //     {price: 60221.9, qty: 6.032},
    //     {price: 60221.8, qty: 0.002},
    //     {price: 60221.5, qty: 0.002},
    //     {price: 60221.4, qty: 0.05},
    //     {price: 60221.3, qty: 0.067}
    //   ],
    //   asks: [
    //     {price: 60222, qty: 1.656},
    //     {price: 60222.2, qty: 0.001},
    //     {price: 60222.3, qty: 0.024},
    //     {price: 60222.4, qty: 0.005},
    //     {price: 60222.8, qty: 0.001}
    //   ],
    //   last_update_id: 12195405474382,
    //   time: 2026-11-21 19:56:50.986
    // }

    // depth 5, 10 or 20: Binance's top-of-book stream (light). First call: loads over REST and
    // subscribes. Later calls: from memory.
    auto top = client.watch_order_book(F, "BTCUSDT", 5);
    std::cout << top << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bids: [
    //     {price: 60214, qty: 1.626},
    //     {price: 60213.9, qty: 0.001},
    //     {price: 60213.8, qty: 0.002},
    //     {price: 60213.7, qty: 0.001},
    //     {price: 60213.4, qty: 0.001}
    //   ],
    //   asks: [
    //     {price: 60214.1, qty: 8.77},
    //     {price: 60214.2, qty: 0.002},
    //     {price: 60214.4, qty: 0.001},
    //     {price: 60214.5, qty: 0.001},
    //     {price: 60214.6, qty: 0.001}
    //   ],
    //   last_update_id: 12195405652888,
    //   time: 2026-11-21 19:56:52.615
    // }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto later = client.watch_order_book(F, "BTCUSDT", 5);
    std::cout << later << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bids: [
    //     {price: 60214, qty: 0.944},
    //     {price: 60213.9, qty: 0.001},
    //     {price: 60213.8, qty: 0.002},
    //     {price: 60213.7, qty: 0.001},
    //     {price: 60213.4, qty: 0.001}
    //   ],
    //   asks: [
    //     {price: 60214.1, qty: 8.652},
    //     {price: 60214.2, qty: 0.002},
    //     {price: 60214.4, qty: 0.001},
    //     {price: 60214.5, qty: 0.001},
    //     {price: 60214.6, qty: 0.001}
    //   ],
    //   last_update_id: 12195405709398,
    //   time: 2026-11-21 19:56:53.617
    // }

    // Any other depth: a full local book (REST snapshot + every change, gap-checked).
    auto full = client.watch_order_book(F, "ETHUSDT", 500);
    std::cout << full->bids.size() << "\n";
    // 500

    // Spot: the same calls with Market::Spot.
    auto spot = client.watch_order_book(S, "BTCUSDT", 5);
    std::cout << spot << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   bids: [
    //     {price: 60248.09, qty: 6.20632},
    //     {price: 60248.08, qty: 0.01204},
    //     {price: 60247.74, qty: 0.00018},
    //     {price: 60247.73, qty: 0.08406},
    //     {price: 60247.51, qty: 0.00018}
    //   ],
    //   asks: [
    //     {price: 60248.1, qty: 1.14722},
    //     {price: 60248.11, qty: 0.0009},
    //     {price: 60249.22, qty: 0.00018},
    //     {price: 60249.23, qty: 0.41592},
    //     {price: 60249.32, qty: 0.00018}
    //   ],
    //   last_update_id: 103400200945,
    //   time: 2026-11-21 19:56:55.984
    // }

    client.unwatch_order_book(F, "BTCUSDT");
    client.unwatch_order_book(F, "ETHUSDT");
    client.unwatch_order_book(S, "BTCUSDT");
}
