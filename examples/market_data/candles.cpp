// Candles (klines): get_candles_rest_api, watch_candles, watch_closed_candles.
// Mainnet market data, no API key needed.
//
//   ./candles

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(public_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request: the last 2 one-minute candles.
    auto rest = client.get_candles_rest_api(F, "BTCUSDT", Interval::M1, 2);
    std::cout << rest << "\n";
    // [
    //   {
    //     open_time: 2026-11-21 19:55:00.000,
    //     close_time: 2026-11-21 19:55:59.999,
    //     open: 60287.3,
    //     high: 60287.3,
    //     low: 60163.9,
    //     close: 60186,
    //     volume: 278.678,
    //     quote_volume: 1.67781e+07,
    //     taker_buy_volume: 69.642,
    //     taker_buy_quote_volume: 4.19189e+06,
    //     trades: 5496,
    //     closed: true
    //   },
    //   {
    //     open_time: 2026-11-21 19:56:00.000,
    //     close_time: 2026-11-21 19:56:59.999,
    //     open: 60186.1,
    //     high: 60191.3,
    //     low: 60165.8,
    //     close: 60191.3,
    //     volume: 59.262,
    //     quote_volume: 3.56614e+06,
    //     taker_buy_volume: 38.834,
    //     taker_buy_quote_volume: 2.33685e+06,
    //     trades: 1330,
    //     closed: false
    //   }
    // ]

    // First call: loads the last 1000 candles over REST and subscribes to the kline stream.
    // Returns the last 2 (0 = all kept); the last one is still forming (closed: false).
    auto candles = client.watch_candles(F, "BTCUSDT", Interval::M1, 2);
    std::cout << candles << "\n";
    // [
    //   {
    //     open_time: 2026-11-21 19:55:00.000,
    //     close_time: 2026-11-21 19:55:59.999,
    //     open: 60287.3,
    //     high: 60287.3,
    //     low: 60163.9,
    //     close: 60186,
    //     volume: 278.678,
    //     quote_volume: 1.67781e+07,
    //     taker_buy_volume: 69.642,
    //     taker_buy_quote_volume: 4.19189e+06,
    //     trades: 5496,
    //     closed: true
    //   },
    //   {
    //     open_time: 2026-11-21 19:56:00.000,
    //     close_time: 2026-11-21 19:56:59.999,
    //     open: 60186.1,
    //     high: 60206.8,
    //     low: 60165.8,
    //     close: 60206.8,
    //     volume: 68.219,
    //     quote_volume: 4.10533e+06,
    //     taker_buy_volume: 47.791,
    //     taker_buy_quote_volume: 2.87604e+06,
    //     trades: 1642,
    //     closed: false
    //   }
    // ]

    // Later calls: from memory, kept up to date by the stream.
    std::this_thread::sleep_for(std::chrono::seconds(2));
    auto later = client.watch_candles(F, "BTCUSDT", Interval::M1, 1);
    std::cout << later << "\n";
    // [
    //   {
    //     open_time: 2026-11-21 19:56:00.000,
    //     close_time: 2026-11-21 19:56:59.999,
    //     open: 60186.1,
    //     high: 60215.3,
    //     low: 60165.8,
    //     close: 60215.2,
    //     volume: 71.155,
    //     quote_volume: 4.2821e+06,
    //     taker_buy_volume: 50.517,
    //     taker_buy_quote_volume: 3.04018e+06,
    //     trades: 1801,
    //     closed: false
    //   }
    // ]

    // Without the forming candle.
    auto closed = client.watch_closed_candles(F, "BTCUSDT", Interval::M1, 1);
    std::cout << closed << "\n";
    // [
    //   {
    //     open_time: 2026-11-21 19:55:00.000,
    //     close_time: 2026-11-21 19:55:59.999,
    //     open: 60287.3,
    //     high: 60287.3,
    //     low: 60163.9,
    //     close: 60186,
    //     volume: 278.678,
    //     quote_volume: 1.67781e+07,
    //     taker_buy_volume: 69.642,
    //     taker_buy_quote_volume: 4.19189e+06,
    //     trades: 5496,
    //     closed: true
    //   }
    // ]

    // Spot: the same calls with Market::Spot.
    auto spot = client.watch_candles(S, "BTCUSDT", Interval::M1, 1);
    std::cout << spot << "\n";
    // [
    //   {
    //     open_time: 2026-11-21 19:56:00.000,
    //     close_time: 2026-11-21 19:56:59.999,
    //     open: 60221.83,
    //     high: 60249.96,
    //     low: 60201.86,
    //     close: 60249.96,
    //     volume: 6.54936,
    //     quote_volume: 394355,
    //     taker_buy_volume: 5.2497,
    //     taker_buy_quote_volume: 316099,
    //     trades: 1055,
    //     closed: false
    //   }
    // ]

    client.unwatch_candles(F, "BTCUSDT", Interval::M1);
    client.unwatch_candles(S, "BTCUSDT", Interval::M1);
}
