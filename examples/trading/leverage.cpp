// Leverage of a symbol: set_leverage. Futures, Demo trading. Sets BTCUSDT to 10x, then
// back to 20x. (The leverage of an open position is in Position::leverage, watch_positions.)
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./leverage

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());

    auto ten = client.set_leverage("BTCUSDT", 10);
    std::cout << ten << "\n";
    // ok

    // 1 - 125 (the maximum depends on the symbol and the position size).
    auto bad = client.set_leverage("BTCUSDT", 500);
    std::cout << bad << "\n";
    // {
    //   code: InvalidArgument,
    //   message: "need a valid symbol and leverage 1-125",
    //   exchange_code: 0
    // }

    auto twenty = client.set_leverage("BTCUSDT", 20);
    std::cout << twenty << "\n";
    // ok
}
