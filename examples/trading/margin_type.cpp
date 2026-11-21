// Margin type of a symbol: set_margin_type. Futures, Demo trading.
// Switches BTCUSDT to isolated and back to cross (needs no open position on BTCUSDT).
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./margin_type

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());

    auto isolated = client.set_margin_type("BTCUSDT", MarginType::Isolated);
    std::cout << isolated << "\n";
    // ok

    auto cross = client.set_margin_type("BTCUSDT", MarginType::Cross);
    std::cout << cross << "\n";
    // ok

    // Already so: success, not an error.
    auto again = client.set_margin_type("BTCUSDT", MarginType::Cross);
    std::cout << again << "\n";
    // ok
}
