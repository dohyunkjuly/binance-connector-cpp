// Your trading fees for a symbol: get_commission_rate_rest_api. Futures, Demo trading.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./commission_rate

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());

    // 0.0002 = 0.02%.
    auto fee = client.get_commission_rate_rest_api("BTCUSDT");
    std::cout << fee << "\n";
    // {
    //   symbol: "BTCUSDT",
    //   maker: 0.0002,
    //   taker: 0.0004
    // }
}
