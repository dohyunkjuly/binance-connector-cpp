// Futures positions: get_positions_rest_api, watch_positions. Demo trading.
// Opens a 0.003 BTCUSDT long with a market order and closes it again.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./positions

#include "../common.hpp"

#include <chrono>
#include <thread>

using namespace examples;

int main() {
    Client client(demo_config());
    constexpr auto F = Market::Futures;

    // One REST request: open positions by symbol, then side.
    auto rest = client.get_positions_rest_api();
    std::cout << rest << "\n";
    // {}

    // First call: loads over REST and opens the account stream.
    auto positions = client.watch_positions();
    std::cout << positions << "\n";
    // {}

    // Open a position: the stream updates what watch_positions returns.
    OrderRequest buy;
    buy.symbol = "BTCUSDT";
    buy.side = Side::Buy;
    buy.type = OrderType::Market;
    buy.qty = *Decimal::parse("0.003");
    if (!client.create_order(F, buy)) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto opened = client.watch_positions();
    std::cout << opened << "\n";
    // {
    //   "BTCUSDT": {
    //     BOTH: {
    //       symbol: "BTCUSDT",
    //       side: BOTH,
    //       amount: 0.003,
    //       entry_price: 60250.3,
    //       break_even_price: 60274.40012,
    //       unrealized_pnl: 0.04546927,
    //       margin_type: CROSS,
    //       isolated_wallet: 0,
    //       leverage: 20,
    //       update_time: 2026-11-21 19:57:29.080
    //     }
    //   }
    // }

    OrderRequest sell = buy;
    sell.side = Side::Sell;
    sell.reduce_only = true;
    if (!client.create_order(F, sell)) return 1;
    std::this_thread::sleep_for(std::chrono::seconds(1));
    auto closed = client.watch_positions();
    std::cout << closed << "\n";
    // {}
}
