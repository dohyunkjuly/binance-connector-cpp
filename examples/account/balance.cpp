// Balances: get_balance_rest_api, watch_balance. Futures and spot are separate accounts.
// Demo trading.
//
//   BINANCE_API_KEY=... BINANCE_API_SECRET=... ./balance

#include "../common.hpp"

using namespace examples;

int main() {
    Client client(demo_config());
    constexpr auto F = Market::Futures;
    constexpr auto S = Market::Spot;

    // One REST request: every asset of the futures account.
    auto rest = client.get_balance_rest_api(F);
    std::cout << rest << "\n";
    // {
    //   "BNB": {
    //     asset: "BNB",
    //     total: 0,
    //     available: 0,
    //     locked: 0
    //   },
    //   "BTC": {
    //     asset: "BTC",
    //     total: 0.01,
    //     available: 0.01,
    //     locked: 0
    //   },
    //   "ETH": {
    //     asset: "ETH",
    //     total: 0,
    //     available: 0,
    //     locked: 0
    //   },
    //   "FDUSD": {
    //     asset: "FDUSD",
    //     total: 0,
    //     available: 0,
    //     locked: 0
    //   },
    //   "U": {
    //     asset: "U",
    //     total: 0,
    //     available: 0,
    //     locked: 0
    //   },
    //   "USD1": {
    //     asset: "USD1",
    //     total: 0,
    //     available: 0,
    //     locked: 0
    //   },
    //   "USDC": {
    //     asset: "USDC",
    //     total: 5000,
    //     available: 5000,
    //     locked: 0
    //   },
    //   "USDT": {
    //     asset: "USDT",
    //     total: 5004.2975861,
    //     available: 5004.2975861,
    //     locked: 0
    //   }
    // }

    // First call: loads over REST and opens the account stream. Later calls: from memory.
    auto balances = client.watch_balance(F);
    std::cout << balances->at("USDT") << "\n";
    // {
    //   asset: "USDT",
    //   total: 5004.2975861,
    //   available: 5004.2975861,
    //   locked: 0
    // }

    // Spot: free + locked per asset (assets with nothing are left out).
    auto spot = client.watch_balance(S);
    std::cout << spot << "\n";
    // {
    //   "BNB": {
    //     asset: "BNB",
    //     total: 1.99915742,
    //     available: 1.99915742,
    //     locked: 0
    //   },
    //   "BTC": {
    //     asset: "BTC",
    //     total: 0.0538,
    //     available: 0.0538,
    //     locked: 0
    //   },
    //   "ETH": {
    //     asset: "ETH",
    //     total: 1,
    //     available: 1,
    //     locked: 0
    //   },
    //   "USDT": {
    //     asset: "USDT",
    //     total: 4672.8188598,
    //     available: 4672.8188598,
    //     locked: 0
    //   }
    // }

    client.unwatch_balance(F);
    client.unwatch_balance(S);
}
