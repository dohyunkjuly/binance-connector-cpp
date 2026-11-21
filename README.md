# binance-connector-cpp

A C++20 connector for **Binance futures** and **spot**, with `watch_*` methods that load
data over the REST API first and then keep it up to date from a live WebSocket stream, along with
plain `get_*_rest_api` methods for one REST request. Reconnects, resyncs after a dropped
connection, listen keys, rate limits and retries are all handled inside the connector.

```cpp
#include <binance/binance.hpp>
using namespace binance;

Client client(config);
// 1st call: loads over REST and subscribes. Later calls: the stream-updated data, from memory.
auto candles = client.watch_candles(Market::Futures, "BTCUSDT", Interval::M1);
// Or one plain REST request:
auto book = client.get_order_book_rest_api(Market::Futures, "BTCUSDT", 100);
```

## Methods

### Market data

| Method | Futures | Spot |
|---|---|---|
| `watch_candles` / `watch_closed_candles` / `get_candles_rest_api` | ✅ | ✅ |
| `watch_order_book` / `get_order_book_rest_api` | ✅ | ✅ |
| `watch_last_trade` / `get_last_trade_rest_api` | ✅ | ✅ |
| `watch_book_ticker` / `get_book_ticker_rest_api` | ✅ | ✅ |
| `watch_ticker_24h` / `get_ticker_24h_rest_api` | ✅ | ✅ |
| `watch_mark_price` / `get_mark_price_rest_api` | ✅ | – |
| `get_instrument` | ✅ | ✅ |

### Account

| Method | Futures | Spot |
|---|---|---|
| `watch_balance` / `get_balance_rest_api` | ✅ | ✅ |
| `watch_open_orders` / `get_open_orders_rest_api` | ✅ | ✅ |
| `watch_order` / `get_order_rest_api` | ✅ | ✅ |
| `watch_positions` / `get_positions_rest_api` | ✅ | – |
| `watch_algo_orders` / `get_algo_orders_rest_api` | ✅ | – |
| `get_commission_rate_rest_api` | ✅ | – |

### Trading

| Method | Futures | Spot |
|---|---|---|
| `create_order` / `create_order_rest_api` | ✅ | ✅ |
| `cancel_order` / `cancel_order_rest_api` | ✅ | ✅ |
| `cancel_all_orders` | ✅ | ✅ |
| `amend_order` / `amend_order_rest_api` | ✅ | – |
| `create_orders` / `cancel_orders` (batch) | ✅ | – |
| `set_leverage` / `set_margin_type` / `set_position_mode` | ✅ | – |

## Building

Requirements: CMake ≥ 3.25, a C++20 compiler (GCC 13+, Clang 17+, MSVC 2022), and
[vcpkg](https://vcpkg.io) for the dependencies (Boost.Asio/Beast, OpenSSL 3, simdjson, spdlog,
GoogleTest).

```sh
export VCPKG_ROOT=/path/to/vcpkg
cmake --preset debug          # or: release
cmake --build --preset debug
ctest --preset debug          # offline unit tests
```

To use it from your own CMake project, add it as a subdirectory (or with `FetchContent`) and link
`binance::connector`:

```cmake
add_subdirectory(binance-connector-cpp)
target_link_libraries(my_bot PRIVATE binance::connector)
```

## Examples

One file per method in [`examples/`](examples).

| | Examples |
|---|---|
| [`market_data/`](examples/market_data) | `candles`, `order_book`, `last_trade`, `book_ticker`, `ticker_24h`, `mark_price` |
| [`account/`](examples/account) | `balance`, `positions`, `open_orders`, `algo_orders`, `order` |
| [`trading/`](examples/trading) | `create_order` (+ `create_order_rest_api`), `amend_order`, `cancel_order`, `cancel_all_orders`, `create_orders`, `cancel_orders`, `leverage`, `margin_type`, `position_mode`, `commission_rate` |

```sh
./build/debug/examples/candles
BINANCE_API_KEY=... BINANCE_API_SECRET=... ./build/debug/examples/open_orders
```

Account and trading need API keys:

```cpp
Config cfg;
cfg.environment = Environment::Demo;               // https://demo.binance.com; omit for mainnet
cfg.credentials = {api_key, hmac_secret, ""};
Client client(cfg);

auto balances = client.watch_balance(Market::Futures);           // asset -> Balance
auto positions = client.watch_positions();                        // symbol -> side -> Position
auto orders = client.watch_open_orders(Market::Futures, "BTCUSDT");  // std::vector<Order>
auto order = client.watch_order(Market::Futures, {"BTCUSDT", order_id});

OrderRequest req;
req.symbol = "BTCUSDT";
req.side = Side::Buy;
req.type = OrderType::Limit;
req.price = *client.round_price(Market::Futures, "BTCUSDT", price, Rounding::Down);
req.qty = *Decimal::parse("0.003");
auto placed = client.create_order(Market::Futures, req);   // or create_order_rest_api: REST only
if (!placed) std::cerr << placed.error().message;   // Binance's code in placed.error().exchange_code
```

## Tests

`ctest` runs the offline unit tests (parsing of captured Binance messages, signing, validation,
decimal math, WebSocket behaviour against a local test server). CI runs them on Linux, macOS and
Windows.

Live tests are skipped unless enabled. Public market data:

```sh
BINANCE_LIVE_TESTS=1 ./build/debug/tests/binance_unit_tests
```

Account and trading tests also need demo trading keys. They place small BTCUSDT orders (futures
and spot) and open and close a 0.003 BTC futures position on that account, and leave it as they
found it:

```sh
BINANCE_LIVE_TESTS=1 BINANCE_DEMO_API_KEY=... BINANCE_DEMO_API_SECRET=... \
    ./build/debug/tests/binance_unit_tests
```

## License

[MIT](LICENSE)
