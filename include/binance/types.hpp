#pragma once

#include <binance/decimal.hpp>

#include <cassert>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

namespace binance {

// =============================================================================================
// Time
// =============================================================================================

using Timestamp = std::chrono::sys_time<std::chrono::milliseconds>;

inline constexpr Timestamp from_epoch_ms(std::int64_t ms) noexcept {
    return Timestamp{std::chrono::milliseconds{ms}};
}
inline constexpr std::int64_t to_epoch_ms(Timestamp t) noexcept {
    return t.time_since_epoch().count();
}

// =============================================================================================
// Result / Error
// =============================================================================================

enum class ErrorCode {
    Network,
    Timeout,          ///< no response in time (outcome of the request is unknown)
    Exchange,         ///< Binance returned an error; see Error::exchange_code
    RateLimited,      ///< HTTP 429 or local throttle refused the request
    IpBanned,         ///< HTTP 418
    InvalidArgument,  ///< rejected locally before sending (tick size, step size, missing field...)
    NotFound,
    NotSubscribed,  ///< get_*() called without a matching watch_*()
    Parse,
    Cancelled,  ///< client shut down while the request was pending
    Internal,
};

struct Error {
    ErrorCode code = ErrorCode::Internal;
    std::string message;
    int exchange_code = 0;  ///< Binance error code (e.g. -2019) when code == Exchange
};

/// Value or Error. Never throws; check ok() before value().
template <class T>
class [[nodiscard]] Result {
public:
    Result(T value) : v_(std::in_place_index<0>, std::move(value)) {}
    Result(Error error) : v_(std::in_place_index<1>, std::move(error)) {}

    [[nodiscard]] bool ok() const noexcept { return v_.index() == 0; }
    explicit operator bool() const noexcept { return ok(); }

    T& value() & {
        assert(ok());
        return std::get<0>(v_);
    }
    const T& value() const& {
        assert(ok());
        return std::get<0>(v_);
    }
    T&& value() && {
        assert(ok());
        return std::get<0>(std::move(v_));
    }

    T* operator->() { return &value(); }
    const T* operator->() const { return &value(); }
    T& operator*() & { return value(); }
    const T& operator*() const& { return value(); }

    const Error& error() const {
        assert(!ok());
        return std::get<1>(v_);
    }

    T value_or(T fallback) const& { return ok() ? std::get<0>(v_) : std::move(fallback); }

private:
    std::variant<T, Error> v_;
};

template <>
class [[nodiscard]] Result<void> {
public:
    Result() = default;
    Result(Error error) : error_(std::move(error)) {}

    [[nodiscard]] bool ok() const noexcept { return !error_.has_value(); }
    explicit operator bool() const noexcept { return ok(); }
    const Error& error() const {
        assert(!ok());
        return *error_;
    }

private:
    std::optional<Error> error_;
};

// =============================================================================================
// Enums
// =============================================================================================

enum class Market { Spot, Futures };  ///< Futures = USD-M

enum class Environment { Mainnet, Demo };

enum class Side { Buy, Sell };

/// Order types by meaning, not by Binance's wire string (which differs between spot and futures:
/// futures "TAKE_PROFIT" is a limit order, spot "TAKE_PROFIT" is a market order).
enum class OrderType {
    Market,
    Limit,
    LimitMaker,          ///< spot only: post-only limit
    StopMarket,          ///< futures STOP_MARKET / spot STOP_LOSS
    StopLimit,           ///< futures STOP / spot STOP_LOSS_LIMIT
    TakeProfitMarket,    ///< futures TAKE_PROFIT_MARKET / spot TAKE_PROFIT
    TakeProfitLimit,     ///< futures TAKE_PROFIT / spot TAKE_PROFIT_LIMIT
    TrailingStopMarket,  ///< futures only
};

enum class TimeInForce {
    GTC,
    IOC,
    FOK,
    GTX,  ///< post-only (futures); spot uses OrderType::LimitMaker instead
    GTD,  ///< good till date (futures)
};

enum class OrderStatus {
    New,
    PartiallyFilled,
    Filled,
    Canceled,
    Rejected,
    Expired,
    Triggered,  ///< algo (conditional) order fired; the real order is in Order::triggered_order_id
    Unknown,
};

/// True while the order can still fill or trigger.
inline constexpr bool is_open(OrderStatus s) noexcept {
    return s == OrderStatus::New || s == OrderStatus::PartiallyFilled;
}

enum class PositionSide { Both, Long, Short };  ///< Both = one-way mode

enum class MarginType { Cross, Isolated };

enum class WorkingType { ContractPrice, MarkPrice };  ///< which price triggers a futures stop

enum class Interval { S1, M1, M3, M5, M15, M30, H1, H2, H4, H6, H8, H12, D1, D3, W1, MN1 };

// --- enum <-> string ------------------------------------------------------------------------

/// Binance interval string: "1s", "1m", ..., "1M".
inline constexpr std::string_view to_string(Interval i) noexcept {
    switch (i) {
        case Interval::S1: return "1s";
        case Interval::M1: return "1m";
        case Interval::M3: return "3m";
        case Interval::M5: return "5m";
        case Interval::M15: return "15m";
        case Interval::M30: return "30m";
        case Interval::H1: return "1h";
        case Interval::H2: return "2h";
        case Interval::H4: return "4h";
        case Interval::H6: return "6h";
        case Interval::H8: return "8h";
        case Interval::H12: return "12h";
        case Interval::D1: return "1d";
        case Interval::D3: return "3d";
        case Interval::W1: return "1w";
        case Interval::MN1: return "1M";
    }
    return "";
}

inline constexpr std::optional<Interval> parse_interval(std::string_view s) noexcept {
    for (auto i :
         {Interval::S1, Interval::M1, Interval::M3, Interval::M5, Interval::M15, Interval::M30,
          Interval::H1, Interval::H2, Interval::H4, Interval::H6, Interval::H8, Interval::H12,
          Interval::D1, Interval::D3, Interval::W1, Interval::MN1}) {
        if (to_string(i) == s) return i;
    }
    return std::nullopt;
}

/// Fixed length of one candle; nullopt for 1M (months vary in length).
inline constexpr std::optional<std::chrono::milliseconds> interval_duration(Interval i) noexcept {
    using namespace std::chrono;
    switch (i) {
        case Interval::S1: return seconds{1};
        case Interval::M1: return minutes{1};
        case Interval::M3: return minutes{3};
        case Interval::M5: return minutes{5};
        case Interval::M15: return minutes{15};
        case Interval::M30: return minutes{30};
        case Interval::H1: return hours{1};
        case Interval::H2: return hours{2};
        case Interval::H4: return hours{4};
        case Interval::H6: return hours{6};
        case Interval::H8: return hours{8};
        case Interval::H12: return hours{12};
        case Interval::D1: return days{1};
        case Interval::D3: return days{3};
        case Interval::W1: return weeks{1};
        case Interval::MN1: return std::nullopt;
    }
    return std::nullopt;
}

inline constexpr std::string_view to_string(Market m) noexcept {
    return m == Market::Spot ? "spot" : "futures";
}
inline constexpr std::string_view to_string(Side s) noexcept {
    return s == Side::Buy ? "BUY" : "SELL";
}
inline constexpr std::string_view to_string(PositionSide s) noexcept {
    switch (s) {
        case PositionSide::Both: return "BOTH";
        case PositionSide::Long: return "LONG";
        case PositionSide::Short: return "SHORT";
    }
    return "";
}
inline constexpr std::string_view to_string(TimeInForce t) noexcept {
    switch (t) {
        case TimeInForce::GTC: return "GTC";
        case TimeInForce::IOC: return "IOC";
        case TimeInForce::FOK: return "FOK";
        case TimeInForce::GTX: return "GTX";
        case TimeInForce::GTD: return "GTD";
    }
    return "";
}
inline constexpr std::string_view to_string(OrderStatus s) noexcept {
    switch (s) {
        case OrderStatus::New: return "NEW";
        case OrderStatus::PartiallyFilled: return "PARTIALLY_FILLED";
        case OrderStatus::Filled: return "FILLED";
        case OrderStatus::Canceled: return "CANCELED";
        case OrderStatus::Rejected: return "REJECTED";
        case OrderStatus::Expired: return "EXPIRED";
        case OrderStatus::Triggered: return "TRIGGERED";
        case OrderStatus::Unknown: return "UNKNOWN";
    }
    return "";
}
// =============================================================================================
// Market data
// =============================================================================================

/// Trading rules for one symbol (from exchangeInfo).
struct Instrument {
    Market market = Market::Futures;
    std::string symbol;
    std::string base_asset;
    std::string quote_asset;
    bool trading = false;       ///< status == TRADING
    std::string contract_type;  ///< futures: "PERPETUAL", "CURRENT_QUARTER", ...; spot: empty

    Decimal tick_size;  ///< PRICE_FILTER
    Decimal min_price;
    Decimal max_price;
    Decimal step_size;  ///< LOT_SIZE (limit orders)
    Decimal min_qty;
    Decimal max_qty;
    Decimal market_step_size;  ///< MARKET_LOT_SIZE (market orders)
    Decimal market_min_qty;
    Decimal market_max_qty;
    Decimal min_notional;  ///< MIN_NOTIONAL / NOTIONAL
    int max_num_orders = 0;
    int max_num_algo_orders = 0;
};

struct Candle {
    Timestamp open_time;
    Timestamp close_time;
    Decimal open;
    Decimal high;
    Decimal low;
    Decimal close;
    // double: volumes can exceed Decimal's range.
    double volume = 0;  ///< base asset
    double quote_volume = 0;
    double taker_buy_volume = 0;
    double taker_buy_quote_volume = 0;
    std::int64_t trades = 0;
    bool closed = false;  ///< false for the candle still forming
};

/// Public market trade (aggTrade).
struct Trade {
    std::string symbol;
    std::int64_t id = 0;
    Decimal price;
    Decimal qty;
    Timestamp time;
    bool buyer_is_maker = false;  ///< true = the aggressor sold
};

struct BookTicker {
    std::string symbol;
    Decimal bid_price;
    Decimal bid_qty;
    Decimal ask_price;
    Decimal ask_qty;
    std::int64_t update_id = 0;
    Timestamp time;  ///< futures only; spot leaves it empty
};

struct Ticker24h {
    std::string symbol;
    Decimal last_price;
    Decimal open_price;
    Decimal high_price;
    Decimal low_price;
    double volume = 0;  ///< base asset
    double quote_volume = 0;
    Decimal price_change;
    Decimal price_change_percent;
    std::int64_t trade_count = 0;
    Timestamp open_time;
    Timestamp close_time;
};

/// Futures only.
struct MarkPrice {
    std::string symbol;
    Decimal mark_price;
    Decimal index_price;
    Decimal funding_rate;
    Timestamp next_funding_time;
    Timestamp time;
};

struct PriceLevel {
    Decimal price;
    Decimal qty;
};

struct OrderBook {
    std::string symbol;
    std::vector<PriceLevel> bids;  ///< best (highest) first
    std::vector<PriceLevel> asks;  ///< best (lowest) first
    std::int64_t last_update_id = 0;
    Timestamp time;
};

// =============================================================================================
// Account data
// =============================================================================================

struct Order {
    Market market = Market::Futures;
    std::string symbol;
    std::int64_t id = 0;                  ///< orderId, or algoId when is_algo
    std::string client_order_id;          ///< clientOrderId / clientAlgoId
    bool is_algo = false;                 ///< futures conditional order held by the algo service
    std::int64_t triggered_order_id = 0;  ///< algo: the real order created when it triggered

    Side side = Side::Buy;
    PositionSide position_side = PositionSide::Both;
    OrderType type = OrderType::Limit;
    TimeInForce time_in_force = TimeInForce::GTC;
    OrderStatus status = OrderStatus::Unknown;

    Decimal price;       ///< limit price (0 for market)
    Decimal stop_price;  ///< trigger price for stop / take-profit
    Decimal avg_price;
    Decimal qty;
    Decimal filled_qty;
    Decimal filled_quote_qty;
    Decimal activation_price;  ///< trailing stop
    Decimal callback_rate;     ///< trailing stop, percent

    bool reduce_only = false;
    bool close_position = false;
    WorkingType working_type = WorkingType::ContractPrice;

    Timestamp create_time;
    Timestamp update_time;
};

/// Futures position. In hedge mode a symbol has a Long and a Short position.
struct Position {
    std::string symbol;
    PositionSide side = PositionSide::Both;
    Decimal amount;  ///< signed: negative = short (one-way mode)
    Decimal entry_price;
    Decimal break_even_price;
    Decimal unrealized_pnl;
    MarginType margin_type = MarginType::Cross;
    Decimal isolated_wallet;
    int leverage = 0;
    Timestamp update_time;
};

struct Balance {
    std::string asset;
    Decimal total;  ///< futures: wallet balance; spot: free + locked
    /// spot: free. futures: re-read over REST after balance / order changes (ignores PnL moves).
    Decimal available;
    Decimal locked;  ///< total - available
};

// =============================================================================================
// Requests
// =============================================================================================

struct OrderRequest {
    std::string symbol;
    Side side = Side::Buy;
    OrderType type = OrderType::Limit;

    std::optional<Decimal> qty;  ///< required unless close_position / quote_qty
    std::optional<Decimal> price;
    std::optional<Decimal> stop_price;         ///< trigger price for stop / take-profit
    std::optional<TimeInForce> time_in_force;  ///< default GTC for limit types
    std::optional<Timestamp> good_till;        ///< with TimeInForce::GTD

    std::optional<std::string> client_order_id;  ///< generated if not set

    // futures
    std::optional<PositionSide> position_side;  ///< required in hedge mode
    bool reduce_only = false;
    bool close_position = false;
    WorkingType working_type = WorkingType::ContractPrice;
    bool price_protect = false;
    std::optional<Decimal> activation_price;  ///< trailing stop
    std::optional<Decimal> callback_rate;     ///< trailing stop, percent (0.1 - 10)

    // spot
    std::optional<Decimal> quote_qty;  ///< market order by quote amount (spend 100 USDT)
};

/// One existing order: by id or by client order id (either is enough).
struct OrderRef {
    std::string symbol;                          ///< required for regular (non-algo) orders
    std::optional<std::int64_t> order_id;        ///< orderId, or algoId when is_algo
    std::optional<std::string> client_order_id;  ///< clientOrderId / clientAlgoId
    bool is_algo = false;                        ///< a conditional order (Order::is_algo)
};

/// Your trading fee rates for a symbol (0.0002 = 0.02%).
struct CommissionRate {
    std::string symbol;
    Decimal maker;
    Decimal taker;
};

struct AmendRequest {
    std::string symbol;
    std::optional<std::int64_t> order_id;
    std::optional<std::string> client_order_id;  ///< one of order_id / client_order_id
    Side side = Side::Buy;                       ///< futures requires it
    Decimal qty;
    Decimal price;
};

// =============================================================================================
// Config
// =============================================================================================

enum class LogLevel { Trace, Debug, Info, Warn, Error };

/// Plug in your own logger; the default writes through spdlog.
class Logger {
public:
    virtual ~Logger() = default;
    virtual void log(LogLevel level, std::string_view message) = 0;
};

struct Credentials {
    std::string api_key;
    std::string hmac_secret;
    std::string ed25519_private_key;  ///< PEM; enables WS API session logon
};

struct Config {
    Credentials credentials;  ///< empty = market data only
    Environment environment = Environment::Mainnet;
    int io_threads = 1;
    std::chrono::milliseconds recv_window{5000};
    std::chrono::milliseconds request_timeout{10000};
    std::shared_ptr<Logger> logger;  ///< null = default spdlog logger
};

}  // namespace binance
