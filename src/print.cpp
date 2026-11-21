// std::cout << x for the public types (see binance/print.hpp).

#include <binance/print.hpp>

#include <chrono>
#include <cstdio>
#include <optional>
#include <string_view>

namespace binance {

namespace detail {

long& print_depth(std::ostream& os) {
    static const int index = std::ios_base::xalloc();
    return os.iword(index);
}

void print_newline(std::ostream& os) {
    os << '\n';
    for (long d = print_depth(os); d > 0; --d) os << "  ";
}

}  // namespace detail

namespace {

/// "2026-11-21 18:43:18.271" (UTC); 0 (never set) prints as 0.
void put_time(std::ostream& os, Timestamp t) {
    using namespace std::chrono;
    if (t == Timestamp{}) {
        os << 0;
        return;
    }
    const auto day = floor<days>(t);
    const year_month_day ymd{day};
    const hh_mm_ss hms{t - day};
    char buf[40];
    std::snprintf(buf, sizeof buf, "%04d-%02u-%02u %02d:%02d:%02d.%03d",
                  static_cast<int>(ymd.year()), static_cast<unsigned>(ymd.month()),
                  static_cast<unsigned>(ymd.day()), static_cast<int>(hms.hours().count()),
                  static_cast<int>(hms.minutes().count()), static_cast<int>(hms.seconds().count()),
                  static_cast<int>(hms.subseconds().count()));
    os << buf;
}

void put(std::ostream& os, const std::string& v) { os << '"' << v << '"'; }
void put(std::ostream& os, bool v) { os << (v ? "true" : "false"); }
void put(std::ostream& os, Timestamp v) { put_time(os, v); }
template <class T>
void put(std::ostream& os, const T& v);
template <class T>
void put(std::ostream& os, const std::optional<T>& v) {
    if (!v) {
        os << "none";
        return;
    }
    put(os, *v);
}
template <class T>
void put(std::ostream& os, const T& v) {
    os << v;
}

/// { name: value, ... }, one field per line.
class Fields {
public:
    explicit Fields(std::ostream& os) : os_(os) {
        os_ << '{';
        ++detail::print_depth(os_);
    }
    ~Fields() {
        --detail::print_depth(os_);
        detail::print_newline(os_);
        os_ << '}';
    }
    Fields(const Fields&) = delete;
    Fields& operator=(const Fields&) = delete;

    template <class V>
    Fields& operator()(std::string_view name, const V& v) {
        if (any_) os_ << ',';
        any_ = true;
        detail::print_newline(os_);
        os_ << name << ": ";
        put(os_, v);
        return *this;
    }

private:
    std::ostream& os_;
    bool any_ = false;
};

}  // namespace

std::ostream& operator<<(std::ostream& os, Market v) { return os << to_string(v); }
std::ostream& operator<<(std::ostream& os, Side v) { return os << to_string(v); }
std::ostream& operator<<(std::ostream& os, PositionSide v) { return os << to_string(v); }
std::ostream& operator<<(std::ostream& os, TimeInForce v) { return os << to_string(v); }
std::ostream& operator<<(std::ostream& os, OrderStatus v) { return os << to_string(v); }
std::ostream& operator<<(std::ostream& os, Interval v) { return os << to_string(v); }

std::ostream& operator<<(std::ostream& os, OrderType v) {
    switch (v) {
        case OrderType::Market: return os << "MARKET";
        case OrderType::Limit: return os << "LIMIT";
        case OrderType::LimitMaker: return os << "LIMIT_MAKER";
        case OrderType::StopMarket: return os << "STOP_MARKET";
        case OrderType::StopLimit: return os << "STOP_LIMIT";
        case OrderType::TakeProfitMarket: return os << "TAKE_PROFIT_MARKET";
        case OrderType::TakeProfitLimit: return os << "TAKE_PROFIT_LIMIT";
        case OrderType::TrailingStopMarket: return os << "TRAILING_STOP_MARKET";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, MarginType v) {
    return os << (v == MarginType::Cross ? "CROSS" : "ISOLATED");
}

std::ostream& operator<<(std::ostream& os, WorkingType v) {
    return os << (v == WorkingType::MarkPrice ? "MARK_PRICE" : "CONTRACT_PRICE");
}

std::ostream& operator<<(std::ostream& os, ErrorCode v) {
    switch (v) {
        case ErrorCode::Network: return os << "Network";
        case ErrorCode::Timeout: return os << "Timeout";
        case ErrorCode::Exchange: return os << "Exchange";
        case ErrorCode::RateLimited: return os << "RateLimited";
        case ErrorCode::IpBanned: return os << "IpBanned";
        case ErrorCode::InvalidArgument: return os << "InvalidArgument";
        case ErrorCode::NotFound: return os << "NotFound";
        case ErrorCode::NotSubscribed: return os << "NotSubscribed";
        case ErrorCode::Parse: return os << "Parse";
        case ErrorCode::Cancelled: return os << "Cancelled";
        case ErrorCode::Internal: return os << "Internal";
    }
    return os;
}

std::ostream& operator<<(std::ostream& os, const Error& v) {
    Fields(os)("code", v.code)("message", v.message)("exchange_code", v.exchange_code);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Instrument& v) {
    Fields(os)("market", v.market)("symbol", v.symbol)("base_asset", v.base_asset)(
        "quote_asset", v.quote_asset)("trading", v.trading)("contract_type", v.contract_type)(
        "tick_size", v.tick_size)("min_price", v.min_price)("max_price", v.max_price)(
        "step_size", v.step_size)("min_qty", v.min_qty)("max_qty", v.max_qty)(
        "market_step_size", v.market_step_size)("market_min_qty", v.market_min_qty)(
        "market_max_qty", v.market_max_qty)("min_notional", v.min_notional)(
        "max_num_orders", v.max_num_orders)("max_num_algo_orders", v.max_num_algo_orders);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Candle& v) {
    Fields(os)("open_time", v.open_time)("close_time", v.close_time)("open", v.open)(
        "high", v.high)("low", v.low)("close", v.close)("volume", v.volume)(
        "quote_volume", v.quote_volume)("taker_buy_volume", v.taker_buy_volume)(
        "taker_buy_quote_volume", v.taker_buy_quote_volume)("trades", v.trades)("closed", v.closed);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Trade& v) {
    Fields(os)("symbol", v.symbol)("id", v.id)("price", v.price)("qty", v.qty)("time", v.time)(
        "buyer_is_maker", v.buyer_is_maker);
    return os;
}

std::ostream& operator<<(std::ostream& os, const BookTicker& v) {
    Fields(os)("symbol", v.symbol)("bid_price", v.bid_price)("bid_qty", v.bid_qty)(
        "ask_price", v.ask_price)("ask_qty", v.ask_qty)("update_id", v.update_id)("time", v.time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Ticker24h& v) {
    Fields(os)("symbol", v.symbol)("last_price", v.last_price)("open_price", v.open_price)(
        "high_price", v.high_price)("low_price", v.low_price)("volume", v.volume)(
        "quote_volume", v.quote_volume)("price_change", v.price_change)(
        "price_change_percent", v.price_change_percent)("trade_count", v.trade_count)(
        "open_time", v.open_time)("close_time", v.close_time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const MarkPrice& v) {
    Fields(os)("symbol", v.symbol)("mark_price", v.mark_price)("index_price", v.index_price)(
        "funding_rate", v.funding_rate)("next_funding_time", v.next_funding_time)("time", v.time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const PriceLevel& v) {
    return os << "{price: " << v.price << ", qty: " << v.qty << "}";  // short: books are long
}

std::ostream& operator<<(std::ostream& os, const OrderBook& v) {
    Fields(os)("symbol", v.symbol)("bids", v.bids)("asks", v.asks)(
        "last_update_id", v.last_update_id)("time", v.time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Order& v) {
    Fields(os)("market", v.market)("symbol", v.symbol)("id", v.id)(
        "client_order_id", v.client_order_id)("is_algo", v.is_algo)(
        "triggered_order_id", v.triggered_order_id)("side", v.side)(
        "position_side", v.position_side)("type", v.type)("time_in_force", v.time_in_force)(
        "status", v.status)("price", v.price)("stop_price", v.stop_price)("avg_price", v.avg_price)(
        "qty", v.qty)("filled_qty", v.filled_qty)("filled_quote_qty", v.filled_quote_qty)(
        "activation_price", v.activation_price)("callback_rate", v.callback_rate)(
        "reduce_only", v.reduce_only)("close_position", v.close_position)(
        "working_type", v.working_type)("create_time", v.create_time)("update_time", v.update_time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Position& v) {
    Fields(os)("symbol", v.symbol)("side", v.side)("amount", v.amount)(
        "entry_price", v.entry_price)("break_even_price", v.break_even_price)(
        "unrealized_pnl", v.unrealized_pnl)("margin_type", v.margin_type)(
        "isolated_wallet", v.isolated_wallet)("leverage", v.leverage)("update_time", v.update_time);
    return os;
}

std::ostream& operator<<(std::ostream& os, const Balance& v) {
    Fields(os)("asset", v.asset)("total", v.total)("available", v.available)("locked", v.locked);
    return os;
}

std::ostream& operator<<(std::ostream& os, const CommissionRate& v) {
    Fields(os)("symbol", v.symbol)("maker", v.maker)("taker", v.taker);
    return os;
}

std::ostream& operator<<(std::ostream& os, const OrderRequest& v) {
    Fields(os)("symbol", v.symbol)("side", v.side)("type", v.type)("qty", v.qty)("price", v.price)(
        "stop_price", v.stop_price)("time_in_force", v.time_in_force)("good_till", v.good_till)(
        "client_order_id", v.client_order_id)("position_side", v.position_side)(
        "reduce_only", v.reduce_only)("close_position", v.close_position)(
        "working_type", v.working_type)("price_protect", v.price_protect)(
        "activation_price", v.activation_price)("callback_rate", v.callback_rate)("quote_qty",
                                                                                  v.quote_qty);
    return os;
}

std::ostream& operator<<(std::ostream& os, const OrderRef& v) {
    Fields(os)("symbol", v.symbol)("order_id", v.order_id)("client_order_id", v.client_order_id)(
        "is_algo", v.is_algo);
    return os;
}

std::ostream& operator<<(std::ostream& os, const AmendRequest& v) {
    Fields(os)("symbol", v.symbol)("order_id", v.order_id)("client_order_id", v.client_order_id)(
        "side", v.side)("qty", v.qty)("price", v.price);
    return os;
}

}  // namespace binance
