#pragma once

// std::cout << x for every result type, std::vector / std::map of them, and Result<T>
// (the value, or the error). All fields, one per line.

#include <binance/client.hpp>
#include <binance/decimal.hpp>
#include <binance/types.hpp>

#include <map>
#include <ostream>
#include <string>
#include <type_traits>
#include <vector>

namespace binance {

// Enums: their Binance names (Market: "futures" / "spot").
std::ostream& operator<<(std::ostream& os, Market v);
std::ostream& operator<<(std::ostream& os, Side v);
std::ostream& operator<<(std::ostream& os, PositionSide v);
std::ostream& operator<<(std::ostream& os, OrderType v);
std::ostream& operator<<(std::ostream& os, TimeInForce v);
std::ostream& operator<<(std::ostream& os, OrderStatus v);
std::ostream& operator<<(std::ostream& os, MarginType v);
std::ostream& operator<<(std::ostream& os, WorkingType v);
std::ostream& operator<<(std::ostream& os, Interval v);
std::ostream& operator<<(std::ostream& os, ErrorCode v);

std::ostream& operator<<(std::ostream& os, const Error& v);
std::ostream& operator<<(std::ostream& os, const Instrument& v);
std::ostream& operator<<(std::ostream& os, const Candle& v);
std::ostream& operator<<(std::ostream& os, const Trade& v);
std::ostream& operator<<(std::ostream& os, const BookTicker& v);
std::ostream& operator<<(std::ostream& os, const Ticker24h& v);
std::ostream& operator<<(std::ostream& os, const MarkPrice& v);
std::ostream& operator<<(std::ostream& os, const PriceLevel& v);
std::ostream& operator<<(std::ostream& os, const OrderBook& v);
std::ostream& operator<<(std::ostream& os, const Order& v);
std::ostream& operator<<(std::ostream& os, const Position& v);
std::ostream& operator<<(std::ostream& os, const Balance& v);
std::ostream& operator<<(std::ostream& os, const CommissionRate& v);
std::ostream& operator<<(std::ostream& os, const OrderRequest& v);
std::ostream& operator<<(std::ostream& os, const OrderRef& v);
std::ostream& operator<<(std::ostream& os, const AmendRequest& v);

namespace detail {
long& print_depth(std::ostream& os);
void print_newline(std::ostream& os);
}  // namespace detail

template <class T>
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v) {
    if (v.empty()) return os << "[]";
    os << '[';
    ++detail::print_depth(os);
    for (std::size_t i = 0; i < v.size(); ++i) {
        if (i) os << ',';
        detail::print_newline(os);
        os << v[i];
    }
    --detail::print_depth(os);
    detail::print_newline(os);
    return os << ']';
}

template <class K, class V>
std::ostream& operator<<(std::ostream& os, const std::map<K, V>& m) {
    if (m.empty()) return os << "{}";
    os << '{';
    ++detail::print_depth(os);
    bool first = true;
    for (const auto& [key, value] : m) {
        if (!first) os << ',';
        first = false;
        detail::print_newline(os);
        if constexpr (std::is_same_v<K, std::string>) {
            os << '"' << key << '"';
        } else {
            os << key;
        }
        os << ": " << value;
    }
    --detail::print_depth(os);
    detail::print_newline(os);
    return os << '}';
}

template <class T>
std::ostream& operator<<(std::ostream& os, const Result<T>& r) {
    if (r) return os << *r;
    return os << r.error();
}

inline std::ostream& operator<<(std::ostream& os, const Result<void>& r) {
    if (r) return os << "ok";
    return os << r.error();
}

}  // namespace binance
