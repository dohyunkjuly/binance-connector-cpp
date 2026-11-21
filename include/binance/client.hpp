#pragma once

#include <binance/decimal.hpp>
#include <binance/types.hpp>

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace binance {

namespace detail {
struct TestHook;  // tests only
}  // namespace detail

/// Balances by asset name ("USDT" -> Balance).
using Balances = std::map<std::string, Balance>;
/// Positions by symbol, then side (one-way mode: PositionSide::Both; hedge mode: Long / Short).
using Positions = std::map<std::string, std::map<PositionSide, Position>>;

class Client {
public:
    explicit Client(Config config);
    ~Client();  ///< stops all streams and IO threads; no callbacks run after it returns

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    const Config& config() const noexcept;

    // --- Instruments -----------------------------------------------------------------------

    /// Load trading rules for every symbol of a market (reloaded hourly). Needed before the
    /// rounding helpers; create_order loads them by itself.
    Result<void> load_instruments(Market market);

    /// nullopt if unknown or not loaded yet.
    std::optional<Instrument> get_instrument(Market market, std::string_view symbol) const;
    std::vector<Instrument> get_instruments(Market market) const;

    /// Rounding::Down for buys and Rounding::Up for sells stays on the passive side.
    Result<Decimal> round_price(Market market, std::string_view symbol, Decimal price,
                                Rounding rounding = Rounding::HalfUp) const;
    /// Round a quantity to the symbol's step size (the market-order step size if market_order).
    Result<Decimal> round_qty(Market market, std::string_view symbol, Decimal qty,
                              Rounding rounding = Rounding::Down, bool market_order = false) const;

    // --- Market data ---------------------------------------------------------------------

    /// Last `limit` candles (0: all kept), oldest first; the last may still be forming. Keeps
    /// max(1000, limit) candles.
    Result<std::vector<Candle>> watch_candles(Market market, std::string_view symbol,
                                              Interval interval, std::size_t limit = 0);
    /// Like watch_candles, without the candle still forming.
    Result<std::vector<Candle>> watch_closed_candles(Market market, std::string_view symbol,
                                                     Interval interval, std::size_t limit = 0);
    void unwatch_candles(Market market, std::string_view symbol, Interval interval);
    /// The last `limit` candles (several requests above 1500 futures / 1000 spot).
    Result<std::vector<Candle>> get_candles_rest_api(Market market, std::string_view symbol,
                                                     Interval interval, std::size_t limit = 500);

    /// depth 5/10/20: Binance's top-of-book stream (light). Other depths (up to 1000 futures /
    /// 5000 spot): a full local book from snapshot + diffs. One depth mode per symbol.
    Result<OrderBook> watch_order_book(Market market, std::string_view symbol,
                                       std::size_t depth = 20);
    void unwatch_order_book(Market market, std::string_view symbol);
    /// Binance's limits: futures 5, 10, 20, 50, 100, 500, 1000; spot 1 - 5000.
    Result<OrderBook> get_order_book_rest_api(Market market, std::string_view symbol,
                                              std::size_t limit = 100);

    /// The newest aggregated trade (aggTrade stream).
    Result<Trade> watch_last_trade(Market market, std::string_view symbol);
    void unwatch_last_trade(Market market, std::string_view symbol);
    Result<Trade> get_last_trade_rest_api(Market market, std::string_view symbol);

    /// Every change (bookTicker stream).
    Result<BookTicker> watch_book_ticker(Market market, std::string_view symbol);
    void unwatch_book_ticker(Market market, std::string_view symbol);
    Result<BookTicker> get_book_ticker_rest_api(Market market, std::string_view symbol);

    /// About once per second.
    Result<Ticker24h> watch_ticker_24h(Market market, std::string_view symbol);
    void unwatch_ticker_24h(Market market, std::string_view symbol);
    Result<Ticker24h> get_ticker_24h_rest_api(Market market, std::string_view symbol);

    // --- Account ---------------------------------------------------------------------------
    //
    // Needs Config::credentials. All account watches of a market share one user data stream;
    // after a reconnect everything watched is reloaded.

    /// Spot leaves out assets with a zero balance.
    Result<Balances> watch_balance(Market market);
    void unwatch_balance(Market market);
    Result<Balances> get_balance_rest_api(Market market);

    /// Oldest first; symbol "" = all symbols (higher REST weight). Futures conditional orders
    /// are in watch_algo_orders; spot stop / take-profit orders are here.
    Result<std::vector<Order>> watch_open_orders(Market market, std::string_view symbol = {});
    /// Stop one symbol, or (no symbol) every open-orders watch.
    void unwatch_open_orders(Market market, std::string_view symbol = {});
    Result<std::vector<Order>> get_open_orders_rest_api(Market market,
                                                        std::string_view symbol = {});

    /// Answers from the symbol's open-orders watch, falling back to REST when the order is not
    /// open there or was last read over REST more than 15 minutes ago. NotFound if Binance does
    /// not know the order.
    Result<Order> watch_order(Market market, OrderRef order);
    Result<Order> get_order_rest_api(Market market, OrderRef order);

    Result<Order> create_order(Market market, OrderRequest request);
    Result<Order> create_order_rest_api(Market market, OrderRequest request);

    /// If the order already ended: its final state.
    Result<Order> cancel_order(Market market, OrderRef order);
    Result<Order> cancel_order_rest_api(Market market, OrderRef order);

    /// Futures only, resting LIMIT orders (not algo). Loses queue priority unless only the
    /// quantity goes down.
    Result<Order> amend_order(Market market, AmendRequest request);
    Result<Order> amend_order_rest_api(Market market, AmendRequest request);

    /// Regular and algo orders (REST).
    Result<void> cancel_all_orders(Market market, std::string_view symbol);

    /// Futures only (REST). One result per request, in the same order; never sent twice.
    std::vector<Result<Order>> create_orders(Market market, std::vector<OrderRequest> requests);

    /// Futures only (REST). One result per order, in the same order.
    std::vector<Result<Order>> cancel_orders(Market market, std::vector<OrderRef> orders);

    // --- Futures only ----------------------------------------------------------------------

    /// Open positions only; closed ones are removed.
    Result<Positions> watch_positions();
    void unwatch_positions();
    Result<Positions> get_positions_rest_api();

    /// Open conditional orders (stop, take-profit, trailing), oldest first; symbol "" = all.
    Result<std::vector<Order>> watch_algo_orders(std::string_view symbol = {});
    void unwatch_algo_orders(std::string_view symbol = {});
    Result<std::vector<Order>> get_algo_orders_rest_api(std::string_view symbol = {});

    /// Once per second.
    Result<MarkPrice> watch_mark_price(std::string_view symbol);
    void unwatch_mark_price(std::string_view symbol);
    Result<MarkPrice> get_mark_price_rest_api(std::string_view symbol);

    Result<void> set_leverage(std::string_view symbol, int leverage);
    /// Already set: success.
    Result<void> set_margin_type(std::string_view symbol, MarginType type);
    /// hedge = false: one-way mode. Whole account; needs no open orders or positions. Already
    /// set: success.
    Result<void> set_position_mode(bool hedge);
    Result<CommissionRate> get_commission_rate_rest_api(std::string_view symbol);

    struct Impl;  // client.cpp

private:
    friend struct detail::TestHook;
    std::unique_ptr<Impl> impl_;
};

}  // namespace binance
