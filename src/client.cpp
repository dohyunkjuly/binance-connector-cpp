#include <binance/client.hpp>
#include <binance/version.hpp>

#include "http.hpp"
#include "websocket.hpp"

#include <boost/asio/as_tuple.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <openssl/pem.h>

#include <simdjson.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <deque>
#include <future>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>
#include <tuple>
#include <unordered_map>

namespace binance {

using Done = std::function<void(Result<void>)>;

namespace asio = boost::asio;
namespace ondemand = simdjson::ondemand;
using namespace std::chrono_literals;

namespace {

constexpr auto kAwait = asio::as_tuple(asio::use_awaitable);

std::int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

Timestamp now_ts() { return from_epoch_ms(now_ms()); }

/// Set on library IO threads; blocking calls refuse to block there.
thread_local bool t_in_io_thread = false;

}  // namespace

// =============================================================================================
// 1. Signing
// =============================================================================================

namespace detail {

struct PkeyDeleter {
    void operator()(EVP_PKEY* k) const noexcept { EVP_PKEY_free(k); }
};
using PkeyPtr = std::unique_ptr<EVP_PKEY, PkeyDeleter>;

std::string hmac_sha256_hex(std::string_view key, std::string_view data) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(), md, &len);
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out(len * 2, '\0');
    for (unsigned int i = 0; i < len; ++i) {
        out[2 * i] = kHex[md[i] >> 4];
        out[2 * i + 1] = kHex[md[i] & 0xF];
    }
    return out;
}

Result<PkeyPtr> load_ed25519_key(std::string_view pem) {
    BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    if (!bio) return Error{ErrorCode::Internal, "BIO_new_mem_buf failed"};
    PkeyPtr key(PEM_read_bio_PrivateKey(bio, nullptr, nullptr, nullptr));
    BIO_free(bio);
    if (!key) {
        return Error{ErrorCode::InvalidArgument, "could not read the Ed25519 private key (PEM)"};
    }
    if (EVP_PKEY_get_id(key.get()) != EVP_PKEY_ED25519) {
        return Error{ErrorCode::InvalidArgument, "the private key is not an Ed25519 key"};
    }
    return key;
}

std::string ed25519_sign_base64(EVP_PKEY* key, std::string_view data) {
    unsigned char sig[64];
    std::size_t sig_len = sizeof(sig);
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestSignInit(ctx, nullptr, nullptr, nullptr, key);
    EVP_DigestSign(ctx, sig, &sig_len, reinterpret_cast<const unsigned char*>(data.data()),
                   data.size());
    EVP_MD_CTX_free(ctx);
    std::string out(4 * ((sig_len + 2) / 3), '\0');
    const int n = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), sig,
                                  static_cast<int>(sig_len));
    out.resize(static_cast<std::size_t>(n));
    return out;
}

/// Loads the key on every call; for tests. The client keeps the key loaded.
Result<std::string> ed25519_sign_base64(std::string_view pem, std::string_view data) {
    auto key = load_ed25519_key(pem);
    if (!key) return key.error();
    return ed25519_sign_base64(key->get(), data);
}

}  // namespace detail

// =============================================================================================
// 2. Endpoints
// =============================================================================================

namespace {

struct Endpoints {
    std::string rest_host;
    std::string api_prefix;
    std::string stream_host;
    std::string ws_api_host;
    std::string ws_api_target;
};

Endpoints endpoints(Market m, Environment env) {
    const bool demo = env == Environment::Demo;
    if (m == Market::Futures) {
        return demo ? Endpoints{"demo-fapi.binance.com", "/fapi/v1", "demo-fstream.binance.com",
                                "testnet.binancefuture.com", "/ws-fapi/v1"}
                    : Endpoints{"fapi.binance.com", "/fapi/v1", "fstream.binance.com",
                                "ws-fapi.binance.com", "/ws-fapi/v1"};
    }
    return demo ? Endpoints{"demo-api.binance.com", "/api/v3", "demo-stream.binance.com",
                            "demo-ws-api.binance.com", "/ws-api/v3"}
                : Endpoints{"api.binance.com", "/api/v3", "stream.binance.com",
                            "ws-api.binance.com", "/ws-api/v3"};
}

/// Conservative defaults until exchangeInfo tells us the real ones.
std::vector<detail::RateLimit> default_rate_limits(Market m) {
    if (m == Market::Futures) return {{1min, 2400, false}, {10s, 300, true}, {1min, 1200, true}};
    return {{1min, 6000, false}, {10s, 100, true}, {24h, 200000, true}};
}

/// Which URL path carries a stream. USD-M futures split streams by kind: book data on /public,
/// everything else (klines, trades, mark price, tickers) on /market. The legacy /stream path
/// accepts every subscription but only delivers the /public ones.
std::string stream_target(Market m, std::string_view stream) {
    if (m == Market::Spot) return "/stream";
    if (stream.find("@bookTicker") != std::string_view::npos ||
        stream.find("@depth") != std::string_view::npos || stream == "!bookTicker") {
        return "/public/stream";
    }
    return "/market/stream";
}

}  // namespace

// =============================================================================================
// 3. JSON parsing
// =============================================================================================

namespace {

template <class T>
bool get_field(ondemand::object& o, std::string_view key, T& out) {
    return !o[key].get(out);
}

bool get_dec(ondemand::object& o, std::string_view key, Decimal& out) {
    std::string_view s;
    if (o[key].get_string().get(s)) return false;
    auto d = Decimal::parse(s);
    if (!d) return false;
    out = *d;
    return true;
}

bool get_dec(simdjson::simdjson_result<ondemand::value> v, Decimal& out) {
    std::string_view s;
    if (v.get_string().get(s)) return false;
    auto d = Decimal::parse(s);
    if (!d) return false;
    out = *d;
    return true;
}

/// Number in a JSON string ("123.45") as double; for volumes.
bool get_vol(ondemand::object& o, std::string_view key, double& out) {
    return !o[key].get_double_in_string().get(out);
}

bool get_vol(simdjson::simdjson_result<ondemand::value> v, double& out) {
    return !v.get_double_in_string().get(out);
}

std::optional<std::chrono::milliseconds> rate_interval(std::string_view unit, std::int64_t n) {
    if (unit == "SECOND") return std::chrono::seconds{n};
    if (unit == "MINUTE") return std::chrono::minutes{n};
    if (unit == "HOUR") return std::chrono::hours{n};
    if (unit == "DAY") return std::chrono::days{n};
    return std::nullopt;
}

Error parse_error(std::string_view what) {
    return Error{ErrorCode::Parse, "unexpected " + std::string(what) + " format from Binance"};
}

/// {"code":-1121,"msg":"Invalid symbol."} -> Error
Error api_error(const detail::HttpResponse& r) {
    simdjson::padded_string ps(r.body);
    ondemand::parser parser;
    ondemand::document doc;
    std::int64_t code = 0;
    std::string_view msg;
    if (!parser.iterate(ps).get(doc) && !doc["code"].get_int64().get(code) &&
        !doc["msg"].get_string().get(msg)) {
        return Error{ErrorCode::Exchange, std::string(msg), static_cast<int>(code)};
    }
    return Error{r.status >= 500 ? ErrorCode::Network : ErrorCode::Exchange,
                 "HTTP " + std::to_string(r.status) + ": " + r.body.substr(0, 200)};
}

/// The "k" object of a kline stream event.
Result<Candle> parse_kline_object(ondemand::object k) {
    Candle c;
    std::int64_t t = 0, close_t = 0, n = 0;
    bool closed = false;
    if (!get_field(k, "t", t) || !get_field(k, "T", close_t) || !get_dec(k, "o", c.open) ||
        !get_dec(k, "c", c.close) || !get_dec(k, "h", c.high) || !get_dec(k, "l", c.low) ||
        !get_vol(k, "v", c.volume) || !get_field(k, "n", n) || !get_field(k, "x", closed) ||
        !get_vol(k, "q", c.quote_volume) || !get_vol(k, "V", c.taker_buy_volume) ||
        !get_vol(k, "Q", c.taker_buy_quote_volume)) {
        return parse_error("kline event");
    }
    c.open_time = from_epoch_ms(t);
    c.close_time = from_epoch_ms(close_t);
    c.trades = n;
    c.closed = closed;
    return c;
}

bool get_str(ondemand::object& o, std::string_view key, std::string& out) {
    std::string_view v;
    if (o[key].get_string().get(v)) return false;
    out = v;
    return true;
}

bool get_time(ondemand::object& o, std::string_view key, Timestamp& out) {
    std::int64_t ms = 0;
    if (o[key].get_int64().get(ms)) return false;
    out = from_epoch_ms(ms);
    return true;
}

/// Like get_dec, but "" (funding rate of delivery contracts) reads as 0.
bool get_dec_or_zero(ondemand::object& o, std::string_view key, Decimal& out) {
    std::string_view v;
    if (o[key].get_string().get(v)) return false;
    if (v.empty()) {
        out = Decimal{};
        return true;
    }
    auto d = Decimal::parse(v);
    if (!d) return false;
    out = *d;
    return true;
}

/// aggTrade. Stream: {"e":"aggTrade","s":..,"a":..,"p":..,"q":..,"T":..,"m":..}. REST rows have
/// no "s": pass the symbol.
Result<Trade> parse_agg_trade(ondemand::object o, std::string_view symbol = {}) {
    Trade t;
    if (symbol.empty()) {
        if (!get_str(o, "s", t.symbol)) return parse_error("aggTrade");
    } else {
        t.symbol = symbol;
    }
    if (!get_field(o, "a", t.id) || !get_dec(o, "p", t.price) || !get_dec(o, "q", t.qty) ||
        !get_time(o, "T", t.time) || !get_field(o, "m", t.buyer_is_maker)) {
        return parse_error("aggTrade");
    }
    return t;
}

/// Stream: {"u":..,"s":..,"b":..,"B":..,"a":..,"A":..} (+ "T" on futures).
Result<BookTicker> parse_book_ticker_stream(ondemand::object o) {
    BookTicker b;
    if (!get_field(o, "u", b.update_id) || !get_str(o, "s", b.symbol) ||
        !get_dec(o, "b", b.bid_price) || !get_dec(o, "B", b.bid_qty) ||
        !get_dec(o, "a", b.ask_price) || !get_dec(o, "A", b.ask_qty)) {
        return parse_error("bookTicker event");
    }
    get_time(o, "T", b.time);
    return b;
}

Result<BookTicker> parse_book_ticker_object(ondemand::object o) {
    BookTicker b;
    if (!get_str(o, "symbol", b.symbol) || !get_dec(o, "bidPrice", b.bid_price) ||
        !get_dec(o, "bidQty", b.bid_qty) || !get_dec(o, "askPrice", b.ask_price) ||
        !get_dec(o, "askQty", b.ask_qty)) {
        return parse_error("bookTicker");
    }
    get_field(o, "lastUpdateId", b.update_id);
    get_time(o, "time", b.time);
    return b;
}

Result<Ticker24h> parse_ticker_stream(ondemand::object o) {
    Ticker24h t;
    if (!get_str(o, "s", t.symbol) || !get_dec(o, "p", t.price_change) ||
        !get_dec(o, "P", t.price_change_percent) || !get_dec(o, "c", t.last_price) ||
        !get_dec(o, "o", t.open_price) || !get_dec(o, "h", t.high_price) ||
        !get_dec(o, "l", t.low_price) || !get_vol(o, "v", t.volume) ||
        !get_vol(o, "q", t.quote_volume) || !get_time(o, "O", t.open_time) ||
        !get_time(o, "C", t.close_time) || !get_field(o, "n", t.trade_count)) {
        return parse_error("24hrTicker event");
    }
    return t;
}

Result<Ticker24h> parse_ticker_object(ondemand::object o) {
    Ticker24h t;
    if (!get_str(o, "symbol", t.symbol) || !get_dec(o, "priceChange", t.price_change) ||
        !get_dec(o, "priceChangePercent", t.price_change_percent) ||
        !get_dec(o, "lastPrice", t.last_price) || !get_dec(o, "openPrice", t.open_price) ||
        !get_dec(o, "highPrice", t.high_price) || !get_dec(o, "lowPrice", t.low_price) ||
        !get_vol(o, "volume", t.volume) || !get_vol(o, "quoteVolume", t.quote_volume) ||
        !get_time(o, "openTime", t.open_time) || !get_time(o, "closeTime", t.close_time) ||
        !get_field(o, "count", t.trade_count)) {
        return parse_error("24hr ticker");
    }
    return t;
}

/// Stream: {"e":"markPriceUpdate","E":..,"s":..,"p":..,"i":..,"r":..,"T":..}
Result<MarkPrice> parse_mark_price_stream(ondemand::object o) {
    MarkPrice m;
    if (!get_time(o, "E", m.time) || !get_str(o, "s", m.symbol) || !get_dec(o, "p", m.mark_price) ||
        !get_dec(o, "i", m.index_price) || !get_dec_or_zero(o, "r", m.funding_rate) ||
        !get_time(o, "T", m.next_funding_time)) {
        return parse_error("markPriceUpdate event");
    }
    return m;
}

Result<MarkPrice> parse_mark_price_object(ondemand::object o) {
    MarkPrice m;
    if (!get_str(o, "symbol", m.symbol) || !get_dec(o, "markPrice", m.mark_price) ||
        !get_dec(o, "indexPrice", m.index_price) ||
        !get_dec_or_zero(o, "lastFundingRate", m.funding_rate) ||
        !get_time(o, "nextFundingTime", m.next_funding_time) || !get_time(o, "time", m.time)) {
        return parse_error("premiumIndex");
    }
    return m;
}

template <class F>
auto parse_root_object(std::string_view json, std::string_view what,
                       F f) -> decltype(f(std::declval<ondemand::object>())) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::object o;
    if (parser.iterate(ps).get(doc) || doc.get_object().get(o)) return parse_error(what);
    return f(o);
}

}  // namespace

namespace detail {

Result<std::vector<Instrument>> parse_exchange_info(Market market, std::string_view json,
                                                    std::vector<RateLimit>* limits) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(ps).get(doc)) return parse_error("exchangeInfo");

    if (limits) {
        limits->clear();
        ondemand::array arr;
        if (!doc["rateLimits"].get_array().get(arr)) {
            for (auto item : arr) {
                ondemand::object o;
                if (item.get_object().get(o)) continue;
                std::string_view type, unit;
                std::int64_t num = 0, limit = 0;
                if (!get_field(o, "rateLimitType", type) || !get_field(o, "interval", unit) ||
                    !get_field(o, "intervalNum", num) || !get_field(o, "limit", limit)) {
                    continue;
                }
                auto iv = rate_interval(unit, num);
                if (!iv || (type != "REQUEST_WEIGHT" && type != "ORDERS")) continue;
                limits->push_back(RateLimit{*iv, static_cast<int>(limit), type == "ORDERS"});
            }
        }
    }

    ondemand::array symbols;
    if (doc["symbols"].get_array().get(symbols)) return parse_error("exchangeInfo");
    std::vector<Instrument> out;
    for (auto item : symbols) {
        ondemand::object o;
        if (item.get_object().get(o)) return parse_error("exchangeInfo symbol");
        Instrument ins;
        ins.market = market;
        std::string_view sym, status, base, quote, contract;
        if (!get_field(o, "symbol", sym) || !get_field(o, "status", status) ||
            !get_field(o, "baseAsset", base) || !get_field(o, "quoteAsset", quote)) {
            return parse_error("exchangeInfo symbol");
        }
        ins.symbol = sym;
        ins.trading = status == "TRADING";
        ins.base_asset = base;
        ins.quote_asset = quote;
        if (market == Market::Futures && get_field(o, "contractType", contract)) {
            ins.contract_type = contract;
        }

        ondemand::array filters;
        if (o["filters"].get_array().get(filters)) return parse_error("exchangeInfo filters");
        for (auto fv : filters) {
            ondemand::object f;
            if (fv.get_object().get(f)) continue;
            std::string_view type;
            if (!get_field(f, "filterType", type)) continue;
            // Values that do not fit Decimal (absurd maximums) are left at 0 = "no limit known".
            if (type == "PRICE_FILTER") {
                get_dec(f, "minPrice", ins.min_price);
                get_dec(f, "maxPrice", ins.max_price);
                get_dec(f, "tickSize", ins.tick_size);
            } else if (type == "LOT_SIZE") {
                get_dec(f, "minQty", ins.min_qty);
                get_dec(f, "maxQty", ins.max_qty);
                get_dec(f, "stepSize", ins.step_size);
            } else if (type == "MARKET_LOT_SIZE") {
                get_dec(f, "minQty", ins.market_min_qty);
                get_dec(f, "maxQty", ins.market_max_qty);
                get_dec(f, "stepSize", ins.market_step_size);
            } else if (type == "MIN_NOTIONAL") {
                // futures: "notional"; old spot filter: "minNotional"
                if (!get_dec(f, "notional", ins.min_notional)) {
                    get_dec(f, "minNotional", ins.min_notional);
                }
            } else if (type == "NOTIONAL") {
                get_dec(f, "minNotional", ins.min_notional);
            } else if (type == "MAX_NUM_ORDERS") {
                std::int64_t v = 0;
                if (get_field(f, "limit", v) || get_field(f, "maxNumOrders", v)) {
                    ins.max_num_orders = static_cast<int>(v);
                }
            } else if (type == "MAX_NUM_ALGO_ORDERS") {
                std::int64_t v = 0;
                if (get_field(f, "limit", v) || get_field(f, "maxNumAlgoOrders", v)) {
                    ins.max_num_algo_orders = static_cast<int>(v);
                }
            }
        }
        // Spot sometimes leaves MARKET_LOT_SIZE step at 0: market orders then use LOT_SIZE.
        if (ins.market_step_size.is_zero()) ins.market_step_size = ins.step_size;
        out.push_back(std::move(ins));
    }
    return out;
}

/// REST /klines: [[openTime,"o","h","l","c","v",closeTime,"q",trades,"V","Q","ignore"],...]
Result<std::vector<Candle>> parse_klines(std::string_view json, Timestamp now) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::array rows;
    if (parser.iterate(ps).get(doc) || doc.get_array().get(rows)) return parse_error("klines");
    std::vector<Candle> out;
    for (auto row_v : rows) {
        ondemand::array row;
        if (row_v.get_array().get(row)) return parse_error("klines");
        Candle c;
        int i = 0;
        bool ok = true;
        for (auto v : row) {
            std::int64_t n = 0;
            switch (i++) {
                case 0:
                    ok = ok && !v.get_int64().get(n);
                    c.open_time = from_epoch_ms(n);
                    break;
                case 1: ok = ok && get_dec(v, c.open); break;
                case 2: ok = ok && get_dec(v, c.high); break;
                case 3: ok = ok && get_dec(v, c.low); break;
                case 4: ok = ok && get_dec(v, c.close); break;
                case 5: ok = ok && get_vol(v, c.volume); break;
                case 6:
                    ok = ok && !v.get_int64().get(n);
                    c.close_time = from_epoch_ms(n);
                    break;
                case 7: ok = ok && get_vol(v, c.quote_volume); break;
                case 8:
                    ok = ok && !v.get_int64().get(n);
                    c.trades = n;
                    break;
                case 9: ok = ok && get_vol(v, c.taker_buy_volume); break;
                case 10: ok = ok && get_vol(v, c.taker_buy_quote_volume); break;
                default: break;
            }
        }
        if (!ok || i < 11) return parse_error("klines row");
        c.closed = c.close_time < now;
        out.push_back(c);
    }
    return out;
}

/// REST aggTrades?limit=1: [{"a":..,"p":..,"q":..,"T":..,"m":..}] -> newest, or nullopt if empty.
Result<std::optional<Trade>> parse_agg_trades_rest(std::string_view json, std::string_view symbol) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::array rows;
    if (parser.iterate(ps).get(doc) || doc.get_array().get(rows)) return parse_error("aggTrades");
    std::optional<Trade> last;
    for (auto row : rows) {
        ondemand::object o;
        if (row.get_object().get(o)) return parse_error("aggTrades");
        auto t = parse_agg_trade(o, symbol);
        if (!t) return t.error();
        last = std::move(*t);
    }
    return last;
}

Result<Trade> parse_agg_trade_event(std::string_view json) {
    return parse_root_object(json, "aggTrade",
                             [](ondemand::object o) { return parse_agg_trade(o); });
}
Result<BookTicker> parse_book_ticker_rest(std::string_view json) {
    return parse_root_object(json, "bookTicker", parse_book_ticker_object);
}
Result<BookTicker> parse_book_ticker_event(std::string_view json) {
    return parse_root_object(json, "bookTicker", parse_book_ticker_stream);
}
Result<Ticker24h> parse_ticker_24h_rest(std::string_view json) {
    return parse_root_object(json, "24hr ticker", parse_ticker_object);
}
Result<Ticker24h> parse_ticker_24h_event(std::string_view json) {
    return parse_root_object(json, "24hr ticker", parse_ticker_stream);
}
Result<MarkPrice> parse_mark_price_rest(std::string_view json) {
    return parse_root_object(json, "premiumIndex", parse_mark_price_object);
}
Result<MarkPrice> parse_mark_price_event(std::string_view json) {
    return parse_root_object(json, "markPrice", parse_mark_price_stream);
}

Result<Candle> parse_kline_event(std::string_view json) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::object k;
    if (parser.iterate(ps).get(doc) || doc["k"].get_object().get(k)) {
        return parse_error("kline event");
    }
    return parse_kline_object(k);
}

}  // namespace detail

// =============================================================================================
// 3b. JSON parsing: futures account (user data stream events, REST snapshots)
// =============================================================================================

namespace detail {

/// Futures account-wide settings (hedge mode: orders need a position side).
struct AccountSettings {
    bool hedge_mode = false;  ///< true: separate Long and Short positions per symbol
    bool multi_assets_margin = false;
};

/// Futures per-symbol settings; they exist even without a position.
struct SymbolSettings {
    std::string symbol;
    int leverage = 0;
    MarginType margin_type = MarginType::Cross;
};

struct UserEvent {
    enum class Kind {
        Order,
        Account,
        Config,
        MarginCall,
        ListenKeyExpired,
        StreamTerminated,  ///< spot: the subscription ended (subscribe again)
        Other
    };
    Kind kind = Kind::Other;
    std::int64_t time = 0;  ///< E: event time, ms
    std::optional<Order> order;
    std::vector<Position> positions;  ///< Account, MarginCall (leverage not included)
    std::vector<Balance> balances;    ///< Account. Futures: total only; spot: all fields
    std::string symbol;               ///< Config: leverage changed for this symbol
    int leverage = 0;
    std::optional<bool> multi_assets;
};

}  // namespace detail

namespace {

std::optional<Side> side_from(std::string_view s) {
    if (s == "BUY") return Side::Buy;
    if (s == "SELL") return Side::Sell;
    return std::nullopt;
}

std::optional<PositionSide> position_side_from(std::string_view s) {
    if (s == "BOTH") return PositionSide::Both;
    if (s == "LONG") return PositionSide::Long;
    if (s == "SHORT") return PositionSide::Short;
    return std::nullopt;
}

std::optional<OrderType> futures_order_type_from(std::string_view s) {
    if (s == "LIMIT") return OrderType::Limit;
    if (s == "MARKET" || s == "LIQUIDATION") return OrderType::Market;
    if (s == "STOP") return OrderType::StopLimit;
    if (s == "STOP_MARKET") return OrderType::StopMarket;
    if (s == "TAKE_PROFIT") return OrderType::TakeProfitLimit;
    if (s == "TAKE_PROFIT_MARKET") return OrderType::TakeProfitMarket;
    if (s == "TRAILING_STOP_MARKET") return OrderType::TrailingStopMarket;
    return std::nullopt;
}

std::optional<TimeInForce> time_in_force_from(std::string_view s) {
    if (s == "GTC" || s == "GTE_GTC") return TimeInForce::GTC;  // GTE_GTC: closePosition orders
    if (s == "IOC") return TimeInForce::IOC;
    if (s == "FOK") return TimeInForce::FOK;
    if (s == "GTX") return TimeInForce::GTX;
    if (s == "GTD") return TimeInForce::GTD;
    return std::nullopt;
}

std::optional<OrderStatus> order_status_from(std::string_view s) {
    if (s == "NEW") return OrderStatus::New;
    if (s == "PARTIALLY_FILLED") return OrderStatus::PartiallyFilled;
    if (s == "FILLED") return OrderStatus::Filled;
    if (s == "CANCELED") return OrderStatus::Canceled;
    if (s == "REJECTED") return OrderStatus::Rejected;
    if (s == "EXPIRED" || s == "EXPIRED_IN_MATCH") return OrderStatus::Expired;
    return OrderStatus::Unknown;
}

/// Algo service states. TRIGGERING / TRIGGERED / FINISHED: the condition fired and a regular
/// order took over (Order::triggered_order_id); its own updates tell how it ends.
std::optional<OrderStatus> algo_status_from(std::string_view s) {
    if (s == "NEW") return OrderStatus::New;
    if (s == "CANCELED") return OrderStatus::Canceled;
    if (s == "TRIGGERING" || s == "TRIGGERED" || s == "FINISHED") return OrderStatus::Triggered;
    if (s == "REJECTED") return OrderStatus::Rejected;
    if (s == "EXPIRED") return OrderStatus::Expired;
    return OrderStatus::Unknown;
}

std::optional<WorkingType> working_type_from(std::string_view s) {
    if (s == "MARK_PRICE") return WorkingType::MarkPrice;
    return WorkingType::ContractPrice;
}

/// "cross" / "isolated" (stream), "CROSSED" / "ISOLATED" (REST).
std::optional<MarginType> margin_type_from(std::string_view s) {
    if (s == "cross" || s == "CROSSED") return MarginType::Cross;
    if (s == "isolated" || s == "ISOLATED") return MarginType::Isolated;
    return std::nullopt;
}

template <class E, class F>
bool get_enum(ondemand::object& o, std::string_view key, E& out, F from) {
    std::string_view s;
    if (o[key].get_string().get(s)) return false;
    auto e = from(s);
    if (!e) return false;
    out = *e;
    return true;
}

/// Optional fields: leave out unchanged when missing or malformed.
void opt_dec(ondemand::object& o, std::string_view key, Decimal& out) {
    (void)get_dec_or_zero(o, key, out);
}
template <class T>
void opt_field(ondemand::object& o, std::string_view key, T& out) {
    (void)get_field(o, key, out);
}

/// An id sent as a string ("123", or "" for none).
bool get_id_string(ondemand::object& o, std::string_view key, std::int64_t& out) {
    std::string_view s;
    if (o[key].get_string().get(s)) return false;
    out = 0;
    if (s.empty()) return true;
    auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc{} && p == s.data() + s.size();
}

/// Event time: a number, or a string in listenKeyExpired ("E": "1795208042302").
bool get_event_time(ondemand::object& o, std::int64_t& out) {
    ondemand::value v;
    if (o["E"].get(v)) return false;
    ondemand::json_type t;
    if (v.type().get(t)) return false;
    if (t == ondemand::json_type::number) return !v.get_int64().get(out);
    if (t == ondemand::json_type::string) return !v.get_int64_in_string().get(out);
    return false;
}

void fill_quote_qty(Order& o) {
    if (auto q = Decimal::mul(o.avg_price, o.filled_qty)) o.filled_quote_qty = *q;
}

/// ORDER_TRADE_UPDATE "o" object.
Result<detail::UserEvent> parse_order_update(ondemand::object o, std::int64_t time) {
    detail::UserEvent ev;
    ev.kind = detail::UserEvent::Kind::Order;
    ev.time = time;
    Order ord;
    std::int64_t t_ms = 0;
    if (!get_str(o, "s", ord.symbol) || !get_str(o, "c", ord.client_order_id) ||
        !get_enum(o, "S", ord.side, side_from) ||
        !get_enum(o, "o", ord.type, futures_order_type_from) ||
        !get_enum(o, "f", ord.time_in_force, time_in_force_from) || !get_dec(o, "q", ord.qty) ||
        !get_dec(o, "p", ord.price) || !get_dec(o, "ap", ord.avg_price) ||
        !get_dec(o, "sp", ord.stop_price) || !get_enum(o, "X", ord.status, order_status_from) ||
        !get_field(o, "i", ord.id) || !get_dec(o, "z", ord.filled_qty) ||
        !get_field(o, "T", t_ms) || !get_field(o, "R", ord.reduce_only) ||
        !get_enum(o, "ps", ord.position_side, position_side_from)) {
        return parse_error("ORDER_TRADE_UPDATE");
    }
    opt_field(o, "cp", ord.close_position);
    (void)get_enum(o, "wt", ord.working_type, working_type_from);
    opt_dec(o, "AP", ord.activation_price);
    opt_dec(o, "cr", ord.callback_rate);
    ord.update_time = from_epoch_ms(t_ms);
    fill_quote_qty(ord);

    ev.order = std::move(ord);
    return ev;
}

/// ALGO_UPDATE "o" object. update_ms: the event's "T".
Result<detail::UserEvent> parse_algo_update(ondemand::object o, std::int64_t time,
                                            std::int64_t update_ms) {
    detail::UserEvent ev;
    ev.kind = detail::UserEvent::Kind::Order;
    ev.time = time;
    Order ord;
    ord.is_algo = true;
    if (!get_str(o, "caid", ord.client_order_id) || !get_field(o, "aid", ord.id) ||
        !get_enum(o, "o", ord.type, futures_order_type_from) || !get_str(o, "s", ord.symbol) ||
        !get_enum(o, "S", ord.side, side_from) ||
        !get_enum(o, "ps", ord.position_side, position_side_from) ||
        !get_enum(o, "f", ord.time_in_force, time_in_force_from) || !get_dec(o, "q", ord.qty) ||
        !get_enum(o, "X", ord.status, algo_status_from) || !get_dec(o, "tp", ord.stop_price) ||
        !get_dec(o, "p", ord.price)) {
        return parse_error("ALGO_UPDATE");
    }
    (void)get_id_string(o, "ai", ord.triggered_order_id);
    (void)get_enum(o, "wt", ord.working_type, working_type_from);
    opt_field(o, "cp", ord.close_position);
    opt_field(o, "R", ord.reduce_only);
    opt_dec(o, "ap", ord.avg_price);
    opt_dec(o, "aq", ord.filled_qty);
    ord.update_time = from_epoch_ms(update_ms);
    fill_quote_qty(ord);
    ev.order = std::move(ord);
    return ev;
}

/// A position in ACCOUNT_UPDATE "P" or MARGIN_CALL "p".
Result<Position> parse_position_event(ondemand::object o, std::int64_t time) {
    Position p;
    if (!get_str(o, "s", p.symbol) || !get_dec(o, "pa", p.amount) ||
        !get_dec(o, "up", p.unrealized_pnl) ||
        !get_enum(o, "mt", p.margin_type, margin_type_from) ||
        !get_dec(o, "iw", p.isolated_wallet) || !get_enum(o, "ps", p.side, position_side_from)) {
        return parse_error("position update");
    }
    opt_dec(o, "ep", p.entry_price);
    opt_dec(o, "bep", p.break_even_price);
    p.update_time = from_epoch_ms(time);
    return p;
}

template <class F>
bool for_each_object(ondemand::object& parent, std::string_view key, F f) {
    ondemand::array arr;
    if (parent[key].get_array().get(arr)) return false;
    for (auto v : arr) {
        ondemand::object o;
        if (v.get_object().get(o) || !f(o)) return false;
    }
    return true;
}

Result<detail::UserEvent> parse_user_event(ondemand::document& doc) {
    using Kind = detail::UserEvent::Kind;
    ondemand::object root;
    std::string_view e;
    std::int64_t time = 0;
    if (doc.get_object().get(root) || root["e"].get_string().get(e) ||
        !get_event_time(root, time)) {
        return parse_error("user data event");
    }

    if (e == "ORDER_TRADE_UPDATE" || e == "ALGO_UPDATE") {
        std::int64_t t_ms = time;
        opt_field(root, "T", t_ms);
        ondemand::object o;
        if (root["o"].get_object().get(o)) return parse_error(e);
        return e == "ALGO_UPDATE" ? parse_algo_update(o, time, t_ms) : parse_order_update(o, time);
    }

    detail::UserEvent ev;
    ev.time = time;
    if (e == "ACCOUNT_UPDATE") {
        ev.kind = Kind::Account;
        ondemand::object a;
        if (root["a"].get_object().get(a)) return parse_error(e);
        bool ok = for_each_object(a, "B", [&](ondemand::object& b) {
            Balance bal;
            if (!get_str(b, "a", bal.asset) || !get_dec(b, "wb", bal.total)) return false;
            ev.balances.push_back(std::move(bal));
            return true;
        });
        ok = ok && for_each_object(a, "P", [&](ondemand::object& p) {
                 auto pos = parse_position_event(p, time);
                 if (!pos) return false;
                 ev.positions.push_back(std::move(*pos));
                 return true;
             });
        if (!ok) return parse_error(e);
        return ev;
    }
    if (e == "ACCOUNT_CONFIG_UPDATE") {
        ev.kind = Kind::Config;
        ondemand::object c;
        if (!root["ac"].get_object().get(c)) {
            std::int64_t lev = 0;
            if (!get_str(c, "s", ev.symbol) || !get_field(c, "l", lev)) return parse_error(e);
            ev.leverage = static_cast<int>(lev);
        } else if (!root["ai"].get_object().get(c)) {
            bool multi = false;
            if (!get_field(c, "j", multi)) return parse_error(e);
            ev.multi_assets = multi;
        }
        return ev;
    }
    if (e == "MARGIN_CALL") {
        ev.kind = Kind::MarginCall;
        bool ok = for_each_object(root, "p", [&](ondemand::object& p) {
            auto pos = parse_position_event(p, time);
            if (!pos) return false;
            ev.positions.push_back(std::move(*pos));
            return true;
        });
        if (!ok) return parse_error(e);
        return ev;
    }
    if (e == "listenKeyExpired") {
        ev.kind = Kind::ListenKeyExpired;
        return ev;
    }
    return ev;  // TRADE_LITE (ORDER_TRADE_UPDATE has more), STRATEGY_UPDATE, ...: not used
}

/// Order from GET /fapi/v1/openOrders and /fapi/v1/order, and the answer to placing,
/// amending or cancelling one (which has no "time", "avgPrice" or "cumQuote" since 2026-06).
Result<Order> parse_rest_order(ondemand::object o) {
    Order ord;
    if (!get_field(o, "orderId", ord.id) || !get_str(o, "symbol", ord.symbol) ||
        !get_enum(o, "status", ord.status, order_status_from) ||
        !get_str(o, "clientOrderId", ord.client_order_id) || !get_dec(o, "price", ord.price) ||
        !get_dec(o, "origQty", ord.qty) || !get_dec(o, "executedQty", ord.filled_qty) ||
        !get_enum(o, "timeInForce", ord.time_in_force, time_in_force_from) ||
        !get_enum(o, "type", ord.type, futures_order_type_from) ||
        !get_field(o, "reduceOnly", ord.reduce_only) || !get_enum(o, "side", ord.side, side_from) ||
        !get_enum(o, "positionSide", ord.position_side, position_side_from) ||
        !get_time(o, "updateTime", ord.update_time)) {
        return parse_error("order");
    }
    if (!get_time(o, "time", ord.create_time)) ord.create_time = ord.update_time;
    opt_dec(o, "avgPrice", ord.avg_price);
    opt_dec(o, "stopPrice", ord.stop_price);
    opt_dec(o, "cumQuote", ord.filled_quote_qty);
    opt_field(o, "closePosition", ord.close_position);
    (void)get_enum(o, "workingType", ord.working_type, working_type_from);
    opt_dec(o, "activatePrice", ord.activation_price);
    opt_dec(o, "priceRate", ord.callback_rate);
    return ord;
}

Result<Order> parse_rest_algo_order(ondemand::object o) {
    Order ord;
    ord.is_algo = true;
    if (!get_field(o, "algoId", ord.id) || !get_str(o, "clientAlgoId", ord.client_order_id) ||
        !get_enum(o, "orderType", ord.type, futures_order_type_from) ||
        !get_str(o, "symbol", ord.symbol) || !get_enum(o, "side", ord.side, side_from) ||
        !get_enum(o, "positionSide", ord.position_side, position_side_from) ||
        !get_enum(o, "timeInForce", ord.time_in_force, time_in_force_from) ||
        !get_dec(o, "quantity", ord.qty) ||
        !get_enum(o, "algoStatus", ord.status, algo_status_from) ||
        !get_dec(o, "triggerPrice", ord.stop_price) || !get_dec(o, "price", ord.price) ||
        !get_time(o, "createTime", ord.create_time) ||
        !get_time(o, "updateTime", ord.update_time)) {
        return parse_error("algo order");
    }
    (void)get_id_string(o, "actualOrderId", ord.triggered_order_id);
    opt_dec(o, "actualQty", ord.filled_qty);
    opt_dec(o, "actualPrice", ord.avg_price);
    opt_field(o, "closePosition", ord.close_position);
    opt_field(o, "reduceOnly", ord.reduce_only);
    (void)get_enum(o, "workingType", ord.working_type, working_type_from);
    opt_dec(o, "activatePrice", ord.activation_price);
    opt_dec(o, "callbackRate", ord.callback_rate);
    return ord;
}

template <class T, class F>
Result<std::vector<T>> parse_array(std::string_view json, std::string_view what, F f) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::array arr;
    if (parser.iterate(ps).get(doc) || doc.get_array().get(arr)) return parse_error(what);
    std::vector<T> out;
    for (auto v : arr) {
        ondemand::object o;
        if (v.get_object().get(o)) return parse_error(what);
        Result<T> r = f(o);
        if (!r) return r.error();
        out.push_back(std::move(*r));
    }
    return out;
}

}  // namespace

namespace detail {

/// json needs no padding (it is copied).
Result<UserEvent> parse_user_event(std::string_view json) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(ps).get(doc)) return parse_error("user data event");
    return ::binance::parse_user_event(doc);
}

Result<std::vector<Order>> parse_open_orders(std::string_view json) {
    return parse_array<Order>(json, "openOrders", parse_rest_order);
}

Result<std::vector<Order>> parse_open_algo_orders(std::string_view json) {
    return parse_array<Order>(json, "openAlgoOrders", parse_rest_algo_order);
}

Result<Order> parse_order(std::string_view json) {
    return parse_root_object(json, "order", parse_rest_order);
}

Result<Order> parse_algo_order(std::string_view json) {
    return parse_root_object(json, "algoOrder", parse_rest_algo_order);
}

/// GET /fapi/v3/positionRisk. Leverage and margin type are not in v3 (see symbolConfig).
Result<std::vector<Position>> parse_position_risk(std::string_view json) {
    return parse_array<Position>(json, "positionRisk", [](ondemand::object o) -> Result<Position> {
        Position p;
        if (!get_str(o, "symbol", p.symbol) ||
            !get_enum(o, "positionSide", p.side, position_side_from) ||
            !get_dec(o, "positionAmt", p.amount) || !get_dec(o, "entryPrice", p.entry_price) ||
            !get_dec(o, "unRealizedProfit", p.unrealized_pnl) ||
            !get_time(o, "updateTime", p.update_time)) {
            return parse_error("positionRisk");
        }
        opt_dec(o, "breakEvenPrice", p.break_even_price);
        opt_dec(o, "isolatedWallet", p.isolated_wallet);
        return p;
    });
}

Result<std::vector<Balance>> parse_futures_balances(std::string_view json) {
    return parse_array<Balance>(json, "balance", [](ondemand::object o) -> Result<Balance> {
        Balance b;
        if (!get_str(o, "asset", b.asset) || !get_dec(o, "balance", b.total) ||
            !get_dec(o, "availableBalance", b.available)) {
            return parse_error("balance");
        }
        b.locked = b.total - b.available;
        return b;
    });
}

Result<std::vector<detail::SymbolSettings>> parse_symbol_config(std::string_view json) {
    return parse_array<detail::SymbolSettings>(
        json, "symbolConfig", [](ondemand::object o) -> Result<detail::SymbolSettings> {
            detail::SymbolSettings s;
            std::int64_t lev = 0;
            if (!get_str(o, "symbol", s.symbol) ||
                !get_enum(o, "marginType", s.margin_type, margin_type_from) ||
                !get_field(o, "leverage", lev)) {
                return parse_error("symbolConfig");
            }
            s.leverage = static_cast<int>(lev);
            return s;
        });
}

Result<detail::AccountSettings> parse_account_config(std::string_view json) {
    return parse_root_object(json, "accountConfig",
                             [](ondemand::object o) -> Result<detail::AccountSettings> {
                                 detail::AccountSettings s;
                                 if (!get_field(o, "dualSidePosition", s.hedge_mode) ||
                                     !get_field(o, "multiAssetsMargin", s.multi_assets_margin)) {
                                     return parse_error("accountConfig");
                                 }
                                 return s;
                             });
}

}  // namespace detail

// =============================================================================================
// 3b2. JSON parsing: spot account (user data stream events, REST snapshots)
// =============================================================================================

namespace {

std::optional<OrderType> spot_order_type_from(std::string_view s) {
    if (s == "LIMIT") return OrderType::Limit;
    if (s == "MARKET") return OrderType::Market;
    if (s == "LIMIT_MAKER") return OrderType::LimitMaker;
    if (s == "STOP_LOSS") return OrderType::StopMarket;
    if (s == "STOP_LOSS_LIMIT") return OrderType::StopLimit;
    if (s == "TAKE_PROFIT") return OrderType::TakeProfitMarket;
    if (s == "TAKE_PROFIT_LIMIT") return OrderType::TakeProfitLimit;
    return std::nullopt;
}

/// Spot adds PENDING_NEW (order-list legs not yet working) and PENDING_CANCEL.
std::optional<OrderStatus> spot_order_status_from(std::string_view s) {
    if (s == "PENDING_NEW") return OrderStatus::New;
    if (s == "PENDING_CANCEL") return OrderStatus::New;  // still open until CANCELED
    return order_status_from(s);
}

void spot_avg_price(Order& o) {
    if (!o.filled_qty.is_positive()) return;
    if (auto p = Decimal::div(o.filled_quote_qty, o.filled_qty)) o.avg_price = *p;
}

/// executionReport. "c" is the client id of the request that caused the update: a cancel
/// request's own id on CANCELED, with the order's client id in "C".
Result<detail::UserEvent> parse_execution_report(ondemand::object o, std::int64_t time) {
    detail::UserEvent ev;
    ev.kind = detail::UserEvent::Kind::Order;
    ev.time = time;
    Order ord;
    ord.market = Market::Spot;
    std::string orig_client_id;
    std::int64_t t_ms = 0, created_ms = 0;
    if (!get_str(o, "s", ord.symbol) || !get_str(o, "c", ord.client_order_id) ||
        !get_enum(o, "S", ord.side, side_from) ||
        !get_enum(o, "o", ord.type, spot_order_type_from) ||
        !get_enum(o, "f", ord.time_in_force, time_in_force_from) || !get_dec(o, "q", ord.qty) ||
        !get_dec(o, "p", ord.price) || !get_dec(o, "P", ord.stop_price) ||
        !get_enum(o, "X", ord.status, spot_order_status_from) || !get_field(o, "i", ord.id) ||
        !get_dec(o, "z", ord.filled_qty) || !get_field(o, "T", t_ms) ||
        !get_dec(o, "Z", ord.filled_quote_qty)) {
        return parse_error("executionReport");
    }
    if (get_str(o, "C", orig_client_id) && !orig_client_id.empty()) {
        ord.client_order_id = std::move(orig_client_id);
    }
    ord.update_time = from_epoch_ms(t_ms);
    ord.create_time = get_field(o, "O", created_ms) && created_ms > 0 ? from_epoch_ms(created_ms)
                                                                      : ord.update_time;
    spot_avg_price(ord);

    ev.order = std::move(ord);
    return ev;
}

/// {"a":"USDT","f":"4665.6","l":"7.2"}: absolute free and locked amounts.
bool parse_spot_balance(ondemand::object& o, std::string_view asset_key, std::string_view free_key,
                        std::string_view locked_key, Balance& b) {
    if (!get_str(o, asset_key, b.asset) || !get_dec(o, free_key, b.available) ||
        !get_dec(o, locked_key, b.locked)) {
        return false;
    }
    b.total = b.available + b.locked;
    return true;
}

/// A spot user data event, as delivered by the WebSocket API subscription
/// ({"subscriptionId":0,"event":{...}}) or bare.
Result<detail::UserEvent> parse_spot_user_event(ondemand::document& doc) {
    using Kind = detail::UserEvent::Kind;
    ondemand::object root;
    if (doc.get_object().get(root)) return parse_error("spot user data event");
    ondemand::object event;
    if (root["event"].get_object().get(event)) {
        root.reset();
        event = root;
    }
    std::string_view e;
    std::int64_t time = 0;
    if (event["e"].get_string().get(e) || !get_event_time(event, time)) {
        return parse_error("spot user data event");
    }
    if (e == "executionReport") return parse_execution_report(event, time);

    detail::UserEvent ev;
    ev.time = time;
    if (e == "outboundAccountPosition") {
        ev.kind = Kind::Account;
        bool ok = for_each_object(event, "B", [&](ondemand::object& b) {
            Balance bal;
            if (!parse_spot_balance(b, "a", "f", "l", bal)) return false;
            ev.balances.push_back(std::move(bal));
            return true;
        });
        if (!ok) return parse_error(e);
        return ev;
    }
    if (e == "eventStreamTerminated") {
        ev.kind = Kind::StreamTerminated;
        return ev;
    }
    return ev;  // balanceUpdate (outboundAccountPosition follows), listStatus, ...: not used
}

/// Order from GET /api/v3/openOrders and /api/v3/order, and the answer to placing (RESULT) or
/// cancelling one. Those answers have "transactTime" instead of "time" / "updateTime"; a cancel
/// answer's "clientOrderId" is the cancel request's own id, the order's is "origClientOrderId".
Result<Order> parse_spot_rest_order(ondemand::object o) {
    Order ord;
    ord.market = Market::Spot;
    std::string orig_client_id;
    if (!get_str(o, "symbol", ord.symbol) || !get_field(o, "orderId", ord.id) ||
        !get_str(o, "clientOrderId", ord.client_order_id) || !get_dec(o, "price", ord.price) ||
        !get_dec(o, "origQty", ord.qty) || !get_dec(o, "executedQty", ord.filled_qty) ||
        !get_dec(o, "cummulativeQuoteQty", ord.filled_quote_qty) ||
        !get_enum(o, "status", ord.status, spot_order_status_from) ||
        !get_enum(o, "timeInForce", ord.time_in_force, time_in_force_from) ||
        !get_enum(o, "type", ord.type, spot_order_type_from) ||
        !get_enum(o, "side", ord.side, side_from)) {
        return parse_error("spot order");
    }
    if (get_str(o, "origClientOrderId", orig_client_id) && !orig_client_id.empty()) {
        ord.client_order_id = std::move(orig_client_id);
    }
    Timestamp transact{};
    (void)get_time(o, "transactTime", transact);
    if (!get_time(o, "updateTime", ord.update_time)) ord.update_time = transact;
    if (!get_time(o, "time", ord.create_time)) ord.create_time = ord.update_time;
    opt_dec(o, "stopPrice", ord.stop_price);
    spot_avg_price(ord);
    return ord;
}

}  // namespace

namespace detail {

/// json needs no padding (it is copied).
Result<UserEvent> parse_spot_user_event(std::string_view json) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(ps).get(doc)) return parse_error("spot user data event");
    return ::binance::parse_spot_user_event(doc);
}

Result<std::vector<Balance>> parse_spot_account(std::string_view json) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::object root;
    if (parser.iterate(ps).get(doc) || doc.get_object().get(root)) return parse_error("account");
    std::vector<Balance> out;
    bool ok = for_each_object(root, "balances", [&](ondemand::object& b) {
        Balance bal;
        if (!parse_spot_balance(b, "asset", "free", "locked", bal)) return false;
        out.push_back(std::move(bal));
        return true;
    });
    if (!ok) return parse_error("account");
    return out;
}

Result<std::vector<Order>> parse_spot_open_orders(std::string_view json) {
    return parse_array<Order>(json, "openOrders", parse_spot_rest_order);
}

Result<Order> parse_spot_order(std::string_view json) {
    return parse_root_object(json, "order", parse_spot_rest_order);
}

}  // namespace detail

// =============================================================================================
// 3c. JSON parsing: order book (depth streams, REST snapshot)
// =============================================================================================

namespace detail {

/// One depth message: a diff (futures U/u/pu, spot U/u), a partial top-N, or a REST snapshot.
struct DepthEvent {
    std::int64_t first_id = 0;  ///< U; 0 for snapshots
    std::int64_t last_id = 0;   ///< u, or lastUpdateId
    std::int64_t prev_id = -1;  ///< pu (futures); -1 when not sent
    Timestamp time{};
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
};

}  // namespace detail

namespace {

bool get_levels(ondemand::object& o, std::string_view key, std::vector<PriceLevel>& out) {
    ondemand::array arr;
    if (o[key].get_array().get(arr)) return false;
    for (auto row_v : arr) {
        ondemand::array row;
        if (row_v.get_array().get(row)) return false;
        PriceLevel level;
        int i = 0;
        for (auto v : row) {
            if (i == 0 && !get_dec(v, level.price)) return false;
            if (i == 1 && !get_dec(v, level.qty)) return false;
            ++i;
        }
        if (i < 2) return false;
        out.push_back(level);
    }
    return true;
}

/// Stream message "data": futures {"e":"depthUpdate","E","U","u","pu","b","a"}, spot diff
/// {"e":"depthUpdate","E","U","u","b","a"}, spot partial {"lastUpdateId","bids","asks"}.
Result<detail::DepthEvent> parse_depth_object(ondemand::object o) {
    detail::DepthEvent e;
    if (!o["lastUpdateId"].get_int64().get(e.last_id)) {  // spot partial depth
        if (!get_levels(o, "bids", e.bids) || !get_levels(o, "asks", e.asks)) {
            return parse_error("partial depth");
        }
        e.time = now_ts();
        return e;
    }
    if (!get_field(o, "U", e.first_id) || !get_field(o, "u", e.last_id) ||
        !get_levels(o, "b", e.bids) || !get_levels(o, "a", e.asks)) {
        return parse_error("depthUpdate");
    }
    opt_field(o, "pu", e.prev_id);
    if (!get_time(o, "E", e.time)) e.time = now_ts();
    return e;
}

enum class DepthApply { Applied, Old, Gap };

/// A local order book kept from a snapshot plus diffs, with Binance's sequence rules:
///   futures: skip u < lastUpdateId; the first applied event has U <= lastUpdateId <= u; then
///            each event's pu equals the previous u.
///   spot:    skip u <= lastUpdateId; the first applied event has U <= lastUpdateId+1 <= u; then
///            each event's U equals the previous u + 1.
struct LocalBook {
    bool futures = true;
    std::map<Decimal, Decimal, std::greater<Decimal>> bids;
    std::map<Decimal, Decimal> asks;
    std::int64_t last_id = 0;
    bool bridged = false;  ///< a diff has been applied on top of the snapshot
    Timestamp time{};

    static constexpr std::size_t kMaxLevels = 10'000;  ///< per side; far levels are dropped

    void install(const detail::DepthEvent& snap) {
        bids.clear();
        asks.clear();
        for (const auto& l : snap.bids) {
            if (l.qty.is_positive()) bids[l.price] = l.qty;
        }
        for (const auto& l : snap.asks) {
            if (l.qty.is_positive()) asks[l.price] = l.qty;
        }
        last_id = snap.last_id;
        bridged = false;
        time = snap.time;
    }

    DepthApply apply(const detail::DepthEvent& e) {
        if (!bridged) {
            if (futures ? e.last_id < last_id : e.last_id <= last_id) return DepthApply::Old;
            if (e.first_id > (futures ? last_id : last_id + 1)) return DepthApply::Gap;
            bridged = true;
        } else if (futures ? e.prev_id != last_id : e.first_id != last_id + 1) {
            return DepthApply::Gap;
        }
        for (const auto& l : e.bids) {
            if (l.qty.is_zero()) {
                bids.erase(l.price);
            } else {
                bids[l.price] = l.qty;
            }
        }
        for (const auto& l : e.asks) {
            if (l.qty.is_zero()) {
                asks.erase(l.price);
            } else {
                asks[l.price] = l.qty;
            }
        }
        while (bids.size() > kMaxLevels) bids.erase(std::prev(bids.end()));
        while (asks.size() > kMaxLevels) asks.erase(std::prev(asks.end()));
        last_id = e.last_id;
        time = e.time;
        return DepthApply::Applied;
    }

    OrderBook top(const std::string& symbol, std::size_t n) const {
        OrderBook b;
        b.symbol = symbol;
        b.last_update_id = last_id;
        b.time = time;
        for (auto it = bids.begin(); it != bids.end() && b.bids.size() < n; ++it) {
            b.bids.push_back({it->first, it->second});
        }
        for (auto it = asks.begin(); it != asks.end() && b.asks.size() < n; ++it) {
            b.asks.push_back({it->first, it->second});
        }
        return b;
    }
};

}  // namespace

namespace detail {

/// GET depth: {"lastUpdateId":..,"E":..,"bids":[..],"asks":[..]} (spot has no E).
Result<DepthEvent> parse_depth_snapshot(std::string_view json) {
    return parse_root_object(json, "depth", [](ondemand::object o) -> Result<DepthEvent> {
        DepthEvent e;
        if (!get_field(o, "lastUpdateId", e.last_id) || !get_levels(o, "bids", e.bids) ||
            !get_levels(o, "asks", e.asks)) {
            return parse_error("depth snapshot");
        }
        if (!get_time(o, "E", e.time)) e.time = now_ts();
        return e;
    });
}

Result<DepthEvent> parse_depth_event(std::string_view json) {
    return parse_root_object(json, "depth", parse_depth_object);
}

/// For tests: a snapshot plus events through a local book. Error (Parse) on a gap.
Result<OrderBook> replay_depth(Market market, const DepthEvent& snapshot,
                               const std::vector<DepthEvent>& events, std::size_t levels) {
    LocalBook book;
    book.futures = market == Market::Futures;
    book.install(snapshot);
    for (const auto& e : events) {
        if (book.apply(e) == DepthApply::Gap) {
            return Error{ErrorCode::Parse, "gap at update " + std::to_string(e.first_id)};
        }
    }
    return book.top("TEST", levels);
}

}  // namespace detail

// =============================================================================================
// 4. Client::Impl
// =============================================================================================

namespace {

class SpdLogger final : public Logger {
public:
    SpdLogger() {
        static std::mutex mu;
        std::lock_guard lock(mu);
        logger_ = spdlog::get("binance");
        if (!logger_) logger_ = spdlog::stdout_color_mt("binance");
    }
    void log(LogLevel level, std::string_view msg) override {
        logger_->log(to_spd(level), "{}", msg);
    }

private:
    static spdlog::level::level_enum to_spd(LogLevel l) {
        switch (l) {
            case LogLevel::Trace: return spdlog::level::trace;
            case LogLevel::Debug: return spdlog::level::debug;
            case LogLevel::Info: return spdlog::level::info;
            case LogLevel::Warn: return spdlog::level::warn;
            case LogLevel::Error: return spdlog::level::err;
        }
        return spdlog::level::info;
    }
    std::shared_ptr<spdlog::logger> logger_;
};

enum class WatchKind { Candles, Trade, BookTicker, Ticker24h, MarkPrice, OrderBook };

/// State of the stream behind a watch.
enum class StreamStatus {
    Connecting,  ///< subscribed, first snapshot not loaded yet
    Live,        ///< up to date
    Stale,       ///< connection lost or silent; data is the last known value
    Resyncing,   ///< reconnected, reloading over REST
};

std::string_view status_name(StreamStatus s) {
    switch (s) {
        case StreamStatus::Connecting: return "connecting";
        case StreamStatus::Live: return "live";
        case StreamStatus::Stale: return "stale";
        case StreamStatus::Resyncing: return "resyncing";
    }
    return "";
}

/// One stream subscription kept in sync with REST; every watch_*() creates one.
/// Life cycle: subscribe the stream -> on ack, load the REST snapshot while buffering stream
/// events -> merge -> Live. Connection lost -> Stale (keep buffering). Connection back -> Resyncing
/// -> reload over REST -> Live.
struct WatchState {
    WatchKind kind{};
    std::string symbol;
    std::string stream;  ///< e.g. "btcusdt@kline_1m"; key in MarketState::watches
    std::size_t conn = 0;

    // Guarded by MarketState::mu.
    StreamStatus status = StreamStatus::Connecting;
    bool subscribed = false;     ///< server confirmed the stream at least once
    bool lost = false;           ///< connection dropped since; resync when it is back
    bool syncing = true;         ///< REST load in progress: stream events are buffered
    std::uint64_t sync_gen = 0;  ///< bumped per sync; older syncs discard their result
    std::vector<Done> waiting;

    virtual ~WatchState() = default;
    virtual bool has_data() const = 0;  ///< MarketState::mu held
};

struct CandleSeries final : WatchState {
    Interval interval{};
    std::size_t depth = 0;
    std::deque<Candle> candles;
    std::vector<Candle> buffered;
    bool has_data() const override { return !candles.empty(); }
};

template <class T>
struct ValueWatch final : WatchState {
    std::optional<T> value;
    std::optional<T> buffered;
    bool has_data() const override { return value.has_value(); }
};

struct BookWatch final : WatchState {
    std::size_t depth = 0;
    bool partial = false;
    LocalBook book;
    std::vector<detail::DepthEvent> buffered;
    bool test_drop_next = false;  ///< tests: lose the next message
    bool has_data() const override { return !book.bids.empty() || !book.asks.empty(); }
};

std::string lower(std::string_view s) {
    std::string out(s);
    for (auto& c : out) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return out;
}

std::string depth_stream(std::string_view symbol, std::size_t depth) {
    const std::string s = lower(symbol);
    if (depth == 5 || depth == 10 || depth == 20)
        return s + "@depth" + std::to_string(depth) + "@100ms";
    return s + "@depth@100ms";
}

std::pair<int, int> depth_snapshot_size(Market m, std::size_t depth, bool partial) {
    if (m == Market::Futures) {
        if (partial) return {static_cast<int>(depth), 2};
        return {1000, 20};
    }
    if (partial) return {static_cast<int>(depth), 5};
    return depth <= 1000 ? std::pair{1000, 50} : std::pair{5000, 250};
}

template <class T>
struct ValueKind;

template <>
struct ValueKind<Trade> {
    static constexpr WatchKind kind = WatchKind::Trade;
    static constexpr std::string_view name = "trades";
    static std::string stream(std::string_view sym);
    static std::string rest(const Endpoints& ep, const std::string& sym) {
        return ep.api_prefix + "/aggTrades?symbol=" + sym + "&limit=1";
    }
    static int weight(Market m) { return m == Market::Futures ? 20 : 4; }
    static Result<std::optional<Trade>> parse_rest(std::string_view json, const std::string& sym) {
        return detail::parse_agg_trades_rest(json, sym);
    }
    static Result<Trade> parse_event(ondemand::object o) { return parse_agg_trade(o); }
    static std::int64_t version(const Trade& t) { return t.id; }
};

template <>
struct ValueKind<BookTicker> {
    static constexpr WatchKind kind = WatchKind::BookTicker;
    static constexpr std::string_view name = "book_ticker";
    static std::string stream(std::string_view sym);
    static std::string rest(const Endpoints& ep, const std::string& sym) {
        return ep.api_prefix + "/ticker/bookTicker?symbol=" + sym;
    }
    static int weight(Market) { return 2; }
    static Result<std::optional<BookTicker>> parse_rest(std::string_view json, const std::string&) {
        auto r = detail::parse_book_ticker_rest(json);
        if (!r) return r.error();
        return std::optional<BookTicker>(std::move(*r));
    }
    static Result<BookTicker> parse_event(ondemand::object o) {
        return parse_book_ticker_stream(o);
    }
    /// Spot REST has no update id (0): it only fills an empty watch.
    static std::int64_t version(const BookTicker& b) { return b.update_id; }
};

template <>
struct ValueKind<Ticker24h> {
    static constexpr WatchKind kind = WatchKind::Ticker24h;
    static constexpr std::string_view name = "ticker_24h";
    static std::string stream(std::string_view sym);
    static std::string rest(const Endpoints& ep, const std::string& sym) {
        return ep.api_prefix + "/ticker/24hr?symbol=" + sym;
    }
    static int weight(Market m) { return m == Market::Futures ? 1 : 2; }
    static Result<std::optional<Ticker24h>> parse_rest(std::string_view json, const std::string&) {
        auto r = detail::parse_ticker_24h_rest(json);
        if (!r) return r.error();
        return std::optional<Ticker24h>(std::move(*r));
    }
    static Result<Ticker24h> parse_event(ondemand::object o) { return parse_ticker_stream(o); }
    static std::int64_t version(const Ticker24h& t) { return to_epoch_ms(t.close_time); }
};

template <>
struct ValueKind<MarkPrice> {
    static constexpr WatchKind kind = WatchKind::MarkPrice;
    static constexpr std::string_view name = "mark_price";
    static std::string stream(std::string_view sym);
    static std::string rest(const Endpoints& ep, const std::string& sym) {
        return ep.api_prefix + "/premiumIndex?symbol=" + sym;
    }
    static int weight(Market) { return 1; }
    static Result<std::optional<MarkPrice>> parse_rest(std::string_view json, const std::string&) {
        auto r = detail::parse_mark_price_rest(json);
        if (!r) return r.error();
        return std::optional<MarkPrice>(std::move(*r));
    }
    static Result<MarkPrice> parse_event(ondemand::object o) { return parse_mark_price_stream(o); }
    static std::int64_t version(const MarkPrice& m) { return to_epoch_ms(m.time); }
};

std::string ValueKind<Trade>::stream(std::string_view sym) { return lower(sym) + "@aggTrade"; }
std::string ValueKind<BookTicker>::stream(std::string_view sym) {
    return lower(sym) + "@bookTicker";
}
std::string ValueKind<Ticker24h>::stream(std::string_view sym) { return lower(sym) + "@ticker"; }
std::string ValueKind<MarkPrice>::stream(std::string_view sym) {
    return lower(sym) + "@markPrice@1s";
}

Error invalid_symbol(std::string_view s) {
    return Error{ErrorCode::InvalidArgument, "invalid symbol '" + std::string(s) + "'"};
}

/// Binance symbols: upper-case letters, digits, '_' (futures delivery contracts).
std::optional<std::string> normalize_symbol(std::string_view s) {
    if (s.empty() || s.size() > 32) return std::nullopt;
    std::string out(s);
    for (auto& c : out) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') return std::nullopt;
    }
    return out;
}

enum class AccountPart { Balance, Positions, OpenOrders, AlgoOrders };

struct OrderScope {
    bool all = false;
    bool all_loaded = false;
    std::map<std::string, bool> symbols;  ///< watched one by one: symbol -> loaded

    bool on() const { return all || !symbols.empty(); }
    bool covers(const std::string& symbol) const { return all || symbols.count(symbol) > 0; }
    /// "" = all symbols.
    bool loaded_for(const std::string& symbol) const {
        if (all && all_loaded) return true;
        if (symbol.empty()) return false;
        auto it = symbols.find(symbol);
        return it != symbols.end() && it->second;
    }
    bool any_loaded() const {
        if (all_loaded) return true;
        for (const auto& [sym, loaded] : symbols) {
            if (loaded) return true;
        }
        return false;
    }
    bool needs_load() const {
        if (all && !all_loaded) return true;
        for (const auto& [sym, loaded] : symbols) {
            if (!loaded) return true;
        }
        return false;
    }
    void forget_loaded() {
        all_loaded = false;
        for (auto& [sym, loaded] : symbols) loaded = false;
    }
};

struct OrdersLoad {
    bool all = false;
    std::vector<std::string> symbols;
    bool any() const { return all || !symbols.empty(); }
};

struct AccountLoad {
    bool settings = false;
    bool balances = false;
    bool positions = false;
    OrdersLoad orders;
    OrdersLoad algo_orders;
};

/// Open orders as fetched: everything, and/or per symbol (an error for a bad symbol).
struct OrdersFetched {
    std::optional<std::vector<Order>> all;
    std::vector<std::pair<std::string, Result<std::vector<Order>>>> by_symbol;
};

struct AccountSnapshot {
    std::optional<detail::AccountSettings> settings;
    std::optional<std::vector<detail::SymbolSettings>> symbols;
    std::optional<std::vector<Position>> positions;
    std::optional<std::vector<Balance>> balances;
    OrdersFetched orders;
    OrdersFetched algo_orders;
};

struct CachedOrder {
    Order order;
    std::int64_t refreshed_ms = 0;  ///< when last read over REST (0: only from the stream)
};

struct EventEffects {
    bool refresh_balances = false;  ///< available balance may have changed: re-read it
    std::vector<std::string> margin_calls;
};

/// Request parameters in order; values are sent as given.
using Params = std::vector<std::pair<std::string, std::string>>;

struct TradeCall {
    std::string ws_method;  ///< "order.place", ...; empty = REST only
    detail::http::verb verb = detail::http::verb::post;
    std::string path;
    Params params;  ///< without apiKey / timestamp / signature
    int weight = 1;
    bool is_order = false;  ///< counts towards the order-rate limits
    Market market = Market::Futures;
};

}  // namespace

struct Client::Impl {
    explicit Impl(Config cfg);
    ~Impl();

    struct MarketState {
        Market market{};
        Endpoints ep;
        mutable std::mutex mu;  ///< everything below
        std::unordered_map<std::string, Instrument> instruments;
        bool instruments_refreshing = false;
        struct Conn {
            std::string target;  ///< "/stream", or futures "/market/stream", "/public/stream"
            std::shared_ptr<detail::WsStream> ws;
            std::size_t load = 0;
            bool open = false;
        };
        std::vector<Conn> conns;
        std::unordered_map<std::string, std::shared_ptr<WatchState>> watches;
    };

    Config config;
    std::shared_ptr<Logger> logger;
    std::optional<asio::io_context> io;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work;
    std::vector<std::thread> threads;
    asio::ssl::context tls;
    detail::RateLimiter limiter;
    std::shared_ptr<detail::HttpClient> http;
    detail::PkeyPtr ed25519_key;
    MarketState futures;
    MarketState spot;

    MarketState& state(Market m) { return m == Market::Futures ? futures : spot; }

    void log(LogLevel level, const std::string& msg) const {
        if (logger) logger->log(level, msg);
    }

    void spawn(asio::awaitable<void> a) {
        asio::co_spawn(*io, std::move(a), [this](std::exception_ptr e) {
            if (!e) return;
            try {
                std::rethrow_exception(e);
            } catch (const std::exception& ex) {
                log(LogLevel::Error, std::string("internal task failed: ") + ex.what());
            }
        });
    }

    /// Public GET; non-200 answers become errors carrying Binance's code.
    asio::awaitable<Result<std::string>> get(MarketState& ms, std::string target, int weight);

    /// Pick a connection with room for one more stream (opening one if needed). Caller holds
    /// ms.mu; subscribe on the returned connection after releasing it.
    std::shared_ptr<detail::WsStream> assign_conn(MarketState& ms, const std::string& stream,
                                                  std::size_t& index);
    void on_stream_message(MarketState& ms, std::string_view msg);
    void on_stream_state(MarketState& ms, std::size_t conn, detail::WsState st,
                         const std::string& reason);

    /// ms.mu held: is w still the registered watch and gen its latest sync?
    static bool is_current(const MarketState& ms, const WatchState& w, std::uint64_t gen);
    /// Register a new watch (caller holds ms.mu); subscribe with the returned function after
    /// releasing the lock.
    std::function<void()> add_watch(MarketState& ms, std::shared_ptr<WatchState> w);
    void start_sync(MarketState& ms, const std::shared_ptr<WatchState>& w, bool full);
    /// ms.mu held: the REST data is in; become Live (or Stale if the connection dropped).
    /// Returns the watch_*() callers to tell (deliver() after releasing ms.mu).
    std::vector<Done> settle(MarketState& ms, WatchState& w);
    static void deliver(std::vector<Done> waiting);
    void fail_watch(MarketState& ms, const std::shared_ptr<WatchState>& w, std::uint64_t gen,
                    Error err);
    void remove_watch(MarketState& ms, const std::string& stream);
    /// Back off before retrying a failed REST load; false if the sync is obsolete.
    asio::awaitable<bool> retry_after_failure(MarketState& ms, const WatchState& w,
                                              std::uint64_t gen, const Error& err,
                                              std::chrono::milliseconds& backoff);

    asio::awaitable<Result<void>> load_instruments(MarketState& ms);

    void on_kline(MarketState& ms, const std::shared_ptr<CandleSeries>& s, ondemand::object data);
    asio::awaitable<void> sync_candles(MarketState& ms, std::shared_ptr<CandleSeries> s,
                                       std::uint64_t gen, bool full);
    asio::awaitable<Result<std::vector<Candle>>> fetch_klines(MarketState& ms,
                                                              const std::string& symbol,
                                                              Interval iv, std::size_t limit,
                                                              std::optional<std::int64_t> start,
                                                              std::optional<std::int64_t> end);
    asio::awaitable<Result<std::vector<Candle>>> fetch_latest_klines(MarketState& ms,
                                                                     const std::string& symbol,
                                                                     Interval iv,
                                                                     std::size_t count);
    asio::awaitable<Result<std::vector<Candle>>> fetch_klines_since(MarketState& ms,
                                                                    const std::string& symbol,
                                                                    Interval iv,
                                                                    std::int64_t start_ms,
                                                                    std::size_t max_count);

    template <class T>
    void watch_value(Market market, std::string_view symbol, Done done);
    template <class T>
    void unwatch_value(Market market, std::string_view symbol);
    template <class T>
    Result<T> get_value(Market market, std::string_view symbol);
    template <class T>
    void on_value(MarketState& ms, const std::shared_ptr<ValueWatch<T>>& w, ondemand::object data);
    template <class T>
    asio::awaitable<void> sync_value(MarketState& ms, std::shared_ptr<ValueWatch<T>> w,
                                     std::uint64_t gen);

    void on_book(MarketState& ms, const std::shared_ptr<BookWatch>& w, ondemand::object data);
    asio::awaitable<void> sync_book(MarketState& ms, std::shared_ptr<BookWatch> w,
                                    std::uint64_t gen);
    /// The order-book watch of a symbol (any depth mode); ms.mu held.
    static std::shared_ptr<BookWatch> find_book(const MarketState& ms, const std::string& symbol);

    asio::awaitable<void> refresh_instruments(MarketState& ms);

    std::atomic<std::int64_t> time_offset_ms{0};  ///< Binance clock - our clock
    std::atomic<bool> time_synced{false};
    std::int64_t server_now() const { return now_ms() + time_offset_ms.load(); }
    std::optional<Error> missing_keys() const;
    asio::awaitable<Result<void>> sync_time();
    /// USER_DATA / TRADE request: adds recvWindow, timestamp, signature. Retries once on -1021
    /// (clock drift) after re-reading the server time.
    asio::awaitable<Result<std::string>> signed_request(detail::http::verb method, std::string path,
                                                        std::string params, int weight,
                                                        bool is_order = false,
                                                        Market market = Market::Futures);
    /// USER_STREAM request (listenKey): API key, no signature.
    asio::awaitable<Result<std::string>> key_request(detail::http::verb method, std::string path);

    struct AccountState {
        Market market = Market::Futures;
        mutable std::mutex mu;  ///< everything below
        bool active = false;
        std::uint64_t gen = 0;  ///< bumped every time the stream starts or stops
        std::shared_ptr<detail::WsStream> ws;
        std::shared_ptr<asio::steady_timer> keepalive;  ///< futures: listenKey keep-alive
        std::string listen_key;
        bool open = false;    ///< stream connected (spot: and subscribed)
        bool syncing = true;  ///< REST load in progress: events are buffered
        bool renewing = false;
        bool full_reload = true;  ///< next load reloads everything watched (after (re)connect)
        std::uint64_t sync_gen = 0;
        bool drop_events = false;  ///< tests: behave as if events got lost
        std::vector<detail::UserEvent> buffered;

        bool balance_on = false, positions_on = false;
        bool balance_loaded = false, positions_loaded = false, settings_loaded = false;
        OrderScope regular, algo;
        struct Waiter {
            AccountPart part;
            std::string symbol;  ///< open orders: "" = all symbols
            Done done;
        };
        std::vector<Waiter> waiting;

        std::map<std::pair<bool, std::int64_t>, CachedOrder> orders;  ///< open, by (is_algo, id)
        Positions positions;                                          ///< futures; non-zero only
        Balances balances;
        std::unordered_map<std::string, detail::SymbolSettings> symbols;
        detail::AccountSettings settings;  ///< futures: hedge mode, needed to check orders
        bool balance_refresh_pending = false;

        OrderScope& scope(bool is_algo) { return is_algo ? algo : regular; }
        bool loaded(AccountPart p, const std::string& symbol) const {
            switch (p) {
                case AccountPart::Balance: return balance_loaded;
                case AccountPart::Positions: return positions_loaded;
                case AccountPart::OpenOrders: return regular.loaded_for(symbol);
                case AccountPart::AlgoOrders: return algo.loaded_for(symbol);
            }
            return false;
        }
        bool any_watch() const { return balance_on || positions_on || regular.on() || algo.on(); }
        bool any_loaded() const {
            return balance_loaded || positions_loaded || regular.any_loaded() ||
                   algo.any_loaded() || settings_loaded;
        }
        bool needs_load() const {
            return (balance_on && !balance_loaded) || (positions_on && !positions_loaded) ||
                   regular.needs_load() || algo.needs_load();
        }
    };
    AccountState futures_account;
    AccountState spot_account;
    AccountState& account_of(Market m) {
        return m == Market::Futures ? futures_account : spot_account;
    }

    /// Turn on one kind of account data (open orders: of one symbol, "" = all); done once it
    /// is loaded.
    void watch_part(AccountState& a, AccountPart part, std::string symbol, Done done);
    void unwatch_part(AccountState& a, AccountPart part, const std::string& symbol);
    /// a.mu held: something new is watched; start the stream or load it. Returns what to do
    /// after releasing the lock.
    enum class AccountKick { None, Start, Reload };
    AccountKick kick_account_locked(AccountState& a);
    void run_kick(AccountState& a, AccountKick kick, std::uint64_t gen);
    /// a.mu held: stop the stream; returns what to stop after releasing the lock.
    struct Stopped {
        std::shared_ptr<detail::WsStream> ws;
        std::vector<std::shared_ptr<asio::steady_timer>> timers;
    };
    Stopped stop_account_locked(AccountState& a);
    void finish_stop(Stopped st);

    asio::awaitable<void> start_account(AccountState& a, std::uint64_t gen);
    asio::awaitable<void> start_futures_stream(AccountState& a, std::uint64_t gen);
    void start_spot_stream(AccountState& a, std::uint64_t gen);
    asio::awaitable<Result<std::string>> create_listen_key();
    asio::awaitable<void> keep_listen_key_alive(std::uint64_t gen,
                                                std::shared_ptr<asio::steady_timer> timer);
    asio::awaitable<void> renew_listen_key(std::uint64_t gen);
    void on_user_message(AccountState& a, std::uint64_t gen, std::string_view msg);
    /// Spot: the answer to userDataStream.subscribe.signature.
    void on_spot_subscribed(AccountState& a, std::uint64_t gen, ondemand::document& doc);
    void on_user_state(AccountState& a, std::uint64_t gen, detail::WsState st,
                       const std::string& reason);
    void start_account_sync(AccountState& a, std::uint64_t gen);
    asio::awaitable<void> sync_account(AccountState& a, std::uint64_t gen, std::uint64_t sync_gen);
    asio::awaitable<Result<AccountSnapshot>> fetch_account_snapshot(Market market,
                                                                    const AccountLoad& load);
    asio::awaitable<Result<OrdersFetched>> fetch_orders(Market market, bool is_algo,
                                                        const OrdersLoad& load);
    void schedule_balance_refresh(std::uint64_t gen);
    asio::awaitable<void> refresh_balances(std::uint64_t gen);
    /// a.mu held. Events older than stale_before (ms) are already in a REST snapshot.
    void apply_user_event(AccountState& a, const detail::UserEvent& ev, std::int64_t stale_before,
                          EventEffects& out);
    void fail_account(AccountState& a, std::uint64_t gen, Error err);
    /// futures_account.mu held: leverage and margin type of a symbol into a position.
    void complete_position(Position& p) const;

    asio::awaitable<Result<detail::AccountSettings>> rest_account_config();
    asio::awaitable<Result<std::vector<Balance>>> rest_balances(Market market);
    asio::awaitable<Result<std::vector<detail::SymbolSettings>>> rest_symbol_config();
    asio::awaitable<Result<std::vector<Position>>> rest_position_risk();
    /// symbol "" = all symbols (more weight).
    asio::awaitable<Result<std::vector<Order>>> rest_open_orders(Market market, bool is_algo,
                                                                 std::string symbol);

    std::mutex trade_mu;  ///< ws_api, spot_ws_api
    std::shared_ptr<detail::WsApi> ws_api;
    std::shared_ptr<detail::WsApi> spot_ws_api;
    std::atomic<std::uint32_t> order_seq{0};
    std::shared_ptr<detail::WsApi> trade_api(Market market = Market::Futures);
    asio::awaitable<Result<void>> ensure_instruments(Market market = Market::Futures);
    /// The order (JSON object) or an error. Timeout = sent, outcome unknown.
    asio::awaitable<Result<std::string>> send_trade(const TradeCall& call);
    asio::awaitable<Result<std::string>> send_trade_rest(const TradeCall& call);
    /// GET order / algoOrder (spot: GET order). NotFound if Binance does not know it.
    asio::awaitable<Result<Order>> lookup_order(const OrderRef& ref,
                                                Market market = Market::Futures);
    /// After a send with unknown outcome: look for the order (a few tries).
    asio::awaitable<Result<Order>> find_after_timeout(const OrderRef& ref,
                                                      Market market = Market::Futures);
    /// rest_only: never over the WebSocket API (the *_rest_api methods).
    asio::awaitable<Result<Order>> create_order(OrderRequest request, bool rest_only = false,
                                                Market market = Market::Futures);
    asio::awaitable<Result<Order>> cancel_order(OrderRef ref, bool rest_only = false,
                                                Market market = Market::Futures);
    asio::awaitable<Result<Order>> amend_order(AmendRequest request, bool rest_only = false);
    asio::awaitable<Result<void>> cancel_all_orders(std::string symbol,
                                                    Market market = Market::Futures);
    /// GET order / algoOrder after checking the reference.
    asio::awaitable<Result<Order>> fetch_order(OrderRef ref, Market market = Market::Futures);
    asio::awaitable<Result<void>> set_position_mode(bool hedge);
    asio::awaitable<std::vector<Result<Order>>> create_orders(std::vector<OrderRequest> requests);
    asio::awaitable<std::vector<Result<Order>>> cancel_orders(std::vector<OrderRef> refs);
    std::string new_client_order_id();
};

Client::Impl::Impl(Config cfg)
    : config(std::move(cfg)), tls(detail::make_tls_context()), limiter(0.9) {
    logger = config.logger ? config.logger : std::make_shared<SpdLogger>();

    if (!config.credentials.ed25519_private_key.empty()) {
        auto key = detail::load_ed25519_key(config.credentials.ed25519_private_key);
        if (!key) throw std::invalid_argument(key.error().message);
        ed25519_key = std::move(key).value();
    }

    io.emplace();
    work.emplace(io->get_executor());
    http = detail::HttpClient::create(io->get_executor(), tls, limiter, config.request_timeout);

    for (Market m : {Market::Futures, Market::Spot}) {
        auto& ms = state(m);
        ms.market = m;
        account_of(m).market = m;
        ms.ep = endpoints(m, config.environment);
        limiter.set_limits(ms.ep.rest_host, default_rate_limits(m));
    }

    const int n = std::max(1, config.io_threads);
    for (int i = 0; i < n; ++i) {
        threads.emplace_back([this] {
            t_in_io_thread = true;
            for (;;) {
                try {
                    io->run();
                    return;
                } catch (const std::exception& e) {
                    log(LogLevel::Error, std::string("exception escaped a callback: ") + e.what());
                }
            }
        });
    }
}

Client::Impl::~Impl() {
    for (MarketState* ms : {&futures, &spot}) {
        std::lock_guard lock(ms->mu);
        for (auto& c : ms->conns) c.ws->stop();
    }
    for (AccountState* a : {&futures_account, &spot_account}) {
        std::lock_guard lock(a->mu);
        a->active = false;
        ++a->gen;
        if (a->ws) a->ws->stop();
    }
    {
        std::lock_guard lock(trade_mu);
        if (ws_api) ws_api->stop();
        if (spot_ws_api) spot_ws_api->stop();
    }
    work.reset();
    io->stop();
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    // Drop sockets and timers while the io_context still exists, then destroy pending handlers
    // (coroutine frames) while everything they reference still exists.
    for (MarketState* ms : {&futures, &spot}) ms->conns.clear();
    for (AccountState* a : {&futures_account, &spot_account}) {
        a->ws.reset();
        a->keepalive.reset();  // timers must go before the io_context they belong to
    }
    ws_api.reset();
    spot_ws_api.reset();
    http.reset();
    io.reset();
}

asio::awaitable<Result<std::string>> Client::Impl::get(MarketState& ms, std::string target,
                                                       int weight) {
    // Requests are built before co_await: GCC 14 crashes on some braced temporaries in co_await.
    detail::HttpRequest req;
    req.host = ms.ep.rest_host;
    req.target = std::move(target);
    req.weight = weight;
    auto r = co_await http->co_request(std::move(req));
    if (!r) co_return r.error();
    if (r->status != 200) co_return api_error(*r);
    co_return std::move(r->body);
}

// =============================================================================================
// 5. Market-data streams
// =============================================================================================

std::shared_ptr<detail::WsStream> Client::Impl::assign_conn(MarketState& ms,
                                                            const std::string& stream,
                                                            std::size_t& index) {
    const std::string target = stream_target(ms.market, stream);
    detail::WsOptions opts;
    opts.max_streams = ms.market == Market::Futures ? 200 : 1024;
    opts.max_messages_per_second = ms.market == Market::Futures ? 4 : 2;
    index = ms.conns.size();
    for (std::size_t i = 0; i < ms.conns.size(); ++i) {
        if (ms.conns[i].target == target && ms.conns[i].load < opts.max_streams) {
            index = i;
            break;
        }
    }
    if (index == ms.conns.size()) {
        const std::size_t i = index;
        auto c = detail::WsStream::create(
            io->get_executor(), tls, {ms.ep.stream_host, "443", target}, opts,
            [this, &ms](std::string_view msg) { on_stream_message(ms, msg); },
            [this, &ms, i](detail::WsState st, const std::string& reason) {
                on_stream_state(ms, i, st, reason);
            });
        ms.conns.push_back({target, c});
        c->start();
    }
    ++ms.conns[index].load;
    return ms.conns[index].ws;
}

void Client::Impl::on_stream_message(MarketState& ms, std::string_view msg) {
    // {"stream":"btcusdt@kline_1m","data":{...}}; msg has kMessagePadding bytes after it.
    thread_local ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(msg.data(), msg.size(), msg.size() + detail::kMessagePadding).get(doc)) {
        log(LogLevel::Warn, "unparseable stream message: " + std::string(msg.substr(0, 200)));
        return;
    }
    std::string_view stream_sv;
    ondemand::object data;
    if (doc["stream"].get_string().get(stream_sv) || doc["data"].get_object().get(data)) return;

    std::shared_ptr<WatchState> w;
    {
        std::lock_guard lock(ms.mu);
        auto it = ms.watches.find(std::string(stream_sv));
        if (it == ms.watches.end()) return;  // unsubscribed meanwhile
        w = it->second;
    }
    switch (w->kind) {
        case WatchKind::Candles:
            on_kline(ms, std::static_pointer_cast<CandleSeries>(w), data);
            break;
        case WatchKind::Trade:
            on_value(ms, std::static_pointer_cast<ValueWatch<Trade>>(w), data);
            break;
        case WatchKind::BookTicker:
            on_value(ms, std::static_pointer_cast<ValueWatch<BookTicker>>(w), data);
            break;
        case WatchKind::Ticker24h:
            on_value(ms, std::static_pointer_cast<ValueWatch<Ticker24h>>(w), data);
            break;
        case WatchKind::MarkPrice:
            on_value(ms, std::static_pointer_cast<ValueWatch<MarkPrice>>(w), data);
            break;
        case WatchKind::OrderBook: on_book(ms, std::static_pointer_cast<BookWatch>(w), data); break;
    }
}

void Client::Impl::on_stream_state(MarketState& ms, std::size_t conn, detail::WsState st,
                                   const std::string& reason) {
    std::string name;
    std::vector<std::shared_ptr<WatchState>> resync;
    {
        std::lock_guard lock(ms.mu);
        if (conn >= ms.conns.size()) return;
        name = std::string(to_string(ms.market)) + " stream " + ms.conns[conn].target + " #" +
               std::to_string(conn);
        if (st == detail::WsState::Connecting) {
            ms.conns[conn].open = false;
            for (auto& [key, w] : ms.watches) {
                if (w->conn != conn || !w->subscribed) continue;
                w->lost = true;
                w->syncing = true;
                if (w->status == StreamStatus::Live || w->status == StreamStatus::Resyncing) {
                    w->status = StreamStatus::Stale;
                }
            }
        } else if (st == detail::WsState::Open) {
            ms.conns[conn].open = true;
            for (auto& [key, w] : ms.watches) {
                if (w->conn != conn || !w->lost) continue;
                w->lost = false;
                if (w->has_data()) w->status = StreamStatus::Resyncing;
                resync.push_back(w);
            }
        }
    }

    if (st == detail::WsState::Connecting) {
        log(LogLevel::Warn, name + " disconnected: " + reason);
    } else if (st == detail::WsState::Open) {
        log(LogLevel::Info, name + " connected" + (reason.empty() ? "" : ": " + reason));
    }
    for (auto& w : resync) start_sync(ms, w, false);
}

// --- Watches (all kinds) ----------------------------------------------------------------------

bool Client::Impl::is_current(const MarketState& ms, const WatchState& w, std::uint64_t gen) {
    auto it = ms.watches.find(w.stream);
    return it != ms.watches.end() && it->second.get() == &w && w.sync_gen == gen;
}

std::function<void()> Client::Impl::add_watch(MarketState& ms, std::shared_ptr<WatchState> w) {
    ms.watches.emplace(w->stream, w);
    auto conn = assign_conn(ms, w->stream, w->conn);
    return [this, &ms, w = std::move(w), conn = std::move(conn)] {
        conn->subscribe({w->stream}, [this, &ms, w](Result<void> r) {
            if (!r) {
                if (r.error().code == ErrorCode::Cancelled) return;  // shutting down
                std::uint64_t gen = 0;
                {
                    std::lock_guard lock(ms.mu);
                    gen = w->sync_gen;
                }
                fail_watch(ms, w, gen, r.error());
                return;
            }
            {
                std::lock_guard lock(ms.mu);
                auto it = ms.watches.find(w->stream);
                if (it == ms.watches.end() || it->second != w || w->subscribed) return;
                w->subscribed = true;
            }
            // Stream first, then the REST snapshot: events in between are buffered.
            start_sync(ms, w, true);
        });
    };
}

void Client::Impl::start_sync(MarketState& ms, const std::shared_ptr<WatchState>& w, bool full) {
    std::uint64_t gen = 0;
    {
        std::lock_guard lock(ms.mu);
        gen = ++w->sync_gen;
        w->syncing = true;
    }
    switch (w->kind) {
        case WatchKind::Candles:
            spawn(sync_candles(ms, std::static_pointer_cast<CandleSeries>(w), gen, full));
            break;
        case WatchKind::Trade:
            spawn(sync_value(ms, std::static_pointer_cast<ValueWatch<Trade>>(w), gen));
            break;
        case WatchKind::BookTicker:
            spawn(sync_value(ms, std::static_pointer_cast<ValueWatch<BookTicker>>(w), gen));
            break;
        case WatchKind::Ticker24h:
            spawn(sync_value(ms, std::static_pointer_cast<ValueWatch<Ticker24h>>(w), gen));
            break;
        case WatchKind::MarkPrice:
            spawn(sync_value(ms, std::static_pointer_cast<ValueWatch<MarkPrice>>(w), gen));
            break;
        case WatchKind::OrderBook:
            spawn(sync_book(ms, std::static_pointer_cast<BookWatch>(w), gen));
            break;
    }
}

std::vector<Done> Client::Impl::settle(MarketState& ms, WatchState& w) {
    std::vector<Done> out;
    const bool conn_up = w.conn < ms.conns.size() && ms.conns[w.conn].open;
    const auto next = conn_up ? StreamStatus::Live : StreamStatus::Stale;
    w.syncing = !conn_up;
    w.status = next;
    if (w.has_data()) out.swap(w.waiting);  // even if Stale: the latest known data
    return out;
}

void Client::Impl::deliver(std::vector<Done> waiting) {
    for (auto& d : waiting) {
        if (d) d(Result<void>{});
    }
}

void Client::Impl::fail_watch(MarketState& ms, const std::shared_ptr<WatchState>& w,
                              std::uint64_t gen, Error err) {
    std::vector<Done> waiting;
    {
        std::lock_guard lock(ms.mu);
        if (!is_current(ms, *w, gen)) return;
        waiting = std::move(w->waiting);
    }
    remove_watch(ms, w->stream);
    log(LogLevel::Error, "watch " + w->stream + " failed: " + err.message);
    for (auto& d : waiting) {
        if (d) d(err);
    }
}

void Client::Impl::remove_watch(MarketState& ms, const std::string& stream) {
    std::shared_ptr<detail::WsStream> conn;
    std::vector<Done> waiting;
    {
        std::lock_guard lock(ms.mu);
        auto it = ms.watches.find(stream);
        if (it == ms.watches.end()) return;
        auto w = it->second;
        ms.watches.erase(it);
        waiting = std::move(w->waiting);
        if (w->conn < ms.conns.size()) {
            auto& c = ms.conns[w->conn];
            if (c.load > 0) --c.load;
            conn = c.ws;
        }
    }
    if (conn) conn->unsubscribe({stream});
    for (auto& d : waiting) {
        if (d) d(Error{ErrorCode::Cancelled, "unwatched"});
    }
}

asio::awaitable<bool> Client::Impl::retry_after_failure(MarketState& ms, const WatchState& w,
                                                        std::uint64_t gen, const Error& err,
                                                        std::chrono::milliseconds& backoff) {
    log(LogLevel::Warn, w.stream + ": loading snapshot failed, retrying: " + err.message);
    asio::steady_timer timer(*io, backoff);
    co_await timer.async_wait(kAwait);
    backoff = std::min(backoff * 2, std::chrono::milliseconds{30'000});
    std::lock_guard lock(ms.mu);
    co_return is_current(ms, w, gen);
}

namespace {

/// Exchange-side rejections (bad symbol, ...) end a watch; network trouble is retried.
bool is_permanent(const Error& e) {
    return e.code == ErrorCode::Exchange || e.code == ErrorCode::Parse;
}

}  // namespace

// =============================================================================================
// 6. Instruments
// =============================================================================================

asio::awaitable<Result<void>> Client::Impl::load_instruments(MarketState& ms) {
    auto body =
        co_await get(ms, ms.ep.api_prefix + "/exchangeInfo", ms.market == Market::Futures ? 1 : 20);
    if (!body) co_return body.error();
    std::vector<detail::RateLimit> limits;
    auto parsed = detail::parse_exchange_info(ms.market, *body, &limits);
    if (!parsed) co_return parsed.error();
    if (!limits.empty()) limiter.set_limits(ms.ep.rest_host, limits);

    std::unordered_map<std::string, Instrument> map;
    for (auto& ins : *parsed) {
        auto key = ins.symbol;
        map.emplace(std::move(key), std::move(ins));
    }
    const auto count = map.size();
    {
        std::lock_guard lock(ms.mu);
        ms.instruments = std::move(map);
    }
    log(LogLevel::Info, "loaded " + std::to_string(count) + " " +
                            std::string(to_string(ms.market)) + " instruments");
    bool start_refresh = false;
    {
        std::lock_guard lock(ms.mu);
        start_refresh = !std::exchange(ms.instruments_refreshing, true);
    }
    if (start_refresh) spawn(refresh_instruments(ms));
    co_return Result<void>{};
}

asio::awaitable<void> Client::Impl::refresh_instruments(MarketState& ms) {
    // Tick sizes, limits and trading status change now and then (listings, delistings).
    asio::steady_timer timer(*io);
    for (;;) {
        timer.expires_after(std::chrono::hours{1});
        auto [ec] = co_await timer.async_wait(kAwait);
        if (ec) co_return;
        auto r = co_await load_instruments(ms);
        if (!r) log(LogLevel::Warn, "hourly instrument reload failed: " + r.error().message);
    }
}

// =============================================================================================
// 7. Candles
// =============================================================================================

namespace {

std::string kline_stream(std::string_view symbol, Interval iv) {
    return lower(symbol) + "@kline_" + std::string(to_string(iv));
}

void upsert(std::deque<Candle>& d, const Candle& c) {
    if (d.empty() || c.open_time > d.back().open_time) {
        d.push_back(c);
        return;
    }
    auto it = std::lower_bound(d.begin(), d.end(), c.open_time,
                               [](const Candle& a, Timestamp t) { return a.open_time < t; });
    if (it != d.end() && it->open_time == c.open_time) {
        *it = c;
    } else {
        d.insert(it, c);
    }
}

void trim(std::deque<Candle>& d, std::size_t depth) {
    while (d.size() > depth) d.pop_front();
}

int klines_weight(Market m, std::size_t limit) {
    if (m == Market::Spot) return 2;
    if (limit < 100) return 1;
    if (limit < 500) return 2;
    if (limit <= 1000) return 5;
    return 10;
}

std::size_t klines_max_limit(Market m) { return m == Market::Futures ? 1500 : 1000; }

}  // namespace

void Client::Impl::on_kline(MarketState& ms, const std::shared_ptr<CandleSeries>& s,
                            ondemand::object data) {
    ondemand::object k;
    if (data["k"].get_object().get(k)) return;
    auto candle = parse_kline_object(k);
    if (!candle) {
        log(LogLevel::Warn, s->stream + ": " + candle.error().message);
        return;
    }
    {
        std::lock_guard lock(ms.mu);
        if (s->syncing) {
            if (s->buffered.size() < 10'000) s->buffered.push_back(*candle);
            return;
        }
        upsert(s->candles, *candle);
        trim(s->candles, s->depth);
    }
}

asio::awaitable<void> Client::Impl::sync_candles(MarketState& ms, std::shared_ptr<CandleSeries> s,
                                                 std::uint64_t gen, bool full) {
    auto backoff = std::chrono::milliseconds{1000};
    for (;;) {
        std::optional<std::int64_t> since;
        std::size_t depth = 0;
        {
            std::lock_guard lock(ms.mu);
            if (!is_current(ms, *s, gen)) co_return;
            depth = s->depth;
            if (!full && !s->candles.empty()) since = to_epoch_ms(s->candles.back().open_time);
        }
        // Too long a gap: cheaper and simpler to reload everything.
        if (since) {
            if (auto d = interval_duration(s->interval)) {
                const auto missing = static_cast<std::size_t>((now_ms() - *since) / d->count()) + 1;
                if (missing >= depth) since.reset();
            }
        }

        auto r = since ? co_await fetch_klines_since(ms, s->symbol, s->interval, *since, depth)
                       : co_await fetch_latest_klines(ms, s->symbol, s->interval, depth);
        if (!r) {
            if (is_permanent(r.error())) {
                fail_watch(ms, s, gen, r.error());
                co_return;
            }
            if (!co_await retry_after_failure(ms, *s, gen, r.error(), backoff)) co_return;
            continue;
        }

        std::vector<Done> settled;
        {
            std::lock_guard lock(ms.mu);
            if (!is_current(ms, *s, gen)) co_return;
            if (!since) s->candles.clear();
            for (const auto& c : *r) upsert(s->candles, c);
            // Stream events received while loading are newer than (or equal to) the REST data.
            for (const auto& c : s->buffered) {
                if (s->candles.empty() || c.open_time >= s->candles.front().open_time) {
                    upsert(s->candles, c);
                }
            }
            s->buffered.clear();
            trim(s->candles, s->depth);
            settled = settle(ms, *s);
        }
        deliver(std::move(settled));
        co_return;
    }
}

asio::awaitable<Result<std::vector<Candle>>> Client::Impl::fetch_klines(
    MarketState& ms, const std::string& symbol, Interval iv, std::size_t limit,
    std::optional<std::int64_t> start, std::optional<std::int64_t> end) {
    std::string target = ms.ep.api_prefix + "/klines?symbol=" + symbol +
                         "&interval=" + std::string(to_string(iv)) +
                         "&limit=" + std::to_string(limit);
    if (start) target += "&startTime=" + std::to_string(*start);
    if (end) target += "&endTime=" + std::to_string(*end);
    auto body = co_await get(ms, std::move(target), klines_weight(ms.market, limit));
    if (!body) co_return body.error();
    co_return detail::parse_klines(*body, now_ts());
}

asio::awaitable<Result<std::vector<Candle>>> Client::Impl::fetch_latest_klines(
    MarketState& ms, const std::string& symbol, Interval iv, std::size_t count) {
    std::vector<std::vector<Candle>> pages;
    std::size_t total = 0;
    std::optional<std::int64_t> end;
    while (total < count) {
        const std::size_t limit = std::min(count - total, klines_max_limit(ms.market));
        auto page = co_await fetch_klines(ms, symbol, iv, limit, std::nullopt, end);
        if (!page) co_return page.error();
        if (page->empty()) break;
        total += page->size();
        end = to_epoch_ms(page->front().open_time) - 1;
        const bool last = page->size() < limit;
        pages.push_back(std::move(*page));
        if (last) break;
    }
    std::vector<Candle> out;
    out.reserve(total);
    for (auto it = pages.rbegin(); it != pages.rend(); ++it) {
        out.insert(out.end(), it->begin(), it->end());
    }
    co_return out;
}

asio::awaitable<Result<std::vector<Candle>>> Client::Impl::fetch_klines_since(
    MarketState& ms, const std::string& symbol, Interval iv, std::int64_t start_ms,
    std::size_t max_count) {
    std::vector<Candle> out;
    std::int64_t start = start_ms;
    while (out.size() < max_count) {
        const std::size_t limit = klines_max_limit(ms.market);
        auto page = co_await fetch_klines(ms, symbol, iv, limit, start, std::nullopt);
        if (!page) co_return page.error();
        if (page->empty()) break;
        start = to_epoch_ms(page->back().open_time) + 1;
        const bool last = page->size() < limit;
        out.insert(out.end(), page->begin(), page->end());
        if (last) break;
    }
    co_return out;
}

// =============================================================================================
// 8. Latest-value watches: trades, book ticker, 24h ticker, mark price
// =============================================================================================

template <class T>
void Client::Impl::on_value(MarketState& ms, const std::shared_ptr<ValueWatch<T>>& w,
                            ondemand::object data) {
    using K = ValueKind<T>;
    auto v = K::parse_event(data);
    if (!v) {
        log(LogLevel::Warn, w->stream + ": " + v.error().message);
        return;
    }
    std::vector<Done> settled;
    {
        std::lock_guard lock(ms.mu);
        if (w->syncing) {
            if (!w->buffered || K::version(*v) >= K::version(*w->buffered)) w->buffered = *v;
            return;
        }
        if (w->value && K::version(*v) < K::version(*w->value)) return;
        w->value = std::move(*v);
        settled.swap(w->waiting);  // REST had nothing (no trade yet): first value
    }
    deliver(std::move(settled));
}

template <class T>
asio::awaitable<void> Client::Impl::sync_value(MarketState& ms, std::shared_ptr<ValueWatch<T>> w,
                                               std::uint64_t gen) {
    using K = ValueKind<T>;
    auto backoff = std::chrono::milliseconds{1000};
    for (;;) {
        auto body = co_await get(ms, K::rest(ms.ep, w->symbol), K::weight(ms.market));
        Result<std::optional<T>> r =
            body ? K::parse_rest(*body, w->symbol) : Result<std::optional<T>>(body.error());
        if (!r) {
            if (is_permanent(r.error())) {
                fail_watch(ms, w, gen, r.error());
                co_return;
            }
            if (!co_await retry_after_failure(ms, *w, gen, r.error(), backoff)) co_return;
            continue;
        }

        std::vector<Done> settled;
        {
            std::lock_guard lock(ms.mu);
            if (!is_current(ms, *w, gen)) co_return;
            for (auto* candidate : {&*r, &w->buffered}) {
                if (*candidate && (!w->value || K::version(**candidate) >= K::version(*w->value))) {
                    w->value = **candidate;
                }
            }
            w->buffered.reset();
            settled = settle(ms, *w);
        }
        deliver(std::move(settled));
        co_return;
    }
}

template <class T>
void Client::Impl::watch_value(Market market, std::string_view symbol, Done done) {
    using K = ValueKind<T>;
    auto sym = normalize_symbol(symbol);
    if (!sym) {
        if (done) done(invalid_symbol(symbol));
        return;
    }
    auto& ms = state(market);
    const std::string stream = K::stream(*sym);
    std::function<void()> subscribe;
    bool ready = false;
    {
        std::lock_guard lock(ms.mu);
        if (auto it = ms.watches.find(stream); it != ms.watches.end()) {
            ready = it->second->has_data();
            if (!ready) it->second->waiting.push_back(std::move(done));
        } else {
            auto w = std::make_shared<ValueWatch<T>>();
            w->kind = K::kind;
            w->symbol = *sym;
            w->stream = stream;
            if (done) w->waiting.push_back(std::move(done));
            subscribe = add_watch(ms, std::move(w));
        }
    }
    if (ready && done) done(Result<void>{});
    if (subscribe) subscribe();
}

template <class T>
void Client::Impl::unwatch_value(Market market, std::string_view symbol) {
    if (auto sym = normalize_symbol(symbol))
        remove_watch(state(market), ValueKind<T>::stream(*sym));
}

template <class T>
Result<T> Client::Impl::get_value(Market market, std::string_view symbol) {
    using K = ValueKind<T>;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    auto& ms = state(market);
    std::lock_guard lock(ms.mu);
    auto it = ms.watches.find(K::stream(*sym));
    if (it == ms.watches.end()) {
        return Error{ErrorCode::NotSubscribed,
                     "the watch was stopped meanwhile (unwatched or failed)"};
    }
    const auto& w = static_cast<const ValueWatch<T>&>(*it->second);
    if (!w.value)
        return Error{ErrorCode::NotFound, std::string(K::name) + " for " + *sym + ": none yet"};
    return *w.value;
}

// =============================================================================================
// 8b. Order book
// =============================================================================================
//
// Partial (5/10/20): every message is a fresh top N and replaces the book; the REST snapshot only
// fills it until the first message. Full: REST snapshot + diffs under LocalBook's sequence rules;
// a gap (missed message) makes the watch Resyncing and rebuilds it from a new snapshot.

std::shared_ptr<BookWatch> Client::Impl::find_book(const MarketState& ms,
                                                   const std::string& symbol) {
    for (std::size_t depth : {std::size_t{0}, std::size_t{5}, std::size_t{10}, std::size_t{20}}) {
        auto it = ms.watches.find(depth_stream(symbol, depth));
        if (it != ms.watches.end()) return std::static_pointer_cast<BookWatch>(it->second);
    }
    return nullptr;
}

void Client::Impl::on_book(MarketState& ms, const std::shared_ptr<BookWatch>& w,
                           ondemand::object data) {
    auto e = parse_depth_object(data);
    if (!e) {
        log(LogLevel::Warn, w->stream + ": " + e.error().message);
        return;
    }
    bool rebuild = false;
    {
        std::lock_guard lock(ms.mu);
        auto it = ms.watches.find(w->stream);
        if (it == ms.watches.end() || it->second != w) return;
        if (std::exchange(w->test_drop_next, false)) return;
        if (w->syncing) {
            if (w->buffered.size() >= 10'000) w->buffered.erase(w->buffered.begin());
            w->buffered.push_back(std::move(*e));
            return;
        }
        if (w->partial) {
            if (e->last_id < w->book.last_id) return;
            w->book.install(*e);
        } else {
            switch (w->book.apply(*e)) {
                case DepthApply::Old: return;
                case DepthApply::Applied: break;
                case DepthApply::Gap:
                    // The book is wrong now: drop it so watch_order_book() waits for the rebuild.
                    w->book.bids.clear();
                    w->book.asks.clear();
                    w->syncing = true;
                    w->buffered.push_back(std::move(*e));
                    w->status = StreamStatus::Resyncing;
                    rebuild = true;
                    break;
            }
        }
    }
    if (rebuild) {
        log(LogLevel::Warn, w->stream + ": missed an update; rebuilding the order book");
        start_sync(ms, w, true);
    }
}

asio::awaitable<void> Client::Impl::sync_book(MarketState& ms, std::shared_ptr<BookWatch> w,
                                              std::uint64_t gen) {
    auto backoff = std::chrono::milliseconds{1000};
    int stale_snapshots = 0;
    for (;;) {
        std::size_t depth = 0;
        {
            std::lock_guard lock(ms.mu);
            depth = w->depth;
        }
        const auto [limit, weight] = depth_snapshot_size(ms.market, depth, w->partial);
        auto body = co_await get(
            ms, ms.ep.api_prefix + "/depth?symbol=" + w->symbol + "&limit=" + std::to_string(limit),
            weight);
        Result<detail::DepthEvent> snap =
            body ? detail::parse_depth_snapshot(*body) : Result<detail::DepthEvent>(body.error());
        if (!snap) {
            if (is_permanent(snap.error())) {
                fail_watch(ms, w, gen, snap.error());
                co_return;
            }
            if (!co_await retry_after_failure(ms, *w, gen, snap.error(), backoff)) co_return;
            continue;
        }

        std::vector<Done> settled;
        bool too_old = false;
        {
            std::lock_guard lock(ms.mu);
            if (!is_current(ms, *w, gen)) co_return;
            w->book.install(*snap);
            for (const auto& e : w->buffered) {
                if (w->partial) {
                    if (e.last_id >= w->book.last_id) w->book.install(e);
                } else if (w->book.apply(e) == DepthApply::Gap) {
                    too_old = true;  // the snapshot predates the buffered events
                    break;
                }
            }
            if (!too_old) {
                w->buffered.clear();
                settled = settle(ms, *w);
            }
        }
        if (too_old) {
            // Binance's snapshot can lag the stream a little: fetch a newer one, keep buffering.
            ++stale_snapshots;
            if (stale_snapshots % 5 == 0) {
                log(LogLevel::Warn, w->stream +
                                        ": order book snapshot still older than the "
                                        "stream after " +
                                        std::to_string(stale_snapshots) + " tries");
            }
            asio::steady_timer t(*io,
                                 std::chrono::milliseconds{250 * std::min(stale_snapshots, 8)});
            co_await t.async_wait(kAwait);
            std::lock_guard lock(ms.mu);
            if (!is_current(ms, *w, gen)) co_return;
            continue;
        }
        deliver(std::move(settled));
        co_return;
    }
}

// =============================================================================================
// 9. Signed requests
// =============================================================================================

std::optional<Error> Client::Impl::missing_keys() const {
    if (config.credentials.api_key.empty() || config.credentials.hmac_secret.empty()) {
        return Error{ErrorCode::InvalidArgument,
                     "account data needs Config::credentials (api_key and hmac_secret)"};
    }
    return std::nullopt;
}

asio::awaitable<Result<void>> Client::Impl::sync_time() {
    const std::int64_t before = now_ms();
    auto body = co_await get(futures, "/fapi/v1/time", 1);
    const std::int64_t after = now_ms();
    if (!body) co_return body.error();
    auto server = parse_root_object(*body, "time", [](ondemand::object o) -> Result<std::int64_t> {
        std::int64_t t = 0;
        if (!get_field(o, "serverTime", t)) return parse_error("time");
        return t;
    });
    if (!server) co_return server.error();
    time_offset_ms = *server - (before + after) / 2;
    time_synced = true;
    co_return Result<void>{};
}

asio::awaitable<Result<std::string>> Client::Impl::signed_request(detail::http::verb method,
                                                                  std::string path,
                                                                  std::string params, int weight,
                                                                  bool is_order, Market market) {
    if (auto e = missing_keys()) co_return *e;
    if (!time_synced) (void)co_await sync_time();  // without it: our own clock
    for (int attempt = 0;; ++attempt) {
        std::string query = params;
        if (!query.empty()) query += '&';
        query += "recvWindow=" + std::to_string(config.recv_window.count()) +
                 "&timestamp=" + std::to_string(server_now());
        query += "&signature=" + detail::hmac_sha256_hex(config.credentials.hmac_secret, query);
        detail::HttpRequest req;
        req.method = method;
        req.host = state(market).ep.rest_host;
        req.target = path + "?" + query;
        req.headers = {{"X-MBX-APIKEY", config.credentials.api_key}};
        req.weight = weight;
        req.is_order = is_order;
        req.idempotent = method == detail::http::verb::get;
        auto r = co_await http->co_request(std::move(req));
        if (!r) co_return r.error();
        if (r->status == 200) co_return std::move(r->body);
        Error err = api_error(*r);
        // 5xx: Binance got the request, but whether it executed is unknown.
        if (r->status >= 500) err.code = ErrorCode::Timeout;
        if (err.exchange_code == -1021 && attempt == 0) {  // timestamp outside recvWindow
            (void)co_await sync_time();
            continue;
        }
        co_return err;
    }
}

asio::awaitable<Result<std::string>> Client::Impl::key_request(detail::http::verb method,
                                                               std::string path) {
    if (auto e = missing_keys()) co_return *e;
    detail::HttpRequest req;
    req.method = method;
    req.host = futures.ep.rest_host;
    req.target = std::move(path);
    req.headers = {{"X-MBX-APIKEY", config.credentials.api_key}};
    auto r = co_await http->co_request(std::move(req));
    if (!r) co_return r.error();
    if (r->status != 200) co_return api_error(*r);
    co_return std::move(r->body);
}

// =============================================================================================
// 10. Account (futures and spot)
// =============================================================================================
//
// One user data stream per market, shared by every account watch; the first watch opens it,
// the last unwatch closes it.
//   futures: /private/ws/<listenKey>, kept alive every 30 min, replaced when Binance says expired.
//   spot:    WebSocket API userDataStream.subscribe.signature after every connect (no listenKey:
//            POST /api/v3/userDataStream answers 410 Gone).
// Load over REST while buffering events, then apply the newer ones. Every (re)connect reloads
// everything; events carry absolute values, so re-applying one is harmless.

namespace detail {
std::string ws_signed_params(Params params, std::string_view api_key, std::string_view secret,
                             std::int64_t timestamp, std::int64_t recv_window);
}  // namespace detail

namespace {

std::string user_stream_target(const std::string& listen_key) {
    return "/private/ws/" + listen_key;
}

void put_position(Positions& all, const Position& p) {
    if (p.amount.is_zero()) {
        auto it = all.find(p.symbol);
        if (it == all.end()) return;
        it->second.erase(p.side);
        if (it->second.empty()) all.erase(it);
    } else {
        all[p.symbol][p.side] = p;
    }
}

/// Positions with leverage and margin type from symbolConfig (positionRisk v3 lacks them).
Positions make_positions(const std::vector<detail::SymbolSettings>& symbols,
                         std::vector<Position> list) {
    std::unordered_map<std::string, const detail::SymbolSettings*> by_symbol;
    for (const auto& s : symbols) by_symbol[s.symbol] = &s;
    Positions out;
    for (auto& p : list) {
        if (auto it = by_symbol.find(p.symbol); it != by_symbol.end()) {
            p.leverage = it->second->leverage;
            p.margin_type = it->second->margin_type;
        }
        put_position(out, p);
    }
    return out;
}

/// From this many symbols on, one request for all symbols weighs less than one per symbol
/// (futures 40 vs 1 each; spot 80 vs 6 each).
std::size_t orders_all_threshold(Market m) { return m == Market::Futures ? 40 : 14; }

OrdersLoad plan_orders(const OrderScope& scope, bool full, Market market) {
    OrdersLoad load;
    if (scope.all) {
        load.all = full || !scope.all_loaded;  // loaded "all" covers every symbol
        return load;
    }
    for (const auto& [sym, loaded] : scope.symbols) {
        if (full || !loaded) load.symbols.push_back(sym);
    }
    if (load.symbols.size() >= orders_all_threshold(market)) {
        load.symbols.clear();
        load.all = true;
    }
    return load;
}

AccountPart orders_part(bool is_algo) {
    return is_algo ? AccountPart::AlgoOrders : AccountPart::OpenOrders;
}

}  // namespace

namespace detail {

/// Tests: what a load fetches of one kind of open orders, on plain data. {all, symbols}.
std::pair<bool, std::vector<std::string>> plan_orders_for_test(
    bool all, bool all_loaded, const std::map<std::string, bool>& symbols, bool full,
    Market market) {
    OrderScope scope;
    scope.all = all;
    scope.all_loaded = all_loaded;
    scope.symbols = symbols;
    auto load = plan_orders(scope, full, market);
    return {load.all, load.symbols};
}

/// Tests: {covers(symbol), loaded_for(symbol)} of a scope ("" = all symbols).
std::pair<bool, bool> order_scope_for_test(bool all, bool all_loaded,
                                           const std::map<std::string, bool>& symbols,
                                           const std::string& symbol) {
    OrderScope scope;
    scope.all = all;
    scope.all_loaded = all_loaded;
    scope.symbols = symbols;
    return {!symbol.empty() && scope.covers(symbol), scope.loaded_for(symbol)};
}

}  // namespace detail

namespace {

const char* market_label(Market m) { return m == Market::Futures ? "futures" : "spot"; }

}  // namespace

void Client::Impl::complete_position(Position& p) const {
    if (auto it = futures_account.symbols.find(p.symbol); it != futures_account.symbols.end()) {
        p.leverage = it->second.leverage;
        p.margin_type = it->second.margin_type;
    }
}

Client::Impl::AccountKick Client::Impl::kick_account_locked(AccountState& a) {
    if (!a.active) {
        a.active = true;
        ++a.gen;
        a.open = a.renewing = false;
        a.syncing = true;
        a.full_reload = true;
        a.drop_events = false;
        a.listen_key.clear();
        a.buffered.clear();
        a.balance_refresh_pending = false;
        return AccountKick::Start;
    }
    // Running: a load brings the new part. While connecting or already loading, the load on
    // Open, or the one queued after the current one, takes care of it.
    return a.open && !a.syncing ? AccountKick::Reload : AccountKick::None;
}

void Client::Impl::run_kick(AccountState& a, AccountKick kick, std::uint64_t gen) {
    if (kick == AccountKick::Start) spawn(start_account(a, gen));
    if (kick == AccountKick::Reload) start_account_sync(a, gen);
}

Client::Impl::Stopped Client::Impl::stop_account_locked(AccountState& a) {
    Stopped st;
    a.active = false;
    ++a.gen;
    st.ws = std::move(a.ws);
    if (a.keepalive) st.timers.push_back(std::move(a.keepalive));
    a.open = false;
    a.syncing = true;
    a.settings_loaded = false;
    a.buffered.clear();
    return st;
}

void Client::Impl::finish_stop(Stopped st) {
    // Futures: the listenKey is left to expire (60 min): Binance hands the same key to every
    // client of this API key, so deleting it could cut off another program.
    for (auto& t : st.timers) asio::post(*io, [t] { t->cancel(); });
    if (st.ws) st.ws->stop();
}

void Client::Impl::watch_part(AccountState& a, AccountPart part, std::string symbol, Done done) {
    if (auto e = missing_keys()) {
        if (done) done(*e);
        return;
    }
    bool ready = false;
    AccountKick kick = AccountKick::None;
    std::uint64_t gen = 0;
    {
        std::lock_guard lock(a.mu);
        switch (part) {
            case AccountPart::Balance: a.balance_on = true; break;
            case AccountPart::Positions: a.positions_on = true; break;
            case AccountPart::OpenOrders:
            case AccountPart::AlgoOrders: {
                auto& sc = a.scope(part == AccountPart::AlgoOrders);
                if (symbol.empty()) {
                    sc.all = true;
                } else {
                    sc.symbols.try_emplace(symbol, sc.all && sc.all_loaded);
                }
                break;
            }
        }
        if (a.loaded(part, symbol)) {
            ready = true;
        } else {
            a.waiting.push_back({part, std::move(symbol), std::move(done)});
            kick = kick_account_locked(a);
            gen = a.gen;
        }
    }
    if (ready) {
        if (done) done(Result<void>{});
        return;
    }
    run_kick(a, kick, gen);
}

void Client::Impl::unwatch_part(AccountState& a, AccountPart part, const std::string& symbol) {
    std::vector<Done> cancelled;
    std::optional<Stopped> stopped;
    {
        std::lock_guard lock(a.mu);
        const bool orders = part == AccountPart::OpenOrders || part == AccountPart::AlgoOrders;
        if (part == AccountPart::Balance) {
            if (!a.balance_on) return;
            a.balance_on = a.balance_loaded = false;
            a.balances.clear();
        } else if (part == AccountPart::Positions) {
            if (!a.positions_on) return;
            a.positions_on = a.positions_loaded = false;
            a.positions.clear();
        } else {
            const bool is_algo = part == AccountPart::AlgoOrders;
            auto& sc = a.scope(is_algo);
            if (symbol.empty()) {
                if (!sc.on()) return;
                sc = OrderScope{};
            } else if (sc.symbols.erase(symbol) == 0) {
                return;
            }
            std::erase_if(a.orders, [&](const auto& kv) {
                return kv.first.first == is_algo && !sc.covers(kv.second.order.symbol);
            });
        }
        for (auto it = a.waiting.begin(); it != a.waiting.end();) {
            if (it->part == part && (!orders || symbol.empty() || it->symbol == symbol)) {
                cancelled.push_back(std::move(it->done));
                it = a.waiting.erase(it);
            } else {
                ++it;
            }
        }
        if (!a.any_watch()) stopped = stop_account_locked(a);
    }
    if (stopped) finish_stop(std::move(*stopped));
    for (auto& d : cancelled) {
        if (d) d(Error{ErrorCode::Cancelled, "unwatched"});
    }
}

void Client::Impl::fail_account(AccountState& a, std::uint64_t gen, Error err) {
    std::vector<Done> waiting;
    Stopped st;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        st = stop_account_locked(a);
        for (auto& w : a.waiting) waiting.push_back(std::move(w.done));
        a.waiting.clear();
        // Nothing can work without the stream (bad API key, ...): every watch ends.
        a.balance_on = a.positions_on = false;
        a.balance_loaded = a.positions_loaded = false;
        a.regular = OrderScope{};
        a.algo = OrderScope{};
        a.orders.clear();
        a.positions.clear();
        a.balances.clear();
    }
    log(LogLevel::Error,
        std::string(market_label(a.market)) + " account watch failed: " + err.message);
    finish_stop(std::move(st));
    for (auto& d : waiting) {
        if (d) d(err);
    }
}

asio::awaitable<void> Client::Impl::start_account(AccountState& a, std::uint64_t gen) {
    if (!time_synced) (void)co_await sync_time();
    if (a.market == Market::Spot) {
        start_spot_stream(a, gen);
        co_return;
    }
    co_await start_futures_stream(a, gen);
}

void Client::Impl::start_spot_stream(AccountState& a, std::uint64_t gen) {
    const auto& ep = spot.ep;
    detail::WsOptions opts;
    opts.raw = true;
    opts.max_messages_per_second = 0;
    std::shared_ptr<detail::WsStream> ws;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        ws = detail::WsStream::create(
            io->get_executor(), tls, {ep.ws_api_host, "443", ep.ws_api_target}, opts,
            [this, &a, gen](std::string_view msg) { on_user_message(a, gen, msg); },
            [this, &a, gen](detail::WsState st, const std::string& reason) {
                on_user_state(a, gen, st, reason);
            });
        a.ws = ws;
    }
    ws->start();
}

asio::awaitable<void> Client::Impl::start_futures_stream(AccountState& a, std::uint64_t gen) {
    auto backoff = std::chrono::milliseconds{1000};
    for (;;) {
        auto key = co_await create_listen_key();
        if (key) {
            auto keepalive = std::make_shared<asio::steady_timer>(*io);
            std::shared_ptr<detail::WsStream> ws;
            {
                std::lock_guard lock(a.mu);
                if (!a.active || a.gen != gen) co_return;
                a.listen_key = *key;
                ws = detail::WsStream::create(
                    io->get_executor(), tls,
                    {futures.ep.stream_host, "443", user_stream_target(*key)}, detail::WsOptions{},
                    [this, &a, gen](std::string_view msg) { on_user_message(a, gen, msg); },
                    [this, &a, gen](detail::WsState st, const std::string& reason) {
                        on_user_state(a, gen, st, reason);
                    });
                a.ws = ws;
                a.keepalive = keepalive;
            }
            ws->start();
            spawn(keep_listen_key_alive(gen, std::move(keepalive)));
            co_return;
        }
        if (is_permanent(key.error())) {
            fail_account(a, gen, key.error());
            co_return;
        }
        log(LogLevel::Warn,
            "account: opening the user data stream failed, retrying: " + key.error().message);
        asio::steady_timer t(*io, backoff);
        co_await t.async_wait(kAwait);
        backoff = std::min(backoff * 2, std::chrono::milliseconds{30'000});
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) co_return;
    }
}

asio::awaitable<Result<std::string>> Client::Impl::create_listen_key() {
    auto body = co_await key_request(detail::http::verb::post, "/fapi/v1/listenKey");
    if (!body) co_return body.error();
    co_return parse_root_object(*body, "listenKey", [](ondemand::object o) -> Result<std::string> {
        std::string key;
        if (!get_str(o, "listenKey", key) || key.empty()) return parse_error("listenKey");
        return key;
    });
}

asio::awaitable<void> Client::Impl::keep_listen_key_alive(
    std::uint64_t gen, std::shared_ptr<asio::steady_timer> timer) {
    auto& a = futures_account;
    std::chrono::minutes wait{30};  // the key lives 60 minutes
    for (;;) {
        timer->expires_after(wait);
        auto [ec] = co_await timer->async_wait(kAwait);
        if (ec) co_return;
        {
            std::lock_guard lock(a.mu);
            if (!a.active || a.gen != gen) co_return;
        }
        (void)co_await sync_time();  // keep the clock offset fresh for signed requests
        auto r = co_await key_request(detail::http::verb::put, "/fapi/v1/listenKey");
        wait = std::chrono::minutes{30};
        if (r) continue;
        if (r.error().exchange_code == -1125) {  // "This listenKey does not exist."
            log(LogLevel::Warn, "account: listenKey is gone; requesting a new one");
            co_await renew_listen_key(gen);
        } else {
            log(LogLevel::Warn, "account: listenKey keep-alive failed: " + r.error().message);
            wait = std::chrono::minutes{1};
        }
    }
}

asio::awaitable<void> Client::Impl::renew_listen_key(std::uint64_t gen) {
    auto& a = futures_account;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen || a.renewing) co_return;
        a.renewing = true;
    }
    auto backoff = std::chrono::milliseconds{1000};
    for (;;) {
        auto key = co_await create_listen_key();
        std::shared_ptr<detail::WsStream> ws;
        {
            std::lock_guard lock(a.mu);
            if (!a.active || a.gen != gen) co_return;
            if (key) {
                a.renewing = false;
                a.listen_key = *key;
                ws = a.ws;
            }
        }
        if (key) {
            if (ws) ws->set_target(user_stream_target(*key));  // reconnect -> reload
            co_return;
        }
        log(LogLevel::Warn,
            "account: requesting a new listenKey failed, retrying: " + key.error().message);
        asio::steady_timer t(*io, backoff);
        co_await t.async_wait(kAwait);
        backoff = std::min(backoff * 2, std::chrono::milliseconds{30'000});
    }
}

void Client::Impl::on_user_message(AccountState& a, std::uint64_t gen, std::string_view msg) {
    using Kind = detail::UserEvent::Kind;
    thread_local ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(msg.data(), msg.size(), msg.size() + detail::kMessagePadding).get(doc)) {
        log(LogLevel::Warn, "account: unparseable message: " + std::string(msg.substr(0, 300)));
        return;
    }
    const bool spot_market = a.market == Market::Spot;
    if (spot_market && msg.starts_with(R"({"id")")) {  // the answer to our subscribe request
        on_spot_subscribed(a, gen, doc);
        return;
    }
    Result<detail::UserEvent> ev = spot_market ? parse_spot_user_event(doc) : parse_user_event(doc);
    if (!ev) {
        log(LogLevel::Warn,
            "account: " + ev.error().message + ": " + std::string(msg.substr(0, 300)));
        return;
    }
    if (ev->kind == Kind::Other) return;

    if (ev->kind == Kind::ListenKeyExpired || ev->kind == Kind::StreamTerminated) {
        std::shared_ptr<detail::WsStream> ws;
        {
            std::lock_guard lock(a.mu);
            if (!a.active || a.gen != gen) return;
            a.syncing = true;  // nothing arrives until a new key / subscription is in use
            a.open = false;
            ws = a.ws;
        }
        if (ev->kind == Kind::ListenKeyExpired) {
            log(LogLevel::Warn, "account: listenKey expired; requesting a new one");
            spawn(renew_listen_key(gen));
        } else {
            log(LogLevel::Warn, "account: spot user data subscription ended; reconnecting");
            if (ws) ws->reconnect();  // connect -> subscribe -> reload
        }
        return;
    }

    EventEffects fx;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        if (a.drop_events) return;
        if (a.syncing) {
            if (a.buffered.size() >= 100'000) {  // reload failing for long: keep newest
                a.buffered.erase(a.buffered.begin(), a.buffered.begin() + 50'000);
            }
            a.buffered.push_back(std::move(*ev));
            return;
        }
        apply_user_event(a, *ev, 0, fx);
    }
    for (const auto& symbol : fx.margin_calls) {
        log(LogLevel::Warn, "account: MARGIN CALL for " + symbol);
    }
    if (fx.refresh_balances) schedule_balance_refresh(gen);
}

void Client::Impl::on_spot_subscribed(AccountState& a, std::uint64_t gen, ondemand::document& doc) {
    // {"id":"uds","status":200,"result":{"subscriptionId":0}} or
    // {"id":"uds","status":400,"error":{"code":-2015,"msg":"..."}}
    std::int64_t status = 0;
    std::ignore = doc["status"].get_int64().get(status);
    if (status == 200) {
        {
            std::lock_guard lock(a.mu);
            if (!a.active || a.gen != gen) return;
            a.open = true;
            a.full_reload = true;  // events may have been missed: reload everything
        }
        log(LogLevel::Info, "spot account stream subscribed");
        start_account_sync(a, gen);
        return;
    }
    std::int64_t code = 0;
    std::string_view text;
    ondemand::object err;
    if (!doc["error"].get_object().get(err)) {
        std::ignore = err["code"].get_int64().get(code);
        std::ignore = err["msg"].get_string().get(text);
    }
    Error e{ErrorCode::Exchange, "user data subscribe: " + std::string(text),
            static_cast<int>(code)};
    bool loaded = false;
    std::shared_ptr<detail::WsStream> ws;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        loaded = a.any_loaded();
        ws = a.ws;
    }
    if (!loaded && status >= 400 && status < 500 && status != 429) {
        fail_account(a, gen, std::move(e));
        return;
    }
    log(LogLevel::Warn, e.message + "; reconnecting");
    if (ws) ws->reconnect();
}

void Client::Impl::on_user_state(AccountState& a, std::uint64_t gen, detail::WsState st,
                                 const std::string& reason) {
    bool reload = false, subscribe = false;
    std::shared_ptr<detail::WsStream> ws;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        if (st == detail::WsState::Connecting) {
            a.open = false;
            a.syncing = true;
        } else if (st == detail::WsState::Open) {
            if (a.market == Market::Spot) {
                subscribe = true;  // open once Binance confirms the subscription
                ws = a.ws;
            } else {
                a.open = true;
                a.full_reload = true;  // events may have been missed: reload everything
                reload = true;
            }
        }
    }
    const std::string name = std::string(market_label(a.market)) + " account stream";
    if (st == detail::WsState::Connecting) {
        log(LogLevel::Warn, name + " disconnected: " + reason);
    } else if (st == detail::WsState::Open) {
        log(LogLevel::Info, name + " connected");
    }
    if (subscribe && ws) {
        const std::string params =
            detail::ws_signed_params({}, config.credentials.api_key, config.credentials.hmac_secret,
                                     server_now(), config.recv_window.count());
        ws->send(R"({"id":"uds","method":"userDataStream.subscribe.signature","params":)" + params +
                 "}");
    }
    if (reload) start_account_sync(a, gen);
}

void Client::Impl::start_account_sync(AccountState& a, std::uint64_t gen) {
    std::uint64_t sync_gen = 0;
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen) return;
        sync_gen = ++a.sync_gen;
        a.syncing = true;
    }
    spawn(sync_account(a, gen, sync_gen));
}

// --- Account REST calls -----------------------------------------------------------------------

asio::awaitable<Result<detail::AccountSettings>> Client::Impl::rest_account_config() {
    auto body = co_await signed_request(detail::http::verb::get, "/fapi/v1/accountConfig", "", 5);
    if (!body) co_return body.error();
    co_return detail::parse_account_config(*body);
}

asio::awaitable<Result<std::vector<Balance>>> Client::Impl::rest_balances(Market market) {
    if (market == Market::Spot) {
        auto body = co_await signed_request(detail::http::verb::get, "/api/v3/account",
                                            "omitZeroBalances=true", 20, false, Market::Spot);
        if (!body) co_return body.error();
        co_return detail::parse_spot_account(*body);
    }
    auto body = co_await signed_request(detail::http::verb::get, "/fapi/v3/balance", "", 5);
    if (!body) co_return body.error();
    co_return detail::parse_futures_balances(*body);
}

asio::awaitable<Result<std::vector<detail::SymbolSettings>>> Client::Impl::rest_symbol_config() {
    auto body = co_await signed_request(detail::http::verb::get, "/fapi/v1/symbolConfig", "", 5);
    if (!body) co_return body.error();
    co_return detail::parse_symbol_config(*body);
}

asio::awaitable<Result<std::vector<Position>>> Client::Impl::rest_position_risk() {
    auto body = co_await signed_request(detail::http::verb::get, "/fapi/v3/positionRisk", "", 5);
    if (!body) co_return body.error();
    co_return detail::parse_position_risk(*body);
}

asio::awaitable<Result<std::vector<Order>>> Client::Impl::rest_open_orders(Market market,
                                                                           bool is_algo,
                                                                           std::string symbol) {
    std::string params = symbol.empty() ? std::string() : "symbol=" + symbol;
    if (market == Market::Spot) {
        const int weight = symbol.empty() ? 80 : 6;
        auto body = co_await signed_request(detail::http::verb::get, "/api/v3/openOrders",
                                            std::move(params), weight, false, Market::Spot);
        if (!body) co_return body.error();
        co_return detail::parse_spot_open_orders(*body);
    }
    const char* path = is_algo ? "/fapi/v1/openAlgoOrders" : "/fapi/v1/openOrders";
    const int weight = symbol.empty() ? 40 : 1;
    auto body = co_await signed_request(detail::http::verb::get, path, std::move(params), weight);
    if (!body) co_return body.error();
    if (is_algo) co_return detail::parse_open_algo_orders(*body);
    co_return detail::parse_open_orders(*body);
}

asio::awaitable<Result<OrdersFetched>> Client::Impl::fetch_orders(Market market, bool is_algo,
                                                                  const OrdersLoad& load) {
    OrdersFetched out;
    if (load.all) {
        auto r = co_await rest_open_orders(market, is_algo, "");
        if (!r) co_return r.error();
        out.all = std::move(*r);
    }
    for (const auto& sym : load.symbols) {
        auto r = co_await rest_open_orders(market, is_algo, sym);
        // Binance rejecting the symbol ends that one watch; trouble reaching it fails the load.
        if (!r && !is_permanent(r.error())) co_return r.error();
        out.by_symbol.emplace_back(sym, std::move(r));
    }
    co_return out;
}

asio::awaitable<Result<AccountSnapshot>> Client::Impl::fetch_account_snapshot(
    Market market, const AccountLoad& load) {
    AccountSnapshot s;
    if (load.settings) {  // futures: the position mode, needed to check orders before sending
        auto r = co_await rest_account_config();
        if (!r) co_return r.error();
        s.settings = *r;
    }
    if (load.positions) {
        auto symbols = co_await rest_symbol_config();
        if (!symbols) co_return symbols.error();
        auto positions = co_await rest_position_risk();
        if (!positions) co_return positions.error();
        s.symbols = std::move(*symbols);
        s.positions = std::move(*positions);
    }
    if (load.balances) {
        auto r = co_await rest_balances(market);
        if (!r) co_return r.error();
        s.balances = std::move(*r);
    }
    if (load.orders.any()) {
        auto r = co_await fetch_orders(market, false, load.orders);
        if (!r) co_return r.error();
        s.orders = std::move(*r);
    }
    if (load.algo_orders.any()) {
        auto r = co_await fetch_orders(market, true, load.algo_orders);
        if (!r) co_return r.error();
        s.algo_orders = std::move(*r);
    }
    co_return s;
}

asio::awaitable<void> Client::Impl::sync_account(AccountState& a, std::uint64_t gen,
                                                 std::uint64_t sync_gen) {
    auto current = [&] { return a.active && a.gen == gen && a.sync_gen == sync_gen; };
    const bool is_futures = a.market == Market::Futures;
    auto backoff = std::chrono::milliseconds{1000};
    for (;;) {
        AccountLoad load;
        bool full = false;
        {
            std::lock_guard lock(a.mu);
            if (!current()) co_return;
            full = a.full_reload;
            load.settings = is_futures && (full || !a.settings_loaded);
            load.balances = a.balance_on && (full || !a.balance_loaded);
            load.positions = a.positions_on && (full || !a.positions_loaded);
            load.orders = plan_orders(a.regular, full, a.market);
            load.algo_orders = plan_orders(a.algo, full, a.market);
        }
        // Events older than this are in the snapshot (1 s of slack for clock error; replaying an
        // event is harmless).
        const std::int64_t started = server_now() - 1000;
        auto snap = co_await fetch_account_snapshot(a.market, load);
        if (!snap) {
            bool loaded = false;
            {
                std::lock_guard lock(a.mu);
                if (!current()) co_return;
                loaded = a.any_loaded();
            }
            if (!loaded && is_permanent(snap.error())) {
                fail_account(a, gen, snap.error());
                co_return;
            }
            log(LogLevel::Warn, std::string(market_label(a.market)) +
                                    " account: loading failed, retrying: " + snap.error().message);
            asio::steady_timer t(*io, backoff);
            co_await t.async_wait(kAwait);
            backoff = std::min(backoff * 2, std::chrono::milliseconds{30'000});
            std::lock_guard lock(a.mu);
            if (!current()) co_return;
            continue;
        }

        std::vector<std::pair<Done, Result<void>>> done;
        EventEffects fx;
        bool again = false;
        {
            std::lock_guard lock(a.mu);
            if (!current()) co_return;
            if (snap->settings) {
                a.settings = *snap->settings;
                a.settings_loaded = true;
            }
            if (snap->balances && a.balance_on) {
                a.balances.clear();
                for (auto& b : *snap->balances) a.balances[b.asset] = b;
                a.balance_loaded = true;
            }
            if (snap->positions && a.positions_on) {
                a.symbols.clear();
                for (auto& sy : *snap->symbols) a.symbols[sy.symbol] = sy;
                a.positions = make_positions(*snap->symbols, std::move(*snap->positions));
                a.positions_loaded = true;
            }

            const std::int64_t now = now_ms();
            for (const bool is_algo : {false, true}) {
                auto& fetched = is_algo ? snap->algo_orders : snap->orders;
                auto& sc = a.scope(is_algo);
                auto put = [&](const Order& o) {
                    if (is_open(o.status) && sc.covers(o.symbol)) {
                        a.orders[{o.is_algo, o.id}] = CachedOrder{o, now};
                    }
                };
                if (fetched.all) {
                    std::erase_if(a.orders,
                                  [&](const auto& kv) { return kv.first.first == is_algo; });
                    for (const auto& o : *fetched.all) put(o);
                    sc.all_loaded = sc.all;
                    for (auto& [sym, loaded] : sc.symbols) loaded = true;
                }
                for (auto& [sym, r] : fetched.by_symbol) {
                    auto it = sc.symbols.find(sym);
                    if (!r) {
                        if (it != sc.symbols.end() && !it->second) sc.symbols.erase(it);
                        const auto part = orders_part(is_algo);
                        for (auto w = a.waiting.begin(); w != a.waiting.end();) {
                            if (w->part == part && w->symbol == sym) {
                                done.emplace_back(std::move(w->done), r.error());
                                w = a.waiting.erase(w);
                            } else {
                                ++w;
                            }
                        }
                        continue;
                    }
                    std::erase_if(a.orders, [&](const auto& kv) {
                        return kv.first.first == is_algo && kv.second.order.symbol == sym;
                    });
                    for (const auto& o : *r) put(o);
                    if (it != sc.symbols.end()) it->second = true;
                }
            }

            for (const auto& ev : a.buffered) apply_user_event(a, ev, started, fx);
            a.buffered.clear();
            if (full) a.full_reload = false;

            for (auto it = a.waiting.begin(); it != a.waiting.end();) {
                if (a.loaded(it->part, it->symbol)) {
                    done.emplace_back(std::move(it->done), Result<void>{});
                    it = a.waiting.erase(it);
                } else {
                    ++it;
                }
            }

            a.syncing = !a.open;
            again = a.open && a.needs_load();  // watched while this load ran
        }
        for (const auto& symbol : fx.margin_calls) {
            log(LogLevel::Warn, "account: MARGIN CALL for " + symbol);
        }
        for (auto& [d, r] : done) {
            if (d) d(std::move(r));
        }
        if (fx.refresh_balances) schedule_balance_refresh(gen);
        if (again) start_account_sync(a, gen);
        co_return;
    }
}

void Client::Impl::schedule_balance_refresh(std::uint64_t gen) {
    auto& a = futures_account;  // spot events carry the free amount: nothing to re-read
    {
        std::lock_guard lock(a.mu);
        if (!a.active || a.gen != gen || !a.balance_on || a.balance_refresh_pending) return;
        a.balance_refresh_pending = true;
    }
    spawn(refresh_balances(gen));
}

asio::awaitable<void> Client::Impl::refresh_balances(std::uint64_t gen) {
    auto& a = futures_account;
    // Wait a moment so a burst of events costs one request (weight 5).
    asio::steady_timer t(*io, std::chrono::milliseconds{1000});
    co_await t.async_wait(kAwait);
    auto balances = co_await rest_balances(Market::Futures);
    std::lock_guard lock(a.mu);
    if (!a.active || a.gen != gen) co_return;
    a.balance_refresh_pending = false;
    if (!balances) {
        log(LogLevel::Debug, "account: balance refresh failed: " + balances.error().message);
        co_return;
    }
    if (a.syncing || !a.balance_on) co_return;  // a reload brings them too
    for (auto& b : *balances) a.balances[b.asset] = b;
}

void Client::Impl::apply_user_event(AccountState& a, const detail::UserEvent& ev,
                                    std::int64_t stale_before, EventEffects& out) {
    using Kind = detail::UserEvent::Kind;
    if (ev.time < stale_before) return;
    const bool is_futures = a.market == Market::Futures;
    switch (ev.kind) {
        case Kind::Order: {
            Order o = *ev.order;
            const auto key = std::make_pair(o.is_algo, o.id);
            // Futures: margin is locked or released, available balance changes.
            if (is_futures && !o.is_algo && o.status != OrderStatus::PartiallyFilled) {
                out.refresh_balances = true;
            }
            if (!a.scope(o.is_algo).covers(o.symbol)) break;
            auto it = a.orders.find(key);
            if (!is_open(o.status)) {
                if (it != a.orders.end()) a.orders.erase(it);
                break;
            }
            if (it != a.orders.end()) {
                const Order& before = it->second.order;
                o.create_time = before.create_time;  // futures events don't carry it
                if (o.activation_price.is_zero()) o.activation_price = before.activation_price;
                if (o.callback_rate.is_zero()) o.callback_rate = before.callback_rate;
                it->second.order = std::move(o);
            } else {
                if (is_futures) o.create_time = o.update_time;
                a.orders[key] = CachedOrder{std::move(o), 0};
            }
            break;
        }
        case Kind::Account:
            if (a.balance_on) {
                for (const auto& b : ev.balances) {
                    if (!is_futures) {
                        a.balances[b.asset] = b;  // spot: absolute free and locked
                        continue;
                    }
                    auto& cur = a.balances[b.asset];
                    cur.asset = b.asset;
                    cur.total = b.total;  // available: re-read over REST (refresh_balances)
                    cur.locked = cur.total - cur.available;
                    out.refresh_balances = true;
                }
            }
            if (a.positions_on) {
                for (Position p : ev.positions) {
                    auto& sy = a.symbols[p.symbol];
                    sy.symbol = p.symbol;
                    sy.margin_type = p.margin_type;
                    complete_position(p);
                    put_position(a.positions, p);
                }
            }
            break;
        case Kind::Config:
            if (!ev.symbol.empty()) {
                auto& sy = a.symbols[ev.symbol];
                sy.symbol = ev.symbol;
                sy.leverage = ev.leverage;
                if (auto it = a.positions.find(ev.symbol); it != a.positions.end()) {
                    for (auto& [side, p] : it->second) p.leverage = ev.leverage;
                }
            }
            if (ev.multi_assets) a.settings.multi_assets_margin = *ev.multi_assets;
            break;
        case Kind::MarginCall:
            for (const auto& p : ev.positions) out.margin_calls.push_back(p.symbol);
            break;
        case Kind::ListenKeyExpired:
        case Kind::StreamTerminated:
        case Kind::Other: break;
    }
}

// =============================================================================================
// 11. Trading (futures and spot)
// =============================================================================================
//
// WebSocket API while its session is up, else REST. WS Network error: not sent -> REST.
// Timeout / 5xx / -1007: sent, outcome unknown. Placing: look the order up by client order id
// and resend (REST, once) only if Binance does not have it. Cancel: resend. Amend: look it up.
// Conditional types go to the Algo Service (algoOrder.place / POST /fapi/v1/algoOrder).

namespace {

bool is_algo_type(OrderType t) {
    return t == OrderType::StopMarket || t == OrderType::StopLimit ||
           t == OrderType::TakeProfitMarket || t == OrderType::TakeProfitLimit ||
           t == OrderType::TrailingStopMarket;
}

bool is_limit_type(OrderType t) {
    return t == OrderType::Limit || t == OrderType::StopLimit || t == OrderType::TakeProfitLimit;
}

/// Executes as a market order: MARKET_LOT_SIZE applies.
bool uses_market_lot(OrderType t) {
    return t == OrderType::Market || t == OrderType::StopMarket ||
           t == OrderType::TakeProfitMarket || t == OrderType::TrailingStopMarket;
}

std::string_view futures_type_string(OrderType t) {
    switch (t) {
        case OrderType::Market: return "MARKET";
        case OrderType::Limit: return "LIMIT";
        case OrderType::LimitMaker: return "LIMIT";  // rejected by validation
        case OrderType::StopMarket: return "STOP_MARKET";
        case OrderType::StopLimit: return "STOP";
        case OrderType::TakeProfitMarket: return "TAKE_PROFIT_MARKET";
        case OrderType::TakeProfitLimit: return "TAKE_PROFIT";
        case OrderType::TrailingStopMarket: return "TRAILING_STOP_MARKET";
    }
    return "";
}

/// Binance: ^[\.A-Z\:/a-z0-9_-]{1,36}$
bool valid_client_order_id(std::string_view s) {
    if (s.empty() || s.size() > 36) return false;
    for (char c : s) {
        if (!std::isalnum(static_cast<unsigned char>(c)) && c != '.' && c != ':' && c != '/' &&
            c != '_' && c != '-') {
            return false;
        }
    }
    return true;
}

std::string url_encode(std::string_view s) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    for (char ch : s) {
        const auto c = static_cast<unsigned char>(ch);
        if (std::isalnum(c) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += ch;
        } else {
            out += '%';
            out += kHex[c >> 4];
            out += kHex[c & 0xF];
        }
    }
    return out;
}

std::string query_string(const Params& params) {
    std::string q;
    for (const auto& [k, v] : params) {
        if (!q.empty()) q += '&';
        q += k + "=" + url_encode(v);
    }
    return q;
}

std::string json_string(std::string_view s) {
    std::string out = "\"";
    for (char ch : s) {
        if (ch == '"' || ch == '\\') {
            out += '\\';
            out += ch;
        } else if (static_cast<unsigned char>(ch) < 0x20) {
            char buf[8];
            std::snprintf(buf, sizeof buf, "\\u%04x", ch);
            out += buf;
        } else {
            out += ch;
        }
    }
    return out + "\"";
}

/// {"id":..,"status":200,"result":{...}} -> the result JSON. Other status -> Error;
/// 5xx means the outcome is unknown (Timeout).
Result<std::string> ws_api_result(const std::string& body) {
    simdjson::padded_string ps(body);
    ondemand::parser parser;
    ondemand::document doc;
    if (parser.iterate(ps).get(doc)) return parse_error("WebSocket API response");
    std::int64_t status = 0;
    std::ignore = doc["status"].get_int64().get(status);
    if (status == 200) {
        std::string_view raw;
        if (doc["result"].raw_json().get(raw)) return parse_error("WebSocket API response");
        return std::string(raw);
    }
    std::int64_t code = 0;
    std::string_view msg = "no error message";
    ondemand::object err;
    if (!doc["error"].get_object().get(err)) {
        std::ignore = err["code"].get_int64().get(code);
        std::ignore = err["msg"].get_string().get(msg);
    }
    return Error{status >= 500 ? ErrorCode::Timeout : ErrorCode::Exchange, std::string(msg),
                 static_cast<int>(code)};
}

/// -1007: "Timeout waiting for response from backend server. Send status unknown".
Error unknown_outcome_as_timeout(Error e) {
    if (e.exchange_code == -1007) e.code = ErrorCode::Timeout;
    return e;
}

}  // namespace

namespace detail {

std::optional<Error> check_order_numbers(const OrderRequest& r, const Instrument& ins,
                                         bool check_notional) {
    auto bad = [&](const std::string& m) {
        return Error{ErrorCode::InvalidArgument, ins.symbol + ": " + m};
    };
    const OrderType t = r.type;
    auto check_price = [&](const char* name, Decimal p) -> std::optional<Error> {
        if (!p.is_positive()) return bad(std::string(name) + " must be positive");
        if (!ins.tick_size.is_zero() && p.round_to(ins.tick_size, Rounding::Down) != p) {
            return bad(std::string(name) + " " + p.to_string() + " is not on the tick size " +
                       ins.tick_size.to_string() + "; use round_price()");
        }
        if (!ins.min_price.is_zero() && p < ins.min_price) {
            return bad(std::string(name) + " " + p.to_string() + " is below the minimum " +
                       ins.min_price.to_string());
        }
        if (!ins.max_price.is_zero() && p > ins.max_price) {
            return bad(std::string(name) + " " + p.to_string() + " is above the maximum " +
                       ins.max_price.to_string());
        }
        return std::nullopt;
    };
    if (r.price) {
        if (auto e = check_price("price", *r.price)) return e;
    }
    if (r.stop_price) {
        if (auto e = check_price("stop_price", *r.stop_price)) return e;
    }
    if (r.activation_price) {
        if (auto e = check_price("activation_price", *r.activation_price)) return e;
    }

    if (r.qty) {
        const Decimal q = *r.qty;
        const bool mkt = uses_market_lot(t);
        const Decimal step = mkt ? ins.market_step_size : ins.step_size;
        const Decimal min = mkt ? ins.market_min_qty : ins.min_qty;
        const Decimal max = mkt ? ins.market_max_qty : ins.max_qty;
        if (!step.is_zero() && q.round_to(step, Rounding::Down) != q) {
            return bad("qty " + q.to_string() + " is not on the step size " + step.to_string() +
                       (mkt ? "; use round_qty(..., market_order = true)" : "; use round_qty()"));
        }
        if (q < min)
            return bad("qty " + q.to_string() + " is below the minimum " + min.to_string());
        if (!max.is_zero() && q > max) {
            return bad("qty " + q.to_string() + " is above the maximum " + max.to_string());
        }
        // Market types: Binance checks the notional with its own price.
        if (r.price && check_notional && !ins.min_notional.is_zero()) {
            auto notional = Decimal::mul(*r.price, q);
            if (notional && *notional < ins.min_notional) {
                return bad("price x qty = " + notional->to_string() + " is below the minimum " +
                           "notional " + ins.min_notional.to_string());
            }
        }
    }
    if (r.quote_qty && check_notional && !ins.min_notional.is_zero() &&
        *r.quote_qty < ins.min_notional) {
        return bad("quote_qty " + r.quote_qty->to_string() + " is below the minimum notional " +
                   ins.min_notional.to_string());
    }
    return std::nullopt;
}

/// hedge_mode: nullopt if unknown.
std::optional<Error> validate_futures_order(const OrderRequest& r, const Instrument& ins,
                                            std::optional<bool> hedge_mode) {
    auto bad = [&](const std::string& m) {
        return Error{ErrorCode::InvalidArgument, ins.symbol + ": " + m};
    };
    const OrderType t = r.type;
    if (!ins.trading) return bad("symbol is not trading");
    if (t == OrderType::LimitMaker) {
        return bad("LimitMaker is spot only; on futures use Limit with TimeInForce::GTX");
    }
    if (r.quote_qty) return bad("quote_qty is spot only");

    const bool limit = is_limit_type(t);
    const bool stop = t == OrderType::StopMarket || t == OrderType::StopLimit ||
                      t == OrderType::TakeProfitMarket || t == OrderType::TakeProfitLimit;
    const bool trailing = t == OrderType::TrailingStopMarket;
    if (limit && !r.price) return bad("a limit order needs a price");
    if (!limit && r.price) return bad("price is only for Limit, StopLimit and TakeProfitLimit");
    if (stop && !r.stop_price) return bad("needs stop_price (the trigger price)");
    if (!stop && r.stop_price) return bad("stop_price is only for stop and take-profit orders");
    if (trailing) {
        if (!r.callback_rate) return bad("a trailing stop needs callback_rate");
        if (*r.callback_rate < Decimal::from_raw(Decimal::kOne / 10) ||
            *r.callback_rate > Decimal::from_int(20)) {
            return bad("callback_rate must be between 0.1 and 20 (percent)");
        }
    } else if (r.callback_rate || r.activation_price) {
        return bad("callback_rate and activation_price are only for TrailingStopMarket");
    }
    if (r.time_in_force && !limit) return bad("time_in_force is only for limit types");
    if ((r.time_in_force == TimeInForce::GTD) != r.good_till.has_value()) {
        return bad("TimeInForce::GTD needs good_till, and good_till needs GTD");
    }

    if (r.close_position) {
        if (t != OrderType::StopMarket && t != OrderType::TakeProfitMarket) {
            return bad("close_position is only for StopMarket and TakeProfitMarket");
        }
        if (r.qty) return bad("close_position closes the whole position: leave qty unset");
        if (r.reduce_only) return bad("close_position and reduce_only cannot be combined");
    } else if (!r.qty || !r.qty->is_positive()) {
        return bad("qty must be set and positive");
    }

    if (hedge_mode) {
        const bool has_side = r.position_side && *r.position_side != PositionSide::Both;
        if (*hedge_mode && !has_side) return bad("hedge mode: set position_side to Long or Short");
        if (*hedge_mode && r.reduce_only) {
            return bad(
                "hedge mode has no reduce_only: close with the opposite side on the same "
                "position_side");
        }
        if (!*hedge_mode && has_side) return bad("one-way mode: position_side must be Both");
    }

    return check_order_numbers(r, ins, !r.reduce_only);
}

/// Parameters of a validated futures order (r.symbol already upper-case).
Params futures_order_params(const OrderRequest& r, const std::string& client_order_id) {
    Params p;
    const bool algo = is_algo_type(r.type);
    if (algo) p.emplace_back("algoType", "CONDITIONAL");
    p.emplace_back("symbol", r.symbol);
    p.emplace_back("side", std::string(to_string(r.side)));
    p.emplace_back("type", std::string(futures_type_string(r.type)));
    if (r.position_side) p.emplace_back("positionSide", std::string(to_string(*r.position_side)));
    if (r.qty) p.emplace_back("quantity", r.qty->to_string());
    if (r.price) p.emplace_back("price", r.price->to_string());
    if (r.stop_price) p.emplace_back("triggerPrice", r.stop_price->to_string());
    if (is_limit_type(r.type)) {
        p.emplace_back("timeInForce",
                       std::string(to_string(r.time_in_force.value_or(TimeInForce::GTC))));
    }
    if (r.good_till) p.emplace_back("goodTillDate", std::to_string(to_epoch_ms(*r.good_till)));
    if (r.reduce_only) p.emplace_back("reduceOnly", "true");
    if (algo) {
        if (r.close_position) p.emplace_back("closePosition", "true");
        p.emplace_back("workingType",
                       r.working_type == WorkingType::MarkPrice ? "MARK_PRICE" : "CONTRACT_PRICE");
        if (r.price_protect) p.emplace_back("priceProtect", "true");
        if (r.activation_price) p.emplace_back("activatePrice", r.activation_price->to_string());
        if (r.callback_rate) p.emplace_back("callbackRate", r.callback_rate->to_string());
        p.emplace_back("clientAlgoId", client_order_id);
    } else {
        p.emplace_back("newClientOrderId", client_order_id);
    }
    return p;
}

std::optional<Error> validate_spot_order(const OrderRequest& r, const Instrument& ins) {
    auto bad = [&](const std::string& m) {
        return Error{ErrorCode::InvalidArgument, ins.symbol + ": " + m};
    };
    const OrderType t = r.type;
    if (!ins.trading) return bad("symbol is not trading");
    if (t == OrderType::TrailingStopMarket) return bad("TrailingStopMarket is futures only");
    if (r.position_side || r.reduce_only || r.close_position || r.price_protect ||
        r.working_type != WorkingType::ContractPrice || r.activation_price || r.callback_rate) {
        return bad(
            "position_side, reduce_only, close_position, price_protect, working_type, "
            "activation_price and callback_rate are futures only");
    }
    if (r.time_in_force == TimeInForce::GTX || r.time_in_force == TimeInForce::GTD || r.good_till) {
        return bad("GTX and GTD are futures only; on spot use OrderType::LimitMaker (post-only)");
    }

    const bool limit = is_limit_type(t);  // Limit, StopLimit, TakeProfitLimit
    const bool priced = limit || t == OrderType::LimitMaker;
    const bool stop = t == OrderType::StopMarket || t == OrderType::StopLimit ||
                      t == OrderType::TakeProfitMarket || t == OrderType::TakeProfitLimit;
    if (priced && !r.price) return bad("a limit order needs a price");
    if (!priced && r.price) {
        return bad("price is only for Limit, LimitMaker, StopLimit and TakeProfitLimit");
    }
    if (stop && !r.stop_price) return bad("needs stop_price (the trigger price)");
    if (!stop && r.stop_price) return bad("stop_price is only for stop and take-profit orders");
    if (r.time_in_force && !limit) {
        return bad("time_in_force is only for Limit, StopLimit and TakeProfitLimit");
    }
    if (r.quote_qty) {
        if (t != OrderType::Market) return bad("quote_qty is only for Market orders");
        if (r.qty) return bad("set qty or quote_qty, not both");
        if (!r.quote_qty->is_positive()) return bad("quote_qty must be positive");
    } else if (!r.qty || !r.qty->is_positive()) {
        return bad("qty (or quote_qty for a Market order) must be set and positive");
    }
    return check_order_numbers(r, ins, true);
}

std::string_view spot_type_string(OrderType t) {
    switch (t) {
        case OrderType::Market: return "MARKET";
        case OrderType::Limit: return "LIMIT";
        case OrderType::LimitMaker: return "LIMIT_MAKER";
        case OrderType::StopMarket: return "STOP_LOSS";
        case OrderType::StopLimit: return "STOP_LOSS_LIMIT";
        case OrderType::TakeProfitMarket: return "TAKE_PROFIT";
        case OrderType::TakeProfitLimit: return "TAKE_PROFIT_LIMIT";
        case OrderType::TrailingStopMarket: return "";  // rejected by validation
    }
    return "";
}

/// Parameters of a validated spot order (r.symbol already upper-case). The answer is the
/// order's state (RESULT), the same for every type.
Params spot_order_params(const OrderRequest& r, const std::string& client_order_id) {
    Params p;
    p.emplace_back("symbol", r.symbol);
    p.emplace_back("side", std::string(to_string(r.side)));
    p.emplace_back("type", std::string(spot_type_string(r.type)));
    if (is_limit_type(r.type)) {
        p.emplace_back("timeInForce",
                       std::string(to_string(r.time_in_force.value_or(TimeInForce::GTC))));
    }
    if (r.qty) p.emplace_back("quantity", r.qty->to_string());
    if (r.quote_qty) p.emplace_back("quoteOrderQty", r.quote_qty->to_string());
    if (r.price) p.emplace_back("price", r.price->to_string());
    if (r.stop_price) p.emplace_back("stopPrice", r.stop_price->to_string());
    p.emplace_back("newClientOrderId", client_order_id);
    p.emplace_back("newOrderRespType", "RESULT");
    return p;
}

/// WebSocket API params object, signed: HMAC-SHA256 over all params sorted by name.
std::string ws_signed_params(Params params, std::string_view api_key, std::string_view secret,
                             std::int64_t timestamp, std::int64_t recv_window) {
    params.emplace_back("apiKey", std::string(api_key));
    params.emplace_back("recvWindow", std::to_string(recv_window));
    params.emplace_back("timestamp", std::to_string(timestamp));
    std::sort(params.begin(), params.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });
    std::string payload;
    for (const auto& [k, v] : params) {
        if (!payload.empty()) payload += '&';
        payload += k + "=" + v;
    }
    const std::string signature = hmac_sha256_hex(secret, payload);
    std::string json = "{";
    for (const auto& [k, v] : params) {
        const bool number = k == "timestamp" || k == "recvWindow" || k == "orderId" ||
                            k == "algoId" || k == "goodTillDate";
        json += "\"" + k + "\":" + (number ? v : json_string(v)) + ",";
    }
    json += "\"signature\":\"" + signature + "\"}";
    return json;
}

}  // namespace detail

std::shared_ptr<detail::WsApi> Client::Impl::trade_api(Market market) {
    std::lock_guard lock(trade_mu);
    auto& api = market == Market::Futures ? ws_api : spot_ws_api;
    if (!api) {
        const auto& ep = state(market).ep;
        const std::string name =
            market == Market::Futures ? "trading session" : "spot trading session";
        limiter.set_limits(ep.ws_api_host, default_rate_limits(market));
        api = detail::WsApi::create(io->get_executor(), tls,
                                    {ep.ws_api_host, "443", ep.ws_api_target}, detail::WsOptions{},
                                    limiter, ep.ws_api_host, config.request_timeout,
                                    [this, name](detail::WsState st, const std::string& reason) {
                                        if (st == detail::WsState::Open) {
                                            log(LogLevel::Info, name + " connected");
                                        } else if (st == detail::WsState::Connecting) {
                                            log(LogLevel::Warn, name + " disconnected: " + reason);
                                        }
                                    });
        api->start();
    }
    return api;
}

std::string Client::Impl::new_client_order_id() {
    thread_local std::mt19937_64 rng{std::random_device{}()};
    auto b36 = [](std::uint64_t v) {
        std::string s;
        do {
            s += "0123456789abcdefghijklmnopqrstuvwxyz"[v % 36];
            v /= 36;
        } while (v);
        return std::string(s.rbegin(), s.rend());
    };
    return "bcpp-" + b36(static_cast<std::uint64_t>(now_ms())) + "-" + b36(rng() % 2176782336ULL) +
           b36(order_seq++);
}

asio::awaitable<Result<void>> Client::Impl::ensure_instruments(Market market) {
    auto& ms = state(market);
    {
        std::lock_guard lock(ms.mu);
        if (!ms.instruments.empty()) co_return Result<void>{};
    }
    co_return co_await load_instruments(ms);
}

asio::awaitable<Result<std::string>> Client::Impl::send_trade_rest(const TradeCall& c) {
    auto r = co_await signed_request(c.verb, c.path, query_string(c.params), c.weight, c.is_order,
                                     c.market);
    if (!r) co_return unknown_outcome_as_timeout(r.error());
    co_return r;
}

asio::awaitable<Result<std::string>> Client::Impl::send_trade(const TradeCall& c) {
    if (auto e = missing_keys()) co_return *e;
    auto api = c.ws_method.empty() ? nullptr : trade_api(c.market);
    if (api && api->is_open()) {
        if (!time_synced) (void)co_await sync_time();
        detail::WsApiCall ws_call{
            c.ws_method,
            detail::ws_signed_params(c.params, config.credentials.api_key,
                                     config.credentials.hmac_secret, server_now(),
                                     config.recv_window.count()),
            1, c.is_order};
        auto r = co_await api->co_request(std::move(ws_call));
        if (r) {
            log(LogLevel::Debug, c.ws_method + " over the WebSocket API");
            auto result = ws_api_result(r->body);
            if (result || result.error().exchange_code != -1021) {
                co_return result ? std::move(result) : unknown_outcome_as_timeout(result.error());
            }
            // -1021: rejected for our clock; REST re-reads the server time and retries.
            (void)co_await sync_time();
        } else if (r.error().code != ErrorCode::Network) {
            co_return r.error();  // Timeout (outcome unknown), RateLimited, Cancelled
        }
        // Network: not sent.
    }
    co_return co_await send_trade_rest(c);
}

asio::awaitable<Result<Order>> Client::Impl::lookup_order(const OrderRef& ref, Market market) {
    const bool spot_market = market == Market::Spot;
    const bool algo = !spot_market && ref.is_algo;
    Params p;
    if (algo) {
        if (ref.order_id) {
            p.emplace_back("algoId", std::to_string(*ref.order_id));
        } else {
            p.emplace_back("clientAlgoId", ref.client_order_id.value_or(""));
        }
    } else {
        p.emplace_back("symbol", ref.symbol);
        if (ref.order_id) {
            p.emplace_back("orderId", std::to_string(*ref.order_id));
        } else {
            p.emplace_back("origClientOrderId", ref.client_order_id.value_or(""));
        }
    }
    const char* path = spot_market ? "/api/v3/order"
                       : algo      ? "/fapi/v1/algoOrder"
                                   : "/fapi/v1/order";
    auto body = co_await signed_request(detail::http::verb::get, path, query_string(p),
                                        spot_market ? 4 : 1, false, market);
    if (!body) {
        if (body.error().exchange_code == -2013) {  // "Order does not exist."
            co_return Error{ErrorCode::NotFound, body.error().message, -2013};
        }
        co_return body.error();
    }
    if (spot_market) co_return detail::parse_spot_order(*body);
    co_return algo ? detail::parse_algo_order(*body) : detail::parse_order(*body);
}

asio::awaitable<Result<Order>> Client::Impl::find_after_timeout(const OrderRef& ref,
                                                                Market market) {
    Result<Order> r = Error{ErrorCode::Timeout, "not looked up"};
    for (int attempt = 0; attempt < 3; ++attempt) {
        // Give an order that is still on its way time to arrive.
        asio::steady_timer t(*io, std::chrono::milliseconds{attempt == 0 ? 500 : 1500});
        co_await t.async_wait(kAwait);
        r = co_await lookup_order(ref, market);
        if (r) co_return r;
    }
    co_return r;
}

asio::awaitable<Result<Order>> Client::Impl::create_order(OrderRequest req, bool rest_only,
                                                          Market market) {
    const bool spot_market = market == Market::Spot;
    if (auto e = missing_keys()) co_return *e;
    auto sym = normalize_symbol(req.symbol);
    if (!sym) {
        co_return invalid_symbol(req.symbol);
    }
    req.symbol = *sym;
    if (req.client_order_id && !valid_client_order_id(*req.client_order_id)) {
        co_return Error{ErrorCode::InvalidArgument,
                        "client_order_id: 1-36 characters of A-Z a-z 0-9 . : / _ -"};
    }
    if (auto loaded = co_await ensure_instruments(market); !loaded) co_return loaded.error();
    Instrument ins;
    {
        auto& ms = state(market);
        std::lock_guard lock(ms.mu);
        auto it = ms.instruments.find(req.symbol);
        if (it == ms.instruments.end()) {
            co_return Error{ErrorCode::InvalidArgument, "unknown symbol '" + req.symbol + "'"};
        }
        ins = it->second;
    }
    if (spot_market) {
        if (auto e = detail::validate_spot_order(req, ins)) co_return *e;
    } else {
        std::optional<bool> hedge;
        {
            std::lock_guard lock(futures_account.mu);
            if (futures_account.active && futures_account.settings_loaded)
                hedge = futures_account.settings.hedge_mode;
        }
        if (auto e = detail::validate_futures_order(req, ins, hedge)) co_return *e;
    }

    const std::string client_id = req.client_order_id.value_or(new_client_order_id());
    const bool algo = !spot_market && is_algo_type(req.type);
    TradeCall call;
    if (spot_market) {
        call = TradeCall{"order.place",
                         detail::http::verb::post,
                         "/api/v3/order",
                         detail::spot_order_params(req, client_id),
                         1,
                         true,
                         Market::Spot};
    } else {
        call = TradeCall{algo ? "algoOrder.place" : "order.place",
                         detail::http::verb::post,
                         algo ? "/fapi/v1/algoOrder" : "/fapi/v1/order",
                         detail::futures_order_params(req, client_id),
                         1,
                         true};
    }
    if (rest_only) call.ws_method.clear();
    const OrderRef ref{req.symbol, std::nullopt, client_id, algo};
    auto parse = [algo, spot_market](const std::string& json) {
        if (spot_market) return detail::parse_spot_order(json);
        return algo ? detail::parse_algo_order(json) : detail::parse_order(json);
    };

    auto r = co_await send_trade(call);
    if (r) co_return parse(*r);
    if (r.error().code != ErrorCode::Timeout) co_return r.error();

    // Sent, but no answer: never send again before knowing Binance does not have it.
    log(LogLevel::Warn, "order " + client_id + ": no answer (" + r.error().message +
                            "); looking it up before deciding");
    auto unknown = [&](const Error& why) {
        return Error{ErrorCode::Timeout,
                     "order " + client_id + ": outcome unknown (" + why.message +
                         "); check it with get_order_rest_api before placing it again"};
    };
    auto found = co_await find_after_timeout(ref, market);
    if (found) co_return found;
    if (found.error().code != ErrorCode::NotFound) co_return unknown(found.error());

    r = co_await send_trade_rest(call);  // Binance does not have it: once more, over REST
    if (r) co_return parse(*r);
    // Duplicate client order id (it arrived after all): futures -4116, spot -2010 "Duplicate ...".
    const bool duplicate = r.error().exchange_code == -4116 ||
                           (r.error().exchange_code == -2010 &&
                            r.error().message.find("Duplicate") != std::string::npos);
    if (r.error().code == ErrorCode::Timeout || duplicate) {
        found = co_await find_after_timeout(ref, market);
        if (found) co_return found;
        co_return unknown(r.error());
    }
    co_return r.error();
}

asio::awaitable<Result<Order>> Client::Impl::cancel_order(OrderRef ref, bool rest_only,
                                                          Market market) {
    if (auto e = missing_keys()) co_return *e;
    if (market == Market::Spot && ref.is_algo) {
        co_return Error{ErrorCode::InvalidArgument, "spot has no algo orders"};
    }
    if (!ref.order_id && !ref.client_order_id) {
        co_return Error{ErrorCode::InvalidArgument, "set order_id or client_order_id"};
    }
    if (!ref.is_algo || !ref.symbol.empty()) {
        auto sym = normalize_symbol(ref.symbol);
        if (!sym) co_return invalid_symbol(ref.symbol);
        ref.symbol = *sym;
    }
    TradeCall call;
    call.verb = detail::http::verb::delete_;
    if (ref.is_algo) {
        call.ws_method = "algoOrder.cancel";
        call.path = "/fapi/v1/algoOrder";
        if (ref.order_id) {
            call.params.emplace_back("algoId", std::to_string(*ref.order_id));
        } else {
            call.params.emplace_back("clientAlgoId", *ref.client_order_id);
        }
    } else {
        call.ws_method = "order.cancel";
        call.path = market == Market::Spot ? "/api/v3/order" : "/fapi/v1/order";
        call.market = market;
        call.params.emplace_back("symbol", ref.symbol);
        if (ref.order_id) {
            call.params.emplace_back("orderId", std::to_string(*ref.order_id));
        } else {
            call.params.emplace_back("origClientOrderId", *ref.client_order_id);
        }
    }

    // The algo cancel answer is just ids: keep what we know of the order to report it.
    std::optional<Order> known;
    if (ref.is_algo) {
        std::lock_guard lock(futures_account.mu);
        for (const auto& [key, cached] : futures_account.orders) {
            const Order& o = cached.order;
            if (o.is_algo && ((ref.order_id && o.id == *ref.order_id) ||
                              (ref.client_order_id && o.client_order_id == *ref.client_order_id))) {
                known = o;
            }
        }
    }

    if (rest_only) call.ws_method.clear();
    auto r = co_await send_trade(call);
    if (!r && r.error().code == ErrorCode::Timeout) r = co_await send_trade_rest(call);
    if (!r) {
        // -2011 "Unknown order sent": maybe it just ended. Report how, if it did.
        const int code = r.error().exchange_code;
        if (code == -2011 || code == -2013) {
            auto o = co_await lookup_order(ref, market);
            if (o && !is_open(o->status)) co_return o;
        }
        co_return r.error();
    }
    if (market == Market::Spot) co_return detail::parse_spot_order(*r);
    if (!ref.is_algo) co_return detail::parse_order(*r);

    // {"algoId":..,"clientAlgoId":"..","code":"200","msg":"success"}: the cancel is confirmed,
    // so the result is Canceled whatever a lookup says. Looking it up right after the cancel
    // gives "not found" or the stale NEW for a moment; it only fills in the details.
    Result<Order> details = Error{ErrorCode::NotFound, "not looked up"};
    if (known) {
        details = std::move(*known);
    } else {
        for (int attempt = 0; attempt < 2 && !details; ++attempt) {
            asio::steady_timer t(*io, std::chrono::milliseconds{300 * (attempt + 1)});
            co_await t.async_wait(kAwait);
            details = co_await lookup_order(ref);
        }
    }
    if (!details) {
        details = parse_root_object(*r, "algo cancel", [&](ondemand::object o) -> Result<Order> {
            Order ord;
            ord.is_algo = true;
            ord.symbol = ref.symbol;
            if (!get_field(o, "algoId", ord.id) ||
                !get_str(o, "clientAlgoId", ord.client_order_id)) {
                return parse_error("algo cancel");
            }
            return ord;
        });
        if (!details) co_return details;
    }
    details->status = OrderStatus::Canceled;
    details->update_time = from_epoch_ms(server_now());
    co_return details;
}

asio::awaitable<Result<Order>> Client::Impl::amend_order(AmendRequest req, bool rest_only) {
    if (auto e = missing_keys()) co_return *e;
    if (!req.order_id && !req.client_order_id) {
        co_return Error{ErrorCode::InvalidArgument, "set order_id or client_order_id"};
    }
    auto sym = normalize_symbol(req.symbol);
    if (!sym) co_return invalid_symbol(req.symbol);
    req.symbol = *sym;
    if (auto loaded = co_await ensure_instruments(); !loaded) co_return loaded.error();
    Instrument ins;
    {
        std::lock_guard lock(futures.mu);
        auto it = futures.instruments.find(req.symbol);
        if (it == futures.instruments.end()) {
            co_return Error{ErrorCode::InvalidArgument, "unknown symbol '" + req.symbol + "'"};
        }
        ins = it->second;
    }
    OrderRequest as_limit;  // the same checks as a new limit order
    as_limit.symbol = req.symbol;
    as_limit.side = req.side;
    as_limit.type = OrderType::Limit;
    as_limit.qty = req.qty;
    as_limit.price = req.price;
    if (auto e = detail::validate_futures_order(as_limit, ins, std::nullopt)) co_return *e;

    TradeCall call{"order.modify", detail::http::verb::put, "/fapi/v1/order", {}, 1, true};
    call.params.emplace_back("symbol", req.symbol);
    if (req.order_id) {
        call.params.emplace_back("orderId", std::to_string(*req.order_id));
    } else {
        call.params.emplace_back("origClientOrderId", *req.client_order_id);
    }
    call.params.emplace_back("side", std::string(to_string(req.side)));
    call.params.emplace_back("quantity", req.qty.to_string());
    call.params.emplace_back("price", req.price.to_string());
    if (rest_only) call.ws_method.clear();

    auto r = co_await send_trade(call);
    if (r) co_return detail::parse_order(*r);
    if (r.error().code != ErrorCode::Timeout) co_return r.error();
    // Unknown whether it applied: report the order as it is now.
    const OrderRef ref{req.symbol, req.order_id, req.client_order_id, false};
    auto o = co_await find_after_timeout(ref);
    if (o) co_return o;
    co_return Error{ErrorCode::Timeout, "amend: outcome unknown (" + r.error().message + ")"};
}

asio::awaitable<Result<void>> Client::Impl::cancel_all_orders(std::string symbol, Market market) {
    auto sym = normalize_symbol(symbol);
    if (!sym) co_return invalid_symbol(symbol);
    if (market == Market::Spot) {
        const TradeCall call{"",
                             detail::http::verb::delete_,
                             "/api/v3/openOrders",
                             {{"symbol", *sym}},
                             1,
                             false,
                             Market::Spot};
        auto r = co_await send_trade_rest(call);
        // -2011 "Unknown order sent.": there was nothing to cancel.
        if (!r && r.error().exchange_code != -2011) co_return r.error();
        co_return Result<void>{};
    }
    for (const char* path : {"/fapi/v1/allOpenOrders", "/fapi/v1/algoOpenOrders"}) {
        const TradeCall call{"", detail::http::verb::delete_, path, {{"symbol", *sym}}, 1, false};
        auto r = co_await send_trade_rest(call);
        if (!r) co_return r.error();
    }
    co_return Result<void>{};
}

asio::awaitable<Result<Order>> Client::Impl::fetch_order(OrderRef ref, Market market) {
    if (auto e = missing_keys()) co_return *e;
    if (market == Market::Spot && ref.is_algo) {
        co_return Error{ErrorCode::InvalidArgument, "spot has no algo orders"};
    }
    if (!ref.order_id && !ref.client_order_id) {
        co_return Error{ErrorCode::InvalidArgument, "set order_id or client_order_id"};
    }
    if (!ref.symbol.empty()) {
        auto sym = normalize_symbol(ref.symbol);
        if (!sym) co_return invalid_symbol(ref.symbol);
        ref.symbol = *sym;
    } else if (!ref.is_algo) {
        co_return Error{ErrorCode::InvalidArgument, "symbol is required"};
    }
    co_return co_await lookup_order(ref, market);
}

asio::awaitable<Result<void>> Client::Impl::set_position_mode(bool hedge) {
    auto r =
        co_await signed_request(detail::http::verb::post, "/fapi/v1/positionSide/dual",
                                std::string("dualSidePosition=") + (hedge ? "true" : "false"), 1);
    if (!r && r.error().exchange_code != -4059) co_return r.error();  // -4059: already so
    // No event tells the stream about this: reload the account.
    std::uint64_t gen = 0;
    {
        std::lock_guard lock(futures_account.mu);
        if (futures_account.active && futures_account.any_loaded()) {
            gen = futures_account.gen;
            futures_account.full_reload = true;
        }
    }
    if (gen) start_account_sync(futures_account, gen);
    co_return Result<void>{};
}

// --- Batch orders -----------------------------------------------------------------------------
//
// POST/DELETE /fapi/v1/batchOrders (REST only: the futures WebSocket API has no batch method).
// The answer is an array in request order; a failed item is {"code":..,"msg":..}.

namespace {

constexpr std::size_t kBatchPlace = 5;
constexpr std::size_t kBatchCancel = 10;

/// [{...},{...}] with every value as a JSON string.
std::string batch_json(const std::vector<Params>& orders) {
    std::string json = "[";
    for (std::size_t i = 0; i < orders.size(); ++i) {
        if (i) json += ',';
        json += '{';
        for (std::size_t j = 0; j < orders[i].size(); ++j) {
            if (j) json += ',';
            json += json_string(orders[i][j].first) + ":" + json_string(orders[i][j].second);
        }
        json += '}';
    }
    return json + "]";
}

}  // namespace

namespace detail {

/// Answer of a batch call: one order or Exchange error per item.
Result<std::vector<Result<Order>>> parse_batch_orders(std::string_view json) {
    simdjson::padded_string ps(json);
    ondemand::parser parser;
    ondemand::document doc;
    ondemand::array arr;
    if (parser.iterate(ps).get(doc) || doc.get_array().get(arr)) return parse_error("batchOrders");
    std::vector<Result<Order>> out;
    for (auto v : arr) {
        ondemand::object o;
        if (v.get_object().get(o)) return parse_error("batchOrders");
        std::int64_t code = 0;
        if (!o["code"].get_int64().get(code)) {
            std::string msg;
            (void)get_str(o, "msg", msg);
            out.emplace_back(Error{ErrorCode::Exchange, msg, static_cast<int>(code)});
            continue;
        }
        auto ord = parse_rest_order(o);
        if (!ord) return ord.error();
        out.emplace_back(std::move(*ord));
    }
    return out;
}

}  // namespace detail

asio::awaitable<std::vector<Result<Order>>> Client::Impl::create_orders(
    std::vector<OrderRequest> requests) {
    std::vector<Result<Order>> out(requests.size(), Error{ErrorCode::Internal, "not processed"});
    auto fail_all = [&](const Error& e) {
        for (auto& r : out) r = e;
    };
    if (auto e = missing_keys()) {
        fail_all(*e);
        co_return out;
    }
    if (auto loaded = co_await ensure_instruments(); !loaded) {
        fail_all(loaded.error());
        co_return out;
    }
    std::optional<bool> hedge;
    {
        std::lock_guard lock(futures_account.mu);
        if (futures_account.active && futures_account.settings_loaded)
            hedge = futures_account.settings.hedge_mode;
    }

    struct Pending {
        std::size_t index;
        OrderRequest request;
    };
    std::vector<Pending> regular;
    for (std::size_t i = 0; i < requests.size(); ++i) {
        OrderRequest& req = requests[i];
        if (is_algo_type(req.type)) {  // not batchable: alone, with all the usual checks
            out[i] = co_await create_order(std::move(req));
            continue;
        }
        auto sym = normalize_symbol(req.symbol);
        if (!sym) {
            out[i] = invalid_symbol(req.symbol);
            continue;
        }
        req.symbol = *sym;
        if (req.client_order_id && !valid_client_order_id(*req.client_order_id)) {
            out[i] = Error{ErrorCode::InvalidArgument,
                           "client_order_id: 1-36 characters of A-Z a-z 0-9 . : / _ -"};
            continue;
        }
        Instrument ins;
        {
            std::lock_guard lock(futures.mu);
            auto it = futures.instruments.find(req.symbol);
            if (it != futures.instruments.end()) ins = it->second;
        }
        if (ins.symbol.empty()) {
            out[i] = Error{ErrorCode::InvalidArgument, "unknown symbol '" + req.symbol + "'"};
            continue;
        }
        if (auto e = detail::validate_futures_order(req, ins, hedge)) {
            out[i] = *e;
            continue;
        }
        if (!req.client_order_id) req.client_order_id = new_client_order_id();
        regular.push_back({i, std::move(req)});
    }

    for (std::size_t from = 0; from < regular.size(); from += kBatchPlace) {
        const std::size_t to = std::min(from + kBatchPlace, regular.size());
        std::vector<Params> orders;
        for (std::size_t k = from; k < to; ++k) {
            orders.push_back(detail::futures_order_params(regular[k].request,
                                                          *regular[k].request.client_order_id));
        }
        const TradeCall call{"",
                             detail::http::verb::post,
                             "/fapi/v1/batchOrders",
                             {{"batchOrders", batch_json(orders)}},
                             5,
                             true};
        auto r = co_await send_trade_rest(call);
        Result<std::vector<Result<Order>>> items =
            r ? detail::parse_batch_orders(*r) : Result<std::vector<Result<Order>>>(r.error());
        if (items && items->size() == to - from) {
            for (std::size_t k = from; k < to; ++k) out[regular[k].index] = (*items)[k - from];
            continue;
        }
        const Error why =
            items ? Error{ErrorCode::Timeout, "batch answer has the wrong size"} : items.error();
        if (why.code != ErrorCode::Timeout && why.code != ErrorCode::Parse) {
            for (std::size_t k = from; k < to; ++k) out[regular[k].index] = why;  // not placed
            continue;
        }
        // Outcome unknown: settle each order by its client id before anything is resent.
        log(LogLevel::Warn, "batch of " + std::to_string(to - from) + " orders: outcome unknown (" +
                                why.message + "); checking each");
        for (std::size_t k = from; k < to; ++k) {
            const auto& req = regular[k].request;
            const OrderRef ref{req.symbol, std::nullopt, req.client_order_id, false};
            auto found = co_await find_after_timeout(ref);
            if (found) {
                out[regular[k].index] = std::move(found);
            } else if (found.error().code == ErrorCode::NotFound) {
                out[regular[k].index] = co_await create_order(req);  // not there: place it alone
            } else {
                out[regular[k].index] = Error{
                    ErrorCode::Timeout, "order " + *req.client_order_id +
                                            ": outcome unknown; check it with get_order_rest_api"};
            }
        }
    }
    co_return out;
}

asio::awaitable<std::vector<Result<Order>>> Client::Impl::cancel_orders(
    std::vector<OrderRef> refs) {
    std::vector<Result<Order>> out(refs.size(), Error{ErrorCode::Internal, "not processed"});
    if (auto e = missing_keys()) {
        for (auto& r : out) r = *e;
        co_return out;
    }
    // Regular orders grouped by symbol and by kind of id (one list type per request).
    std::map<std::pair<std::string, bool>, std::vector<std::size_t>> groups;
    for (std::size_t i = 0; i < refs.size(); ++i) {
        OrderRef& ref = refs[i];
        if (ref.is_algo) {
            out[i] = co_await cancel_order(ref);
            continue;
        }
        auto sym = normalize_symbol(ref.symbol);
        if (!sym || (!ref.order_id && !ref.client_order_id)) {
            out[i] = Error{ErrorCode::InvalidArgument,
                           "each order needs a symbol and an order_id or client_order_id"};
            continue;
        }
        ref.symbol = *sym;
        groups[{ref.symbol, ref.order_id.has_value()}].push_back(i);
    }

    for (const auto& [key, indexes] : groups) {
        const auto& [symbol, by_id] = key;
        for (std::size_t from = 0; from < indexes.size(); from += kBatchCancel) {
            const std::size_t to = std::min(from + kBatchCancel, indexes.size());
            std::string list = "[";
            for (std::size_t k = from; k < to; ++k) {
                if (k > from) list += ',';
                const auto& ref = refs[indexes[k]];
                list += by_id ? std::to_string(*ref.order_id) : json_string(*ref.client_order_id);
            }
            list += "]";
            const TradeCall call{
                "",
                detail::http::verb::delete_,
                "/fapi/v1/batchOrders",
                {{"symbol", symbol}, {by_id ? "orderIdList" : "origClientOrderIdList", list}},
                1,
                false};
            auto r = co_await send_trade_rest(call);
            if (!r && r.error().code == ErrorCode::Timeout) r = co_await send_trade_rest(call);
            Result<std::vector<Result<Order>>> items =
                r ? detail::parse_batch_orders(*r) : Result<std::vector<Result<Order>>>(r.error());
            for (std::size_t k = from; k < to; ++k) {
                const std::size_t i = indexes[k];
                if (!items || items->size() != to - from) {
                    out[i] = items ? Error{ErrorCode::Parse, "batch answer has the wrong size"}
                                   : items.error();
                    continue;
                }
                out[i] = (*items)[k - from];
                // -2011 "Unknown order sent": it may have just ended. Report how, if it did.
                if (!out[i] && out[i].error().exchange_code == -2011) {
                    auto o = co_await lookup_order(refs[i]);
                    if (o && !is_open(o->status)) out[i] = std::move(o);
                }
            }
        }
    }
    co_return out;
}

// --- Test hook ------------------------------------------------------------------------------

namespace detail {

/// Access for tests/unit/client_test.cpp (declared there identically).
struct TestHook {
    /// true: drop account events from now on, as if the connection were dead; false: stop.
    static void drop_account_events(Client& c, Market market, bool drop);
    /// Reconnect the account stream now (-> Stale, Resyncing, reload, Live).
    static void reconnect_account(Client& c, Market market);
    static void drop_next_book_update(Client& c, Market market, const std::string& symbol);
    /// Internal state of an order-book watch: "live", "resyncing", ...; "" if not watched.
    static std::string book_status(Client& c, Market market, const std::string& symbol);
};

std::string TestHook::book_status(Client& c, Market market, const std::string& symbol) {
    auto& ms = c.impl_->state(market);
    std::lock_guard lock(ms.mu);
    auto w = Client::Impl::find_book(ms, symbol);
    return w ? std::string(status_name(w->status)) : std::string();
}

void TestHook::drop_next_book_update(Client& c, Market market, const std::string& symbol) {
    auto& ms = c.impl_->state(market);
    std::lock_guard lock(ms.mu);
    if (auto w = Client::Impl::find_book(ms, symbol)) w->test_drop_next = true;
}

void TestHook::drop_account_events(Client& c, Market market, bool drop) {
    auto& a = c.impl_->account_of(market);
    std::lock_guard lock(a.mu);
    a.drop_events = drop;
}

void TestHook::reconnect_account(Client& c, Market market) {
    auto& a = c.impl_->account_of(market);
    std::shared_ptr<WsStream> ws;
    {
        std::lock_guard lock(a.mu);
        ws = a.ws;
    }
    if (ws) ws->reconnect();
}

}  // namespace detail

// =============================================================================================
// 12. Public API
// =============================================================================================

namespace {

/// Blocking on a library IO thread would wait for that very thread: a deadlock.
Error blocked_on_io_thread() {
    return Error{ErrorCode::InvalidArgument,
                 "a blocking call from a library thread would deadlock"};
}

/// Run an Impl coroutine on the IO threads and wait for its value; nullopt on an IO thread.
template <class T>
std::optional<T> run_and_wait(Client::Impl& impl, asio::awaitable<T> op) {
    if (t_in_io_thread) return std::nullopt;
    auto promise = std::make_shared<std::promise<T>>();
    auto future = promise->get_future();
    impl.spawn(
        [](asio::awaitable<T> task, std::shared_ptr<std::promise<T>> p) -> asio::awaitable<void> {
            p->set_value(co_await std::move(task));
        }(std::move(op), promise));
    return future.get();
}

template <class T>
Result<T> block(Client::Impl& impl, asio::awaitable<Result<T>> op) {
    auto r = run_and_wait(impl, std::move(op));
    if (!r) return blocked_on_io_thread();
    return std::move(*r);
}

Error amend_not_on_spot() {
    return Error{ErrorCode::InvalidArgument,
                 "amend_order is futures only: cancel the spot order and place a new one"};
}

Error batch_not_on_spot() {
    return Error{ErrorCode::InvalidArgument,
                 "batch orders are futures only (Binance has none for spot): use create_order / "
                 "cancel_order per order"};
}

std::optional<Error> unavailable(Market m) {
#ifndef BINANCE_ENABLE_SPOT
    if (m == Market::Spot) {
        return Error{ErrorCode::InvalidArgument,
                     "spot support was compiled out (BINANCE_ENABLE_SPOT=OFF)"};
    }
#endif
    (void)m;
    return std::nullopt;
}

}  // namespace

Client::Client(Config config) : impl_(std::make_unique<Impl>(std::move(config))) {}

Client::~Client() = default;

const Config& Client::config() const noexcept { return impl_->config; }

// --- Instruments ------------------------------------------------------------------------------

Result<void> Client::load_instruments(Market market) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->load_instruments(impl_->state(market)));
}

std::optional<Instrument> Client::get_instrument(Market market, std::string_view symbol) const {
    auto sym = normalize_symbol(symbol);
    if (!sym) return std::nullopt;
    auto& ms = impl_->state(market);
    std::lock_guard lock(ms.mu);
    auto it = ms.instruments.find(*sym);
    if (it == ms.instruments.end()) return std::nullopt;
    return it->second;
}

std::vector<Instrument> Client::get_instruments(Market market) const {
    auto& ms = impl_->state(market);
    std::lock_guard lock(ms.mu);
    std::vector<Instrument> out;
    out.reserve(ms.instruments.size());
    for (const auto& [sym, ins] : ms.instruments) out.push_back(ins);
    std::sort(out.begin(), out.end(),
              [](const Instrument& a, const Instrument& b) { return a.symbol < b.symbol; });
    return out;
}

Result<Decimal> Client::round_price(Market market, std::string_view symbol, Decimal price,
                                    Rounding rounding) const {
    if (auto e = unavailable(market)) return *e;
    auto ins = get_instrument(market, symbol);
    if (!ins) {
        return Error{ErrorCode::NotFound,
                     "unknown symbol '" + std::string(symbol) + "' (call load_instruments first)"};
    }
    if (ins->tick_size.is_zero()) return price;
    return price.round_to(ins->tick_size, rounding);
}

Result<Decimal> Client::round_qty(Market market, std::string_view symbol, Decimal qty,
                                  Rounding rounding, bool market_order) const {
    if (auto e = unavailable(market)) return *e;
    auto ins = get_instrument(market, symbol);
    if (!ins) {
        return Error{ErrorCode::NotFound,
                     "unknown symbol '" + std::string(symbol) + "' (call load_instruments first)"};
    }
    const Decimal step = market_order ? ins->market_step_size : ins->step_size;
    if (step.is_zero()) return qty;
    return qty.round_to(step, rounding);
}

// --- Watch and REST helpers -------------------------------------------------------------------

namespace {

Error watch_ended() {
    return Error{ErrorCode::NotSubscribed, "the watch was stopped meanwhile (unwatched or failed)"};
}

/// First call of a watch_*(): start it and wait until its data is loaded. Gives up after
/// 3 x request_timeout (the watch keeps loading; a later call returns the data).
template <class Start>
Result<void> wait_loaded(const Config& config, Start start) {
    if (t_in_io_thread) return blocked_on_io_thread();
    auto promise = std::make_shared<std::promise<Result<void>>>();
    auto future = promise->get_future();
    start([promise](Result<void> r) { promise->set_value(std::move(r)); });
    if (future.wait_for(config.request_timeout * 3) == std::future_status::timeout) {
        return Error{ErrorCode::Timeout, "still loading (it keeps trying); call again later"};
    }
    return future.get();
}

constexpr std::size_t kCandleDepth = 1000;  ///< candles kept per watch at least

void start_candles(Client::Impl& impl, Market market, const std::string& sym, Interval interval,
                   std::size_t depth, Done done) {
    auto& ms = impl.state(market);
    const std::string stream = kline_stream(sym, interval);
    std::function<void()> subscribe;
    std::shared_ptr<CandleSeries> existing;
    bool ready = false, resync = false;
    {
        std::lock_guard lock(ms.mu);
        if (auto it = ms.watches.find(stream); it != ms.watches.end()) {
            existing = std::static_pointer_cast<CandleSeries>(it->second);
            if (depth > existing->depth) {
                existing->depth = depth;
                resync = existing->subscribed;  // load more history first
            }
            ready = existing->has_data() && !resync;
            if (!ready) existing->waiting.push_back(std::move(done));
        } else {
            auto s = std::make_shared<CandleSeries>();
            s->kind = WatchKind::Candles;
            s->symbol = sym;
            s->interval = interval;
            s->stream = stream;
            s->depth = depth;
            if (done) s->waiting.push_back(std::move(done));
            subscribe = impl.add_watch(ms, std::move(s));
        }
    }
    if (ready && done) done(Result<void>{});
    if (resync) impl.start_sync(ms, existing, true);
    if (subscribe) subscribe();
}

Result<std::vector<Candle>> read_candles(Client::Impl& impl, Market market, const std::string& sym,
                                         Interval interval, std::size_t limit, bool closed_only) {
    auto& ms = impl.state(market);
    std::lock_guard lock(ms.mu);
    auto it = ms.watches.find(kline_stream(sym, interval));
    if (it == ms.watches.end()) return watch_ended();
    const auto& candles = static_cast<const CandleSeries&>(*it->second).candles;
    auto end = candles.end();
    if (closed_only && !candles.empty() && !candles.back().closed) --end;
    const auto size = static_cast<std::size_t>(end - candles.begin());
    const std::size_t n = limit == 0 ? size : std::min(limit, size);
    return std::vector<Candle>(end - static_cast<std::ptrdiff_t>(n), end);
}

Result<std::vector<Candle>> watch_candles_impl(Client::Impl& impl, Market market,
                                               std::string_view symbol, Interval interval,
                                               std::size_t limit, bool closed_only) {
    if (auto e = unavailable(market)) return *e;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    if (market == Market::Futures && interval == Interval::S1) {
        return Error{ErrorCode::InvalidArgument, "1s candles exist only on spot"};
    }
    // One more for the forming candle when only closed ones are wanted.
    const std::size_t depth = std::max(kCandleDepth, limit + (closed_only ? 1 : 0));
    auto loaded = wait_loaded(impl.config, [&](Done d) {
        start_candles(impl, market, *sym, interval, depth, std::move(d));
    });
    if (!loaded) return loaded.error();
    return read_candles(impl, market, *sym, interval, limit, closed_only);
}

void start_book(Client::Impl& impl, Market market, const std::string& sym, std::size_t depth,
                Done done) {
    const bool partial = depth == 5 || depth == 10 || depth == 20;
    auto& ms = impl.state(market);
    const std::string stream = depth_stream(sym, depth);
    std::function<void()> subscribe;
    std::string conflict;
    bool ready = false;
    {
        std::lock_guard lock(ms.mu);
        if (auto current = Client::Impl::find_book(ms, sym)) {
            if (current->stream != stream) {
                conflict = "the order book of " + sym + " is already watched with depth " +
                           std::to_string(current->depth) + "; unwatch it first";
            } else {
                current->depth = std::max(current->depth, depth);
                ready = current->has_data();
                if (!ready) current->waiting.push_back(std::move(done));
            }
        } else {
            auto w = std::make_shared<BookWatch>();
            w->kind = WatchKind::OrderBook;
            w->symbol = sym;
            w->stream = stream;
            w->depth = depth;
            w->partial = partial;
            w->book.futures = market == Market::Futures;
            if (done) w->waiting.push_back(std::move(done));
            subscribe = impl.add_watch(ms, std::move(w));
        }
    }
    if (!conflict.empty()) {
        if (done) done(Error{ErrorCode::InvalidArgument, conflict});
        return;
    }
    if (ready && done) done(Result<void>{});
    if (subscribe) subscribe();
}

int depth_weight(Market m, std::size_t limit) {
    if (m == Market::Futures) {
        if (limit <= 50) return 2;
        if (limit <= 100) return 5;
        return limit <= 500 ? 10 : 20;
    }
    if (limit <= 100) return 5;
    if (limit <= 500) return 25;
    return limit <= 1000 ? 50 : 250;
}

asio::awaitable<Result<OrderBook>> rest_order_book(Client::Impl& impl, Market market,
                                                   std::string sym, std::size_t limit) {
    auto& ms = impl.state(market);
    std::string target =
        ms.ep.api_prefix + "/depth?symbol=" + sym + "&limit=" + std::to_string(limit);
    auto body = co_await impl.get(ms, std::move(target), depth_weight(market, limit));
    if (!body) co_return body.error();
    auto snap = detail::parse_depth_snapshot(*body);
    if (!snap) co_return snap.error();
    LocalBook book;
    book.futures = market == Market::Futures;
    book.install(*snap);
    co_return book.top(sym, limit);
}

template <class T>
Result<T> watch_value_impl(Client::Impl& impl, Market market, std::string_view symbol) {
    if (auto e = unavailable(market)) return *e;
    auto loaded = wait_loaded(impl.config,
                              [&](Done d) { impl.watch_value<T>(market, symbol, std::move(d)); });
    if (!loaded) return loaded.error();
    return impl.get_value<T>(market, symbol);
}

template <class T>
asio::awaitable<Result<T>> rest_value(Client::Impl& impl, Market market, std::string sym) {
    using K = ValueKind<T>;
    auto& ms = impl.state(market);
    std::string target = K::rest(ms.ep, sym);
    auto body = co_await impl.get(ms, std::move(target), K::weight(market));
    if (!body) co_return body.error();
    auto r = K::parse_rest(*body, sym);
    if (!r) co_return r.error();
    if (!*r) co_return Error{ErrorCode::NotFound, std::string(K::name) + ": none for " + sym};
    co_return std::move(**r);
}

template <class T>
Result<T> rest_value_impl(Client::Impl& impl, Market market, std::string_view symbol) {
    if (auto e = unavailable(market)) return *e;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    return block(impl, rest_value<T>(impl, market, *sym));
}

/// "" -> all; otherwise a valid upper-cased symbol.
Result<std::string> symbol_or_all(std::string_view s) {
    if (s.empty()) return std::string();
    auto sym = normalize_symbol(s);
    if (!sym) return invalid_symbol(s);
    return std::move(*sym);
}

std::vector<Order> sorted_by_creation(std::vector<Order> orders) {
    std::stable_sort(orders.begin(), orders.end(),
                     [](const Order& x, const Order& y) { return x.create_time < y.create_time; });
    return orders;
}

/// Watch an account part and read it once loaded (read runs under a.mu).
template <class Read>
auto watch_account_part(Client::Impl& impl, Market market, AccountPart part,
                        const std::string& symbol,
                        Read read) -> decltype(read(impl.account_of(market))) {
    if (auto e = unavailable(market)) return *e;
    auto& a = impl.account_of(market);
    auto loaded =
        wait_loaded(impl.config, [&](Done d) { impl.watch_part(a, part, symbol, std::move(d)); });
    if (!loaded) return loaded.error();
    std::lock_guard lock(a.mu);
    if (!a.loaded(part, symbol)) return watch_ended();
    return read(a);
}

Result<std::vector<Order>> watch_orders_impl(Client::Impl& impl, Market market, bool is_algo,
                                             std::string_view symbol) {
    auto sym = symbol_or_all(symbol);
    if (!sym) return sym.error();
    return watch_account_part(
        impl, market, is_algo ? AccountPart::AlgoOrders : AccountPart::OpenOrders, *sym,
        [&](const Client::Impl::AccountState& a) -> Result<std::vector<Order>> {
            std::vector<Order> out;
            for (const auto& [key, c] : a.orders) {
                if (key.first == is_algo && (sym->empty() || c.order.symbol == *sym)) {
                    out.push_back(c.order);
                }
            }
            return sorted_by_creation(std::move(out));
        });
}

Result<std::vector<Order>> rest_orders_impl(Client::Impl& impl, Market market, bool is_algo,
                                            std::string_view symbol) {
    if (auto e = unavailable(market)) return *e;
    if (auto e = impl.missing_keys()) return *e;
    auto sym = symbol_or_all(symbol);
    if (!sym) return sym.error();
    auto r = block(impl, impl.rest_open_orders(market, is_algo, *sym));
    if (!r) return r;
    return sorted_by_creation(std::move(*r));
}

bool same_order_ref(const Order& o, const OrderRef& ref) {
    return o.is_algo == ref.is_algo && o.symbol == ref.symbol &&
           ((ref.order_id && o.id == *ref.order_id) ||
            (ref.client_order_id && o.client_order_id == *ref.client_order_id));
}

constexpr std::int64_t kOrderRefreshMs = 15 * 60 * 1000;

}  // namespace

// --- Candles ----------------------------------------------------------------------------------

Result<std::vector<Candle>> Client::watch_candles(Market market, std::string_view symbol,
                                                  Interval interval, std::size_t limit) {
    return watch_candles_impl(*impl_, market, symbol, interval, limit, false);
}

Result<std::vector<Candle>> Client::watch_closed_candles(Market market, std::string_view symbol,
                                                         Interval interval, std::size_t limit) {
    return watch_candles_impl(*impl_, market, symbol, interval, limit, true);
}

void Client::unwatch_candles(Market market, std::string_view symbol, Interval interval) {
    if (auto sym = normalize_symbol(symbol)) {
        impl_->remove_watch(impl_->state(market), kline_stream(*sym, interval));
    }
}

Result<std::vector<Candle>> Client::get_candles_rest_api(Market market, std::string_view symbol,
                                                         Interval interval, std::size_t limit) {
    if (auto e = unavailable(market)) return *e;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    if (limit == 0) return Error{ErrorCode::InvalidArgument, "limit must be at least 1"};
    return block(*impl_, impl_->fetch_latest_klines(impl_->state(market), *sym, interval, limit));
}

// --- Order book -------------------------------------------------------------------------------

Result<OrderBook> Client::watch_order_book(Market market, std::string_view symbol,
                                           std::size_t depth) {
    if (auto e = unavailable(market)) return *e;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    const std::size_t max = market == Market::Futures ? 1000 : 5000;
    if (depth == 0 || depth > max) {
        return Error{ErrorCode::InvalidArgument, "depth must be 1 - " + std::to_string(max)};
    }
    auto loaded = wait_loaded(
        impl_->config, [&](Done d) { start_book(*impl_, market, *sym, depth, std::move(d)); });
    if (!loaded) return loaded.error();
    auto& ms = impl_->state(market);
    std::lock_guard lock(ms.mu);
    auto w = Impl::find_book(ms, *sym);
    if (!w) return watch_ended();
    if (!w->has_data()) {
        return Error{ErrorCode::NotFound, "the order book of " + *sym + " is being rebuilt"};
    }
    return w->book.top(*sym, depth);
}

void Client::unwatch_order_book(Market market, std::string_view symbol) {
    auto sym = normalize_symbol(symbol);
    if (!sym) return;
    auto& ms = impl_->state(market);
    std::string stream;
    {
        std::lock_guard lock(ms.mu);
        if (auto w = Impl::find_book(ms, *sym)) stream = w->stream;
    }
    if (!stream.empty()) impl_->remove_watch(ms, stream);
}

Result<OrderBook> Client::get_order_book_rest_api(Market market, std::string_view symbol,
                                                  std::size_t limit) {
    if (auto e = unavailable(market)) return *e;
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    if (limit == 0) return Error{ErrorCode::InvalidArgument, "limit must be at least 1"};
    return block(*impl_, rest_order_book(*impl_, market, *sym, limit));
}

// --- Prices -----------------------------------------------------------------------------------

Result<Trade> Client::watch_last_trade(Market market, std::string_view symbol) {
    return watch_value_impl<Trade>(*impl_, market, symbol);
}
void Client::unwatch_last_trade(Market market, std::string_view symbol) {
    impl_->unwatch_value<Trade>(market, symbol);
}
Result<Trade> Client::get_last_trade_rest_api(Market market, std::string_view symbol) {
    return rest_value_impl<Trade>(*impl_, market, symbol);
}

Result<BookTicker> Client::watch_book_ticker(Market market, std::string_view symbol) {
    return watch_value_impl<BookTicker>(*impl_, market, symbol);
}
void Client::unwatch_book_ticker(Market market, std::string_view symbol) {
    impl_->unwatch_value<BookTicker>(market, symbol);
}
Result<BookTicker> Client::get_book_ticker_rest_api(Market market, std::string_view symbol) {
    return rest_value_impl<BookTicker>(*impl_, market, symbol);
}

Result<Ticker24h> Client::watch_ticker_24h(Market market, std::string_view symbol) {
    return watch_value_impl<Ticker24h>(*impl_, market, symbol);
}
void Client::unwatch_ticker_24h(Market market, std::string_view symbol) {
    impl_->unwatch_value<Ticker24h>(market, symbol);
}
Result<Ticker24h> Client::get_ticker_24h_rest_api(Market market, std::string_view symbol) {
    return rest_value_impl<Ticker24h>(*impl_, market, symbol);
}

Result<MarkPrice> Client::watch_mark_price(std::string_view symbol) {
    return watch_value_impl<MarkPrice>(*impl_, Market::Futures, symbol);
}
void Client::unwatch_mark_price(std::string_view symbol) {
    impl_->unwatch_value<MarkPrice>(Market::Futures, symbol);
}
Result<MarkPrice> Client::get_mark_price_rest_api(std::string_view symbol) {
    return rest_value_impl<MarkPrice>(*impl_, Market::Futures, symbol);
}

// --- Account ----------------------------------------------------------------------------------

Result<Balances> Client::watch_balance(Market market) {
    return watch_account_part(
        *impl_, market, AccountPart::Balance, "",
        [](const Impl::AccountState& a) -> Result<Balances> { return a.balances; });
}
void Client::unwatch_balance(Market market) {
    impl_->unwatch_part(impl_->account_of(market), AccountPart::Balance, "");
}
Result<Balances> Client::get_balance_rest_api(Market market) {
    if (auto e = unavailable(market)) return *e;
    if (auto e = impl_->missing_keys()) return *e;
    auto r = block(*impl_, impl_->rest_balances(market));
    if (!r) return r.error();
    Balances out;
    for (auto& b : *r) out[b.asset] = std::move(b);
    return out;
}

Result<Positions> Client::watch_positions() {
    return watch_account_part(
        *impl_, Market::Futures, AccountPart::Positions, "",
        [](const Impl::AccountState& a) -> Result<Positions> { return a.positions; });
}
void Client::unwatch_positions() {
    impl_->unwatch_part(impl_->futures_account, AccountPart::Positions, "");
}
Result<Positions> Client::get_positions_rest_api() {
    if (auto e = impl_->missing_keys()) return *e;
    return block(*impl_, [](Impl& im) -> asio::awaitable<Result<Positions>> {
        auto symbols = co_await im.rest_symbol_config();
        if (!symbols) co_return symbols.error();
        auto positions = co_await im.rest_position_risk();
        if (!positions) co_return positions.error();
        co_return make_positions(*symbols, std::move(*positions));
    }(*impl_));
}

Result<std::vector<Order>> Client::watch_open_orders(Market market, std::string_view symbol) {
    return watch_orders_impl(*impl_, market, false, symbol);
}
void Client::unwatch_open_orders(Market market, std::string_view symbol) {
    if (auto sym = symbol_or_all(symbol)) {
        impl_->unwatch_part(impl_->account_of(market), AccountPart::OpenOrders, *sym);
    }
}
Result<std::vector<Order>> Client::get_open_orders_rest_api(Market market,
                                                            std::string_view symbol) {
    return rest_orders_impl(*impl_, market, false, symbol);
}

Result<std::vector<Order>> Client::watch_algo_orders(std::string_view symbol) {
    return watch_orders_impl(*impl_, Market::Futures, true, symbol);
}
void Client::unwatch_algo_orders(std::string_view symbol) {
    if (auto sym = symbol_or_all(symbol)) {
        impl_->unwatch_part(impl_->futures_account, AccountPart::AlgoOrders, *sym);
    }
}
Result<std::vector<Order>> Client::get_algo_orders_rest_api(std::string_view symbol) {
    return rest_orders_impl(*impl_, Market::Futures, true, symbol);
}

Result<Order> Client::watch_order(Market market, OrderRef order) {
    if (auto e = unavailable(market)) return *e;
    if (market == Market::Spot && order.is_algo) {
        return Error{ErrorCode::InvalidArgument, "spot has no algo orders"};
    }
    if (!order.order_id && !order.client_order_id) {
        return Error{ErrorCode::InvalidArgument, "set order_id or client_order_id"};
    }
    auto sym = normalize_symbol(order.symbol);
    if (!sym) return Error{ErrorCode::InvalidArgument, "a valid symbol is required"};
    order.symbol = *sym;

    // The symbol's open orders are watched: an open order read over REST in the last 15
    // minutes is answered from memory.
    auto open = order.is_algo ? watch_algo_orders(*sym) : watch_open_orders(market, *sym);
    if (!open) return open.error();
    auto& a = impl_->account_of(market);
    {
        std::lock_guard lock(a.mu);
        for (const auto& [key, c] : a.orders) {
            if (same_order_ref(c.order, order) && now_ms() - c.refreshed_ms < kOrderRefreshMs) {
                return c.order;
            }
        }
    }

    auto fresh = block(*impl_, impl_->fetch_order(order, market));
    if (!fresh) return fresh;
    std::lock_guard lock(a.mu);
    auto it = a.orders.find({fresh->is_algo, fresh->id});
    if (it == a.orders.end()) return fresh;  // not open, or its event is still on the way
    if (it->second.order.update_time > fresh->update_time) {
        it->second.refreshed_ms = now_ms();  // the stream is newer than REST
        return it->second.order;
    }
    if (!is_open(fresh->status)) {
        a.orders.erase(it);
    } else {
        const Timestamp created = it->second.order.create_time;
        it->second = CachedOrder{*fresh, now_ms()};
        if (it->second.order.create_time == Timestamp{}) it->second.order.create_time = created;
    }
    return fresh;
}

Result<Order> Client::get_order_rest_api(Market market, OrderRef order) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->fetch_order(std::move(order), market));
}

// --- Trading ----------------------------------------------------------------------------------

Result<Order> Client::create_order(Market market, OrderRequest request) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->create_order(std::move(request), false, market));
}
Result<Order> Client::create_order_rest_api(Market market, OrderRequest request) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->create_order(std::move(request), true, market));
}

Result<Order> Client::cancel_order(Market market, OrderRef order) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->cancel_order(std::move(order), false, market));
}
Result<Order> Client::cancel_order_rest_api(Market market, OrderRef order) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->cancel_order(std::move(order), true, market));
}

Result<Order> Client::amend_order(Market market, AmendRequest request) {
    if (market == Market::Spot) return amend_not_on_spot();
    return block(*impl_, impl_->amend_order(std::move(request)));
}
Result<Order> Client::amend_order_rest_api(Market market, AmendRequest request) {
    if (market == Market::Spot) return amend_not_on_spot();
    return block(*impl_, impl_->amend_order(std::move(request), true));
}

Result<void> Client::cancel_all_orders(Market market, std::string_view symbol) {
    if (auto e = unavailable(market)) return *e;
    return block(*impl_, impl_->cancel_all_orders(std::string(symbol), market));
}

// --- Batch orders (futures) ------------------------------------------------------------------

namespace {

std::vector<Result<Order>> all_failed(std::size_t n, const Error& e) {
    return std::vector<Result<Order>>(n, e);
}

}  // namespace

std::vector<Result<Order>> Client::create_orders(Market market,
                                                 std::vector<OrderRequest> requests) {
    const std::size_t n = requests.size();
    if (market == Market::Spot) return all_failed(n, batch_not_on_spot());
    auto r = run_and_wait(*impl_, impl_->create_orders(std::move(requests)));
    return r ? std::move(*r) : all_failed(n, blocked_on_io_thread());
}

std::vector<Result<Order>> Client::cancel_orders(Market market, std::vector<OrderRef> orders) {
    const std::size_t n = orders.size();
    if (market == Market::Spot) return all_failed(n, batch_not_on_spot());
    auto r = run_and_wait(*impl_, impl_->cancel_orders(std::move(orders)));
    return r ? std::move(*r) : all_failed(n, blocked_on_io_thread());
}

// --- Futures settings -------------------------------------------------------------------------

namespace {

asio::awaitable<Result<void>> post_setting(Client::Impl& impl, std::string path, std::string params,
                                           int already_code) {
    auto r = co_await impl.signed_request(detail::http::verb::post, std::move(path),
                                          std::move(params), 1);
    if (!r && r.error().exchange_code != already_code) co_return r.error();
    co_return Result<void>{};
}

}  // namespace

Result<void> Client::set_leverage(std::string_view symbol, int leverage) {
    auto sym = normalize_symbol(symbol);
    if (!sym || leverage < 1 || leverage > 125) {
        return Error{ErrorCode::InvalidArgument, "need a valid symbol and leverage 1-125"};
    }
    return block(*impl_,
                 post_setting(*impl_, "/fapi/v1/leverage",
                              "symbol=" + *sym + "&leverage=" + std::to_string(leverage), 0));
}

Result<void> Client::set_margin_type(std::string_view symbol, MarginType type) {
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    const char* t = type == MarginType::Isolated ? "ISOLATED" : "CROSSED";
    return block(*impl_,
                 post_setting(*impl_, "/fapi/v1/marginType", "symbol=" + *sym + "&marginType=" + t,
                              -4046));  // -4046: already so
}

Result<void> Client::set_position_mode(bool hedge) {
    return block(*impl_, impl_->set_position_mode(hedge));
}

Result<CommissionRate> Client::get_commission_rate_rest_api(std::string_view symbol) {
    auto sym = normalize_symbol(symbol);
    if (!sym) return invalid_symbol(symbol);
    if (auto e = impl_->missing_keys()) return *e;
    return block(*impl_,
                 [](Client::Impl& im, std::string name) -> asio::awaitable<Result<CommissionRate>> {
                     auto body = co_await im.signed_request(
                         detail::http::verb::get, "/fapi/v1/commissionRate", "symbol=" + name, 20);
                     if (!body) co_return body.error();
                     co_return parse_root_object(
                         *body, "commissionRate", [](ondemand::object o) -> Result<CommissionRate> {
                             CommissionRate c;
                             if (!get_str(o, "symbol", c.symbol) ||
                                 !get_dec(o, "makerCommissionRate", c.maker) ||
                                 !get_dec(o, "takerCommissionRate", c.taker)) {
                                 return parse_error("commissionRate");
                             }
                             return c;
                         });
                 }(*impl_, *sym));
}

}  // namespace binance
