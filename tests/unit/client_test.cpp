#include <binance/client.hpp>

#include "http.hpp"

#include <gtest/gtest.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <mutex>
#include <string>
#include <thread>

using namespace binance;
using namespace std::chrono_literals;

// Internals of client.cpp under test (not declared in any header).
namespace binance::detail {
std::string hmac_sha256_hex(std::string_view key, std::string_view data);
Result<std::string> ed25519_sign_base64(std::string_view pem, std::string_view data);
Result<std::vector<Instrument>> parse_exchange_info(Market market, std::string_view json,
                                                    std::vector<RateLimit>* limits);
Result<std::vector<Candle>> parse_klines(std::string_view json, Timestamp now);
Result<Candle> parse_kline_event(std::string_view json);
Result<std::optional<Trade>> parse_agg_trades_rest(std::string_view json, std::string_view symbol);
Result<Trade> parse_agg_trade_event(std::string_view json);
Result<BookTicker> parse_book_ticker_rest(std::string_view json);
Result<BookTicker> parse_book_ticker_event(std::string_view json);
Result<Ticker24h> parse_ticker_24h_rest(std::string_view json);
Result<Ticker24h> parse_ticker_24h_event(std::string_view json);
Result<MarkPrice> parse_mark_price_rest(std::string_view json);
Result<MarkPrice> parse_mark_price_event(std::string_view json);
}  // namespace binance::detail

namespace {

Decimal d(std::string_view s) { return *Decimal::parse(s); }

/// A Client that only logs errors (keeps test output readable).
class QuietLogger : public Logger {
public:
    void log(LogLevel level, std::string_view msg) override {
        if (level >= LogLevel::Warn) std::cerr << "[binance] " << msg << "\n";
    }
};

Config quiet_config() {
    Config c;
    c.logger = std::make_shared<QuietLogger>();
    return c;
}

}  // namespace

// --- Signing ---------------------------------------------------------------------------------

TEST(Signing, HmacMatchesBinanceDocExample) {
    // From the Binance API docs ("SIGNED endpoint examples").
    EXPECT_EQ(detail::hmac_sha256_hex(
                  "NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
                  "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&"
                  "recvWindow=5000&timestamp=1499827319559"),
              "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
}

TEST(Signing, Ed25519MatchesRfc8032Vectors) {
    // RFC 8032 section 7.1, tests 1 and 2 (public test keys).
    auto sig1 = detail::ed25519_sign_base64(
        "-----BEGIN PRIVATE KEY-----\n"
        "MC4CAQAwBQYDK2VwBCIEIJ1hsZ3v/VpguoRK9JLsLMREScVpezJpGXA7rAMcrn9g\n"
        "-----END PRIVATE KEY-----\n",
        "");
    ASSERT_TRUE(sig1.ok()) << sig1.error().message;
    EXPECT_EQ(
        *sig1,
        "5VZDAMNgrHKQhuLMgG6CioSHfx645dl02HPgZSJJAVVfuIIVkKM7rMYeOXAc+bRr0lv18FlbviRlUUFDjnoQCw==");

    auto sig2 = detail::ed25519_sign_base64(
        "-----BEGIN PRIVATE KEY-----\n"
        "MC4CAQAwBQYDK2VwBCIEIEzNCJso/5banbbDRuwRTg9bijGfNaumJNqM9u1PuKb7\n"
        "-----END PRIVATE KEY-----\n",
        "\x72");
    ASSERT_TRUE(sig2.ok()) << sig2.error().message;
    EXPECT_EQ(
        *sig2,
        "kqAJqfDUyrhyDoILX2QlQKKye1QWUD+Ps3YiI+vbadoIWsHkPhWZbkWPNhPQ8R2MOHsurrQwKu6wDSkWErsMAA==");
}

TEST(Signing, RejectsBadKeys) {
    auto garbage = detail::ed25519_sign_base64("not a key", "x");
    ASSERT_FALSE(garbage.ok());
    EXPECT_EQ(garbage.error().code, ErrorCode::InvalidArgument);

    // A valid PEM key of the wrong type (EC P-256).
    EVP_PKEY* ec = EVP_EC_gen("P-256");
    BIO* bio = BIO_new(BIO_s_mem());
    PEM_write_bio_PrivateKey(bio, ec, nullptr, nullptr, 0, nullptr, nullptr);
    char* data = nullptr;
    const long len = BIO_get_mem_data(bio, &data);
    const std::string pem(data, static_cast<std::size_t>(len));
    BIO_free(bio);
    EVP_PKEY_free(ec);

    auto wrong = detail::ed25519_sign_base64(pem, "x");
    ASSERT_FALSE(wrong.ok());
    EXPECT_NE(wrong.error().message.find("not an Ed25519"), std::string::npos);

    Config c = quiet_config();
    c.credentials.ed25519_private_key = pem;
    EXPECT_THROW(Client{c}, std::invalid_argument);
}

// --- Parsing ---------------------------------------------------------------------------------

TEST(Parse, FuturesExchangeInfo) {
    const std::string json = R"({
      "timezone":"UTC","serverTime":1700000000000,
      "rateLimits":[
        {"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400},
        {"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1200},
        {"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":300}],
      "symbols":[{
        "symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING",
        "baseAsset":"BTC","quoteAsset":"USDT","marginAsset":"USDT",
        "filters":[
          {"minPrice":"261.10","maxPrice":"809484","filterType":"PRICE_FILTER","tickSize":"0.10"},
          {"stepSize":"0.001","filterType":"LOT_SIZE","maxQty":"1000","minQty":"0.001"},
          {"stepSize":"0.001","filterType":"MARKET_LOT_SIZE","maxQty":"120","minQty":"0.001"},
          {"limit":200,"filterType":"MAX_NUM_ORDERS"},
          {"limit":10,"filterType":"MAX_NUM_ALGO_ORDERS"},
          {"notional":"100","filterType":"MIN_NOTIONAL"},
          {"multiplierDown":"0.9500","multiplierUp":"1.0500","multiplierDecimal":"4",
           "filterType":"PERCENT_PRICE"}]},
       {"symbol":"OLDUSDT","contractType":"PERPETUAL","status":"SETTLING",
        "baseAsset":"OLD","quoteAsset":"USDT","filters":[]}]})";

    std::vector<detail::RateLimit> limits;
    auto r = detail::parse_exchange_info(Market::Futures, json, &limits);
    ASSERT_TRUE(r.ok()) << r.error().message;
    ASSERT_EQ(r->size(), 2u);
    const auto& b = (*r)[0];
    EXPECT_EQ(b.symbol, "BTCUSDT");
    EXPECT_TRUE(b.trading);
    EXPECT_EQ(b.contract_type, "PERPETUAL");
    EXPECT_EQ(b.tick_size, d("0.1"));
    EXPECT_EQ(b.min_price, d("261.1"));
    EXPECT_EQ(b.max_price, d("809484"));
    EXPECT_EQ(b.step_size, d("0.001"));
    EXPECT_EQ(b.max_qty, d("1000"));
    EXPECT_EQ(b.market_max_qty, d("120"));
    EXPECT_EQ(b.min_notional, d("100"));
    EXPECT_EQ(b.max_num_orders, 200);
    EXPECT_EQ(b.max_num_algo_orders, 10);
    EXPECT_FALSE((*r)[1].trading);

    ASSERT_EQ(limits.size(), 3u);
    EXPECT_EQ(limits[0].interval, 1min);
    EXPECT_EQ(limits[0].max, 2400);
    EXPECT_FALSE(limits[0].counts_orders);
    EXPECT_EQ(limits[2].interval, 10s);
    EXPECT_TRUE(limits[2].counts_orders);
}

TEST(Parse, SpotExchangeInfo) {
    const std::string json = R"({
      "rateLimits":[{"rateLimitType":"RAW_REQUESTS","interval":"MINUTE","intervalNum":5,"limit":61000}],
      "symbols":[{
        "symbol":"ETHBTC","status":"TRADING","baseAsset":"ETH","quoteAsset":"BTC",
        "filters":[
          {"filterType":"PRICE_FILTER","minPrice":"0.00001000","maxPrice":"922327.00000000","tickSize":"0.00001000"},
          {"filterType":"LOT_SIZE","minQty":"0.00010000","maxQty":"100000.00000000","stepSize":"0.00010000"},
          {"filterType":"MARKET_LOT_SIZE","minQty":"0.00000000","maxQty":"2245.68000000","stepSize":"0.00000000"},
          {"filterType":"NOTIONAL","minNotional":"0.00010000","applyMinToMarket":true,"maxNotional":"9000000.00000000"},
          {"filterType":"MAX_NUM_ORDERS","maxNumOrders":200},
          {"filterType":"MAX_NUM_ALGO_ORDERS","maxNumAlgoOrders":5}]}]})";
    std::vector<detail::RateLimit> limits;
    auto r = detail::parse_exchange_info(Market::Spot, json, &limits);
    ASSERT_TRUE(r.ok()) << r.error().message;
    ASSERT_EQ(r->size(), 1u);
    const auto& e = (*r)[0];
    EXPECT_EQ(e.tick_size, d("0.00001"));
    EXPECT_EQ(e.min_notional, d("0.0001"));
    EXPECT_EQ(e.market_step_size, d("0.0001"));  // falls back to LOT_SIZE when 0
    EXPECT_EQ(e.max_num_orders, 200);
    EXPECT_EQ(e.max_num_algo_orders, 5);
    EXPECT_TRUE(e.contract_type.empty());
    EXPECT_TRUE(limits.empty());  // RAW_REQUESTS is not tracked
}

TEST(Parse, RestKlines) {
    const std::string json =
        R"([[1700000000000,"36000.10","36100.00","35900.50","36050.00","123.456",1700000059999,)"
        R"("4450000.12",1500,"60.5","2180000.5","0"],)"
        R"([1700000060000,"36050.00","36060.00","36040.00","36055.00","1.0",1700000119999,)"
        R"("36055.0",10,"0.5","18027.5","0"]])";
    auto r = detail::parse_klines(json, from_epoch_ms(1700000100000));
    ASSERT_TRUE(r.ok()) << r.error().message;
    ASSERT_EQ(r->size(), 2u);
    const auto& c = (*r)[0];
    EXPECT_EQ(to_epoch_ms(c.open_time), 1700000000000);
    EXPECT_EQ(to_epoch_ms(c.close_time), 1700000059999);
    EXPECT_EQ(c.open, d("36000.1"));
    EXPECT_EQ(c.high, d("36100"));
    EXPECT_EQ(c.low, d("35900.5"));
    EXPECT_EQ(c.close, d("36050"));
    EXPECT_DOUBLE_EQ(c.volume, 123.456);
    EXPECT_DOUBLE_EQ(c.quote_volume, 4450000.12);
    EXPECT_EQ(c.trades, 1500);
    EXPECT_DOUBLE_EQ(c.taker_buy_volume, 60.5);
    EXPECT_DOUBLE_EQ(c.taker_buy_quote_volume, 2180000.5);
    EXPECT_TRUE(c.closed);
    EXPECT_FALSE((*r)[1].closed);  // closes after "now"
}

TEST(Parse, HugeVolumesFit) {
    // Real BTCUSDT monthly candle: quote volume far above Decimal's ~9.2e10 maximum.
    const std::string json =
        R"([[1698796800000,"34500.00","38000.00","34100.00","37700.00","9000000.123",1701388799999,)"
        R"("321456789012.34",45000000,"4500000.5","160000000000.5","0"]])";
    auto r = detail::parse_klines(json, from_epoch_ms(1701388800000));
    ASSERT_TRUE(r.ok()) << r.error().message;
    EXPECT_DOUBLE_EQ((*r)[0].quote_volume, 321456789012.34);
    EXPECT_DOUBLE_EQ((*r)[0].taker_buy_quote_volume, 160000000000.5);
}

TEST(Parse, RestKlinesRejectsGarbage) {
    EXPECT_FALSE(detail::parse_klines(R"({"code":-1121})", {}).ok());
    EXPECT_FALSE(detail::parse_klines(R"([[1,"x"]])", {}).ok());
}

TEST(Parse, KlineStreamEvent) {
    const std::string json = R"({"e":"kline","E":1700000030000,"s":"BTCUSDT","k":{
        "t":1700000000000,"T":1700000059999,"s":"BTCUSDT","i":"1m","f":1,"L":2,
        "o":"36000.10","c":"36010.00","h":"36020.00","l":"35990.00","v":"10.5","n":42,
        "x":true,"q":"378000.5","V":"5.25","Q":"189000.25","B":"0"}})";
    auto r = detail::parse_kline_event(json);
    ASSERT_TRUE(r.ok()) << r.error().message;
    EXPECT_EQ(r->open, d("36000.1"));
    EXPECT_EQ(r->close, d("36010"));
    EXPECT_EQ(r->trades, 42);
    EXPECT_TRUE(r->closed);
    EXPECT_DOUBLE_EQ(r->taker_buy_quote_volume, 189000.25);
}

TEST(Parse, AggTrade) {
    auto ev = detail::parse_agg_trade_event(
        R"({"e":"aggTrade","E":1700000000100,"s":"BTCUSDT","a":5933014,"p":"36000.10","q":"0.250",)"
        R"("nq":"0.250","f":100,"l":105,"T":1700000000050,"m":true})");
    ASSERT_TRUE(ev.ok()) << ev.error().message;
    EXPECT_EQ(ev->symbol, "BTCUSDT");
    EXPECT_EQ(ev->id, 5933014);
    EXPECT_EQ(ev->price, d("36000.1"));
    EXPECT_EQ(ev->qty, d("0.25"));
    EXPECT_EQ(to_epoch_ms(ev->time), 1700000000050);
    EXPECT_TRUE(ev->buyer_is_maker);

    auto rest = detail::parse_agg_trades_rest(
        R"([{"a":26129,"p":"0.01633102","q":"4.70443515","f":27781,"l":27781,"T":1498793709153,)"
        R"("m":false,"M":true}])",
        "LTCBTC");
    ASSERT_TRUE(rest.ok()) << rest.error().message;
    ASSERT_TRUE(rest->has_value());
    EXPECT_EQ((*rest)->symbol, "LTCBTC");
    EXPECT_EQ((*rest)->price, d("0.01633102"));
    EXPECT_FALSE((*rest)->buyer_is_maker);

    auto empty = detail::parse_agg_trades_rest("[]", "LTCBTC");
    ASSERT_TRUE(empty.ok());
    EXPECT_FALSE(empty->has_value());  // no trade yet: not an error
}

TEST(Parse, BookTicker) {
    auto fut = detail::parse_book_ticker_event(
        R"({"e":"bookTicker","u":400900217,"E":1568014460893,"T":1568014460891,"s":"BNBUSDT",)"
        R"("b":"25.3519","B":"31.21","a":"25.3652","A":"40.66"})");
    ASSERT_TRUE(fut.ok()) << fut.error().message;
    EXPECT_EQ(fut->update_id, 400900217);
    EXPECT_EQ(fut->bid_price, d("25.3519"));
    EXPECT_EQ(fut->ask_qty, d("40.66"));
    EXPECT_EQ(to_epoch_ms(fut->time), 1568014460891);

    auto spot = detail::parse_book_ticker_event(
        R"({"u":400900217,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000","a":"25.36520000",)"
        R"("A":"40.66000000"})");
    ASSERT_TRUE(spot.ok()) << spot.error().message;
    EXPECT_EQ(to_epoch_ms(spot->time), 0);  // spot sends no time

    auto rest = detail::parse_book_ticker_rest(
        R"({"symbol":"BTCUSDT","bidPrice":"4.00000000","bidQty":"431.00000000",)"
        R"("askPrice":"4.00000200","askQty":"9.00000000","time":1589437530011,"lastUpdateId":1027024})");
    ASSERT_TRUE(rest.ok()) << rest.error().message;
    EXPECT_EQ(rest->update_id, 1027024);
    EXPECT_EQ(rest->ask_price, d("4.000002"));
}

TEST(Parse, Ticker24h) {
    auto ev = detail::parse_ticker_24h_event(
        R"({"e":"24hrTicker","E":123456789,"s":"BTCUSDT","p":"-150.5","P":"-0.412","w":"36500",)"
        R"("c":"36350.1","Q":"10","o":"36500.6","h":"37000","l":"36000","v":"123456789.5",)"
        R"("q":"4500000000000.25","O":1700000000000,"C":1700086399999,"F":0,"L":18150,"n":18151})");
    ASSERT_TRUE(ev.ok()) << ev.error().message;
    EXPECT_EQ(ev->price_change, d("-150.5"));
    EXPECT_EQ(ev->price_change_percent, d("-0.412"));
    EXPECT_EQ(ev->last_price, d("36350.1"));
    EXPECT_DOUBLE_EQ(ev->quote_volume, 4500000000000.25);  // > Decimal range: why it is double
    EXPECT_EQ(ev->trade_count, 18151);
    EXPECT_EQ(to_epoch_ms(ev->close_time), 1700086399999);

    auto rest = detail::parse_ticker_24h_rest(
        R"({"symbol":"BTCUSDT","priceChange":"-94.99999800","priceChangePercent":"-95.960",)"
        R"("weightedAvgPrice":"0.29628482","lastPrice":"4.00000200","lastQty":"200.00000000",)"
        R"("openPrice":"99.00000000","highPrice":"100.00000000","lowPrice":"0.10000000",)"
        R"("volume":"8913.30000000","quoteVolume":"15.30000000","openTime":1499783499040,)"
        R"("closeTime":1499869899040,"firstId":28385,"lastId":28460,"count":76})");
    ASSERT_TRUE(rest.ok()) << rest.error().message;
    EXPECT_EQ(rest->open_price, d("99"));
    EXPECT_DOUBLE_EQ(rest->volume, 8913.3);
    EXPECT_EQ(rest->trade_count, 76);
}

TEST(Parse, MarkPrice) {
    auto ev = detail::parse_mark_price_event(
        R"({"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT","p":"11794.15000000",)"
        R"("ap":"11784.62659091","P":"11784.25641265","i":"11784.62659091","r":"-0.00038167",)"
        R"("T":1562306400000})");
    ASSERT_TRUE(ev.ok()) << ev.error().message;
    EXPECT_EQ(ev->mark_price, d("11794.15"));
    EXPECT_EQ(ev->index_price, d("11784.62659091"));
    EXPECT_EQ(ev->funding_rate, d("-0.00038167"));
    EXPECT_EQ(to_epoch_ms(ev->next_funding_time), 1562306400000);
    EXPECT_EQ(to_epoch_ms(ev->time), 1562305380000);

    auto rest = detail::parse_mark_price_rest(
        R"({"symbol":"BTCUSDT_250627","markPrice":"11793.63104562","indexPrice":"11781.80495970",)"
        R"("estimatedSettlePrice":"11781.16138815","lastFundingRate":"","interestRate":"",)"
        R"("nextFundingTime":0,"time":1597370495002})");
    ASSERT_TRUE(rest.ok()) << rest.error().message;  // delivery contract: empty funding rate
    EXPECT_TRUE(rest->funding_rate.is_zero());
    EXPECT_EQ(rest->symbol, "BTCUSDT_250627");
}

// --- Intervals: "1m" is a minute, "1M" a month -----------------------------------------------

TEST(Interval, RoundTripsAllValues) {
    for (auto s : {"1s", "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d",
                   "3d", "1w", "1M"}) {
        auto i = parse_interval(s);
        ASSERT_TRUE(i.has_value()) << s;
        EXPECT_EQ(to_string(*i), s);
    }
}

TEST(Interval, RejectsUnknown) {
    EXPECT_FALSE(parse_interval("2m").has_value());
    EXPECT_FALSE(parse_interval("1H").has_value());  // case matters
    EXPECT_FALSE(parse_interval("").has_value());
}

TEST(Interval, Duration) {
    using namespace std::chrono;
    EXPECT_EQ(interval_duration(Interval::M1), milliseconds{60'000});
    EXPECT_EQ(interval_duration(Interval::H4), milliseconds{4 * 3'600'000});
    EXPECT_EQ(interval_duration(Interval::W1), milliseconds{7 * 86'400'000LL});
    EXPECT_FALSE(interval_duration(Interval::MN1).has_value());
}

// --- Client without network ------------------------------------------------------------------

TEST(Client, LocalErrors) {
    Client client(quiet_config());
    constexpr auto F = Market::Futures;

    auto p = client.round_price(F, "BTCUSDT", d("1.23"));
    ASSERT_FALSE(p.ok());
    EXPECT_EQ(p.error().code, ErrorCode::NotFound);

    // Rejected before any network I/O.
    EXPECT_EQ(client.watch_candles(F, "BTC/USDT", Interval::M1).error().code,
              ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_candles(F, "BTCUSDT", Interval::S1).error().code,
              ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_last_trade(F, "").error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_order_book(F, "BTCUSDT", 0).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.watch_order_book(F, "BTCUSDT", 5000).error().code, ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_candles_rest_api(F, "BTCUSDT", Interval::M1, 0).error().code,
              ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_order_book_rest_api(F, "BTC USDT").error().code,
              ErrorCode::InvalidArgument);
    EXPECT_EQ(client.get_mark_price_rest_api("").error().code, ErrorCode::InvalidArgument);

    // Unwatching what is not watched is harmless.
    client.unwatch_candles(F, "BTCUSDT", Interval::M1);
    client.unwatch_last_trade(F, "BTCUSDT");
    client.unwatch_order_book(F, "BTCUSDT");
}

// --- Live (network) --------------------------------------------------------------------------
// Run with BINANCE_LIVE_TESTS=1. Public data only, no API key.

class LiveClient : public ::testing::Test {
protected:
    void SetUp() override {
        if (!std::getenv("BINANCE_LIVE_TESTS")) GTEST_SKIP() << "set BINANCE_LIVE_TESTS=1";
    }

    static void expect_consecutive(const std::vector<Candle>& v, std::chrono::milliseconds step) {
        for (std::size_t i = 1; i < v.size(); ++i) {
            ASSERT_EQ(v[i].open_time - v[i - 1].open_time, step) << "at " << i;
        }
    }

    template <class Pred>
    static bool eventually(Pred pred, std::chrono::milliseconds timeout = 15s) {
        const auto end = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < end) {
            if (pred()) return true;
            std::this_thread::sleep_for(100ms);
        }
        return pred();
    }
};

TEST_F(LiveClient, FuturesInstrumentsAndRounding) {
    Client client(quiet_config());
    constexpr auto F = Market::Futures;
    auto r = client.load_instruments(F);
    ASSERT_TRUE(r.ok()) << r.error().message;

    auto btc = client.get_instrument(F, "btcusdt");  // case-insensitive
    ASSERT_TRUE(btc.has_value());
    EXPECT_TRUE(btc->trading);
    EXPECT_EQ(btc->contract_type, "PERPETUAL");
    EXPECT_EQ(btc->tick_size, d("0.1"));
    EXPECT_EQ(btc->step_size, d("0.001"));
    EXPECT_GT(btc->max_num_orders, 0);
    EXPECT_GT(client.get_instruments(F).size(), 100u);

    EXPECT_EQ(*client.round_price(F, "BTCUSDT", d("65000.17"), Rounding::Down), d("65000.1"));
    EXPECT_EQ(*client.round_price(F, "BTCUSDT", d("65000.11"), Rounding::Up), d("65000.2"));
    EXPECT_EQ(*client.round_qty(F, "BTCUSDT", d("0.0129")), d("0.012"));
}

TEST_F(LiveClient, SpotInstruments) {
    Client client(quiet_config());
    auto r = client.load_instruments(Market::Spot);
    ASSERT_TRUE(r.ok()) << r.error().message;
    auto btc = client.get_instrument(Market::Spot, "BTCUSDT");
    ASSERT_TRUE(btc.has_value());
    EXPECT_TRUE(btc->trading);
    EXPECT_FALSE(btc->tick_size.is_zero());
    EXPECT_GT(client.get_instruments(Market::Spot).size(), 1000u);
}

TEST_F(LiveClient, FuturesCandlesPaginatedAndLive) {
    Client client(quiet_config());
    constexpr auto F = Market::Futures;

    auto first = client.watch_candles(F, "BTCUSDT", Interval::M1, 5);
    ASSERT_TRUE(first.ok()) << first.error().message;
    ASSERT_EQ(first->size(), 5u);
    EXPECT_FALSE(first->back().closed);  // the forming candle
    EXPECT_GT(first->back().close_time, std::chrono::system_clock::now() - 1min);

    auto all = client.watch_candles(F, "BTCUSDT", Interval::M1);  // all kept: the default 1000
    ASSERT_TRUE(all.ok());
    EXPECT_EQ(all->size(), 1000u);
    expect_consecutive(*all, 1min);
    EXPECT_EQ(all->back().open_time, first->back().open_time);

    // A deeper call loads more history first (> 1500: two REST pages).
    auto deep = client.watch_candles(F, "BTCUSDT", Interval::M1, 2000);
    ASSERT_TRUE(deep.ok()) << deep.error().message;
    ASSERT_EQ(deep->size(), 2000u);
    expect_consecutive(*deep, 1min);
    EXPECT_TRUE(deep->front().closed);

    auto closed = client.watch_closed_candles(F, "BTCUSDT", Interval::M1, 3);
    ASSERT_EQ(closed->size(), 3u);
    EXPECT_TRUE(closed->back().closed);
    EXPECT_LT(closed->back().open_time, deep->back().open_time);

    const auto before = deep->back();
    EXPECT_TRUE(eventually([&] {
        auto now = client.watch_candles(F, "BTCUSDT", Interval::M1, 1);
        return now && (now->back().close != before.close || now->back().trades != before.trades ||
                       now->back().open_time != before.open_time);
    })) << "no kline update from the stream";

    auto rest = client.get_candles_rest_api(F, "BTCUSDT", Interval::M1, 10);
    ASSERT_TRUE(rest.ok()) << rest.error().message;
    EXPECT_EQ(rest->size(), 10u);
    expect_consecutive(*rest, 1min);
    client.unwatch_candles(F, "BTCUSDT", Interval::M1);
}

TEST_F(LiveClient, SpotCandlesAndManyWatchesAtOnce) {
    Client client(quiet_config());
    constexpr auto S = Market::Spot;
    const std::vector<std::pair<const char*, Interval>> watches = {
        {"BTCUSDT", Interval::S1}, {"BTCUSDT", Interval::M1}, {"ETHUSDT", Interval::M15},
        {"BNBUSDT", Interval::H4}, {"BTCUSDT", Interval::W1}, {"BTCUSDT", Interval::MN1}};
    std::vector<std::thread> threads;
    std::vector<Result<std::vector<Candle>>> results(watches.size(),
                                                     Error{ErrorCode::Internal, "not run"});
    for (std::size_t i = 0; i < watches.size(); ++i) {
        threads.emplace_back([&, i] {
            results[i] = client.watch_candles(S, watches[i].first, watches[i].second, 1200);
        });
    }
    for (auto& t : threads) t.join();
    for (auto& r : results) ASSERT_TRUE(r.ok()) << r.error().message;
    ASSERT_EQ(results[1]->size(), 1200u);  // > 1000: two REST pages on spot
    expect_consecutive(*results[1], 1min);
    EXPECT_GT(results[4]->size(), 300u);  // all weekly history (< 1200)
    expect_consecutive(*results[4], std::chrono::weeks{1});
}

TEST_F(LiveClient, UnknownSymbolFailsTheWatch) {
    Client client(quiet_config());
    auto r = client.watch_candles(Market::Futures, "NOTACOINUSDT", Interval::M1);
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().code, ErrorCode::Exchange);
    EXPECT_EQ(r.error().exchange_code, -1121);
    auto rest = client.get_candles_rest_api(Market::Futures, "NOTACOINUSDT", Interval::M1);
    ASSERT_FALSE(rest.ok());
    EXPECT_EQ(rest.error().exchange_code, -1121);
}

TEST_F(LiveClient, DemoEnvironment) {
    Config c = quiet_config();
    c.environment = Environment::Demo;
    Client client(c);
    auto f = client.watch_candles(Market::Futures, "BTCUSDT", Interval::M1, 10);
    ASSERT_TRUE(f.ok()) << f.error().message;
    EXPECT_EQ(f->size(), 10u);
    auto s = client.watch_candles(Market::Spot, "BTCUSDT", Interval::M1, 10);
    ASSERT_TRUE(s.ok()) << s.error().message;
}

TEST_F(LiveClient, FuturesPrices) {
    Client client(quiet_config());
    constexpr auto F = Market::Futures;

    const auto now = std::chrono::system_clock::now();
    auto book = client.watch_book_ticker(F, "BTCUSDT");
    ASSERT_TRUE(book.ok()) << book.error().message;
    EXPECT_LT(book->bid_price, book->ask_price);
    EXPECT_GT(book->update_id, 0);

    auto trade = client.watch_last_trade(F, "BTCUSDT");
    ASSERT_TRUE(trade.ok()) << trade.error().message;
    EXPECT_GT(trade->id, 0);
    EXPECT_TRUE(trade->price.is_positive());
    EXPECT_GT(trade->time, now - 5min);

    auto ticker = client.watch_ticker_24h(F, "BTCUSDT");
    ASSERT_TRUE(ticker.ok()) << ticker.error().message;
    EXPECT_LE(ticker->low_price, ticker->last_price);
    EXPECT_GE(ticker->high_price, ticker->last_price);
    EXPECT_GT(ticker->quote_volume, 1e6);

    auto mark = client.watch_mark_price("BTCUSDT");
    ASSERT_TRUE(mark.ok()) << mark.error().message;
    EXPECT_TRUE(mark->mark_price.is_positive());
    EXPECT_TRUE(mark->index_price.is_positive());
    EXPECT_GT(mark->next_funding_time, now);

    // Book ticker comes from /public, the rest from /market: both connections deliver.
    EXPECT_TRUE(eventually([&] {
        auto b = client.watch_book_ticker(F, "BTCUSDT");
        return b && b->update_id != book->update_id;
    }));
    EXPECT_TRUE(eventually([&] {  // mark price updates every second
        auto m = client.watch_mark_price("BTCUSDT");
        return m && m->time > mark->time;
    }));

    auto rest_book = client.get_book_ticker_rest_api(F, "BTCUSDT");
    ASSERT_TRUE(rest_book.ok()) << rest_book.error().message;
    EXPECT_LT(rest_book->bid_price, rest_book->ask_price);
    EXPECT_TRUE(client.get_last_trade_rest_api(F, "BTCUSDT")->price.is_positive());
    EXPECT_GT(client.get_ticker_24h_rest_api(F, "BTCUSDT")->quote_volume, 1e6);
    EXPECT_TRUE(client.get_mark_price_rest_api("BTCUSDT")->mark_price.is_positive());

    client.unwatch_last_trade(F, "BTCUSDT");
}

TEST_F(LiveClient, SpotPrices) {
    Client client(quiet_config());
    constexpr auto S = Market::Spot;
    auto book = client.watch_book_ticker(S, "ETHUSDT");
    ASSERT_TRUE(book.ok()) << book.error().message;
    EXPECT_LT(book->bid_price, book->ask_price);
    EXPECT_TRUE(client.watch_last_trade(S, "ETHUSDT")->price.is_positive());
    EXPECT_GT(client.watch_ticker_24h(S, "ETHUSDT")->trade_count, 0);
    EXPECT_TRUE(client.get_last_trade_rest_api(S, "ETHUSDT")->price.is_positive());
    EXPECT_GT(client.get_ticker_24h_rest_api(S, "ETHUSDT")->trade_count, 0);
}

TEST_F(LiveClient, UnknownSymbolFailsPriceWatches) {
    Client client(quiet_config());
    auto r = client.watch_book_ticker(Market::Futures, "NOTACOINUSDT");
    ASSERT_FALSE(r.ok());
    EXPECT_EQ(r.error().exchange_code, -1121);
    auto m = client.watch_mark_price("NOTACOINUSDT");
    ASSERT_FALSE(m.ok());
    EXPECT_EQ(m.error().code, ErrorCode::Exchange);
}
