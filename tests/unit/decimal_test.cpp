#include <gtest/gtest.h>
#include <binance/decimal.hpp>

#include <limits>

using binance::Decimal;
using binance::Rounding;

namespace {

Decimal D(std::string_view s) {
    auto d = Decimal::parse(s);
    EXPECT_TRUE(d.has_value()) << "failed to parse: " << s;
    return d.value_or(Decimal{});
}

}  // namespace

// --- parse ----------------------------------------------------------------------------------

TEST(DecimalParse, BinanceFormats) {
    EXPECT_EQ(D("0.00100000").raw(), 100'000);
    EXPECT_EQ(D("104242.10").raw(), 10'424'210'000'000);
    EXPECT_EQ(D("1").raw(), Decimal::kOne);
    EXPECT_EQ(D("-0.5").raw(), -50'000'000);
    EXPECT_EQ(D("+2").raw(), 2 * Decimal::kOne);
    EXPECT_EQ(D("0").raw(), 0);
    EXPECT_EQ(D("-0").raw(), 0);
    EXPECT_EQ(D(".5").raw(), 50'000'000);
    EXPECT_EQ(D("5.").raw(), 5 * Decimal::kOne);
    EXPECT_EQ(D("0.00000001").raw(), 1);
}

TEST(DecimalParse, ExtraTrailingZerosAccepted) {
    EXPECT_EQ(D("1.0000000000000").raw(), Decimal::kOne);
    EXPECT_EQ(D("0.000000010").raw(), 1);
}

TEST(DecimalParse, Rejects) {
    for (auto s : {"", "-", "+", ".", "-.", "abc", "1.2.3", " 1", "1 ", "1e5", "1,5", "--1",
                   "0.000000001", "0.123456789", "nan"}) {
        EXPECT_FALSE(Decimal::parse(s).has_value()) << "should reject: \"" << s << '"';
    }
}

TEST(DecimalParse, Range) {
    constexpr auto kMax = std::numeric_limits<std::int64_t>::max();
    constexpr auto kMin = std::numeric_limits<std::int64_t>::min();
    EXPECT_EQ(D("92233720368.54775807").raw(), kMax);
    EXPECT_EQ(D("-92233720368.54775808").raw(), kMin);
    EXPECT_FALSE(Decimal::parse("92233720368.54775808").has_value());
    EXPECT_FALSE(Decimal::parse("-92233720368.54775809").has_value());
    EXPECT_FALSE(Decimal::parse("100000000000").has_value());
    EXPECT_FALSE(Decimal::parse("99999999999999999999999").has_value());
}

// --- to_string ------------------------------------------------------------------------------

TEST(DecimalToString, ShortestForm) {
    EXPECT_EQ(D("0.00100000").to_string(), "0.001");
    EXPECT_EQ(D("100").to_string(), "100");
    EXPECT_EQ(D("-12.50").to_string(), "-12.5");
    EXPECT_EQ(D("0").to_string(), "0");
    EXPECT_EQ(D("-0.00000001").to_string(), "-0.00000001");
    EXPECT_EQ(Decimal::from_raw(std::numeric_limits<std::int64_t>::min()).to_string(),
              "-92233720368.54775808");
}

TEST(DecimalToString, RoundTrip) {
    for (auto s : {"0.001", "104242.1", "-3.14159265", "92233720368.54775807", "1"}) {
        EXPECT_EQ(D(s).to_string(), s);
    }
}

// --- arithmetic -----------------------------------------------------------------------------

TEST(DecimalArithmetic, AddSubCompare) {
    EXPECT_EQ(D("0.1") + D("0.2"), D("0.3"));  // exact, unlike double
    EXPECT_EQ(D("1") - D("1.5"), D("-0.5"));
    EXPECT_EQ(-D("2"), D("-2"));
    EXPECT_LT(D("0.00000001"), D("0.00000002"));
    EXPECT_GT(D("-1"), D("-2"));
    EXPECT_EQ(Decimal::from_int(7), D("7"));
}

TEST(DecimalArithmetic, MulNotional) {
    EXPECT_EQ(Decimal::mul(D("104242.1"), D("0.003")), D("312.7263"));
    EXPECT_EQ(Decimal::mul(D("-2"), D("0.5")), D("-1"));
    EXPECT_EQ(Decimal::mul(D("-2"), D("-0.5")), D("1"));
    EXPECT_EQ(Decimal::mul(D("0"), D("123")), D("0"));
}

TEST(DecimalArithmetic, MulRounding) {
    // 0.00000001 * 0.5 = 0.000000005 -> needs rounding to 8 decimals
    const auto tiny = D("0.00000001"), half = D("0.5");
    EXPECT_EQ(Decimal::mul(tiny, half, Rounding::HalfUp), D("0.00000001"));
    EXPECT_EQ(Decimal::mul(tiny, half, Rounding::TowardZero), D("0"));
    EXPECT_EQ(Decimal::mul(tiny, half, Rounding::Down), D("0"));
    EXPECT_EQ(Decimal::mul(tiny, half, Rounding::Up), D("0.00000001"));
    EXPECT_EQ(Decimal::mul(-tiny, half, Rounding::HalfUp), D("-0.00000001"));
    EXPECT_EQ(Decimal::mul(-tiny, half, Rounding::Down), D("-0.00000001"));
    EXPECT_EQ(Decimal::mul(-tiny, half, Rounding::Up), D("0"));
}

TEST(DecimalArithmetic, MulOverflow) {
    EXPECT_FALSE(Decimal::mul(D("10000000000"), D("10")).has_value());
    EXPECT_TRUE(Decimal::mul(D("9000000000"), D("10")).has_value());
}

TEST(DecimalArithmetic, Div) {
    EXPECT_EQ(Decimal::div(D("1"), D("4")), D("0.25"));
    EXPECT_EQ(Decimal::div(D("312.7263"), D("104242.1")), D("0.003"));
    EXPECT_EQ(Decimal::div(D("1"), D("3")), D("0.33333333"));
    EXPECT_EQ(Decimal::div(D("2"), D("3")), D("0.66666667"));
    EXPECT_EQ(Decimal::div(D("2"), D("3"), Rounding::TowardZero), D("0.66666666"));
    EXPECT_EQ(Decimal::div(D("-1"), D("4")), D("-0.25"));
    EXPECT_EQ(Decimal::div(D("1"), D("-4")), D("-0.25"));
    EXPECT_FALSE(Decimal::div(D("1"), D("0")).has_value());
    EXPECT_FALSE(Decimal::div(D("90000000000"), D("0.1")).has_value());
}

// --- step rounding --------------------------------------------------------------------------

TEST(DecimalStep, TickSize) {
    const auto tick = D("0.1");
    EXPECT_EQ(D("104242.17").floor_to(tick), D("104242.1"));
    EXPECT_EQ(D("104242.17").ceil_to(tick), D("104242.2"));
    EXPECT_EQ(D("104242.15").round_to(tick, Rounding::HalfUp), D("104242.2"));
    EXPECT_EQ(D("104242.14").round_to(tick, Rounding::HalfUp), D("104242.1"));
    EXPECT_EQ(D("104242.1").floor_to(tick), D("104242.1"));
}

TEST(DecimalStep, LotStep) {
    const auto step = D("0.001");
    EXPECT_EQ(D("0.0129").floor_to(step), D("0.012"));
    EXPECT_EQ(D("0.0129").round_to(step, Rounding::TowardZero), D("0.012"));
    EXPECT_TRUE(D("0.012").is_multiple_of(step));
    EXPECT_FALSE(D("0.0125").is_multiple_of(step));
}

TEST(DecimalStep, NonPowerOfTenStep) {
    const auto tick = D("0.5");
    EXPECT_EQ(D("10.7").floor_to(tick), D("10.5"));
    EXPECT_EQ(D("10.7").ceil_to(tick), D("11"));
    EXPECT_EQ(D("10.75").round_to(tick, Rounding::HalfUp), D("11"));
}

TEST(DecimalStep, Negative) {
    const auto tick = D("0.1");
    EXPECT_EQ(D("-1.25").floor_to(tick), D("-1.3"));
    EXPECT_EQ(D("-1.25").ceil_to(tick), D("-1.2"));
    EXPECT_EQ(D("-1.25").round_to(tick, Rounding::TowardZero), D("-1.2"));
    EXPECT_EQ(D("-1.25").round_to(tick, Rounding::HalfUp), D("-1.3"));
}
