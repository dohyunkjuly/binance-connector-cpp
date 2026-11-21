#pragma once

#include <cassert>
#include <compare>
#include <cstdint>
#include <iosfwd>
#include <optional>
#include <string>
#include <string_view>

namespace binance {

enum class Rounding {
    Down,  ///< toward negative infinity (floor)
    Up,    ///< toward positive infinity (ceil)
    TowardZero,
    HalfUp,  ///< to nearest; ties away from zero
};

/// Exact fixed-point decimal, 8 fractional digits (int64 units of 1e-8), range about +/-92e9.
/// + and - are unchecked; mul()/div() return nullopt on overflow or division by zero.
class Decimal {
public:
    static constexpr int kScale = 8;
    static constexpr std::int64_t kOne = 100'000'000;

    constexpr Decimal() noexcept = default;

    static constexpr Decimal from_raw(std::int64_t raw) noexcept {
        Decimal d;
        d.raw_ = raw;
        return d;
    }

    /// Caller ensures |v| <= ~92e9.
    static constexpr Decimal from_int(std::int64_t v) noexcept { return from_raw(v * kOne); }

    /// Accepts "123", "-0.001", ".5"; rejects whitespace, exponents and more than 8 decimals.
    static std::optional<Decimal> parse(std::string_view s) noexcept;

    /// Shortest exact form: "0.001", "-12.5", "100", "0".
    [[nodiscard]] std::string to_string() const;

    /// Lossy; for indicators/statistics only, never for order values.
    [[nodiscard]] double to_double() const noexcept {
        return static_cast<double>(raw_) / static_cast<double>(kOne);
    }

    [[nodiscard]] constexpr std::int64_t raw() const noexcept { return raw_; }
    [[nodiscard]] constexpr bool is_zero() const noexcept { return raw_ == 0; }
    [[nodiscard]] constexpr bool is_negative() const noexcept { return raw_ < 0; }
    [[nodiscard]] constexpr bool is_positive() const noexcept { return raw_ > 0; }

    // --- arithmetic -------------------------------------------------------------------------

    constexpr Decimal operator-() const noexcept { return from_raw(-raw_); }
    constexpr Decimal& operator+=(Decimal o) noexcept {
        raw_ += o.raw_;
        return *this;
    }
    constexpr Decimal& operator-=(Decimal o) noexcept {
        raw_ -= o.raw_;
        return *this;
    }
    friend constexpr Decimal operator+(Decimal a, Decimal b) noexcept { return a += b; }
    friend constexpr Decimal operator-(Decimal a, Decimal b) noexcept { return a -= b; }

    /// nullopt on overflow.
    static std::optional<Decimal> mul(Decimal a, Decimal b, Rounding r = Rounding::HalfUp) noexcept;

    /// nullopt on overflow or b == 0.
    static std::optional<Decimal> div(Decimal a, Decimal b, Rounding r = Rounding::HalfUp) noexcept;

    // --- rounding to a step (tick size / lot step). step must be > 0. ------------------------

    [[nodiscard]] Decimal round_to(Decimal step, Rounding r) const noexcept;
    [[nodiscard]] Decimal floor_to(Decimal step) const noexcept {
        return round_to(step, Rounding::Down);
    }
    [[nodiscard]] Decimal ceil_to(Decimal step) const noexcept {
        return round_to(step, Rounding::Up);
    }

    [[nodiscard]] constexpr bool is_multiple_of(Decimal step) const noexcept {
        assert(step.raw_ > 0);
        return raw_ % step.raw_ == 0;
    }

    // --- comparison -------------------------------------------------------------------------

    friend constexpr auto operator<=>(Decimal, Decimal) noexcept = default;
    friend constexpr bool operator==(Decimal, Decimal) noexcept = default;

private:
    std::int64_t raw_ = 0;
};

std::ostream& operator<<(std::ostream& os, Decimal d);

}  // namespace binance
