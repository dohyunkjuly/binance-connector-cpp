#include <binance/decimal.hpp>

#include <limits>
#include <ostream>

// Native 128-bit integers exist on GCC/Clang but not MSVC. The portable path is also used when
// BINANCE_FORCE_PORTABLE_INT128 is defined, so it can be tested on any platform.
#if defined(__SIZEOF_INT128__) && !defined(BINANCE_FORCE_PORTABLE_INT128)
#define BINANCE_NATIVE_INT128 1
#endif

namespace binance {
namespace {

constexpr std::uint64_t kInt64MaxMag =
    static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max());
constexpr std::uint64_t kInt64MinMag = kInt64MaxMag + 1;  // |INT64_MIN|

constexpr std::uint64_t magnitude(std::int64_t v) noexcept {
    return v < 0 ? static_cast<std::uint64_t>(-(v + 1)) + 1 : static_cast<std::uint64_t>(v);
}

struct U128 {
    std::uint64_t hi = 0;
    std::uint64_t lo = 0;
};

#ifdef BINANCE_NATIVE_INT128
__extension__ typedef unsigned __int128 uint128;  // GCC/Clang extension, not ISO C++
#endif

U128 mul_64x64(std::uint64_t a, std::uint64_t b) noexcept {
#ifdef BINANCE_NATIVE_INT128
    const uint128 p = static_cast<uint128>(a) * b;
    return {static_cast<std::uint64_t>(p >> 64), static_cast<std::uint64_t>(p)};
#else
    const std::uint64_t a_lo = a & 0xFFFFFFFFu, a_hi = a >> 32;
    const std::uint64_t b_lo = b & 0xFFFFFFFFu, b_hi = b >> 32;
    const std::uint64_t ll = a_lo * b_lo;
    const std::uint64_t lh = a_lo * b_hi;
    const std::uint64_t hl = a_hi * b_lo;
    const std::uint64_t hh = a_hi * b_hi;
    const std::uint64_t mid = (ll >> 32) + (lh & 0xFFFFFFFFu) + (hl & 0xFFFFFFFFu);
    return {hh + (lh >> 32) + (hl >> 32) + (mid >> 32), (mid << 32) | (ll & 0xFFFFFFFFu)};
#endif
}

// n / d with remainder. Requires 0 < d < 2^63.
U128 divmod(U128 n, std::uint64_t d, std::uint64_t& rem) noexcept {
#ifdef BINANCE_NATIVE_INT128
    const uint128 nn = (static_cast<uint128>(n.hi) << 64) | n.lo;
    const uint128 q = nn / d;
    rem = static_cast<std::uint64_t>(nn % d);
    return {static_cast<std::uint64_t>(q >> 64), static_cast<std::uint64_t>(q)};
#else
    U128 q;
    std::uint64_t r = 0;
    for (int i = 127; i >= 0; --i) {
        const std::uint64_t bit = i >= 64 ? (n.hi >> (i - 64)) & 1u : (n.lo >> i) & 1u;
        r = (r << 1) | bit;  // r < d < 2^63, so no overflow
        if (r >= d) {
            r -= d;
            if (i >= 64)
                q.hi |= std::uint64_t{1} << (i - 64);
            else
                q.lo |= std::uint64_t{1} << i;
        }
    }
    rem = r;
    return q;
#endif
}

// round(a * b / c) with the given rounding mode; nullopt on overflow or c == 0.
std::optional<std::int64_t> mul_div(std::int64_t a, std::int64_t b, std::int64_t c,
                                    Rounding mode) noexcept {
    if (c == 0) return std::nullopt;
    const bool neg = ((a < 0) != (b < 0)) != (c < 0);
    const std::uint64_t uc = magnitude(c);
    if (uc > kInt64MaxMag) return std::nullopt;  // c == INT64_MIN; never used

    std::uint64_t rem = 0;
    U128 q = divmod(mul_64x64(magnitude(a), magnitude(b)), uc, rem);
    if (q.hi != 0) return std::nullopt;
    std::uint64_t mag = q.lo;

    bool bump = false;  // increase magnitude by one unit
    if (rem != 0) {
        switch (mode) {
            case Rounding::TowardZero: break;
            case Rounding::Down: bump = neg; break;
            case Rounding::Up: bump = !neg; break;
            case Rounding::HalfUp: bump = rem >= uc - rem; break;  // 2*rem >= c without overflow
        }
    }
    if (bump) {
        if (mag == std::numeric_limits<std::uint64_t>::max()) return std::nullopt;
        ++mag;
    }

    if (neg) {
        if (mag > kInt64MinMag) return std::nullopt;
        return mag == kInt64MinMag ? std::numeric_limits<std::int64_t>::min()
                                   : -static_cast<std::int64_t>(mag);
    }
    if (mag > kInt64MaxMag) return std::nullopt;
    return static_cast<std::int64_t>(mag);
}

constexpr bool is_digit(char c) noexcept { return c >= '0' && c <= '9'; }

}  // namespace

std::optional<Decimal> Decimal::parse(std::string_view s) noexcept {
    std::size_t i = 0;
    bool neg = false;
    if (i < s.size() && (s[i] == '-' || s[i] == '+')) {
        neg = s[i] == '-';
        ++i;
    }

    constexpr std::uint64_t kMaxIntPart = kInt64MinMag / static_cast<std::uint64_t>(kOne);
    std::uint64_t int_part = 0;
    std::size_t digits = 0;
    for (; i < s.size() && is_digit(s[i]); ++i, ++digits) {
        int_part = int_part * 10 + static_cast<std::uint64_t>(s[i] - '0');
        if (int_part > kMaxIntPart) return std::nullopt;
    }

    std::uint64_t frac = 0;
    if (i < s.size() && s[i] == '.') {
        ++i;
        int frac_digits = 0;
        for (; i < s.size() && is_digit(s[i]); ++i, ++digits) {
            if (frac_digits < kScale) {
                frac = frac * 10 + static_cast<std::uint64_t>(s[i] - '0');
                ++frac_digits;
            } else if (s[i] != '0') {
                return std::nullopt;  // more precision than we can represent exactly
            }
        }
        for (; frac_digits < kScale; ++frac_digits) frac *= 10;
    }

    if (digits == 0 || i != s.size()) return std::nullopt;

    const std::uint64_t mag = int_part * static_cast<std::uint64_t>(kOne) + frac;
    if (neg) {
        if (mag > kInt64MinMag) return std::nullopt;
        return from_raw(mag == kInt64MinMag ? std::numeric_limits<std::int64_t>::min()
                                            : -static_cast<std::int64_t>(mag));
    }
    if (mag > kInt64MaxMag) return std::nullopt;
    return from_raw(static_cast<std::int64_t>(mag));
}

std::string Decimal::to_string() const {
    const std::uint64_t mag = magnitude(raw_);
    const std::uint64_t one = static_cast<std::uint64_t>(kOne);
    std::string out = raw_ < 0 ? "-" : "";
    out += std::to_string(mag / one);

    std::uint64_t frac = mag % one;
    if (frac != 0) {
        char buf[kScale];
        for (int k = kScale - 1; k >= 0; --k) {
            buf[k] = static_cast<char>('0' + frac % 10);
            frac /= 10;
        }
        int len = kScale;
        while (buf[len - 1] == '0') --len;
        out += '.';
        out.append(buf, static_cast<std::size_t>(len));
    }
    return out;
}

std::optional<Decimal> Decimal::mul(Decimal a, Decimal b, Rounding r) noexcept {
    auto raw = mul_div(a.raw_, b.raw_, kOne, r);
    if (!raw) return std::nullopt;
    return from_raw(*raw);
}

std::optional<Decimal> Decimal::div(Decimal a, Decimal b, Rounding r) noexcept {
    auto raw = mul_div(a.raw_, kOne, b.raw_, r);
    if (!raw) return std::nullopt;
    return from_raw(*raw);
}

Decimal Decimal::round_to(Decimal step, Rounding r) const noexcept {
    assert(step.raw_ > 0);
    const std::int64_t s = step.raw_;
    std::int64_t q = raw_ / s;  // truncated toward zero
    const std::int64_t rem = raw_ % s;
    if (rem != 0) {
        switch (r) {
            case Rounding::TowardZero: break;
            case Rounding::Down:
                if (raw_ < 0) --q;
                break;
            case Rounding::Up:
                if (raw_ > 0) ++q;
                break;
            case Rounding::HalfUp: {
                const std::uint64_t mrem = magnitude(rem);
                if (mrem >= static_cast<std::uint64_t>(s) - mrem) q += raw_ < 0 ? -1 : 1;
                break;
            }
        }
    }
    return from_raw(q * s);
}

std::ostream& operator<<(std::ostream& os, Decimal d) { return os << d.to_string(); }

}  // namespace binance
