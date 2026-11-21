#pragma once

// Shared by the examples: the client configuration.

#include <binance/binance.hpp>

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string_view>

namespace examples {

using namespace binance;

/// Prints the library's warnings and errors to stderr; keeps its info lines out of the output.
class WarningsOnly : public Logger {
public:
    void log(LogLevel level, std::string_view msg) override {
        if (level >= LogLevel::Warn) std::cerr << "[binance] " << msg << "\n";
    }
};

/// Market data: mainnet, no API key needed.
inline Config public_config() {
    Config cfg;
    cfg.logger = std::make_shared<WarningsOnly>();
    return cfg;
}

/// Account and trading: Demo trading (https://demo.binance.com), keys from
/// BINANCE_API_KEY / BINANCE_API_SECRET.
inline Config demo_config() {
    const char* key = std::getenv("BINANCE_API_KEY");
    const char* secret = std::getenv("BINANCE_API_SECRET");
    if (!key || !secret) {
        std::cerr << "set BINANCE_API_KEY and BINANCE_API_SECRET (Demo trading keys)\n";
        std::exit(1);
    }
    Config cfg;
    cfg.environment = Environment::Demo;  // remove for mainnet
    cfg.credentials = {key, secret, ""};
    cfg.logger = std::make_shared<WarningsOnly>();
    return cfg;
}

}  // namespace examples
