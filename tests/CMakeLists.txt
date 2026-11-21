find_package(GTest CONFIG REQUIRED)
include(GoogleTest)

add_executable(binance_unit_tests
    unit/account_test.cpp
    unit/client_test.cpp
    unit/decimal_test.cpp
    unit/http_test.cpp
    unit/websocket_test.cpp
    unit/orderbook_test.cpp
    unit/trading_test.cpp
)

target_link_libraries(binance_unit_tests PRIVATE
    binance::connector
    Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto  # internal headers under test use them
    GTest::gtest_main
)

# Unit tests may reach into src/ to test internal components.
target_include_directories(binance_unit_tests PRIVATE ${PROJECT_SOURCE_DIR}/src)

# MSVC: the Boost.Asio-heavy tests exceed the default number of sections in one object file.
if(MSVC)
    target_compile_options(binance_unit_tests PRIVATE /bigobj /utf-8)
endif()

# PRE_TEST: list the tests when ctest runs, not right after linking (on Windows the vcpkg DLLs
# are copied next to the executable only after that).
gtest_discover_tests(binance_unit_tests DISCOVERY_MODE PRE_TEST)

# Same decimal tests, but forcing the portable 128-bit math that MSVC uses, so the Windows
# code path is exercised on every platform.
add_executable(binance_decimal_portable_tests
    unit/decimal_test.cpp
    ${PROJECT_SOURCE_DIR}/src/decimal.cpp
)
target_include_directories(binance_decimal_portable_tests PRIVATE ${PROJECT_SOURCE_DIR}/include)
target_compile_definitions(binance_decimal_portable_tests PRIVATE BINANCE_FORCE_PORTABLE_INT128)
target_link_libraries(binance_decimal_portable_tests PRIVATE GTest::gtest_main)
gtest_discover_tests(binance_decimal_portable_tests TEST_PREFIX "portable." DISCOVERY_MODE PRE_TEST)
