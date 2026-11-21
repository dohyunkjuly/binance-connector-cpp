cmake_minimum_required(VERSION 3.25)

project(binance_connector
    VERSION 0.1.0
    DESCRIPTION "Binance spot and USD-M futures connector"
    LANGUAGES CXX)

# Tests and examples only when this is the main project, not when used via add_subdirectory /
# FetchContent.
option(BINANCE_BUILD_TESTS    "Build unit tests"            ${PROJECT_IS_TOP_LEVEL})
option(BINANCE_BUILD_EXAMPLES "Build examples"              ${PROJECT_IS_TOP_LEVEL})
option(BINANCE_ENABLE_SPOT    "Compile spot market support" ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
# No C++20 modules here. Without this, CMake >= 3.28 with Clang + Ninja runs clang-scan-deps on
# every file and fails where that tool is not installed (common on Linux).
set(CMAKE_CXX_SCAN_FOR_MODULES OFF)
if(PROJECT_IS_TOP_LEVEL)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

# ---------------------------------------------------------------------------
# Dependencies (provided by vcpkg, see vcpkg.json)
# ---------------------------------------------------------------------------
find_package(Boost REQUIRED CONFIG COMPONENTS system asio beast)
find_package(OpenSSL 3 REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)

# ---------------------------------------------------------------------------
# Library
# ---------------------------------------------------------------------------
add_library(binance_connector
    src/client.cpp
    src/decimal.cpp
    src/http.cpp
    src/print.cpp
    src/websocket.cpp
    src/version.cpp
)
add_library(binance::connector ALIAS binance_connector)

# The public headers need C++20: code that links the library is compiled as C++20 too.
target_compile_features(binance_connector PUBLIC cxx_std_20)

target_include_directories(binance_connector
    PUBLIC  ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(binance_connector
    PUBLIC  Boost::system
    PRIVATE Boost::asio Boost::beast OpenSSL::SSL OpenSSL::Crypto
            simdjson::simdjson spdlog::spdlog
            $<$<PLATFORM_ID:Windows>:crypt32>
)

target_compile_definitions(binance_connector
    PUBLIC
        BOOST_ASIO_NO_DEPRECATED
        $<$<BOOL:${BINANCE_ENABLE_SPOT}>:BINANCE_ENABLE_SPOT=1>
        $<$<PLATFORM_ID:Windows>:_WIN32_WINNT=0x0A00>
)

if(MSVC)
    target_compile_options(binance_connector PRIVATE /W4 /permissive- /utf-8 /bigobj)
else()
    target_compile_options(binance_connector PRIVATE -Wall -Wextra -Wpedantic -Wconversion -Wshadow)
endif()

# ---------------------------------------------------------------------------
# Tests / examples
# ---------------------------------------------------------------------------
if(BINANCE_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(BINANCE_BUILD_EXAMPLES AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/examples/CMakeLists.txt)
    add_subdirectory(examples)
endif()
