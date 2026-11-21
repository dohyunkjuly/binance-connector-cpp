# One executable per example, named after the file: market_data/candles.cpp -> candles.
file(GLOB example_sources CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/market_data/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/account/*.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/trading/*.cpp)
foreach(source ${example_sources})
    get_filename_component(name ${source} NAME_WE)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE binance::connector)
endforeach()
