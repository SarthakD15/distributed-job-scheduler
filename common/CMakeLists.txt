add_library(common STATIC
    src/util.cpp
    src/messaging.cpp)

target_include_directories(common PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/include")

# PUBLIC: anything linking 'common' also gets ZeroMQ + jsoncons paths/libs.
target_link_libraries(common PUBLIC ext_deps)