cmake_minimum_required(VERSION 3.16)
project(JobScheduler LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ------------------------------------------------------------------
# External dependencies root (headers in include/, libs in lib/, dlls in bin/)
# The project lives at <root>/ccpp_projects/project, so <root> is two levels up.
# Can be overridden with: cmake -DEXT_ROOT=<path> ...
# ------------------------------------------------------------------
get_filename_component(DEFAULT_EXT_ROOT "${CMAKE_SOURCE_DIR}/../.." ABSOLUTE)
set(EXT_ROOT "${DEFAULT_EXT_ROOT}" CACHE PATH "Root folder of external installs")
message(STATUS "EXT_ROOT = ${EXT_ROOT}")

# ------------------------------------------------------------------
# Output folders
#   build/bin : where the compiler puts the exes (safe to delete with build/)
#   run/bin   : our permanent run folder; exes are copied here after each build
# ------------------------------------------------------------------
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
set(RUN_BIN_DIR "${CMAKE_SOURCE_DIR}/run/bin")
file(MAKE_DIRECTORY "${RUN_BIN_DIR}")

# Copies a built executable into run/bin after every successful build.
function(copy_to_run_bin target)
    add_custom_command(TARGET ${target} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
                $<TARGET_FILE:${target}> "${RUN_BIN_DIR}"
        COMMENT "Copying ${target} to run/bin")
endfunction()

# ------------------------------------------------------------------
# ZeroMQ (libzmq) + cppzmq + jsoncons
# ------------------------------------------------------------------
find_library(ZMQ_LIBRARY NAMES zmq libzmq PATHS "${EXT_ROOT}/lib" NO_DEFAULT_PATH)
if(NOT ZMQ_LIBRARY)
    message(FATAL_ERROR "libzmq not found in ${EXT_ROOT}/lib")
endif()
message(STATUS "ZMQ_LIBRARY = ${ZMQ_LIBRARY}")

# Copy the libzmq dll into run/bin so the exes can run from there.
file(GLOB ZMQ_DLLS "${EXT_ROOT}/bin/libzmq*.dll")
if(ZMQ_DLLS)
    file(COPY ${ZMQ_DLLS} DESTINATION "${RUN_BIN_DIR}")
else()
    message(WARNING "No libzmq*.dll found in ${EXT_ROOT}/bin - copy it to run/bin manually")
endif()

# One interface target that carries all external include paths and libraries.
# Every component links to this instead of repeating paths.
add_library(ext_deps INTERFACE)
target_include_directories(ext_deps INTERFACE
    "${EXT_ROOT}/include"
    "${EXT_ROOT}/include/zmq")
target_link_libraries(ext_deps INTERFACE ${ZMQ_LIBRARY} ws2_32 iphlpapi)

find_library(REPROCXX_LIBRARY NAMES reproc++ PATHS "${EXT_ROOT}/lib" NO_DEFAULT_PATH)
find_library(REPROC_LIBRARY   NAMES reproc   PATHS "${EXT_ROOT}/lib" NO_DEFAULT_PATH)
if(NOT REPROCXX_LIBRARY OR NOT REPROC_LIBRARY)
    message(FATAL_ERROR "reproc/reproc++ not found in ${EXT_ROOT}/lib")
endif()

add_library(reproc_deps INTERFACE)
target_include_directories(reproc_deps INTERFACE "${EXT_ROOT}/include")
# Order matters for static libs: reproc++ uses reproc, reproc uses ws2_32.
target_link_libraries(reproc_deps INTERFACE ${REPROCXX_LIBRARY} ${REPROC_LIBRARY} ws2_32)

# ------------------------------------------------------------------
# libpqxx (PostgreSQL C++ client; used by the broker only)
# Installed with pacman into msys64/mingw64, which CMake searches by default.
# Its package file also pulls in libpq (PostgreSQL's C library).
# ------------------------------------------------------------------
find_package(libpqxx CONFIG REQUIRED)

# libcurl (HTTP client for the LLM API; used by jobctl only)
find_package(CURL REQUIRED)
# ------------------------------------------------------------------
# Components
# ------------------------------------------------------------------
add_subdirectory(common)
add_subdirectory(broker)
add_subdirectory(worker)   # enabled in feature B
add_subdirectory(jobctl)