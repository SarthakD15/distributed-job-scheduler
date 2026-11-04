add_executable(worker
    src/main.cpp
    src/worker.cpp
    src/process_executor.cpp)

target_include_directories(worker PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/include")

# Only the worker runs processes, so only the worker links reproc.
target_link_libraries(worker PRIVATE common reproc_deps)

copy_to_run_bin(worker)