add_executable(jobctl
    src/main.cpp
    src/jobctl.cpp
    src/failure_explainer.cpp)

target_include_directories(jobctl PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}/include")

target_link_libraries(jobctl PRIVATE common CURL::libcurl)

copy_to_run_bin(jobctl)