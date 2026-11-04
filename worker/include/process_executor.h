#ifndef JOBSCHED_PROCESS_EXECUTOR_H
#define JOBSCHED_PROCESS_EXECUTOR_H

#include <chrono>
#include <string>
#include <vector>

#include <reproc++/reproc.hpp>

namespace jobsched
{

// Runs one external process at a time without blocking the caller.
// Thin wrapper around reproc++, which handles the OS-specific parts
// (starting the process, quoting arguments, waiting, killing) on both
// Windows and Linux.
//
//   start()  launches the process; stdout + stderr go to a log file
//   poll()   non-blocking "has it finished?"; call it from the worker loop
//   kill()   stops the process; the next poll() then reports it finished
class ProcessExecutor
{
public:
    ProcessExecutor();
    ~ProcessExecutor();

    // Returns false if the process could not be started; errorMessage() says why.
    bool start(const std::vector<std::string>& command, const std::string& logPath);

    // Returns true once the process has finished. After that the getters are valid.
    bool poll();

    void kill();

    bool               isRunning() const;
    int                exitCode() const;
    const std::string& output() const;        // first 64 KB of the log, valid UTF-8
    long long          durationMs() const;
    const std::string& errorMessage() const;

private:
    void finish(int exitCode);

    reproc::process                       m_process;
    std::string                           m_logPath;
    bool                                  m_running;
    int                                   m_exitCode;
    std::string                           m_output;
    std::chrono::steady_clock::time_point m_startTime;
    long long                             m_durationMs;
    std::string                           m_errorMessage;
};

} // namespace jobsched

#endif // JOBSCHED_PROCESS_EXECUTOR_H