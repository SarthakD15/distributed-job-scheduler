#include "process_executor.h"

#include <filesystem>
#include <fstream>
#include <system_error>

#include <reproc/reproc.h>   // REPROC_ETIMEDOUT

namespace jobsched
{

namespace
{

// Only this much of the log is sent back to the broker. The full log
// stays on disk in the worker's logs folder.
const std::size_t kMaxOutputBytes = 64 * 1024;

bool isTimeout(const std::error_code& ec)
{
    return ec.value() == -REPROC_ETIMEDOUT;
}

// Reads at most maxBytes from the start of a file.
std::string readFileHead(const std::string& path, std::size_t maxBytes, bool& truncated)
{
    truncated = false;
    std::ifstream file(path.c_str(), std::ios::binary);
    if (!file)
    {
        return std::string();
    }

    std::string data(maxBytes, '\0');
    file.read(&data[0], static_cast<std::streamsize>(maxBytes));
    data.resize(static_cast<std::size_t>(file.gcount()));

    // If there is still something left to read, the file was bigger than the limit.
    truncated = (file.peek() != std::char_traits<char>::eof());
    return data;
}

// Replaces every byte that is not part of a valid UTF-8 character with '?'.
// Programs can print anything (e.g. Windows console code pages), but JSON
// must be valid UTF-8, otherwise the broker cannot parse the RESULT.
std::string sanitizeUtf8(const std::string& text)
{
    std::string clean;
    clean.reserve(text.size());

    std::size_t i = 0;
    while (i < text.size())
    {
        unsigned char lead = static_cast<unsigned char>(text[i]);

        std::size_t   length = 0;
        unsigned long code   = 0;
        if (lead < 0x80)                    { length = 1; code = lead; }
        else if (lead >= 0xC2 && lead <= 0xDF) { length = 2; code = lead & 0x1F; }
        else if (lead >= 0xE0 && lead <= 0xEF) { length = 3; code = lead & 0x0F; }
        else if (lead >= 0xF0 && lead <= 0xF4) { length = 4; code = lead & 0x07; }

        bool valid = (length > 0) && (i + length <= text.size());
        for (std::size_t k = 1; valid && k < length; ++k)
        {
            unsigned char next = static_cast<unsigned char>(text[i + k]);
            valid = (next & 0xC0) == 0x80;
            code  = (code << 6) | (next & 0x3F);
        }

        // Reject over-long forms, UTF-16 surrogates and values above U+10FFFF.
        if (valid)
        {
            bool overlong  = (length == 3 && code < 0x800) || (length == 4 && code < 0x10000);
            bool surrogate = code >= 0xD800 && code <= 0xDFFF;
            valid = !overlong && !surrogate && code <= 0x10FFFF;
        }

        if (valid)
        {
            clean.append(text, i, length);
            i += length;
        }
        else
        {
            clean.push_back('?');
            ++i;
        }
    }
    return clean;
}

} // anonymous namespace

ProcessExecutor::ProcessExecutor()
    : m_process(),
      m_logPath(),
      m_running(false),
      m_exitCode(-1),
      m_output(),
      m_startTime(),
      m_durationMs(0),
      m_errorMessage()
{
}

ProcessExecutor::~ProcessExecutor()
{
    // Never leave a job running behind when the worker shuts down.
    if (m_running)
    {
        kill();
    }
}

bool ProcessExecutor::start(const std::vector<std::string>& command, const std::string& logPath)
{
    if (m_running)
    {
        m_errorMessage = "a process is already running";
        return false;
    }

    m_process      = reproc::process();
    m_logPath      = logPath;
    m_exitCode     = -1;
    m_output.clear();
    m_durationMs   = 0;
    m_errorMessage.clear();

    if (command.empty())
    {
        m_errorMessage = "empty command";
        return false;
    }

    std::error_code ignored;
    std::filesystem::remove(m_logPath, ignored);

    reproc::options options;
    options.redirect.in.type  = reproc::redirect::discard;   // no keyboard input
    options.redirect.out.type = reproc::redirect::path_;     // stdout -> log file
    options.redirect.out.path = m_logPath.c_str();
    options.redirect.err.type = reproc::redirect::stdout_;   // stderr -> same log file

    // When the reproc::process object is destroyed while the child still runs:
    // kill it and wait at most 2 seconds for it to go away.
    options.stop.first.action  = reproc::stop::kill;
    options.stop.first.timeout = reproc::milliseconds(2000);

    std::error_code ec = m_process.start(command, options);
    if (ec)
    {
        m_errorMessage = "failed to start '" + command[0] + "': " + ec.message();
        return false;
    }

    m_startTime = std::chrono::steady_clock::now();
    m_running   = true;
    return true;
}

bool ProcessExecutor::poll()
{
    if (!m_running)
    {
        return false;
    }

    // wait() with a timeout of 0 returns immediately.
    std::pair<int, std::error_code> status = m_process.wait(reproc::milliseconds(0));
    if (isTimeout(status.second))
    {
        return false;   // still running
    }

    if (status.second)
    {
        // Not a timeout but some other failure: report the job as failed.
        m_errorMessage = "waiting for process failed: " + status.second.message();
        finish(-1);
        return true;
    }

    finish(status.first);
    return true;
}

void ProcessExecutor::kill()
{
    if (m_running)
    {
        m_process.kill();
    }
}

bool ProcessExecutor::isRunning() const
{
    return m_running;
}

int ProcessExecutor::exitCode() const
{
    return m_exitCode;
}

const std::string& ProcessExecutor::output() const
{
    return m_output;
}

long long ProcessExecutor::durationMs() const
{
    return m_durationMs;
}

const std::string& ProcessExecutor::errorMessage() const
{
    return m_errorMessage;
}

void ProcessExecutor::finish(int exitCode)
{
    m_running    = false;
    m_exitCode   = exitCode;
    m_durationMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::steady_clock::now() - m_startTime).count();

    bool truncated = false;
    m_output = sanitizeUtf8(readFileHead(m_logPath, kMaxOutputBytes, truncated));
    if (truncated)
    {
        m_output += "\n[output truncated - full log: " + m_logPath + "]";
    }
}

} // namespace jobsched