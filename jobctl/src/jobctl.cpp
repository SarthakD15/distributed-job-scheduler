#include "jobctl.h"

#include <iomanip>
#include <iostream>
#include <stdexcept>

#include "messaging.h"
#include "failure_explainer.h"

namespace jobsched
{

namespace
{

const int kReplyTimeoutMs = 3000;

// Joins the command back into one line for display only.
std::string joinCommand(const std::vector<std::string>& command)
{
    std::string line;
    for (std::size_t i = 0; i < command.size(); ++i)
    {
        if (i > 0)
        {
            line += " ";
        }
        line += command[i];
    }
    return line;
}

// Exit code for finished jobs, "-" for jobs that have not finished.
std::string exitColumn(const JobRecord& record)
{
    if (record.state == JobState::Succeeded || record.state == JobState::Failed)
    {
        return std::to_string(record.result.exitCode);
    }
    return "-";
}

} // anonymous namespace

JobCtl::JobCtl()
    : m_context(1),
      m_socket(m_context, zmq::socket_type::req)
{
    m_socket.set(zmq::sockopt::linger, 0);
    m_socket.set(zmq::sockopt::rcvtimeo, kReplyTimeoutMs);
    m_socket.connect(kFrontendConnect);
}

JobCtl::~JobCtl()
{
    m_socket.close();
}

int JobCtl::submit(const std::vector<std::string>& command)
{
    JobSpec spec;
    spec.command = command;

    MessageType replyType = MessageType::Unknown;
    std::string replyPayload;
    bool reachable = request(MessageType::Submit, jobSpecToJson(spec), replyType, replyPayload);
    if (!reachable || replyType != MessageType::SubmitReply)
    {
        return reportFailure(reachable, replyPayload);
    }

    std::cout << "submitted " << replyPayload << std::endl;
    return 0;
}

int JobCtl::status(const std::string& jobId)
{
    JobRecord record;
    if (!fetchRecord(jobId, record))
    {
        return 1;
    }

    std::cout << "id       : " << record.spec.id << "\n"
              << "state    : " << toString(record.state) << "\n"
              << "worker   : " << (record.workerId.empty() ? "-" : record.workerId) << "\n"
              << "command  : " << joinCommand(record.spec.command) << "\n";

    bool finished = record.state == JobState::Succeeded || record.state == JobState::Failed;
    if (finished)
    {
        std::cout << "exit     : " << record.result.exitCode << "\n"
                  << "duration : " << record.result.durationMs << " ms\n";
        if (!record.result.error.empty())
        {
            std::cout << "error    : " << record.result.error << "\n";
        }
        std::cout << "------------- output -------------\n"
                  << record.result.output;
        if (!record.result.output.empty() && record.result.output.back() != '\n')
        {
            std::cout << "\n";
        }
        std::cout << "----------------------------------\n";
    }
    std::cout.flush();
    return 0;
}

int JobCtl::explain(const std::string& jobId)
{
    JobRecord record;
    if (!fetchRecord(jobId, record))
    {
        return 1;
    }

    if (record.state == JobState::Queued || record.state == JobState::Running)
    {
        std::cout << jobId << " has not finished yet (" << toString(record.state) << ")" << std::endl;
        return 1;
    }
    if (record.state == JobState::Succeeded)
    {
        std::cout << jobId << " succeeded, nothing to explain" << std::endl;
        return 0;
    }

    // The model is called from jobctl, never from the broker: a model can take
    // many seconds to answer, and the broker's single-threaded loop must not
    // stop handling heartbeats and results while it waits.
    FailureExplainer explainer;
    std::cout << jobId << " FAILED (exit " << record.result.exitCode << "), asking model '"
              << explainer.model() << "'..." << std::endl;

    std::string diagnosis;
    std::string error;
    if (!explainer.explain(record, diagnosis, error))
    {
        std::cerr << "error: could not get a diagnosis: " << error << "\n"
                  << "(the job's own output is still available with: jobctl status " << jobId << ")"
                  << std::endl;
        return 1;
    }

    std::cout << "\nAI suggestion (may be wrong - check the log):\n"
              << diagnosis << std::endl;
    return 0;
}

int JobCtl::list()
{
    MessageType replyType = MessageType::Unknown;
    std::string replyPayload;
    bool reachable = request(MessageType::List, "", replyType, replyPayload);
    if (!reachable || replyType != MessageType::ListReply)
    {
        return reportFailure(reachable, replyPayload);
    }

    std::vector<JobRecord> records;
    try
    {
        records = jobRecordListFromJson(replyPayload);
    }
    catch (const std::runtime_error& e)
    {
        std::cerr << "error: bad reply from broker: " << e.what() << std::endl;
        return 1;
    }

    if (records.empty())
    {
        std::cout << "no jobs" << std::endl;
        return 0;
    }

    std::cout << std::left
              << std::setw(10) << "ID"
              << std::setw(12) << "STATE"
              << std::setw(14) << "WORKER"
              << std::setw(6)  << "EXIT"
              << "COMMAND" << "\n";
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        std::cout << std::setw(10) << records[i].spec.id
                  << std::setw(12) << toString(records[i].state)
                  << std::setw(14) << (records[i].workerId.empty() ? "-" : records[i].workerId)
                  << std::setw(6)  << exitColumn(records[i])
                  << joinCommand(records[i].spec.command) << "\n";
    }
    std::cout.flush();
    return 0;
}

bool JobCtl::request(MessageType type,
                     const std::string& payload,
                     MessageType& replyType,
                     std::string& replyPayload)
{
    // REQ adds the empty delimiter frame itself, so we send only [type][payload].
    std::vector<std::string> frames;
    frames.push_back(toString(type));
    frames.push_back(payload);
    sendFrames(m_socket, frames);

    if (!receiveFrames(m_socket, frames))
    {
        return false;
    }
    if (frames.size() != 2)
    {
        replyType    = MessageType::Error;
        replyPayload = "malformed reply from broker";
        return true;
    }

    replyType    = messageTypeFromString(frames[0]);
    replyPayload = frames[1];
    return true;
}

bool JobCtl::fetchRecord(const std::string& jobId, JobRecord& record)
{
    MessageType replyType = MessageType::Unknown;
    std::string replyPayload;
    bool reachable = request(MessageType::Status, jobId, replyType, replyPayload);
    if (!reachable || replyType != MessageType::StatusReply)
    {
        reportFailure(reachable, replyPayload);
        return false;
    }

    try
    {
        record = jobRecordFromJson(replyPayload);
    }
    catch (const std::runtime_error& e)
    {
        std::cerr << "error: bad reply from broker: " << e.what() << std::endl;
        return false;
    }
    return true;
}

int JobCtl::reportFailure(bool reachable, const std::string& replyPayload)
{
    if (!reachable)
    {
        std::cerr << "error: broker not reachable at " << kFrontendConnect << std::endl;
    }
    else
    {
        std::cerr << "error: " << replyPayload << std::endl;
    }
    return 1;
}

} // namespace jobsched