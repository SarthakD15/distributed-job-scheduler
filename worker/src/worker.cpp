#include "worker.h"

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "messaging.h"

namespace jobsched
{

namespace
{
// While a job runs we check on it often. When idle we still wake up
// regularly, so heartbeats go out on time.
const int kBusyPollMs = 50;
const int kIdlePollMs = 250;

// Each job's stdout + stderr is written to <kLogDir>/<job id>.log
const char* const kLogDir = "logs";
}

Worker::Worker(const std::string& workerId)
    : m_workerId(workerId),
      m_context(1),
      m_socket(m_context, zmq::socket_type::dealer),
      m_executor(),
      m_currentJobId(),
      m_lastHeartbeatSent(),
      m_lastBrokerContact(),
      m_brokerAlive(true)
{
    // The routing id is what the broker's ROUTER sees as frames[0].
    // Must be set BEFORE connect, otherwise ZeroMQ picks a random binary id.
    m_socket.set(zmq::sockopt::routing_id, m_workerId);
    m_socket.set(zmq::sockopt::linger, 0);
    m_socket.connect(kBackendConnect);

    std::filesystem::create_directories(kLogDir);

    std::cout << "[" << m_workerId << "] connected to " << kBackendConnect << std::endl;
}

Worker::~Worker()
{
    m_socket.close();
    std::cout << "[" << m_workerId << "] stopped" << std::endl;
}

void Worker::run()
{
    sendToBroker(MessageType::Ready, "");
    m_lastHeartbeatSent = std::chrono::steady_clock::now();
    m_lastBrokerContact = m_lastHeartbeatSent;

    zmq::pollitem_t items[] = {
        { m_socket.handle(), 0, ZMQ_POLLIN, 0 }
    };

    while (true)
    {
        int timeoutMs = m_executor.isRunning() ? kBusyPollMs : kIdlePollMs;
        zmq::poll(items, 1, std::chrono::milliseconds(timeoutMs));

        if (items[0].revents & ZMQ_POLLIN)
        {
            onBrokerMessage();
        }

        if (m_executor.isRunning() && m_executor.poll())
        {
            finishJob();
        }

        sendHeartbeatIfDue();
        checkBrokerAlive();
    }
}

void Worker::onBrokerMessage()
{
    std::vector<std::string> frames;
    receiveFrames(m_socket, frames);

    // From the broker's ROUTER to our DEALER: [type][payload]
    if (frames.size() != 2)
    {
        std::cout << "[" << m_workerId << "] dropped malformed message ("
                  << frames.size() << " frames)" << std::endl;
        return;
    }

    // Any message proves the broker is alive, not just acks.
    m_lastBrokerContact = std::chrono::steady_clock::now();
    if (!m_brokerAlive)
    {
        m_brokerAlive = true;
        std::cout << "[" << m_workerId << "] broker is back" << std::endl;
    }

    MessageType type = messageTypeFromString(frames[0]);
    if (type == MessageType::Job)
    {
        startJob(frames[1]);
    }
    else if (type == MessageType::HeartbeatAck)
    {
        // Nothing
    }
    else
    {
        std::cout << "[" << m_workerId << "] unexpected message '"
                  << frames[0] << "'" << std::endl;
    }
}

void Worker::startJob(const std::string& payload)
{
    JobSpec spec;
    try
    {
        spec = jobSpecFromJson(payload);
    }
    catch (const std::runtime_error& e)
    {
        // Without a job id we cannot report a result; just ask for more work.
        std::cout << "[" << m_workerId << "] bad job payload: " << e.what() << std::endl;
        sendToBroker(MessageType::Ready, "");
        return;
    }

    if (m_executor.isRunning())
    {
        // The broker only sends jobs to idle workers, so this means a bug.
        std::cout << "[" << m_workerId << "] busy with " << m_currentJobId
                  << ", ignoring " << spec.id << std::endl;
        return;
    }

    m_currentJobId = spec.id;

    std::string logPath = std::string(kLogDir) + "/" + spec.id + ".log";
    if (!m_executor.start(spec.command, logPath))
    {
        JobResult result;
        result.jobId    = spec.id;
        result.exitCode = -1;
        result.error    = m_executor.errorMessage();

        std::cout << "[" << m_workerId << "] " << spec.id << " could not start: "
                  << result.error << std::endl;
        sendResult(result);
        return;
    }

    std::cout << "[" << m_workerId << "] started " << spec.id << " (log: " << logPath << ")" << std::endl;
}

void Worker::finishJob()
{
    JobResult result;
    result.jobId      = m_currentJobId;
    result.exitCode   = m_executor.exitCode();
    result.output     = m_executor.output();
    result.durationMs = m_executor.durationMs();
    result.error      = m_executor.errorMessage();   // normally empty

    std::cout << "[" << m_workerId << "] finished " << result.jobId
              << " (exit " << result.exitCode << ", " << result.durationMs << " ms)" << std::endl;
    sendResult(result);
}

void Worker::sendResult(const JobResult& result)
{
    sendToBroker(MessageType::Result, jobResultToJson(result));
    m_currentJobId.clear();
}

void Worker::sendHeartbeatIfDue()
{
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    if (now - m_lastHeartbeatSent < std::chrono::milliseconds(kHeartbeatIntervalMs))
    {
        return;
    }

    // The payload tells the broker what we are doing. A broker that has just
    // restarted (or wrongly declared us dead) uses it to take us back.
    sendToBroker(MessageType::Heartbeat, m_currentJobId);
    m_lastHeartbeatSent = now;
}

void Worker::checkBrokerAlive()
{
    if (!m_brokerAlive)
    {
        return;   // already reported; keep heartbeating until it answers
    }

    std::chrono::steady_clock::duration silence = std::chrono::steady_clock::now() - m_lastBrokerContact;
    if (silence > std::chrono::milliseconds(kHeartbeatTimeoutMs))
    {
        m_brokerAlive = false;

        // ZeroMQ reconnects the TCP connection by itself, and messages we send
        // meanwhile (heartbeats, a finished job's RESULT) are queued and
        // delivered once the broker is back. So we only report it and carry on;
        // a running job keeps running.
        std::cout << "[" << m_workerId << "] lost contact with broker, waiting for it to come back"
                  << std::endl;
    }
}

void Worker::sendToBroker(MessageType type, const std::string& payload)
{
    std::vector<std::string> frames;
    frames.push_back(toString(type));
    frames.push_back(payload);
    sendFrames(m_socket, frames);
}

} // namespace jobsched