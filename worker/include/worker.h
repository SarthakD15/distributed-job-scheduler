#ifndef JOBSCHED_WORKER_H
#define JOBSCHED_WORKER_H

#include <chrono>
#include <string>

#include <zmq.hpp>

#include "process_executor.h"
#include "util.h"

namespace jobsched
{

// Connects to the broker, receives jobs and runs them one at a time.
// Single-threaded: the loop alternates between checking the socket and
// checking the running process, so it never blocks on either. That is what
// lets it keep sending heartbeats while a long job runs.
class Worker
{
public:
    explicit Worker(const std::string& workerId);
    ~Worker();

    // Blocks forever.
    void run();

private:
    void onBrokerMessage();
    void startJob(const std::string& payload);
    void finishJob();
    void sendResult(const JobResult& result);
    void sendHeartbeatIfDue();
    void checkBrokerAlive();
    void sendToBroker(MessageType type, const std::string& payload);

    std::string     m_workerId;
    zmq::context_t  m_context;
    zmq::socket_t   m_socket;        // DEALER, connects to the broker backend
    ProcessExecutor m_executor;      // destroyed first: kills a running job before the socket closes
    std::string     m_currentJobId;  // empty when idle

    std::chrono::steady_clock::time_point m_lastHeartbeatSent;
    std::chrono::steady_clock::time_point m_lastBrokerContact;  // last message of any kind from the broker
    bool                                  m_brokerAlive;
};

} // namespace jobsched

#endif // JOBSCHED_WORKER_H