#ifndef JOBSCHED_BROKER_H
#define JOBSCHED_BROKER_H

#include <chrono>
#include <deque>
#include <map>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "job_store.h"
#include "util.h"
#include "worker_registry.h"

namespace jobsched
{

// Central scheduler.
//  - frontend (ROUTER): jobctl submits jobs and asks for status/list
//  - backend  (ROUTER): workers say READY, receive jobs, send back RESULTs
// Jobs wait in a FIFO queue until a worker is idle, then get dispatched.
// Workers that stop sending heartbeats are removed and their jobs re-queued.
// Every job change is written to the JobStore first, so a restarted broker
// continues where the previous one stopped.
class Broker
{
public:
    // The store is created by main() and passed in (dependency injection):
    // the broker works with any JobStore, not just PostgreSQL.
    explicit Broker(JobStore& store);
    ~Broker();

    // Blocks forever, polling both sockets.
    void run();

private:
    // ---- frontend (clients) ----
    void onFrontendMessage();
    void handleClientRequest(const std::string& clientId,
                             MessageType type,
                             const std::string& payload);
    void handleSubmit(const std::string& clientId, const std::string& payload);
    void handleStatus(const std::string& clientId, const std::string& jobId);
    void handleList(const std::string& clientId);
    void replyToClient(const std::string& clientId,
                       MessageType type,
                       const std::string& payload);

    // ---- backend (workers) ----
    void onBackendMessage();
    void handleReady(const std::string& workerId);
    void handleHeartbeat(const std::string& workerId, const std::string& runningJobId, bool isNewWorker);
    void handleResult(const std::string& workerId, const std::string& payload);
    void sendToWorker(const std::string& workerId,
                      MessageType type,
                      const std::string& payload);

    // ---- scheduling ----
    // Pairs waiting jobs with idle workers until one of the two runs out.
    void dispatchJobs();
    // Removes silent workers and puts their running jobs back in the queue.
    void removeDeadWorkers();
    void requeueJob(const std::string& jobId, const std::string& deadWorkerId);

    // ---- recovery after a restart ----
    // Loads every job from the store; QUEUED jobs go back in the queue.
    void loadJobs();
    // Called once the grace period is over: RUNNING jobs that no worker
    // has claimed through its heartbeat are re-queued.
    void requeueUnclaimedJobs();

    JobStore&                        m_store;         // durable copy; written before memory changes
    zmq::context_t                   m_context;
    zmq::socket_t                    m_frontend;      // ROUTER, faces jobctl
    zmq::socket_t                    m_backend;       // ROUTER, faces workers
    std::map<std::string, JobRecord> m_jobs;          // job id -> record (fast lookup)
    std::vector<std::string>         m_jobOrder;      // job ids in submission order (for list)
    std::deque<std::string>          m_jobQueue;      // ids of QUEUED jobs, oldest first
    WorkerRegistry                   m_workers;       // connected workers, idle queue, liveness

    bool                                  m_recoveryPending;   // true until the grace period has passed
    std::chrono::steady_clock::time_point m_recoveryDeadline;
};

} // namespace jobsched

#endif // JOBSCHED_BROKER_H