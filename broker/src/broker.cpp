#include "broker.h"

#include <chrono>
#include <iostream>
#include <stdexcept>

#include "messaging.h"

namespace jobsched
{

namespace
{
// How long one poll waits before returning even if nothing arrived.
// Dead-worker checks run on this tick, so it must be well below kHeartbeatTimeoutMs.
const int kPollTimeoutMs = 500;
}

Broker::Broker(JobStore& store)
    : m_store(store),
      m_context(1),
      m_frontend(m_context, zmq::socket_type::router),
      m_backend(m_context, zmq::socket_type::router),
      m_jobs(),
      m_jobOrder(),
      m_jobQueue(),
      m_workers(),
      m_recoveryPending(false),
      m_recoveryDeadline()
{
    m_frontend.set(zmq::sockopt::linger, 0);
    m_backend.set(zmq::sockopt::linger, 0);

    m_frontend.bind(kFrontendBind);
    m_backend.bind(kBackendBind);

    std::cout << "[broker] clients on " << kFrontendBind
              << ", workers on " << kBackendBind << std::endl;

    loadJobs();
}

Broker::~Broker()
{
    m_backend.close();
    m_frontend.close();
    std::cout << "[broker] stopped" << std::endl;
}

void Broker::run()
{
    zmq::pollitem_t items[] = {
        { m_frontend.handle(), 0, ZMQ_POLLIN, 0 },
        { m_backend.handle(),  0, ZMQ_POLLIN, 0 }
    };

    while (true)
    {
        zmq::poll(items, 2, std::chrono::milliseconds(kPollTimeoutMs));

        if (items[0].revents & ZMQ_POLLIN)
        {
            onFrontendMessage();
        }
        if (items[1].revents & ZMQ_POLLIN)
        {
            onBackendMessage();
        }

        if (m_recoveryPending && std::chrono::steady_clock::now() >= m_recoveryDeadline)
        {
            requeueUnclaimedJobs();
        }

        removeDeadWorkers();
        dispatchJobs();

        removeDeadWorkers();
        dispatchJobs();
    }
}

// ==================================================================
// Frontend: jobctl
// ==================================================================

void Broker::onFrontendMessage()
{
    std::vector<std::string> frames;
    receiveFrames(m_frontend, frames);

    // A REQ client's message arrives at the ROUTER as:
    //   [client identity][empty delimiter][type][payload]
    if (frames.size() != 4 || !frames[1].empty())
    {
        std::cout << "[broker] dropped malformed client message ("
                  << frames.size() << " frames)" << std::endl;
        return;
    }

    handleClientRequest(frames[0], messageTypeFromString(frames[2]), frames[3]);
}

void Broker::handleClientRequest(const std::string& clientId,
                                 MessageType type,
                                 const std::string& payload)
{
    switch (type)
    {
    case MessageType::Submit:
        handleSubmit(clientId, payload);
        break;
    case MessageType::Status:
        handleStatus(clientId, payload);
        break;
    case MessageType::List:
        handleList(clientId);
        break;
    default:
        replyToClient(clientId, MessageType::Error, "unsupported request type");
        break;
    }
}

void Broker::handleSubmit(const std::string& clientId, const std::string& payload)
{
    JobSpec spec;
    try
    {
        spec = jobSpecFromJson(payload);
    }
    catch (const std::runtime_error& e)
    {
        replyToClient(clientId, MessageType::Error, e.what());
        return;
    }

    if (spec.command.empty())
    {
        replyToClient(clientId, MessageType::Error, "command is empty");
        return;
    }

    // Database first: the client only gets an id once the job is safely stored.
    // The id itself comes from the database, so it never repeats after a restart.
    JobRecord record;
    record.spec    = spec;
    record.spec.id = m_store.createJob(spec);
    record.state   = JobState::Queued;

    m_jobs[record.spec.id] = record;
    m_jobOrder.push_back(record.spec.id);
    m_jobQueue.push_back(record.spec.id);

    std::cout << "[broker] queued " << record.spec.id
              << " : " << record.spec.command[0]
              << " (" << record.spec.command.size() - 1 << " args)" << std::endl;

    replyToClient(clientId, MessageType::SubmitReply, record.spec.id);
}

void Broker::handleStatus(const std::string& clientId, const std::string& jobId)
{
    std::map<std::string, JobRecord>::const_iterator it = m_jobs.find(jobId);
    if (it == m_jobs.end())
    {
        replyToClient(clientId, MessageType::Error, "no such job: " + jobId);
        return;
    }

    replyToClient(clientId, MessageType::StatusReply, jobRecordToJson(it->second));
}

void Broker::handleList(const std::string& clientId)
{
    std::vector<JobRecord> records;
    for (std::size_t i = 0; i < m_jobOrder.size(); ++i)
    {
        records.push_back(m_jobs[m_jobOrder[i]]);
    }

    replyToClient(clientId, MessageType::ListReply, jobRecordListToJson(records));
}

void Broker::replyToClient(const std::string& clientId,
                           MessageType type,
                           const std::string& payload)
{
    // Same envelope back: identity first so ROUTER knows where to send it.
    std::vector<std::string> frames;
    frames.push_back(clientId);
    frames.push_back("");
    frames.push_back(toString(type));
    frames.push_back(payload);
    sendFrames(m_frontend, frames);
}

// ==================================================================
// Backend: workers
// ==================================================================

void Broker::onBackendMessage()
{
    std::vector<std::string> frames;
    receiveFrames(m_backend, frames);

    // A DEALER worker's message arrives at the ROUTER as:
    //   [worker identity][type][payload]      (DEALER adds no empty frame)
    if (frames.size() != 3)
    {
        std::cout << "[broker] dropped malformed worker message ("
                  << frames.size() << " frames)" << std::endl;
        return;
    }

    const std::string& workerId = frames[0];
    MessageType        type     = messageTypeFromString(frames[1]);

    // Any message proves the worker is alive, not just heartbeats.
    bool isNewWorker = m_workers.touch(workerId);
    if (isNewWorker)
    {
        std::cout << "[broker] " << workerId << " connected ("
                  << m_workers.workerCount() << " workers)" << std::endl;
    }

    switch (type)
    {
    case MessageType::Ready:
        handleReady(workerId);
        break;
    case MessageType::Heartbeat:
        handleHeartbeat(workerId, frames[2], isNewWorker);
        break;
    case MessageType::Result:
        handleResult(workerId, frames[2]);
        break;
    default:
        std::cout << "[broker] unexpected message '" << frames[1]
                  << "' from " << workerId << std::endl;
        break;
    }
}

void Broker::handleReady(const std::string& workerId)
{
    m_workers.markIdle(workerId);
    std::cout << "[broker] " << workerId << " is ready ("
              << m_workers.idleCount() << " idle)" << std::endl;
}

void Broker::handleHeartbeat(const std::string& workerId,
                             const std::string& runningJobId,
                             bool isNewWorker)
{
    sendToWorker(workerId, MessageType::HeartbeatAck, "");

    // A heartbeat from a worker we do not know means it was declared dead
    // earlier, or the broker restarted. The heartbeat says what the worker
    // is doing, so we can take it back without it sending READY again.
    // (For known workers our own bookkeeping is newer than the heartbeat,
    // e.g. a job may be on its way to the worker right now, so we ignore it.)
    if (!isNewWorker)
    {
        return;
    }

    if (runningJobId.empty())
    {
        handleReady(workerId);
    }
    else
    {
        m_workers.assignJob(workerId, runningJobId);
        std::cout << "[broker] " << workerId << " is still running "
                  << runningJobId << std::endl;
    }
}

void Broker::handleResult(const std::string& workerId, const std::string& payload)
{
    // Whatever happens below, a worker that sends RESULT is free again.
    JobResult result;
    try
    {
        result = jobResultFromJson(payload);
    }
    catch (const std::runtime_error& e)
    {
        std::cout << "[broker] bad result from " << workerId << ": " << e.what() << std::endl;
        handleReady(workerId);
        return;
    }

    // Accept the result only if this worker is really the one running that job.
    // Anything else is a stale or duplicate result and must not overwrite the record.
    std::map<std::string, JobRecord>::iterator it = m_jobs.find(result.jobId);
    if (it == m_jobs.end()
        || it->second.state != JobState::Running
        || it->second.workerId != workerId)
    {
        std::cout << "[broker] ignored unexpected result for " << result.jobId
                  << " from " << workerId << std::endl;
        handleReady(workerId);
        return;
    }

    JobState finalState = (result.error.empty() && result.exitCode == 0) ? JobState::Succeeded
            : JobState::Failed;
    m_store.markFinished(result.jobId, finalState, result);

    JobRecord& record = it->second;
    record.result = result;
    record.state  = finalState;

    std::cout << "[broker] " << record.spec.id << " " << toString(record.state)
              << " on " << workerId << " (exit " << result.exitCode
              << ", " << result.durationMs << " ms)" << std::endl;

    handleReady(workerId);
}

void Broker::sendToWorker(const std::string& workerId,
                          MessageType type,
                          const std::string& payload)
{
    std::vector<std::string> frames;
    frames.push_back(workerId);
    frames.push_back(toString(type));
    frames.push_back(payload);
    sendFrames(m_backend, frames);
}

// ==================================================================
// Scheduling
// ==================================================================

void Broker::dispatchJobs()
{
    while (!m_jobQueue.empty() && m_workers.hasIdleWorker())
    {
        std::string jobId    = m_jobQueue.front();
        std::string workerId = m_workers.takeIdleWorker();
        m_jobQueue.pop_front();

        // Database before the JOB message: if the broker crashes right after
        // this line, the job is RUNNING in the database but no worker has it.
        // That is safe, because after a restart nobody claims it and it is
        // re-queued. The opposite order could lose track of a job that IS running.
        m_store.markRunning(jobId, workerId);

        JobRecord& record = m_jobs[jobId];
        record.state    = JobState::Running;
        record.workerId = workerId;
        m_workers.assignJob(workerId, jobId);

        sendToWorker(workerId, MessageType::Job, jobSpecToJson(record.spec));
        std::cout << "[broker] dispatched " << jobId << " -> " << workerId << std::endl;
    }
}

void Broker::removeDeadWorkers()
{
    std::vector<WorkerInfo> dead =
        m_workers.removeDead(std::chrono::milliseconds(kHeartbeatTimeoutMs));

    for (std::size_t i = 0; i < dead.size(); ++i)
    {
        std::cout << "[broker] " << dead[i].id << " is dead (no message for "
                  << kHeartbeatTimeoutMs << " ms)" << std::endl;

        if (!dead[i].currentJobId.empty())
        {
            requeueJob(dead[i].currentJobId, dead[i].id);
        }
    }
}

void Broker::requeueJob(const std::string& jobId, const std::string& deadWorkerId)
{
    std::map<std::string, JobRecord>::iterator it = m_jobs.find(jobId);

    // Only re-queue if the job is still recorded as running on that worker.
    if (it == m_jobs.end()
        || it->second.state != JobState::Running
        || it->second.workerId != deadWorkerId)
    {
        return;
    }

    m_store.markQueued(jobId);

    it->second.state = JobState::Queued;
    it->second.workerId.clear();

    // Front of the queue: it has already waited once, it should not wait
    // behind every job submitted since.
    m_jobQueue.push_front(jobId);

    std::cout << "[broker] re-queued " << jobId << " (worker " << deadWorkerId << " is gone)" << std::endl;
}

// ==================================================================
// Recovery after a restart
// ==================================================================

void Broker::loadJobs()
{
    std::vector<JobRecord> records = m_store.loadAllJobs();

    std::size_t queued  = 0;
    std::size_t running = 0;
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        const JobRecord& record = records[i];
        m_jobs[record.spec.id] = record;
        m_jobOrder.push_back(record.spec.id);   // records arrive oldest first

        if (record.state == JobState::Queued)
        {
            m_jobQueue.push_back(record.spec.id);
            ++queued;
        }
        else if (record.state == JobState::Running)
        {
            ++running;
        }
    }

    std::cout << "[broker] loaded " << records.size() << " jobs from the database ("
              << queued << " queued, " << running << " running)" << std::endl;

    // RUNNING jobs may still be running on workers that will reconnect within
    // one heartbeat timeout. Give them that long to claim their jobs.
    if (running > 0)
    {
        m_recoveryPending  = true;
        m_recoveryDeadline = std::chrono::steady_clock::now()
                           + std::chrono::milliseconds(kHeartbeatTimeoutMs);
        std::cout << "[broker] waiting " << kHeartbeatTimeoutMs
                  << " ms for workers to claim running jobs" << std::endl;
    }
}

void Broker::requeueUnclaimedJobs()
{
    m_recoveryPending = false;

    // Collect first, then re-queue: requeueJob() changes m_jobs entries
    // while we would otherwise still be iterating over them.
    std::vector<JobRecord> unclaimed;
    for (std::map<std::string, JobRecord>::const_iterator it = m_jobs.begin(); it != m_jobs.end(); ++it)
    {
        const JobRecord& record = it->second;
        bool claimed = (m_workers.currentJob(record.workerId) == record.spec.id);
        if (record.state == JobState::Running && !claimed)
        {
            unclaimed.push_back(record);
        }
    }

    for (std::size_t i = 0; i < unclaimed.size(); ++i)
    {
        requeueJob(unclaimed[i].spec.id, unclaimed[i].workerId);
    }

    std::cout << "[broker] recovery done: " << unclaimed.size()
              << " unclaimed running jobs re-queued" << std::endl;
}

} // namespace jobsched