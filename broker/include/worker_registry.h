#ifndef JOBSCHED_WORKER_REGISTRY_H
#define JOBSCHED_WORKER_REGISTRY_H

#include <chrono>
#include <deque>
#include <map>
#include <string>
#include <vector>

namespace jobsched
{

// What the broker knows about one connected worker.
// Lives here rather than in util.h because only the broker uses it.
struct WorkerInfo
{
    std::string                           id;
    std::string                           currentJobId;   // empty when idle
    std::chrono::steady_clock::time_point lastSeen;       // last time ANY message arrived from it

    WorkerInfo();
};

// The broker's bookkeeping of workers: who is connected, who is idle,
// who runs which job, and who has gone silent for too long.
class WorkerRegistry
{
public:
    WorkerRegistry();
    ~WorkerRegistry();

    // Records that a message just arrived from this worker.
    // Returns true if the worker was not known before (new, or came back after being declared dead).
    bool touch(const std::string& workerId);

    // The worker is free: forget its job and put it at the back of the idle queue.
    void markIdle(const std::string& workerId);

    // Remembers that this worker is now running this job.
    void assignJob(const std::string& workerId, const std::string& jobId);

    // Id of the job this worker is running; empty if it is idle or unknown.
    std::string currentJob(const std::string& workerId) const;

    bool        hasIdleWorker() const;
    std::string takeIdleWorker();          // call only when hasIdleWorker() is true

    // Removes every worker silent for longer than 'timeout' and returns them,
    // so the broker can put their running jobs back in the queue.
    std::vector<WorkerInfo> removeDead(std::chrono::milliseconds timeout);

    std::size_t workerCount() const;
    std::size_t idleCount() const;

private:
    std::map<std::string, WorkerInfo> m_workers;   // worker id -> info
    std::deque<std::string>           m_idle;      // idle worker ids, longest-idle first
};

} // namespace jobsched

#endif // JOBSCHED_WORKER_REGISTRY_H