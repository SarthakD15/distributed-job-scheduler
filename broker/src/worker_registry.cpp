#include "worker_registry.h"

#include <algorithm>

namespace jobsched
{

WorkerInfo::WorkerInfo()
    : id(), currentJobId(), lastSeen()
{
}

WorkerRegistry::WorkerRegistry()
    : m_workers(), m_idle()
{
}

WorkerRegistry::~WorkerRegistry()
{
}

bool WorkerRegistry::touch(const std::string& workerId)
{
    bool isNew = (m_workers.find(workerId) == m_workers.end());

    WorkerInfo& info = m_workers[workerId];
    info.id       = workerId;
    info.lastSeen = std::chrono::steady_clock::now();
    return isNew;
}

void WorkerRegistry::markIdle(const std::string& workerId)
{
    std::map<std::string, WorkerInfo>::iterator it = m_workers.find(workerId);
    if (it == m_workers.end())
    {
        return;
    }
    it->second.currentJobId.clear();

    if (std::find(m_idle.begin(), m_idle.end(), workerId) == m_idle.end())
    {
        m_idle.push_back(workerId);
    }
}

void WorkerRegistry::assignJob(const std::string& workerId, const std::string& jobId)
{
    std::map<std::string, WorkerInfo>::iterator it = m_workers.find(workerId);
    if (it != m_workers.end())
    {
        it->second.currentJobId = jobId;
    }
}

std::string WorkerRegistry::currentJob(const std::string& workerId) const
{
    std::map<std::string, WorkerInfo>::const_iterator it = m_workers.find(workerId);
    if (it == m_workers.end())
    {
        return std::string();
    }
    return it->second.currentJobId;
}

bool WorkerRegistry::hasIdleWorker() const
{
    return !m_idle.empty();
}

std::string WorkerRegistry::takeIdleWorker()
{
    std::string workerId = m_idle.front();
    m_idle.pop_front();
    return workerId;
}

std::vector<WorkerInfo> WorkerRegistry::removeDead(std::chrono::milliseconds timeout)
{
    std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now();
    std::vector<WorkerInfo> dead;

    std::map<std::string, WorkerInfo>::iterator it = m_workers.begin();
    while (it != m_workers.end())
    {
        if (now - it->second.lastSeen > timeout)
        {
            dead.push_back(it->second);
            m_idle.erase(std::remove(m_idle.begin(), m_idle.end(), it->first), m_idle.end());
            it = m_workers.erase(it);
        }
        else
        {
            ++it;
        }
    }
    return dead;
}

std::size_t WorkerRegistry::workerCount() const
{
    return m_workers.size();
}

std::size_t WorkerRegistry::idleCount() const
{
    return m_idle.size();
}

} // namespace jobsched