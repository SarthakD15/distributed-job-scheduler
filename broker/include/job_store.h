#ifndef JOBSCHED_JOB_STORE_H
#define JOBSCHED_JOB_STORE_H

#include <string>
#include <vector>

#include "util.h"

namespace jobsched
{

// Where the broker keeps jobs durably. The broker only knows this interface,
// never the database behind it (Dependency Inversion): PostgreSQL today,
// SQLite or an in-memory fake for tests could replace it without touching
// the broker.
//
// Every method writes immediately and throws std::exception on failure.
class JobStore
{
public:
    JobStore();
    virtual ~JobStore();

    // Saves a new QUEUED job and returns the id the store gave it ("job-<n>").
    virtual std::string createJob(const JobSpec& spec) = 0;

    // The job was sent to this worker.
    virtual void markRunning(const std::string& jobId, const std::string& workerId) = 0;

    // The job finished (state is Succeeded or Failed).
    virtual void markFinished(const std::string& jobId, JobState state, const JobResult& result) = 0;

    // The job goes back to the queue (its worker died, or nobody claimed it after a restart).
    virtual void markQueued(const std::string& jobId) = 0;

    // Every job, oldest first. Used once, when the broker starts.
    virtual std::vector<JobRecord> loadAllJobs() = 0;

    // A store owns a database connection; copying it would make no sense.
    JobStore(const JobStore&)            = delete;
    JobStore& operator=(const JobStore&) = delete;
};

} // namespace jobsched

#endif // JOBSCHED_JOB_STORE_H