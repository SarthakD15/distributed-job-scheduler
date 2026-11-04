#ifndef JOBSCHED_POSTGRES_JOB_STORE_H
#define JOBSCHED_POSTGRES_JOB_STORE_H

#include <string>
#include <vector>

#include <pqxx/pqxx>

#include "job_store.h"

namespace jobsched
{

// JobStore backed by a PostgreSQL table, using libpqxx.
// Creates the 'jobs' table on first use if it does not exist yet.
class PostgresJobStore : public JobStore
{
public:
    // connectionString example: "host=localhost port=5432 dbname=jobsched user=postgres"
    // Throws if the database cannot be reached.
    explicit PostgresJobStore(const std::string& connectionString);
    ~PostgresJobStore() override;

    std::string            createJob(const JobSpec& spec) override;
    void                   markRunning(const std::string& jobId, const std::string& workerId) override;
    void                   markFinished(const std::string& jobId, JobState state, const JobResult& result) override;
    void                   markQueued(const std::string& jobId) override;
    std::vector<JobRecord> loadAllJobs() override;

private:
    void createTableIfMissing();

    pqxx::connection m_connection;
};

} // namespace jobsched

#endif // JOBSCHED_POSTGRES_JOB_STORE_H