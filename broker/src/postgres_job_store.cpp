#include "postgres_job_store.h"

#include <stdexcept>

namespace jobsched
{

namespace
{

// Job ids are "job-<n>" everywhere else; the table stores just the number n.
const std::string kIdPrefix = "job-";

long long jobNumber(const std::string& jobId)
{
    if (jobId.compare(0, kIdPrefix.size(), kIdPrefix) != 0)
    {
        throw std::invalid_argument("bad job id: " + jobId);
    }
    return std::stoll(jobId.substr(kIdPrefix.size()));
}

std::string jobIdFromNumber(long long number)
{
    return kIdPrefix + std::to_string(number);
}

} // anonymous namespace

PostgresJobStore::PostgresJobStore(const std::string& connectionString)
    : JobStore(),
      m_connection(connectionString)
{
    createTableIfMissing();
}

PostgresJobStore::~PostgresJobStore()
{
}

void PostgresJobStore::createTableIfMissing()
{
    pqxx::work tx(m_connection);
    tx.exec(
        "CREATE TABLE IF NOT EXISTS jobs ("
        "  id          BIGSERIAL   PRIMARY KEY,"
        "  command     JSONB       NOT NULL,"
        "  state       TEXT        NOT NULL,"
        "  worker_id   TEXT,"
        "  exit_code   INTEGER,"
        "  output      TEXT,"
        "  error       TEXT,"
        "  duration_ms BIGINT,"
        "  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),"
        "  started_at  TIMESTAMPTZ,"
        "  finished_at TIMESTAMPTZ"
        ")");
    tx.exec("CREATE INDEX IF NOT EXISTS jobs_state_idx ON jobs (state)");
    tx.commit();
}

std::string PostgresJobStore::createJob(const JobSpec& spec)
{
    pqxx::work tx(m_connection);

    // $1, $2 are parameters: the values travel separately from the SQL text,
    // so a command containing quotes can never break (or inject into) the query.
    pqxx::row row = tx.exec(
        "INSERT INTO jobs (command, state) VALUES ($1::jsonb, $2) RETURNING id",
        pqxx::params{ commandToJson(spec.command), toString(JobState::Queued) }).one_row();

    tx.commit();
    return jobIdFromNumber(row[0].as<long long>());
}

void PostgresJobStore::markRunning(const std::string& jobId, const std::string& workerId)
{
    pqxx::work tx(m_connection);
    tx.exec(
        "UPDATE jobs SET state = $1, worker_id = $2, started_at = now() WHERE id = $3",
        pqxx::params{ toString(JobState::Running), workerId, jobNumber(jobId) });
    tx.commit();
}

void PostgresJobStore::markFinished(const std::string& jobId, JobState state, const JobResult& result)
{
    pqxx::work tx(m_connection);
    tx.exec(
        "UPDATE jobs SET state = $1, exit_code = $2, output = $3, error = $4,"
        "                duration_ms = $5, finished_at = now()"
        " WHERE id = $6",
        pqxx::params{ toString(state), result.exitCode, result.output, result.error,
                      result.durationMs, jobNumber(jobId) });
    tx.commit();
}

void PostgresJobStore::markQueued(const std::string& jobId)
{
    pqxx::work tx(m_connection);
    tx.exec(
        "UPDATE jobs SET state = $1, worker_id = NULL, started_at = NULL WHERE id = $2",
        pqxx::params{ toString(JobState::Queued), jobNumber(jobId) });
    tx.commit();
}

std::vector<JobRecord> PostgresJobStore::loadAllJobs()
{
    pqxx::work tx(m_connection);

    // COALESCE turns NULLs (e.g. no exit code yet) into the same defaults
    // our structs use, so every column can be read without NULL checks.
    pqxx::result rows = tx.exec(
        "SELECT id, command::text, state,"
        "       COALESCE(worker_id, ''), COALESCE(exit_code, -1),"
        "       COALESCE(output, ''), COALESCE(error, ''), COALESCE(duration_ms, 0)"
        "  FROM jobs ORDER BY id");
    tx.commit();

    std::vector<JobRecord> records;
    records.reserve(rows.size());
    for (pqxx::result::size_type i = 0; i < rows.size(); ++i)
    {
        JobRecord record;
        record.spec.id           = jobIdFromNumber(rows[i][0].as<long long>());
        record.spec.command      = commandFromJson(rows[i][1].as<std::string>());
        record.state             = jobStateFromString(rows[i][2].as<std::string>());
        record.workerId          = rows[i][3].as<std::string>();
        record.result.jobId      = record.spec.id;
        record.result.exitCode   = rows[i][4].as<int>();
        record.result.output     = rows[i][5].as<std::string>();
        record.result.error      = rows[i][6].as<std::string>();
        record.result.durationMs = rows[i][7].as<long long>();
        records.push_back(record);
    }
    return records;
}

} // namespace jobsched