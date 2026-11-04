#ifndef JOBSCHED_UTIL_H
#define JOBSCHED_UTIL_H

#include <string>
#include <vector>

namespace jobsched
{

// ---------------- Network endpoints ----------------
const char* const kFrontendBind    = "tcp://*:5555";          // broker listens for jobctl here
const char* const kFrontendConnect = "tcp://localhost:5555";  // jobctl connects here
const char* const kBackendBind     = "tcp://*:5556";          // broker listens for workers here
const char* const kBackendConnect  = "tcp://localhost:5556";  // workers connect here

// ---------------- Heartbeats ----------------
// Workers send HEARTBEAT this often; the broker answers each one with HEARTBEAT_ACK.
const int kHeartbeatIntervalMs = 1000;
// Silence longer than this means the other side is gone (allows 2 missed heartbeats).
const int kHeartbeatTimeoutMs  = 3000;

// ---------------- Enums ----------------

// First frame of every message: what kind of message this is.
enum class MessageType
{
    Submit,         // jobctl -> broker : please queue this job      (payload: JobSpec JSON)
    SubmitReply,    // broker -> jobctl : here is the new job id      (payload: job id)
    Status,         // jobctl -> broker : details of one job please   (payload: job id)
    StatusReply,    // broker -> jobctl : here is that job            (payload: JobRecord JSON)
    List,           // jobctl -> broker : all jobs please             (payload: empty)
    ListReply,      // broker -> jobctl : here are all jobs           (payload: array of JobRecord JSON)
    Ready,          // worker -> broker : I am free, give me a job    (payload: empty)
    Job,            // broker -> worker : run this job                (payload: JobSpec JSON)
    Result,         // worker -> broker : job finished, I am free     (payload: JobResult JSON)
    Heartbeat,      // worker -> broker : I am alive                  (payload: id of running job, empty if idle)
    HeartbeatAck,   // broker -> worker : I am alive too              (payload: empty)
    Error,          // either direction : something went wrong        (payload: reason text)
    Unknown         // could not recognise the type frame
};

// Lifecycle of a job inside the broker.
enum class JobState
{
    Queued,     // waiting for a free worker
    Running,    // sent to a worker, not finished yet
    Succeeded,  // process exited with code 0
    Failed      // non-zero exit code, or the process could not be started
};

// ---------------- Structs ----------------

// What the user wants to run.
struct JobSpec
{
    std::string              id;        // assigned by the broker; empty on submit
    std::vector<std::string> command;   // program followed by its arguments

    JobSpec();
};

// What happened when a worker ran a job.
struct JobResult
{
    std::string jobId;
    int         exitCode;     // process exit code; -1 if it never started
    std::string output;       // captured stdout + stderr (UTF-8)
    std::string error;        // why the worker could not run it; empty if it ran
    long long   durationMs;   // wall-clock run time

    JobResult();
};

// The broker's record of one job.
struct JobRecord
{
    JobSpec     spec;
    JobState    state;
    std::string workerId;   // which worker got the job; empty while queued
    JobResult   result;     // filled in once state is Succeeded or Failed

    JobRecord();
};

// ---------------- Conversions ----------------
std::string  toString(MessageType type);
MessageType  messageTypeFromString(const std::string& text);
std::string  toString(JobState state);
JobState     jobStateFromString(const std::string& text);

// All *FromJson functions throw std::runtime_error on malformed input.
std::string  jobSpecToJson(const JobSpec& spec);
JobSpec      jobSpecFromJson(const std::string& text);

std::string  jobResultToJson(const JobResult& result);
JobResult    jobResultFromJson(const std::string& text);

// Just the command array, e.g. ["ping","-n","4","127.0.0.1"] (stored in the database).
std::string               commandToJson(const std::vector<std::string>& command);
std::vector<std::string>  commandFromJson(const std::string& text);

std::string  jobRecordToJson(const JobRecord& record);
JobRecord    jobRecordFromJson(const std::string& text);

std::string             jobRecordListToJson(const std::vector<JobRecord>& records);
std::vector<JobRecord>  jobRecordListFromJson(const std::string& text);

} // namespace jobsched

#endif // JOBSCHED_UTIL_H