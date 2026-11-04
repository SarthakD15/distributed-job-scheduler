#include "util.h"

#include <stdexcept>

#include <jsoncons/json.hpp>

namespace jobsched
{

// ==================================================================
// Struct constructors
// ==================================================================

JobSpec::JobSpec()
    : id(), command()
{
}

JobResult::JobResult()
    : jobId(), exitCode(-1), output(), error(), durationMs(0)
{
}

JobRecord::JobRecord()
    : spec(), state(JobState::Queued), workerId(), result()
{
}

// ==================================================================
// Enum <-> string
// ==================================================================

std::string toString(MessageType type)
{
    switch (type)
    {
    case MessageType::Submit:      return "SUBMIT";
    case MessageType::SubmitReply: return "SUBMIT_REPLY";
    case MessageType::Status:      return "STATUS";
    case MessageType::StatusReply: return "STATUS_REPLY";
    case MessageType::List:        return "LIST";
    case MessageType::ListReply:   return "LIST_REPLY";
    case MessageType::Ready:       return "READY";
    case MessageType::Job:         return "JOB";
    case MessageType::Result:      return "RESULT";
    case MessageType::Heartbeat:    return "HEARTBEAT";
    case MessageType::HeartbeatAck: return "HEARTBEAT_ACK";
    case MessageType::Error:       return "ERROR";
    default:                       return "UNKNOWN";
    }
}

MessageType messageTypeFromString(const std::string& text)
{
    if (text == "SUBMIT")       return MessageType::Submit;
    if (text == "SUBMIT_REPLY") return MessageType::SubmitReply;
    if (text == "STATUS")       return MessageType::Status;
    if (text == "STATUS_REPLY") return MessageType::StatusReply;
    if (text == "LIST")         return MessageType::List;
    if (text == "LIST_REPLY")   return MessageType::ListReply;
    if (text == "READY")        return MessageType::Ready;
    if (text == "JOB")          return MessageType::Job;
    if (text == "RESULT")       return MessageType::Result;
    if (text == "HEARTBEAT")     return MessageType::Heartbeat;
    if (text == "HEARTBEAT_ACK") return MessageType::HeartbeatAck;
    if (text == "ERROR")        return MessageType::Error;
    return MessageType::Unknown;
}

std::string toString(JobState state)
{
    switch (state)
    {
    case JobState::Queued:    return "QUEUED";
    case JobState::Running:   return "RUNNING";
    case JobState::Succeeded: return "SUCCEEDED";
    case JobState::Failed:    return "FAILED";
    default:                  return "UNKNOWN";
    }
}

JobState jobStateFromString(const std::string& text)
{
    if (text == "QUEUED")    return JobState::Queued;
    if (text == "RUNNING")   return JobState::Running;
    if (text == "SUCCEEDED") return JobState::Succeeded;
    if (text == "FAILED")    return JobState::Failed;
    throw std::runtime_error("unknown job state: " + text);
}

// ==================================================================
// Internal helpers: struct <-> jsoncons::json object.
// Private to this file. The public functions below only use std::string,
// so no other file ever needs to include jsoncons.
// ==================================================================
namespace
{

jsoncons::json parseText(const std::string& text)
{
    try
    {
        return jsoncons::json::parse(text);
    }
    catch (const std::exception& e)
    {
        throw std::runtime_error(std::string("invalid JSON: ") + e.what());
    }
}

std::string dumpText(const jsoncons::json& obj)
{
    std::string text;
    obj.dump(text);
    return text;
}

void requireField(const jsoncons::json& obj, const char* key)
{
    if (!obj.is_object() || !obj.contains(key))
    {
        throw std::runtime_error(std::string("missing JSON field: ") + key);
    }
}

// ---- command array ----
jsoncons::json commandToArray(const std::vector<std::string>& command)
{
    jsoncons::json array(jsoncons::json_array_arg);
    for (std::size_t i = 0; i < command.size(); ++i)
    {
        array.push_back(command[i]);
    }
    return array;
}

std::vector<std::string> commandFromArray(const jsoncons::json& array)
{
    if (!array.is_array())
    {
        throw std::runtime_error("'command' must be an array");
    }

    std::vector<std::string> command;
    for (std::size_t i = 0; i < array.size(); ++i)
    {
        command.push_back(array[i].as<std::string>());
    }
    return command;
}

// ---- JobSpec ----
jsoncons::json specToObject(const JobSpec& spec)
{
    jsoncons::json obj;
    obj["id"]      = spec.id;
    obj["command"] = commandToArray(spec.command);
    return obj;
}

JobSpec specFromObject(const jsoncons::json& obj)
{
    requireField(obj, "id");
    requireField(obj, "command");

    JobSpec spec;
    spec.id      = obj.at("id").as<std::string>();
    spec.command = commandFromArray(obj.at("command"));
    return spec;
}

// ---- JobResult ----
jsoncons::json resultToObject(const JobResult& result)
{
    jsoncons::json obj;
    obj["job_id"]      = result.jobId;
    obj["exit_code"]   = result.exitCode;
    obj["output"]      = result.output;
    obj["error"]       = result.error;
    obj["duration_ms"] = result.durationMs;
    return obj;
}

JobResult resultFromObject(const jsoncons::json& obj)
{
    requireField(obj, "job_id");
    requireField(obj, "exit_code");
    requireField(obj, "output");
    requireField(obj, "error");
    requireField(obj, "duration_ms");

    JobResult result;
    result.jobId      = obj.at("job_id").as<std::string>();
    result.exitCode   = obj.at("exit_code").as<int>();
    result.output     = obj.at("output").as<std::string>();
    result.error      = obj.at("error").as<std::string>();
    result.durationMs = obj.at("duration_ms").as<long long>();
    return result;
}

// ---- JobRecord ----
jsoncons::json recordToObject(const JobRecord& record)
{
    jsoncons::json obj;
    obj["spec"]      = specToObject(record.spec);
    obj["state"]     = toString(record.state);
    obj["worker_id"] = record.workerId;
    obj["result"]    = resultToObject(record.result);
    return obj;
}

JobRecord recordFromObject(const jsoncons::json& obj)
{
    requireField(obj, "spec");
    requireField(obj, "state");
    requireField(obj, "worker_id");
    requireField(obj, "result");

    JobRecord record;
    record.spec     = specFromObject(obj.at("spec"));
    record.state    = jobStateFromString(obj.at("state").as<std::string>());
    record.workerId = obj.at("worker_id").as<std::string>();
    record.result   = resultFromObject(obj.at("result"));
    return record;
}

} // anonymous namespace

// ==================================================================
// Public: struct <-> JSON text
// ==================================================================

std::string jobSpecToJson(const JobSpec& spec)
{
    return dumpText(specToObject(spec));
}

JobSpec jobSpecFromJson(const std::string& text)
{
    return specFromObject(parseText(text));
}

std::string jobResultToJson(const JobResult& result)
{
    return dumpText(resultToObject(result));
}

JobResult jobResultFromJson(const std::string& text)
{
    return resultFromObject(parseText(text));
}

std::string commandToJson(const std::vector<std::string>& command)
{
    return dumpText(commandToArray(command));
}

std::vector<std::string> commandFromJson(const std::string& text)
{
    return commandFromArray(parseText(text));
}

std::string jobRecordToJson(const JobRecord& record)
{
    return dumpText(recordToObject(record));
}

JobRecord jobRecordFromJson(const std::string& text)
{
    return recordFromObject(parseText(text));
}

std::string jobRecordListToJson(const std::vector<JobRecord>& records)
{
    jsoncons::json list(jsoncons::json_array_arg);
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        list.push_back(recordToObject(records[i]));
    }
    return dumpText(list);
}

std::vector<JobRecord> jobRecordListFromJson(const std::string& text)
{
    jsoncons::json list = parseText(text);
    if (!list.is_array())
    {
        throw std::runtime_error("job list must be a JSON array");
    }

    std::vector<JobRecord> records;
    for (std::size_t i = 0; i < list.size(); ++i)
    {
        records.push_back(recordFromObject(list[i]));
    }
    return records;
}

} // namespace jobsched