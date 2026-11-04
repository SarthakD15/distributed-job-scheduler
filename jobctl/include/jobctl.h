#ifndef JOBSCHED_JOBCTL_H
#define JOBSCHED_JOBCTL_H

#include <string>
#include <vector>

#include <zmq.hpp>

#include "util.h"

namespace jobsched
{

// Command-line client: sends one request to the broker and prints the reply.
// Each public function returns the process exit code for main().
class JobCtl
{
public:
    JobCtl();
    ~JobCtl();

    int submit(const std::vector<std::string>& command);
    int status(const std::string& jobId);
    int list();

    int explain(const std::string& jobId);   // AI diagnosis of a failed job

private:
    // Sends one request and waits for one reply.
    // Returns false if the broker did not answer in time.
    bool request(MessageType type,
                 const std::string& payload,
                 MessageType& replyType,
                 std::string& replyPayload);

    // Gets one job's record from the broker. On failure prints why and returns false.
    bool fetchRecord(const std::string& jobId, JobRecord& record);

    // Prints the error for a failed request and returns exit code 1.
    int reportFailure(bool reachable, const std::string& replyPayload);

    zmq::context_t m_context;
    zmq::socket_t  m_socket;   // REQ, connects to the broker frontend
};

} // namespace jobsched

#endif // JOBSCHED_JOBCTL_H