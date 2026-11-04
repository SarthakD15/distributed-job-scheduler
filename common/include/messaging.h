#ifndef JOBSCHED_MESSAGING_H
#define JOBSCHED_MESSAGING_H

#include <string>
#include <vector>

#include <zmq.hpp>

namespace jobsched
{

// Sends all frames as one multipart message.
void sendFrames(zmq::socket_t& socket, const std::vector<std::string>& frames);

// Receives one complete multipart message into 'frames'.
// Returns false if the receive timed out (only possible when the
// socket has a receive timeout set).
bool receiveFrames(zmq::socket_t& socket, std::vector<std::string>& frames);

} // namespace jobsched

#endif // JOBSCHED_MESSAGING_H