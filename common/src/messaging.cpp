#include "messaging.h"

namespace jobsched
{

void sendFrames(zmq::socket_t& socket, const std::vector<std::string>& frames)
{
    for (std::size_t i = 0; i < frames.size(); ++i)
    {
        zmq::send_flags flags = (i + 1 < frames.size()) ? zmq::send_flags::sndmore
                                                        : zmq::send_flags::none;
        socket.send(zmq::buffer(frames[i]), flags);
    }
}

bool receiveFrames(zmq::socket_t& socket, std::vector<std::string>& frames)
{
    frames.clear();
    while (true)
    {
        zmq::message_t part;
        zmq::recv_result_t received = socket.recv(part, zmq::recv_flags::none);
        if (!received)
        {
            return false;   // timed out
        }
        frames.push_back(part.to_string());
        if (!part.more())
        {
            return true;    // that was the last frame
        }
    }
}

} // namespace jobsched