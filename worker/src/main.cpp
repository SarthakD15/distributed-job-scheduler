#include <iostream>
#include <string>

#include <process.h>
#include <zmq.hpp>

#include "worker.h"

int main(int argc, char* argv[])
{
    // Optional name: "worker w1". Default: worker-<process id>, which is
    // unique on this machine so several workers can run side by side.
    std::string workerId;
    if (argc >= 2)
    {
        workerId = argv[1];
    }
    else
    {
        workerId = "worker-" + std::to_string(_getpid());
    }

    try
    {
        jobsched::Worker worker(workerId);
        worker.run();
    }
    catch (const zmq::error_t& e)
    {
        std::cerr << "[" << workerId << "] ZeroMQ error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}