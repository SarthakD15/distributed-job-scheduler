#include <exception>
#include <iostream>
#include <string>
#include <vector>

#include <zmq.hpp>

#include "jobctl.h"

namespace
{

void printUsage()
{
    std::cerr << "usage:\n"
              << "  jobctl submit <program> [args...]\n"
              << "  jobctl status <job-id>\n"
              << "  jobctl list\n"
              << "  jobctl explain <job-id>      (AI diagnosis of a failed job)\n";
}

} // anonymous namespace

int main(int argc, char* argv[])
{
    if (argc < 2)
    {
        printUsage();
        return 1;
    }

    std::string action = argv[1];

    try
    {
        jobsched::JobCtl client;

        if (action == "submit" && argc >= 3)
        {
            std::vector<std::string> command(argv + 2, argv + argc);
            return client.submit(command);
        }
        if (action == "status" && argc == 3)
        {
            return client.status(argv[2]);
        }
        if (action == "explain" && argc == 3)
        {
            return client.explain(argv[2]);
        }
        if (action == "list" && argc == 2)
        {
            return client.list();
        }

        printUsage();
        return 1;
    }
    catch (const zmq::error_t& e)
    {
        std::cerr << "ZeroMQ error: " << e.what() << std::endl;
        return 1;
    }
    catch (const std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return 1;
    }
}