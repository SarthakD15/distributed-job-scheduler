#include <exception>
#include <iostream>
#include <string>

#include <zmq.hpp>

#include "broker.h"
#include "postgres_job_store.h"

namespace
{
// Used when no connection string is given on the command line.
const char* const kDefaultConnection = "host=localhost port=5432 dbname=jobsched user=postgres";
}

int main(int argc, char* argv[])
{
    std::string connection = (argc >= 2) ? argv[1] : kDefaultConnection;

    try
    {
        // main() decides which store to use; the broker only sees a JobStore.
        jobsched::PostgresJobStore store(connection);
        // (The connection string is not printed: it may contain a password.)
        std::cout << "[broker] connected to database" << std::endl;

        jobsched::Broker broker(store);
        broker.run();
    }
    catch (const zmq::error_t& e)
    {
        std::cerr << "[broker] ZeroMQ error: " << e.what() << std::endl;
        return 1;
    }
    catch (const std::exception& e)
    {
        // Database errors end up here. We stop rather than carry on without
        // durable storage; on restart the broker recovers from the database.
        std::cerr << "[broker] fatal error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}