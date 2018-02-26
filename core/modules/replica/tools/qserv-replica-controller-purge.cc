#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica/CmdParser.h"
#include "replica/ReplicaFinder.h"
#include "replica/Controller.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestTracker.h"
#include "replica/DeleteRequest.h"
#include "replica/ServiceProvider.h"


namespace rc = lsst::qserv::replica;

namespace {

// Command line parameters

std::string  databaseName;
unsigned int numReplicas;
bool         progressReport;
bool         errorReport;
std::string  configUrl;

/// Run the test
bool test () {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        rc::ServiceProvider provider (configUrl);

        rc::Controller::pointer controller = rc::Controller::create (provider);

        controller->run();

        ////////////////////////////////////////
        // Find all replicas accross all workers

        rc::ReplicaFinder finder (controller,
                                 databaseName,
                                 std::cout,
                                 progressReport,
                                 errorReport);

        //////////////////////////////////////////////////////////////
        // Analyse results and prepare a purge plan to shave off extra
        // replocas while trying to keep all nodes equally loaded
            
        // Workers hosting a chunk
        std::map<unsigned int, std::list<std::string>> chunk2workers;

        // Chunks hosted by a worker
        std::map<std::string, std::list<unsigned int>> worker2chunks;

        for (const auto& request: finder.requests)

            if ((request->state()         == rc::Request::State::FINISHED) &&
                (request->extendedState() == rc::Request::ExtendedState::SUCCESS))

                for (const auto &replica: request->responseData ())
                    if (replica.status() == rc::ReplicaInfo::Status::COMPLETE) {
                        chunk2workers[replica.chunk ()].push_back(replica.worker());
                        worker2chunks[replica.worker()].push_back(replica.chunk ());
                    }


        /////////////////////////////////////////////////////////////////////// 
        // Check which chunk replicas need to be eliminated. Then find the most
        // loaded worker holding the chunk and launch a delete request.
        //
        // TODO: this algorithm is way to simplistic as it won't take into
        //       an account other chunks. Ideally, it neees to be a two-pass
        //       scan.

        rc::CommonRequestTracker<rc::DeleteRequest> tracker (std::cout,
                                                             progressReport,
                                                             errorReport);
 
        for (auto &entry: chunk2workers) {

            const unsigned int chunk{entry.first};

            // This collection is going to be modified
            std::list<std::string> replicas{entry.second};

            // Note that some chunks may have fewer replicas than required. In that case
            // the difference would be negative.
            const int numReplicas2delete =  replicas.size() - numReplicas;

            for (int i = 0; i < numReplicas2delete; ++i) {

                // Find a candidate worker with the most number of chunks.
                // This worker will be select as the 'destinationWorker' for the new replica.

                std::string destinationWorker;
                size_t      numChunksPerDestinationWorker = 0;

                for (const auto &worker: replicas) {
                    if (worker2chunks[worker].size() > numChunksPerDestinationWorker) {
                        destinationWorker = worker;
                        numChunksPerDestinationWorker = worker2chunks[worker].size();
                    }
                }
                if (destinationWorker.empty()) {
                    std::cerr << "failed to find the most populated worker for replicating chunk: " << chunk
                        << ", skipping this chunk" << std::endl;
                    break;
                }
                 
                // Remove this chunk with the worker to decrease the number of chunks per
                // the worker so that this updated stats will be accounted for later as
                // the replication process goes.
                std::remove(worker2chunks[destinationWorker].begin(),
                            worker2chunks[destinationWorker].end(),
                            chunk);

                // Also register the worker in the chunk2workers[chunk] to prevent it
                // from being select as the 'destinationWorker' for the same replica
                // in case if more than one replica needs to be created.
                // Also remove the worker from the local copy of the replicas, so that
                // it won't be tries again
                std::remove(replicas.begin(),
                            replicas.end(),
                            destinationWorker);
                
                // Finally, launch and register for further tracking the deletion
                // request.
                
                tracker.add (
                    controller->deleteReplica (
                        destinationWorker,
                        databaseName,
                        chunk,
                        [&tracker] (rc::DeleteRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }
                    )
                );
            }
        }

        // Wait before all request are finished. Then analyze results
        // and print a report on failed requests (if any)

        tracker.track();

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return true;
}
} /// namespace

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        rc::CmdParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <database> <num-replicas> [--progress-report] [--error-report] [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <database>         - the name of a database to inspect\n"
            "  <num-replicas>     - purge the number of replicas in each chunk to this level\n"
            "\n"
            "Flags and options:\n"
            "  --progress-report  - the flag triggering progress report when executing batches of requests\n"
            "  --error-report     - the flag triggering detailed report on failed requests\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::databaseName   = parser.parameter<std::string>(1);
        ::numReplicas    = parser.parameter<int>        (2);
        ::progressReport = parser.flag                  ("progress-report");
        ::errorReport    = parser.flag                  ("error-report");
        ::configUrl      = parser.option   <std::string>("config", "file:replication.cfg");

    } catch (std::exception &ex) {
        return 1;
    } 
    ::test();
    return 0;
}
