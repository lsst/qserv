/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
 *
 * This product includes software developed by the
 * LSST Project (http://www.lsst.org/).
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the LSST License Statement and
 * the GNU General Public License along with this program.  If not,
 * see <http://www.lsstcorp.org/LegalNotices/>.
 */

/// qserv-replica-controller-replicate.cc is a Controller application for
/// testing the corresponding request.

// System headers
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <list>
#include <map>
#include <vector>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ReplicaFinder.h"
#include "replica/ReplicaInfo.h"
#include "replica/RequestTracker.h"
#include "replica/ReplicationRequest.h"
#include "replica/ServiceProvider.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv;

namespace {

// Command line parameters

std::string  databaseName;
unsigned int numReplicas;
bool         saveReplicaInfo;
bool         progressReport;
bool         errorReport;
std::string  configUrl;

/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests
        // Note that omFinish callbak which are activated upon a completion
        // of the requsts will be run in that Controller's thread.

        replica::ServiceProvider::Ptr const provider   = replica::ServiceProvider::create(configUrl);
        replica::Controller::Ptr      const controller = replica::Controller::create(provider);

        controller->run();

        ////////////////////////////////////////
        // Find all replicas accross all workers

        replica::ReplicaFinder finder(controller,
                                      databaseName,
                                      saveReplicaInfo,
                                      std::cout,
                                      progressReport,
                                      errorReport);

        /////////////////////////////////////////////////////////////////
        // Analyse results and prepare a replication plan to create extra
        // replocas for under-represented chunks 
            
        // All workers which have a chunk
        std::map<unsigned int, std::list<std::string>> chunk2workers;

        /// All chunks hosted by a worker
        std::map<std::string, std::list<unsigned int>> worker2chunks;

        // Failed workers
        std::set<std::string> failedWorkers;

        for (auto const& ptr: finder.requests)

            if ((ptr->state()         == replica::Request::State::FINISHED) &&
                (ptr->extendedState() == replica::Request::ExtendedState::SUCCESS)) {

                for (auto const &replica: ptr->responseData ())
                    if (replica.status() == replica::ReplicaInfo::Status::COMPLETE) {
                        chunk2workers[replica.chunk ()].push_back(replica.worker());
                        worker2chunks[replica.worker()].push_back(replica.chunk ());
                    }

            } else{
                failedWorkers.insert(ptr->worker());
            }

        /////////////////////////////////////////////////////////////////////
        // Check which chunks are under-represented. Then find a least loaded
        // worker and launch a replication request.

        replica::CommonRequestTracker<replica::ReplicationRequest>
            tracker(std::cout,
                    progressReport,
                    errorReport);

        // This counter will be used for optimization purposes as the upper limit for
        // the number of chunks per worker in the load balancing algorithm below.
        size_t const numUniqueChunks = chunk2workers.size();

        for (auto &entry: chunk2workers) {

            unsigned int const chunk{entry.first};

            // Take a copy of the non-modified list of workers with chunk's replicas
            // and cache it here to know which workers are allowed to be used as reliable
            // sources vs chunk2workers[chunk] which will be modified below as new replicas
            // will get created.
            std::list<std::string> const replicas{entry.second};

            // Pick the first worker which has this chunk as the 'sourceWorker'
            // in case if we'll decide to replica the chunk within the loop below
            std::string const& sourceWorker = *(replicas.begin());

            // Note that some chunks may have more replicas than required. In that case
            // the difference would be negative.
            int const numReplicas2create = numReplicas - replicas.size();

            for (int i = 0; i < numReplicas2create; ++i) {

                // Find a candidate worker with the least number of chunks.
                // This worker will be select as the 'destinationWorker' for the new replica.
                //
                // ATTENTION: workers which were previously found as 'failed'
                //            are going to be excluded from the search.

                std::string destinationWorker;
                size_t      numChunksPerDestinationWorker = numUniqueChunks;

                for (auto const& worker: provider->config()->workers()) {

                    // Exclude failed workers

                    if (failedWorkers.count(worker)) { continue; }

                    // Exclude workers which already have this chunk, or for which
                    // there is an outstanding replication requsts. Both kinds of
                    // replicas are registered in chunk2workers[chunk]

                    if (chunk2workers[chunk].end() == std::find(chunk2workers[chunk].begin(),
                                                                chunk2workers[chunk].end(),
                                                                worker)) {
                        if (worker2chunks[worker].size() < numChunksPerDestinationWorker) {
                            destinationWorker = worker;
                            numChunksPerDestinationWorker = worker2chunks[worker].size();
                        }
                    }
                }
                if (destinationWorker.empty()) {
                    std::cerr << "failed to find the least populated worker for replicating chunk: " << chunk
                        << ", skipping this chunk" << std::endl;
                    break;
                }
                 
                // Register this chunk with the worker to bump the number of chunks per
                // the worker so that this updated stats will be accounted for later as
                // the replication process goes.
                worker2chunks[destinationWorker].push_back(chunk);

                // Also register the worker in the chunk2workers[chunk] to prevent it
                // from being select as the 'destinationWorker' for the same replica
                // in case if more than one replica needs to be created.
                chunk2workers[chunk].push_back(destinationWorker);
                
                // Finally, launch and register for further tracking the replication
                // request.
                
                tracker.add(
                    controller->replicate(
                        destinationWorker,
                        sourceWorker,
                        databaseName,
                        chunk,
                        [&tracker] (replica::ReplicationRequest::Ptr ptr) {
                            tracker.onFinish(ptr);
                        }
                    )
                );
            }
        }

        // Wait before all request are finished and report
        // failed requests.

        tracker.track();

        ///////////////////////////////////////////////////
        // Shutdown the controller and join with its thread

        controller->stop();
        controller->join();

    } catch (std::exception const& ex) {
        std::cerr << ex.what() << std::endl;
    }
    return true;
}
} /// namespace

int main(int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <database> <num-replicas>\n"
            "             [--progress-report] [--error-report] [--config=<url>]\n"
            "             [--do-not-save-replica]\n"
            "\n"
            "Parameters:\n"
            "  <database>         - the name of a database to inspect\n"
            "  <num-replicas>     - increase the number of replicas in each chunk to this level\n"
            "\n"
            "Flags and options:\n"
            "  --do-not-save-replica - do not save replica info in a database"
            "  --progress-report     - the flag triggering progress report when executing batches of requests\n"
            "  --error-report        - the flag triggering detailed report on failed requests\n"
            "  --config              - a configuration URL (a configuration file or a set of the database\n"
            "                          connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::databaseName   = parser.parameter<std::string>(1);
        ::numReplicas    = parser.parameter<int>(2);
        ::configUrl      = parser.option<std::string>("config", "file:replication.cfg");

        ::saveReplicaInfo = not parser.flag("do-not-save-replica");
        ::progressReport  =     parser.flag("progress-report");
        ::errorReport     =     parser.flag("error-report");

    } catch (std::exception &ex) {
        return 1;
    }  
    ::test();
    return 0;
}
