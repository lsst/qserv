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

/// qserv-replica-status.cc is report a status of the replication
/// system.

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "replica/ClusterHealthJob.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

// Command line parameters

std::string  configUrl;
unsigned int timeoutSec;
bool         allWorkers;


/// Run the test
bool test() {

    try {

        ///////////////////////////////////////////////////////////////////////////
        // Start the provider in its own thread pool before initiating any requests
        // or jobs.
        //
        // Note that onFinish callbacks which are activated upon the completion of
        // the requests or jobs will be run by a thread from the pool.

        auto const provider   = ServiceProvider::create(configUrl);
        auto const controller = Controller::create(provider);

        provider->run();

        // No parent job
        std::string const jobId;

        // Launch test requests to both the Replication system's and Qserv workers

        std::atomic<bool> finished(false);
 
        auto const job = ClusterHealthJob::create(
            timeoutSec,
            allWorkers,
            controller,
            jobId,
            [&finished] (ClusterHealthJob::Ptr const& job) {
                finished = true;
            }
        );
        job->start();

        // Wait before all request are finished

        util::BlockPost blockPost(1000, 2000);
        while (not finished) {
            blockPost.wait();
        };

        //////////////////////////////
        // Analyze and display results

        std::cout
            << "ClusterHealth job finished: " << job->state2string() << std::endl;

        if (job->extendedState() == Job::ExtendedState::SUCCESS) {
            ClusterHealth const& health = job->clusterHealth();
            std::cout
                << "ClusterHealth report \n"
                << "  in overall good state: " << (health.good() ? "YES" : "NO") << "\n"
                << "  replication worker agents\n";
            for (auto&& entry: health.replication()) {
                std::cout
                    << "    " << entry.first << ":\t " << (entry.second ? "UP" : "*") << "\n";
            }
            std::cout
                << "  qserv workers\n";
            for (auto&& entry: health.qserv()) {
                std::cout
                    << "    " << entry.first << ":\t " << (entry.second ? "UP" : "*") << "\n";
            }
        }

        //////////////////////////////////////////////////
        // Shutdown the provider and join with its threads

        provider->stop();

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
            "\n"
            "  [--config=<url>] [--timeout=<seconds>] [--all-workers]\n"
            "\n"
            "Flags and options:\n"
            "\n"
            "  --config\n"
            "      configuration URL [ DEFAULT: file:replication.cfg ]\n"
            "\n"
            "  --timeout\n"
            "      timeout (seconds) for status requests sent to\n"
            "      the Replication system and Qserv workers [DEFAULT: 10]\n"
            "\n"
            "  --all-workers\n"
            "      send probes to all known workers instead of the active ones\n"
            "      (those which are both enabled and not in the read-only state\n");

        ::configUrl  = parser.option<std::string>("config", "file:replication.cfg");
        ::timeoutSec = parser.option<unsigned int>("timeout", 10);
        ::allWorkers = parser.flag("all-workers");

    } catch (std::exception const& ex) {
        return 1;
    } 
 
    ::test();
    return 0;
}
