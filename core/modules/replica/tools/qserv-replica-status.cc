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
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/QservMgtServices.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "replica/TestEchoQservMgtRequest.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"

using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

// Command line parameters

std::string reportJobs;
std::string reportRequests;
std::string configUrl;

unsigned int timeoutSec;


/// Run the test
bool test() {

    try {

        ////////////////////////////////////////////////////////////////////////
        // Start the controller in its own thread before injecting any requests.
        // Note that callbacks (onFinish) are invoked in different threads.

        auto const provider   = ServiceProvider::create(configUrl);
        auto const controller = Controller::create(provider);

        controller->run();

        // Data to be sent for testing purposes
        std::string const data = "test";

        // No parent job
        std::string const jobId;

        // Launch test requests to both the Replication system's and Qserv workers

        std::atomic<unsigned int> requestsNumFinished(0);
        std::atomic<unsigned int> qservRequestsNumFinished(0);

        std::vector<ServiceStatusRequest::Ptr>    requests;
        std::vector<TestEchoQservMgtRequest::Ptr> qservRequests;

        for (auto const& worker: provider->config()->workers()) {
            requests.push_back(
                controller->statusOfWorkerService(
                    worker,
                    [&requestsNumFinished] (ServiceStatusRequest::Ptr const& ptr) {
                        requestsNumFinished++;
                    },
                    jobId,
                    timeoutSec
                )
            );
            qservRequests.push_back(
                provider->qservMgtServices()->echo(
                    worker,
                    data,
                    jobId,
                    [&qservRequestsNumFinished] (TestEchoQservMgtRequest::Ptr const& ptr) {
                        qservRequestsNumFinished++;
                    },
                    timeoutSec
                )
            );
        }

        // Wait before all request are finished

        util::BlockPost blockPost(1000, 2000);
        while (requestsNumFinished + qservRequestsNumFinished < requests.size() + qservRequests.size()) {
            blockPost.wait();
            std::cout
                << "finished: " << (requestsNumFinished + qservRequestsNumFinished)
                << "/" + (requests.size() + qservRequests.size()) << std::endl;
        };

        //////////////////////////////
        // Analyse and display results


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
            "  [--config=<url>] [--timeout=<seconds>]\n"
            "\n"
            "Flags and options:\n"
            "  --jobs     - report active jobs\n"
            "  --requests - report active requests\n"
            "  --config   - configuration URL [ DEFAULT: file:replication.cfg ]\n"
            "  --timeout  - timeout (seconds) for status requests sent to\n"
            "               the Replication system and Qserv wokers [DEFAULT: 10]\n");

        ::reportJobs     = parser.flag("jobs");
        ::reportRequests = parser.flag("requests");

        ::configUrl  = parser.option<std::string>("config", "file:replication.cfg");
        ::timeoutSec = parser.option<unsigned int>("timeout", 10);

    } catch (std::exception const& ex) {
        return 1;
    } 
 
    ::test();
    return 0;
}
