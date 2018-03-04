#include <iomanip>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <string>

#include "proto/replication.pb.h"
#include "replica/Controller.h"
#include "replica/RequestTracker.h"
#include "replica/ServiceManagementRequest.h"
#include "replica/ServiceProvider.h"
#include "util/CmdLineParser.h"

namespace rc   = lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

// Command line parameters

std::string operation;
bool        progressReport;
bool        errorReport;
std::string configUrl;


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

        //////////////////////////////////////
        // Launch requests against all workers
        //
        // ATTENTION: calbacks on the request completion callbacks of the requests will
        //            be executed within the Contoller's thread. Watch for proper
        //            synchronization when inspecting/updating shared variables.

        rc::CommonRequestTracker<rc::ServiceManagementRequestBase> tracker (std::cout,
                                                                            progressReport,
                                                                            errorReport);
        for (auto const& worker: provider.config()->workers())

            if (operation == "STATUS")
                tracker.add (
                    controller->statusOfWorkerService (
                        worker,
                        [&tracker] (rc::ServiceStatusRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }));
            else if (operation == "SUSPEND")
                tracker.add (
                    controller->suspendWorkerService (
                        worker,
                        [&tracker] (rc::ServiceSuspendRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }));
            else if (operation == "RESUME")
                tracker.add (
                    controller->resumeWorkerService (
                        worker,
                        [&tracker] (rc::ServiceResumeRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }));
            else if (operation == "REQUESTS")
                tracker.add (
                    controller->requestsOfWorkerService (
                        worker,
                        [&tracker] (rc::ServiceRequestsRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }));
            else if (operation == "DRAIN")
                tracker.add (
                    controller->drainWorkerService (
                        worker,
                        [&tracker] (rc::ServiceDrainRequest::pointer ptr) {
                            tracker.onFinish(ptr);
                        }));

        // Wait before all request are finished

        tracker.track();

        //////////////////////////////
        // Analyse and display results

        std::cout
            << "\n"
            << "WORKERS:";
        for (auto const& worker: provider.config()->workers()) {
            std::cout << " " << worker;
        }
        std::cout
            << "\n"
            << std::endl;

        std::cout
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n"
            << "   worker | started (seconds ago) | state               |         new | in-progress |    finished \n"
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n";

        for (auto const& ptr: tracker.requests) {

            if ((ptr->state()         == rc::Request::State::FINISHED) &&
                (ptr->extendedState() == rc::Request::ExtendedState::SUCCESS)) {

                const uint32_t startedSecondsAgo = (rc::PerformanceUtils::now() - ptr->getServiceState().startTime) / 1000.0f;
                std::cout
                    << " "   << std::setw (8) << ptr->worker()
                    << " | " << std::setw(21) << startedSecondsAgo
                    << " | " << std::setw(19) << ptr->getServiceState().state2string()
                    << " | " << std::setw(11) << ptr->getServiceState().numNewRequests
                    << " | " << std::setw(11) << ptr->getServiceState().numInProgressRequests
                    << " | " << std::setw(11) << ptr->getServiceState().numFinishedRequests
                    << "\n";
            } else {
                std::cout
                    << " "   << std::setw (8) << ptr->worker()
                    << " | " << std::setw(21) << "*"
                    << " | " << std::setw(19) << "*"
                    << " | " << std::setw(11) << "*"
                    << " | " << std::setw(11) << "*"
                    << " | " << std::setw(11) << "*"
                    << "\n";
            }
        }
        std::cout
            << "----------+-----------------------+---------------------+-------------+-------------+-------------\n"
            << std::endl;

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

int main (int argc, const char* const argv[]) {

    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser (
            argc,
            argv,
            "\n"
            "Usage:\n"
            "  <command> [--progress-report] [--error-report] [--config=<url>]\n"
            "\n"
            "Parameters:\n"
            "  <command>   - the name of an operation. Allowed values are listed below:\n"
            "\n"
            "      STATUS   : request and display the status of each server \n"
            "      SUSPEND  : suspend all servers\n"
            "      RESUME   : resume all server\n"
            "      REQUESTS : pull and display info on requests known to all server\n"
            "      DRAIN    : cancel all queued and on-going requests\n"
            "\n"
            "Flags and options:\n"
            "  --progress-report  - the flag triggering progress report when executing batches of requests\n"
            "  --error-report     - the flag triggering detailed report on failed requests\n"
            "  --config           - a configuration URL (a configuration file or a set of the database\n"
            "                       connection parameters [ DEFAULT: file:replication.cfg ]\n");

        ::operation = parser.parameterRestrictedBy (1, {"STATUS",
                                                        "SUSPEND",
                                                         "RESUME",
                                                         "REQUESTS",
                                                         "DRAIN"});

        ::progressReport = parser.flag                ("progress-report");
        ::errorReport    = parser.flag                ("error-report");
        ::configUrl      = parser.option<std::string> ("config", "file:replication.cfg");

    } catch (std::exception const& ex) {
        return 1;
    } 
 
    ::test();
    return 0;
}
