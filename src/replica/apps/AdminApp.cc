/*
 * LSST Data Management System
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

// Class header
#include "replica/apps/AdminApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/contr/Controller.h"
#include "replica/requests/RequestTracker.h"
#include "replica/util/Performance.h"
#include "replica/requests/ServiceManagementRequest.h"
#include "util/TablePrinter.h"
#include "util/TimeUtils.h"

using namespace std;

namespace {

string const description = "This is a Controller application for launching worker management requests.";

bool const injectDatabaseOptions = true;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

}  // namespace

namespace lsst::qserv::replica {

AdminApp::Ptr AdminApp::create(int argc, char* argv[]) { return Ptr(new AdminApp(argc, argv)); }

AdminApp::AdminApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    // Configure the command line parser

    parser().commands("operation", {"STATUS", "SUSPEND", "RESUME", "REQUESTS", "DRAIN"}, _operation)
            .flag("all-workers",
                  "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY).",
                  _allWorkers)
            .flag("progress-report",
                  "The flag triggering progress report when executing batches of requests.", _progressReport)
            .flag("error-report", "The flag triggering detailed report on failed requests.", _errorReport)
            .flag("tables-vertical-separator",
                  "Print vertical separator when displaying tabular data in reports.", _verticalSeparator);

    parser().command("STATUS").description("Retrieve and display the status of each worker.");

    parser().command("SUSPEND").description(
            "Suspend workers services on all workers. Cancel requests which are being processed"
            " and put them back into the input queue. The operation won't affect requests"
            " which have already completed.");

    parser().command("RESUME").description("Resume workers services on all workers");

    parser().command("REQUESTS")
            .description(
                    "Retrieve and display the information of all (regardless of their processing status)"
                    " requests from all workers.")
            .flag("dump-request-info", "Print detailed info on requests obtained from the workers.",
                  _dumpRequestInfo);

    parser().command("DRAIN").description(
            "Cancel the in-progress (if any) requests on all workers, then empty all queues.");
}

int AdminApp::runImpl() {
    auto controller = Controller::create(serviceProvider());

    // Launch requests against a collection of workers

    CommonRequestTracker<ServiceManagementRequestBase> tracker(cout, _progressReport, _errorReport);

    auto const workerNames =
            _allWorkers ? serviceProvider()->config()->allWorkers() : serviceProvider()->config()->workers();

    for (auto&& workerName : workerNames) {
        if (_operation == "STATUS") {
            tracker.add(controller->statusOfWorkerService(
                    workerName, [&tracker](ServiceStatusRequest::Ptr const& ptr) { tracker.onFinish(ptr); }));

        } else if (_operation == "SUSPEND") {
            tracker.add(controller->suspendWorkerService(
                    workerName,
                    [&tracker](ServiceSuspendRequest::Ptr const& ptr) { tracker.onFinish(ptr); }));

        } else if (_operation == "RESUME") {
            tracker.add(controller->resumeWorkerService(
                    workerName, [&tracker](ServiceResumeRequest::Ptr const& ptr) { tracker.onFinish(ptr); }));

        } else if (_operation == "REQUESTS") {
            tracker.add(controller->requestsOfWorkerService(
                    workerName,
                    [&tracker](ServiceRequestsRequest::Ptr const& ptr) { tracker.onFinish(ptr); }));

        } else if (_operation == "DRAIN") {
            tracker.add(controller->drainWorkerService(
                    workerName, [&tracker](ServiceDrainRequest::Ptr const& ptr) { tracker.onFinish(ptr); }));

        } else {
            throw logic_error("AdminApp::" + string(__func__) + "  unsupported operation: " + _operation);
        }
    }

    // Wait before all request are finished

    tracker.track();

    // Analyze and display results

    vector<string> workerName;
    vector<string> startedSecondsAgo;
    vector<string> state;
    vector<string> numNewRequests;
    vector<string> numInProgressRequests;
    vector<string> numFinishedRequests;

    for (auto const& ptr : tracker.requests) {
        workerName.push_back(ptr->workerName());
        if ((ptr->state() == Request::State::FINISHED) &&
            (ptr->extendedState() == Request::ExtendedState::SUCCESS)) {
            startedSecondsAgo.push_back(
                    to_string((util::TimeUtils::now() - ptr->getServiceState().startTime) / 1000));
            state.push_back(ptr->getServiceState().state2string());
            numNewRequests.push_back(to_string(ptr->getServiceState().numNewRequests));
            numInProgressRequests.push_back(to_string(ptr->getServiceState().numInProgressRequests));
            numFinishedRequests.push_back(to_string(ptr->getServiceState().numFinishedRequests));
        } else {
            startedSecondsAgo.push_back("*");
            state.push_back("*");
            numNewRequests.push_back("*");
            numInProgressRequests.push_back("*");
            numFinishedRequests.push_back("*");
        }
    }
    util::ColumnTablePrinter tableWorkers("WORKERS:", "  ", _verticalSeparator);

    tableWorkers.addColumn("worker", workerName, util::ColumnTablePrinter::Alignment::LEFT);
    tableWorkers.addColumn("started (seconds ago)", startedSecondsAgo);
    tableWorkers.addColumn("state", state, util::ColumnTablePrinter::Alignment::LEFT);
    tableWorkers.addColumn("queued", numNewRequests);
    tableWorkers.addColumn("in-progress", numInProgressRequests);
    tableWorkers.addColumn("finished", numFinishedRequests);

    cout << "\n";
    tableWorkers.print(cout, false, false);

    if (_dumpRequestInfo) {
        vector<string> workerName;
        vector<string> requestId;
        vector<string> requestType;
        vector<string> queue;
        vector<uint32_t> priority;

        auto analyzeRemoteRequestInfo = [&](string const& workerName_, string const& queueName,
                                            ProtocolServiceResponseInfo const& info) {
            workerName.push_back(workerName_);
            requestId.push_back(info.id());
            requestType.push_back(ProtocolQueuedRequestType_Name(info.queued_type()));
            queue.push_back(queueName);
            priority.push_back(info.priority());
        };
        for (auto const& ptr : tracker.requests) {
            if ((ptr->state() == Request::State::FINISHED) &&
                (ptr->extendedState() == Request::ExtendedState::SUCCESS)) {
                for (auto&& info : ptr->getServiceState().newRequests) {
                    analyzeRemoteRequestInfo(ptr->workerName(), "QUEUED", info);
                }
                for (auto&& info : ptr->getServiceState().inProgressRequests) {
                    analyzeRemoteRequestInfo(ptr->workerName(), "IN-PROGRESS", info);
                }
                for (auto&& info : ptr->getServiceState().finishedRequests) {
                    analyzeRemoteRequestInfo(ptr->workerName(), "FINISHED", info);
                }
            }
        }
        util::ColumnTablePrinter tableRequests("REQUESTS:", "  ", _verticalSeparator);

        tableRequests.addColumn("worker", workerName, util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("id", requestId, util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("type", requestType, util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("queue", queue, util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("priority", priority);

        cout << "\n";
        tableRequests.print(cout, false, false);
    }
    return 0;
}

}  // namespace lsst::qserv::replica
