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
#include "replica/AdminApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/Performance.h"
#include "replica/RequestTracker.h"
#include "replica/ServiceManagementRequest.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This is a Controller application for launching worker management requests.";

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

AdminApp::Ptr AdminApp::create(int argc, char* argv[]) {
    return Ptr(
        new AdminApp(argc, argv)
    );
}


AdminApp::AdminApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            true    /* injectDatabaseOptions */,
            true    /* boostProtobufVersionCheck */,
            true    /* enableServiceProvider */
        ) {

    // Configure the command line parser

    parser().commands(
        "operation",
        {"STATUS", "SUSPEND", "RESUME", "REQUESTS", "DRAIN"},
        _operation);

    parser().command("STATUS").description(
        "Retrieve and display the status of each worker.");

    parser().command("SUSPEND").description(
        "Suspend workers services on all workers. Cancel requests which are being processed"
        " and put them back into the input queue. The operation won't affect requests"
        " which have already completed.");

    parser().command("RESUME").description(
        "Resume workers services on all workers");

    parser().command("REQUESTS").description(
        "Retrieve and display the information of all (regardless of their processing status)"
        " requests from all workers.");

    parser().command("REQUESTS").flag(
        "dump-request-info",
        "Print detailed info on requests obtained from the workers.",
        _dumpRequestInfo);

    parser().command("DRAIN").description(
        "Cancel the in-progress (if any) requests on all workers, then empty all queues.");

    parser().option(
        "timeout",
        "Maximum timeout (seconds) for the management request.s",
        _requestExpirationIvalSec);

    parser().flag(
        "all-workers",
        "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY).",
        _allWorkers);

    parser().flag(
        "progress-report",
        "The flag triggering progress report when executing batches of requests.",
        _progressReport);

    parser().flag(
        "error-report",
        "The flag triggering detailed report on failed requests.",
        _errorReport);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in reports.",
        _verticalSeparator);
}


int AdminApp::runImpl() {

    auto controller = Controller::create(serviceProvider());

    // Launch requests against a collection of workers

    CommonRequestTracker<ServiceManagementRequestBase> tracker(cout, _progressReport, _errorReport);

    auto const workers = _allWorkers ?
        serviceProvider()->config()->allWorkers() :
        serviceProvider()->config()->workers();

    string const emptyJobId;

    for (auto&& worker: workers) {

        if (_operation == "STATUS") {
            tracker.add(
                controller->statusOfWorkerService(
                    worker,
                    [&tracker] (ServiceStatusRequest::Ptr const& ptr) { tracker.onFinish(ptr); },
                    emptyJobId,
                    _requestExpirationIvalSec));

        } else if (_operation == "SUSPEND") {
            tracker.add(
                controller->suspendWorkerService(
                    worker,
                    [&tracker] (ServiceSuspendRequest::Ptr const& ptr) { tracker.onFinish(ptr); },
                    emptyJobId,
                    _requestExpirationIvalSec));

        } else if (_operation == "RESUME") {
            tracker.add(
                controller->resumeWorkerService(
                    worker,
                    [&tracker] (ServiceResumeRequest::Ptr const& ptr) { tracker.onFinish(ptr); },
                    emptyJobId,
                    _requestExpirationIvalSec));

        } else if (_operation == "REQUESTS") {
            tracker.add(
                controller->requestsOfWorkerService(
                    worker,
                    [&tracker] (ServiceRequestsRequest::Ptr const& ptr) { tracker.onFinish(ptr); },
                    emptyJobId,
                    _requestExpirationIvalSec));

        } else if (_operation == "DRAIN") {
            tracker.add(
                controller->drainWorkerService(
                    worker,
                    [&tracker] (ServiceDrainRequest::Ptr const& ptr) { tracker.onFinish(ptr); },
                    emptyJobId,
                    _requestExpirationIvalSec));

        } else {
            throw logic_error(
                    "AdminApp::" + string(__func__) + "  unsupported operation: " + _operation);
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

    for (auto const& ptr: tracker.requests) {

        workerName.push_back(ptr->worker());

        if ((ptr->state()         == Request::State::FINISHED) &&
            (ptr->extendedState() == Request::ExtendedState::SUCCESS)) {

            startedSecondsAgo    .push_back(to_string((PerformanceUtils::now() - ptr->getServiceState().startTime) / 1000));
            state                .push_back(                                     ptr->getServiceState().state2string());
            numNewRequests       .push_back(to_string(                           ptr->getServiceState().numNewRequests));
            numInProgressRequests.push_back(to_string(                           ptr->getServiceState().numInProgressRequests));
            numFinishedRequests  .push_back(to_string(                           ptr->getServiceState().numFinishedRequests));

        } else {
            startedSecondsAgo    .push_back("*");
            state                .push_back("*");
            numNewRequests       .push_back("*");
            numInProgressRequests.push_back("*");
            numFinishedRequests  .push_back("*");
        }
    }
    util::ColumnTablePrinter tableWorkers("WORKERS:", "  ", _verticalSeparator);

    tableWorkers.addColumn("worker",                workerName, util::ColumnTablePrinter::Alignment::LEFT);
    tableWorkers.addColumn("started (seconds ago)", startedSecondsAgo);
    tableWorkers.addColumn("state",                 state,      util::ColumnTablePrinter::Alignment::LEFT);
    tableWorkers.addColumn("queued",                numNewRequests);
    tableWorkers.addColumn("in-progress",           numInProgressRequests);
    tableWorkers.addColumn("finished",              numFinishedRequests);

    cout << "\n";
    tableWorkers.print(cout, false, false);

    if (_dumpRequestInfo) {

        vector<string>   workerName;
        vector<string>   requestId;
        vector<string>   requestType;
        vector<string>   queue;
        vector<uint32_t> priority;
        vector<string>   database;
        vector<uint32_t> chunk;
        vector<string>   sourceWorkerName;

        auto analyzeRemoteRequestInfo = [&](string const& worker,
                                            string const& queueName,
                                            ProtocolServiceResponseInfo const& info) {
            workerName      .push_back(worker);
            requestId       .push_back(info.id());
            requestType     .push_back(ProtocolReplicaRequestType_Name(info.replica_type()));
            queue           .push_back(queueName);
            priority        .push_back(info.priority());
            database        .push_back(info.database());
            chunk           .push_back(info.chunk());
            sourceWorkerName.push_back(info.worker());
        };
        for (auto const& ptr: tracker.requests) {

            if ((ptr->state()         == Request::State::FINISHED) &&
                (ptr->extendedState() == Request::ExtendedState::SUCCESS)) {

                for (auto&& info: ptr->getServiceState().newRequests) {
                    analyzeRemoteRequestInfo(ptr->worker(), "QUEUED", info);
                }
                for (auto&& info: ptr->getServiceState().inProgressRequests) {
                    analyzeRemoteRequestInfo(ptr->worker(), "IN-PROGRESS", info);
                }
                for (auto&& info: ptr->getServiceState().finishedRequests) {
                    analyzeRemoteRequestInfo(ptr->worker(), "FINISHED", info);
                }
            }
        }
        util::ColumnTablePrinter tableRequests("REQUESTS:", "  ", _verticalSeparator);

        tableRequests.addColumn("worker",        workerName,       util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("id",            requestId,        util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("type",          requestType,      util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("queue",         queue,            util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("priority",      priority);
        tableRequests.addColumn("database",      database,         util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("chunk",         chunk);
        tableRequests.addColumn("source worker", sourceWorkerName, util::ColumnTablePrinter::Alignment::LEFT);

        cout << "\n";
        tableRequests.print(cout, false, false);
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
