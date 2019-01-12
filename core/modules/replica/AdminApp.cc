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

string const description {
    "This is a Controller application for launching worker management requests."
};

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

AdminApp::Ptr AdminApp::create(int argc,
                               const char* const argv[]) {
    return Ptr(
        new AdminApp(
            argc,
            argv
        )
    );
}


AdminApp::AdminApp(int argc,
                   const char* const argv[])
    :   Application(
            argc,
            argv,
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

    parser().option(
        "timeout",
        "maximum timeout (seconds) for the management requests",
        _requestExpirationIvalSec);

    parser().flag(
        "all-workers",
        "the flag for selecting all workers regardless of their status (DISABLED or READ-ONLY)",
        _allWorkers);

    parser().flag(
        "progress-report",
        "the flag triggering progress report when executing batches of requests",
        _progressReport);

    parser().flag(
        "error-report",
        "the flag triggering detailed report on failed requests",
        _errorReport);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in reports",
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
            throw logic_error("unsupported operation: " + _operation);
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

            startedSecondsAgo.push_back(    to_string((PerformanceUtils::now() - ptr->getServiceState().startTime) / 1000));
            state.push_back(                                                     ptr->getServiceState().state2string());
            numNewRequests.push_back(       to_string(                           ptr->getServiceState().numNewRequests));
            numInProgressRequests.push_back(to_string(                           ptr->getServiceState().numInProgressRequests));
            numFinishedRequests.push_back(  to_string(                           ptr->getServiceState().numFinishedRequests));

        } else {
            startedSecondsAgo.push_back(    "*");
            state.push_back(                "*");
            numNewRequests.push_back(       "*");
            numInProgressRequests.push_back("*");
            numFinishedRequests.push_back(  "*");
        }
    }

    cout << "\n";

    util::ColumnTablePrinter table("WORKERS:", "  ", _verticalSeparator);

    table.addColumn("worker",                workerName, util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("started (seconds ago)", startedSecondsAgo);
    table.addColumn("state",                 state,      util::ColumnTablePrinter::Alignment::LEFT);
    table.addColumn("queued",                numNewRequests);
    table.addColumn("in-progress",           numInProgressRequests);
    table.addColumn("finished",              numFinishedRequests);

    table.print(cout, false, false);

    return 0;
}

}}} // namespace lsst::qserv::replica
