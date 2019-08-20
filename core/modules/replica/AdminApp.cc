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
#include <iostream>
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/Controller.h"
#include "replica/Performance.h"
#include "replica/ServiceManagementJob.h"
#include "util/TablePrinter.h"

using namespace std;

namespace {

string const description =
    "This is an application for launching worker management requests.";

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
        "command",
        {"STATUS", "SUSPEND", "RESUME", "REQUESTS", "DRAIN", "RECONFIG"},
        _command);

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

    parser().command("RECONFIG").description(
        "Reload Configuration. Requests known to workers won't be affected.");

    parser().option(
        "timeout",
        "Maximum timeout (seconds) for the management requests.",
        _timeout);

    parser().flag(
        "all-workers",
        "The flag for selecting all workers regardless of their status (DISABLED or READ-ONLY).",
        _allWorkers);

    parser().flag(
        "tables-vertical-separator",
        "Print vertical separator when displaying tabular data in reports.",
        _verticalSeparator);
}


int AdminApp::runImpl() {

    auto const controller = Controller::create(serviceProvider());

    ServiceManagementBaseJob::Ptr job;
    if      (_command == "STATUS")   job = ServiceStatusJob::create(  _allWorkers, _timeout, controller);
    else if (_command == "REQUESTS") job = ServiceRequestsJob::create(_allWorkers, _timeout, controller);
    else if (_command == "SUSPEND")  job = ServiceSuspendJob::create( _allWorkers, _timeout, controller);
    else if (_command == "RESUME")   job = ServiceResumeJob::create(  _allWorkers, _timeout, controller);
    else if (_command == "DRAIN")    job = ServiceDrainJob::create(   _allWorkers, _timeout, controller);
    else if (_command == "RECONFIG") job = ServiceReconfigJob::create(_allWorkers, _timeout, controller);
    else throw logic_error("AdminApp::" + string(__func__) + "  unsupported operation: " + _command);

    job->start();
    job->wait();

    // Analyze and display results

    vector<string> workerName;
    vector<string> startedSecondsAgo;
    vector<string> state;
    vector<string> numNewRequests;
    vector<string> numInProgressRequests;
    vector<string> numFinishedRequests;

    auto&& result = job->getResultData();
    for (auto&& itr: result.workers) {
        auto&& worker = itr.first;
        bool const success = itr.second;

        workerName.push_back(worker);

        if (success) {
            auto&& serviceState = result.serviceState.at(worker);
            startedSecondsAgo    .push_back(to_string((PerformanceUtils::now() - serviceState.startTime) / 1000));
            state                .push_back(                                     serviceState.state2string());
            numNewRequests       .push_back(to_string(                           serviceState.numNewRequests));
            numInProgressRequests.push_back(to_string(                           serviceState.numInProgressRequests));
            numFinishedRequests  .push_back(to_string(                           serviceState.numFinishedRequests));

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

        auto analyzeRemoteRequestInfo = [&](string const& worker,
                                            string const& queueName,
                                            ProtocolServiceResponseInfo const& info) {
            workerName .push_back(worker);
            requestId  .push_back(info.id());
            requestType.push_back(ProtocolQueuedRequestType_Name(info.queued_type()));
            queue      .push_back(queueName);
            priority   .push_back(info.priority());
        };
        for (auto&& itr: result.workers) {
            auto&& worker = itr.first;
            bool const success = itr.second;
            if (success) {
                auto&& serviceState = result.serviceState.at(worker);
                for (auto&& info: serviceState.newRequests) {
                    analyzeRemoteRequestInfo(worker, "QUEUED", info);
                }
                for (auto&& info: serviceState.inProgressRequests) {
                    analyzeRemoteRequestInfo(worker, "IN-PROGRESS", info);
                }
                for (auto&& info: serviceState.finishedRequests) {
                    analyzeRemoteRequestInfo(worker, "FINISHED", info);
                }
            }
        }
        util::ColumnTablePrinter tableRequests("REQUESTS:", "  ", _verticalSeparator);

        tableRequests.addColumn("worker",   workerName,  util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("id",       requestId,   util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("type",     requestType, util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("queue",    queue,       util::ColumnTablePrinter::Alignment::LEFT);
        tableRequests.addColumn("priority", priority);

        cout << "\n";
        tableRequests.print(cout, false, false);
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
