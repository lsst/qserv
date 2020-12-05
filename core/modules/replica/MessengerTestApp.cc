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
#include "replica/MessengerTestApp.h"

// System headers
#include <iomanip>
#include <iostream>
#include <sstream>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/EchoRequest.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "Class MessengerTestApp implements a tool which tests the Messenger Network"
    " w/o leaving side effects on the workers. The tool will be sending and tracking"
    " requests of class EchoRequest.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

MessengerTestApp::Ptr MessengerTestApp::create(int argc, char* argv[]) {
    return Ptr(new MessengerTestApp(argc, argv));
}


MessengerTestApp::MessengerTestApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            ::description,
            injectDatabaseOptions, boostProtobufVersionCheck, enableServiceProvider) {

    parser().required(
        "worker",
        "The name of a worker to be used during the testing.",
        _workerName
    ).option(
        "data",
        "The data payload to be sent to the worker and expected to be back.",
        _data
    ).option(
        "proc-time-sec",
        "The 'processing' time (seconds) of requests by the worker's threads."
        " This interval doesn't include any latencies for delivering requests to"
        " the threads and retreiving results back. If a value of the parameter"
        " is set to 0 then requests will be instantly answered by the worker"
        " w/o putting them into a queue for further processing by the workers'"
        " threads. Zero value is also good for testing the performance of the protocol.",
        _proccesingTimeSec
    ).option(
        "priority",
        "The priority level of requests.",
        _priority
    ).option(
        "expiration-ival-sec",
        "The request expiration interval (seconds). The parameter specifies"
        " thw maximum 'life expectancy' of the requests after they're submitted and,"
        " before they succeeded, failed or expired. The default value of 0 will result"
        " in fetching a value of the parameter from the Replication System's configuration.",
        _requestExpirationIvalSec
    ).option(
        "requests",
        "The total number of requests to be submitted to the worker (must be"
        " strictly greater than 0).",
        _totalRequests
    ).option(
        "max-active-requests",
        "The number of active (in-flight) requests not to exceed at any given moment"
        "  of time. This parameter is used for flow control, and to prevent the application"
        " from consuming too much memory.  A value of the parameter should not be"
        " less than the total number of requests to be submitted to the worker.",
        _maxActiveRequests
    ).option(
        "events-report-ival-sec",
        "The interval for reporting events. If a value of the parameter"
        "is 0 then events won't be reported",
        _eventsReportIvalSec
    ).flag(
        "report-request-events",
        "Enable extended reporting on sending requests and analysing responses.",
        _reportRequestEvents
    );
}


int MessengerTestApp::runImpl() {
    string const context = string(__func__) + "  ";

    if (_totalRequests <= 0) {
        throw invalid_argument(
                context + "the total number of requests must be strictly greater than 0");
    }
    if (_maxActiveRequests >  _totalRequests) {
        throw invalid_argument(
                context + "the number of active requests can't exceed the total number "
                + to_string(_totalRequests) + " of requests.");
    }

    auto const controller = Controller::create(serviceProvider());
    bool const keepTracking = true;
    string const noParentJobId;

    for (int i = 0; i < _totalRequests; ++i) {

        // Wait here if (while) the number of active requests is at
        // the allowed maximum.
        unique_lock<mutex> lock(_mtx);
        _onNumActiveCv.wait(lock, [&]{ return _numActive < _maxActiveRequests; });

        // Submit the next request.
        auto const request = controller->echo(
            _workerName,
            _data,
            _proccesingTimeSec,
            [&](EchoRequest::Ptr request) {
                {
                    unique_lock<mutex> lock(_mtx);
                    _numActive--;
                    _numFinished++;
                    switch (request->extendedState()) {
                        case Request::SUCCESS: _numSuccess++; break;
                        case Request::TIMEOUT_EXPIRED: _numExpired++; break;
                        default: _numFailed++; break;
                    }
                }
                _onNumActiveCv.notify_one();

                _logEvent(
                    unique_lock<mutex>(_mtx),
                    request->performance().c_finish_time,
                    "RECV " + request->id() + " " + Request::state2string(request->extendedState())
                );
            },
            _priority,
            keepTracking,
            noParentJobId,
            _requestExpirationIvalSec
        );
        _numActive++;

        _logEvent(
            lock,
            request->performance().c_create_time,
            "SEND " + request->id()
        );
    }

    // Wait before all requests will finish.
    util::BlockPost blockPost(1000, 1001);
    while (_numFinished < _totalRequests) {
        blockPost.wait();
    }
 
    // Always make the final report to clear up remaining entries (if any)
    // in the event log.
    _reportEvents(unique_lock<mutex>(_mtx));
    return 0;
}


void MessengerTestApp::_logEvent(unique_lock<mutex> const& lock,
                                 uint64_t timeMs,
                                 std::string const& event) {
    if (_reportRequestEvents) {
        _eventLog.push_back(make_pair(timeMs, event));
    }
    _reportEvents(lock);
}


void MessengerTestApp::_reportEvents(unique_lock<mutex> const& lock) {

    if (_eventsReportIvalSec == 0) return;
    if (_prevEventsReportMs == 0) _prevEventsReportMs = PerformanceUtils::now();

    uint64_t const currentTimeMs = PerformanceUtils::now();
    if ((currentTimeMs - _prevEventsReportMs) / 1000 >= _eventsReportIvalSec) {
        _prevEventsReportMs = currentTimeMs;

        // First report events on requests (if any). Note that the event log must
        // be cleared to avoid double reporting.
        for (auto const& entry: _eventLog) {
            cout << PerformanceUtils::toDateTimeString(chrono::milliseconds(entry.first))
                << "  " << entry.second << "\n";
        }
        _eventLog.clear();

        // Then report the general statistics.
        cout << PerformanceUtils::toDateTimeString(chrono::milliseconds(currentTimeMs)) << "  "
             << "STAT"
             << " active: "  << setw(6) << _numActive
             << " finished: " << setw(6) << _numFinished
             << " success: " << setw(6) << _numSuccess
             << " expired: " << setw(6) << _numExpired
             << " failed: "  << setw(6) << _numFailed << endl;
    }
}

}}} // namespace lsst::qserv::replica
