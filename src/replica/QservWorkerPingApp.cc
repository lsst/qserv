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
#include "replica/QservWorkerPingApp.h"

// System headers
#include <atomic>
#include <functional>
#include <iomanip>
#include <iostream>
#include <mutex>

// Qserv headers
#include "replica/QservMgtServices.h"
#include "replica/ServiceProvider.h"
#include "replica/TestEchoQservMgtRequest.h"
#include "util/BlockPost.h"

using namespace std;

namespace {

string const description =
    "This is an application for testing a communication"
    " path with Qserv workers. The application will be sending multiple requests"
    " containing a string that is expected to be echoed back by a worker.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = true;
bool const enableServiceProvider = true;
bool const injectXrootdOptions = true;

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

QservWorkerPingApp::Ptr QservWorkerPingApp::create(int argc, char* argv[]) {
    return Ptr(new QservWorkerPingApp(argc, argv));
}


QservWorkerPingApp::QservWorkerPingApp(int argc, char* argv[])
    :   Application(
            argc, argv,
            description,
            injectDatabaseOptions,
            boostProtobufVersionCheck,
            enableServiceProvider,
            injectXrootdOptions
        ) {

    parser().required(
        "worker",
        "The name of a Qserv worker.",
        _worker
    ).required(
        "data",
        "The data string to be sent to the worker.",
        _data
    ).option(
        "num-requests",
        "The total number of requests to be launched. The parameter must be set"
        "to 1 or greater",
        _numRequests
    ).option(
        "max-requests",
        "The maximum number of requests to be in flight at any moment. The parameter must be set"
        "to 1 or greater",
        _maxRequests
    ).option(
        "expiration-ival-sec",
        "Request expiration interval. Requests will be cancelled if no response"
        " received before the specified timeout expires. Zero value of the parameter"
        " corresponds to the corresponding default set in the configuration.",
        _requestExpirationIvalSec
    ).flag(
        "verbose",
        "For reporting a progress of the testing.",
        _verbose
    );
}


int QservWorkerPingApp::runImpl() {

    if (_numRequests < 1) {
        cerr << "error: parameter 'num-requests' should have a value of 1 or higher." << endl;
        return 1;
    }
    if (_maxRequests < 1) {
        cerr << "error: parameter 'max-requests' should have a value of 1 or higher." << endl;
        return 1;
    }

    std::string const noParentJobId;

    atomic<size_t> numActive{0};
    atomic<size_t> numSuccess{0};
    atomic<size_t> numFailed{0};
    mutex mtx;
    condition_variable cv;

    auto const logEvent = [&](unique_lock<mutex> const& lock,
                              TestEchoQservMgtRequest::Ptr const& request,
                              string const& event) {
        if (!_verbose) return;
        cout << "active: "  << setw(6) << numActive
             << "success: " << setw(6) << numSuccess
             << "failed: "  << setw(6) << numFailed
             << " id=" << request->id() << " state=" << request->state2string()
             << " " << event << endl;
    };

    auto const onStart = [&] (unique_lock<mutex> const& lock,
                              TestEchoQservMgtRequest::Ptr const& request) {
        numActive++;
        logEvent(lock, request, "started");
    };

    auto const onFinish = [&] (TestEchoQservMgtRequest::Ptr const& request) {
        {
            unique_lock<mutex> lock(mtx);
            numActive--;
            if (request->extendedState() == QservMgtRequest::SUCCESS) {
                numSuccess++;
            } else {
                numFailed++;
            }
            logEvent(lock, request, "finished");
        }
        cv.notify_one();
    };

    for (size_t i = 0; i < _numRequests; ++i) {
        auto const request = serviceProvider()->qservMgtServices()->echo(
                _worker, _data, noParentJobId,
                onFinish,
                _requestExpirationIvalSec);
        unique_lock<mutex> lock(mtx);
        onStart(lock, request);
        cv.wait(lock, [&] { return numActive < _maxRequests; });
    }

    // Wait in the timed loop before all active requests will finish.
    int const sleepTimeMs = 1000;
    while (numActive > 0) {
        util::BlockPost::wait(sleepTimeMs);
        unique_lock<mutex> lock(mtx);
        cout << "active: "  << setw(6) << numActive
             << "success: " << setw(6) << numSuccess
             << "failed: "  << setw(6) << numFailed
             << endl;
    }
    return 0;
}

}}} // namespace lsst::qserv::replica
