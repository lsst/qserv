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
#include "replica/apps/QservXrootdSsiApp.h"

// System headers
#include <atomic>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

// Third-party headers
#include "XrdSsi/XrdSsiErrInfo.hh"
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiRequest.hh"
#include "XrdSsi/XrdSsiResource.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "util/BlockPost.h"
#include "util/TimeUtils.h"

using namespace std;
using namespace lsst::qserv;

extern XrdSsiProvider* XrdSsiProviderClient;

namespace {
string const description =
        "This application sends requests to Qserv workers over XROOTD/SSI for a purpose of testing"
        " the performance, scalability and stability of the message delivery services.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

double const millisecondsInSecond = 1000.;

/// @return 'YYYY-MM-DD HH:MM:SS.mmm  '
string timestamp() {
    return util::TimeUtils::toDateTimeString(chrono::milliseconds(util::TimeUtils::now())) + "  ";
}

string getErrorText(XrdSsiErrInfo const& e) {
    ostringstream os;
    int errCode;
    os << "XrdSsiError error: " << e.Get(errCode);
    os << ", code=" << errCode;
    return os.str();
}

class SsiRequest : public XrdSsiRequest {
public:
    explicit SsiRequest(string const& id, atomic<uint64_t>& numFinishedRequests)
            : _id(id), _numFinishedRequests(numFinishedRequests) {}
    virtual ~SsiRequest() {}
    char* GetRequest(int& requestLength) override {
        // cout << "SsiRequest::" << __func__ << " id: " << _id << endl;
        requestLength = 16;
        return _requestData;
    }
    bool ProcessResponse(XrdSsiErrInfo const& eInfo, XrdSsiRespInfo const& rInfo) override {
        int errCode;
        eInfo.Get(errCode);
        if (errCode != 0) {
            // cout << "SsiRequest::" << __func__ << " id: " << _id << ": " << ::getErrorText(eInfo) << endl;
        }
        // Finished();
        //_numFinishedRequests.fetch_add(1);
        return true;
    }
    void ProcessResponseData(XrdSsiErrInfo const& eInfo, char* buff, int blen, bool last) override {
        int errCode;
        eInfo.Get(errCode);
        if (errCode != 0) {
            cout << "SsiRequest::" << __func__ << " id: " << _id << ": " << ::getErrorText(eInfo) << endl;
        }
    }

private:
    string _id;
    atomic<uint64_t>& _numFinishedRequests;
    char _requestData[1024];
};

}  // namespace

namespace lsst::qserv::replica {

QservXrootdSsiApp::Ptr QservXrootdSsiApp::create(int argc, char* argv[]) {
    return Ptr(new QservXrootdSsiApp(argc, argv));
}

QservXrootdSsiApp::QservXrootdSsiApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().required("url", "The connection URL for the XROOTD/SSI services.", _url)
            .option("num-threads", "The number of threads for running the test.", _numThreads)
            .option("report-interval-ms",
                    "An interval (milliseonds) for reporting the performance counters. Must be greater than "
                    "0.",
                    _reportIntervalMs)
            .flag("progress", "The flag which would turn on periodic progress report on the requests.",
                  _progress)
            .flag("verbose", "The flag which would turn on detailed report on the requests.", _verbose);
}

int QservXrootdSsiApp::runImpl() {
    // Connect to the service
    XrdSsiErrInfo eInfo;
    XrdSsiService* xrdSsiService = XrdSsiProviderClient->GetService(eInfo, _url);
    if (!xrdSsiService) {
        cerr << "Error obtaining XrdSsiService: serviceUrl=" << _url << ", " << ::getErrorText(eInfo) << endl;
        return 1;
    }

    // Counters updated by the requests
    atomic<uint64_t> numRequests(0);
    atomic<uint64_t> numFinishedRequests(0);

    // The requests
    vector<shared_ptr<::SsiRequest>> requests;
    mutex requestsMutex;

    // Launch all threads in the pool
    atomic<size_t> numThreadsActive{0};
    vector<thread> threads;
    for (size_t i = 0; i < _numThreads; ++i) {
        numThreadsActive.fetch_add(1);
        threads.push_back(thread([&]() {
            for (int chunk = 0; chunk < 150000; ++chunk) {
                string const id = to_string(i) + ":" + to_string(chunk);
                XrdSsiResource::Affinity const affinity = XrdSsiResource::Strong;
                XrdSsiResource resource("/chk/wise_01/" + to_string(chunk), "", id, "", 0, affinity);
                shared_ptr<::SsiRequest> request(new ::SsiRequest(id, numFinishedRequests));
                xrdSsiService->ProcessRequest(*(request.get()), resource);
                ++numRequests;
                lock_guard<mutex> lock(requestsMutex);
                requests.push_back(request);
            }
            // util::BlockPost bp(10*1000, 20*1000);
            // bp.wait();
            numThreadsActive.fetch_sub(1);
            cout << ::timestamp() << "Thread " << i << " finished" << endl;
        }));
    }

    // Begin the monitoring & reporting cycle
    util::BlockPost bp(_reportIntervalMs, _reportIntervalMs + 1);
    while (numThreadsActive.load() > 0) {
        uint64_t beginNumRequests = numRequests;
        bp.wait(_reportIntervalMs);
        uint64_t const endNumRequests = numRequests;
        double const requestsPerSecond =
                (endNumRequests - beginNumRequests) / (_reportIntervalMs / millisecondsInSecond);
        if (_progress) {
            cout << ::timestamp() << "Sent: " << setprecision(7) << requestsPerSecond << " Req/s" << endl;
        }
        beginNumRequests = endNumRequests;
    }
    for (auto&& t : threads) {
        t.join();
    }
    // while (numFinishedRequests.load() < numRequests) {
    //     cout << ::timestamp() << "Waiting for all requests to finish: " << numFinishedRequests.load() << "
    //     / "
    //          << numRequests.load() << endl;
    //     bp.wait(1000);
    // }
    // cout << ::timestamp() << "All requests reported as fiished" << endl;
    cout << ::timestamp() << "All threads finished, calling Finished() on " << requests.size() << " requests"
         << endl;
    for (auto&& request : requests) {
        request->Finished(true);
    }
    cout << ::timestamp() << "Done calling Finished() on the requests" << endl;
    util::BlockPost bp1(10 * 1000, 20 * 1000);
    bp1.wait();
    return 0;
}

}  // namespace lsst::qserv::replica
