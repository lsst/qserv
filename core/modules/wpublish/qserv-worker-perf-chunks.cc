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

/**
 * This application tests the performance of the XRootD/SSI protocol
 * using Qserv workers as servers. The application also supports
 * the multi-threaded option for initiating requests.
 */

// System header
#include <condition_variable>
#include <iostream>
#include <iomanip>
#include <list>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "proto/worker.pb.h"
#include "util/BlockPost.h"
#include "util/CmdLineParser.h"
#include "util/File.h"
#include "wpublish/TestEchoQservRequest.h"

/// This C++ symbol is provided by the SSI shared library
extern XrdSsiProvider* XrdSsiProviderClient;

namespace global   = lsst::qserv;
namespace util     = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

using namespace std;
using RequestT = wpublish::TestEchoQservRequest;

namespace {

// Command line parameters

string fileName;
unsigned int numRequests;
string value;
string serviceProviderLocation;
unsigned int numResources;
bool resourceFirst;
unsigned int numThreads;
unsigned int flowControlLimit;
bool silent;
int xrootdCBThreadsMax;
int xrootdCBThreadsInit;


/**
 * The synchronize counter, which is used to limit the number of the "in-flight"
 * requests if the flow-control is enabled. The later is enabled if a value of
 * the contructor's parameter 'maxRequestsAllowed' is not equal to 0.
 */
class Counter {
public:
    Counter() = delete;
    Counter(Counter const&) = delete;
    Counter& operator=(Counter const&) = delete;
    ~Counter() = default;

    explicit Counter(unsigned int maxRequestsAllowed)
        :   _maxRequestsAllowed(maxRequestsAllowed) {
    }

    void inc() {
        if (_maxRequestsAllowed == 0) {
            ++_counter;
        } else {
            unique_lock<mutex> lock(_mtx);
            _cv.wait(lock, [&]() {
                return _counter < _maxRequestsAllowed;
            });
            ++_counter;
        }
    }

    void dec() {
        if (_maxRequestsAllowed == 0) {
            --_counter;
        } else {
            unique_lock<mutex> lock(_mtx);
            --_counter;
            lock.unlock();
            _cv.notify_one();
        }
    }
    
    unsigned int counter() const {
        return _counter;
    }

private:
    unsigned int _maxRequestsAllowed;
    atomic<unsigned int> _counter{0};
    std::mutex _mtx;
    std::condition_variable _cv;
};


RequestT::Ptr makeRequest(
        Counter& numRequestsInFlight,
        XrdSsiService* serviceProvider,
        string const& resourcePath) {
    auto request = RequestT::create(
        value,
        [&numRequestsInFlight](RequestT::Status status,
                               string const& error,
                               string const& sent,
                               string const& received) {
            if (not silent) {
                if (status != RequestT::Status::SUCCESS) {
                    cout << "status: " << RequestT::status2str(status) << ", error: " << error << endl;
                } else {
                    cout << "value sent: '" << sent << "', received: '" << received << "'" << endl;
                }
            }
            numRequestsInFlight.dec();
        }
    );
    numRequestsInFlight.inc();
    XrdSsiResource resource(resourcePath);
    serviceProvider->ProcessRequest(*request, resource);
    return request;
}


int test() {

    vector<string> const resources = util::File::getLines(fileName, true);
    if (not numResources or (resources.size() < numResources)) {
        cerr << "error: specified number of resources not in the valid range: 1.."
             << numResources << endl;
        return 1;
    }

    // Configure threads at the XRootD/SSI client
    XrdSsiProviderClient->SetCBThreads(xrootdCBThreadsMax, xrootdCBThreadsInit);

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    XrdSsiService* serviceProvider = XrdSsiProviderClient->GetService(
        errInfo, serviceProviderLocation
    );
    if (nullptr == serviceProvider) {
        cerr << "failed to contact service provider at: " << serviceProviderLocation
             << ", error: " << errInfo.Get() << endl;
        return 1;
    }
    cout << "connected to service provider at: " << serviceProviderLocation << endl;

    // Build a complete list of jobs (resource path names) to be processed
    // in the specified order. Note that the actual (run time) ordering may
    // be different if running this test in the multi-threaded mode.
    vector<string> jobs;
    if (resourceFirst) {
        for (unsigned int j = 0; j < numResources; ++j) {
            auto&& resourcePath = resources[j];
            for (unsigned int i = 0; i < numRequests; ++i) {
                jobs.push_back(resourcePath);
            }
        }

    } else {
        for (unsigned int i = 0; i < numRequests; ++i) {
            for (unsigned int j = 0; j < numResources; ++j) {
                auto&& resourcePath = resources[j];
                jobs.push_back(resourcePath);
            }
        }
    }

    // Make allocation of jobs to threads using the 'round-robin' method
    vector<vector<string>> thread2jobs(numThreads);
    for (unsigned int i = 0; i < jobs.size(); ++i) {
        auto const threadIdx = i % numThreads;
        auto&& resourcePath = jobs[i];
        thread2jobs[threadIdx].push_back(resourcePath);
    }

    Counter numRequestsInFlight(flowControlLimit);

    // Launch threads
    vector<unique_ptr<thread>> threads;
    for (unsigned int tIdx = 0; tIdx < numThreads; ++tIdx) {

        threads.emplace_back(new thread(
            [&numRequestsInFlight](vector<string> const& jobs,
                                   XrdSsiService* serviceProvider) {
                // Launch requests
                vector<RequestT::Ptr> requests;
                for (auto&& resourcePath: jobs) {
                    requests.push_back(makeRequest(numRequestsInFlight, serviceProvider, resourcePath));
                }
    
                // Block while at least one request is in progress
                util::BlockPost blockPost(1000, 2000);
                while (numRequestsInFlight.counter() != 0) {
                    blockPost.wait(200);
                }
            },
            thread2jobs[tIdx],
            serviceProvider
        ));
    }

    // Wait before all threads will finish to avoid crashing the application
    for (auto&& t: threads) {
        t->join();
    }
    return 0;
}
} // namespace


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
            "  <resources-file-name> <num-requests> <value>\n"
            "  [--service=<provider>]\n"
            "  [--num-resources=<value>]\n"
            "  [--resources-first]\b"
            "  [--cancel-after=<milliseconds>]\n"
            "\n"
            "Flags an options:\n"
            "  --service=<provider>    - location of a service provider (default: 'localhost:1094')\n"
            "  --num-resources=<value> - the number of resources (default: 1, range: 1..*)\n"
            "  --resource-first        - iterate over resources, then over requests\n"
            "  --num-threads=<value>   - the number of parallel threads (default: 1, range: 1..*)\n"
            "  --flow-control=<limit>  - if the value is not 0 then it will turn on the flow control\n"
            "                            for the requests processing. In this case a value of the parameter\n"
            "                            puts a cap on the maximum number of requests being processed at each\n"
            "                            moment of time (default: 0, range: 0..*)\n"
            "  --silent                - do not report any status or error messages, including\n"
            "                            the ones sent via the LSST Logger API.\n"
            "\n"
            "Parameters:\n"
            "  <resources-file-name>  - a file with resource paths (one resource per line)\n"
            "  <num-requests>         - number of requests per resource\n"
            "  <value>                - arbitrary string\n");

        ::fileName    = parser.parameter<string>(1);
        ::numRequests = parser.parameter<unsigned int>(2);
        ::value       = parser.parameter<string>(3);

        ::serviceProviderLocation = parser.option<string>("service", "localhost:1094");
        ::numResources            = parser.option<unsigned int>("num-resources", 1);
        ::resourceFirst           = parser.flag("resource-first");
        ::numThreads              = parser.option<unsigned int>("num-threads", 1);
        ::flowControlLimit        = parser.option<unsigned int>("flow-control", 0);
        ::silent                  = parser.flag("silent");
        ::xrootdCBThreadsMax      = parser.option<int>("xrootd-cdb-threads-max", 0);
        ::xrootdCBThreadsInit     = parser.option<int>("xrootd-cdb-threads-init", 0);

    } catch (exception const& ex) {
        return 1;
    }
    return ::test();
}
