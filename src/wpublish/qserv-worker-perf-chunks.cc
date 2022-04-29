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
#include <functional>
#include <iostream>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <utility>

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

namespace global = lsst::qserv;
namespace util = lsst::qserv::util;
namespace wpublish = lsst::qserv::wpublish;

using namespace std;
using namespace std::placeholders;

using RequestT = wpublish::TestEchoQservRequest;

namespace {

// Command line parameters

string fileName;
unsigned int numRequests;
char value;
unsigned int valueSize;
string serviceProviderLocation;
unsigned int numResources;
bool resourceFirst;
unsigned int numThreads;
unsigned int flowControlLimit;
bool silent;
int xrootdCBThreadsMax;
int xrootdCBThreadsInit;
bool memoryCleanup;

/**
 * The synchronize counter, which is used to limit the number of the "in-flight"
 * requests if the flow-control is enabled. The later is enabled if a value of
 * the constructor's parameter 'maxRequestsAllowed' is not equal to 0.
 */
class Counter {
public:
    typedef shared_ptr<Counter> Ptr;

    static Ptr create(unsigned int maxRequestsAllowed) { return Ptr(new Counter(maxRequestsAllowed)); }

    Counter() = delete;
    Counter(Counter const&) = delete;
    Counter& operator=(Counter const&) = delete;
    ~Counter() = default;

    void inc() {
        if (_maxRequestsAllowed == 0) {
            ++_counter;
        } else {
            unique_lock<mutex> lock(_mtx);
            _cv.wait(lock, [&]() { return _counter < _maxRequestsAllowed; });
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

    unsigned int counter() const { return _counter; }

private:
    explicit Counter(unsigned int maxRequestsAllowed) : _maxRequestsAllowed(maxRequestsAllowed) {}

    unsigned int _maxRequestsAllowed;
    atomic<unsigned int> _counter{0};
    std::mutex _mtx;
    std::condition_variable _cv;
};

class RequestManager : public enable_shared_from_this<RequestManager> {
public:
    typedef shared_ptr<RequestManager> Ptr;
    typedef function<void(RequestT::Status, string const&, string const&, string const&)> OnFinish;

    static Ptr create(Counter::Ptr const& numRequestsInFlight, XrdSsiService* serviceProvider,
                      string const& resourcePath, string const& payload, OnFinish const& onFinish,
                      bool memoryCleanup) {
        return Ptr(new RequestManager(numRequestsInFlight, serviceProvider, resourcePath, payload, onFinish,
                                      memoryCleanup));
    }

    RequestManager(RequestManager const&) = delete;
    RequestManager& operator=(RequestManager const&) = delete;

    void start() {
        _ptr = RequestT::create(_payload,
                                bind(&RequestManager::_finished, shared_from_this(), _1, _2, _3, _4));
        _numRequestsInFlight->inc();
        XrdSsiResource resource(_resourcePath);
        _serviceProvider->ProcessRequest(*_ptr, resource);
    }

private:
    void _finished(RequestT::Status status, string const& error, string const& sent, string const& received) {
        _onFinish(status, error, sent, received);
        _numRequestsInFlight->dec();
        if (_memoryCleanup) _ptr = nullptr;
    }

    RequestManager(Counter::Ptr const& numRequestsInFlight, XrdSsiService* serviceProvider,
                   string const& resourcePath, string const& payload, OnFinish const& onFinish,
                   bool memoryCleanup)
            : _numRequestsInFlight(numRequestsInFlight),
              _serviceProvider(serviceProvider),
              _resourcePath(resourcePath),
              _payload(payload),
              _onFinish(onFinish),
              _memoryCleanup(memoryCleanup) {}

    Counter::Ptr const _numRequestsInFlight;
    XrdSsiService* _serviceProvider;
    string const _resourcePath;
    string const _payload;
    OnFinish const _onFinish;
    bool const _memoryCleanup;
    RequestT::Ptr _ptr;
};

int test() {
    string const payload = string(valueSize, value);

    vector<string> const resources = util::File::getLines(fileName, true);
    if (not numResources or (resources.size() < numResources)) {
        cerr << "error: specified number of resources not in the valid range: 1.." << numResources << endl;
        return 1;
    }

    // Configure threads at the XRootD/SSI client
    XrdSsiProviderClient->SetCBThreads(xrootdCBThreadsMax, xrootdCBThreadsInit);

    // Connect to a service provider
    XrdSsiErrInfo errInfo;
    XrdSsiService* serviceProvider = XrdSsiProviderClient->GetService(errInfo, serviceProviderLocation);
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

    Counter::Ptr const numRequestsInFlight = Counter::create(flowControlLimit);
    bool memoryCleanup = ::memoryCleanup;

    // Launch threads
    vector<unique_ptr<thread>> threads;
    for (unsigned int tIdx = 0; tIdx < numThreads; ++tIdx) {
        threads.emplace_back(new thread(
                [&numRequestsInFlight, &payload, memoryCleanup](vector<string> const& jobs,
                                                                XrdSsiService* serviceProvider) {
                    // Launch requests and keep them in the collection until they finish
                    // to prevent request being deleted while there are unhandled callbacks
                    // on request completion.
                    vector<RequestManager::Ptr> requests;

                    for (auto&& resourcePath : jobs) {
                        auto const ptr = RequestManager::create(
                                numRequestsInFlight, serviceProvider, resourcePath, payload,
                                [](RequestT::Status status, string const& error, string const& sent,
                                   string const& received) {
                                    if (not silent) {
                                        if (status != RequestT::Status::SUCCESS) {
                                            cout << "status: " << RequestT::status2str(status)
                                                 << ", error: " << error << endl;
                                        } else {
                                            cout << "value sent: '" << sent << "', received: '" << received
                                                 << "'" << endl;
                                        }
                                    }
                                },
                                memoryCleanup);
                        requests.push_back(ptr);
                        ptr->start();
                    }

                    // Block while at least one request is in progress
                    util::BlockPost blockPost(1000, 2000);
                    while (numRequestsInFlight->counter() != 0) {
                        blockPost.wait(200);
                    }
                },
                thread2jobs[tIdx], serviceProvider));
    }

    // Wait before all threads will finish to avoid crashing the application
    for (auto&& t : threads) {
        t->join();
    }
    return 0;
}
}  // namespace

int main(int argc, const char* const argv[]) {
    // Verify that the version of the library that we linked against is
    // compatible with the version of the headers we compiled against.

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Parse command line parameters
    try {
        util::CmdLineParser parser(
                argc, argv,
                "\n"
                "General syntax:\n"
                "  <resources-file-name> <num-requests> [flags, options...]\n"
                "\n"
                "Parameters:\n"
                "  <resources-file-name>     - A file with resource paths (one resource per line).\n"
                "  <num-requests>            - The number of requests per resource.\n"
                "\n"
                "Flags and options:\n"
                "  --service=<provider>      - A location of a service provider (default: "
                "'localhost:1094').\n"
                "  --num-resources=<value>   - The number of resources (default: 1, range: 1..*).\n"
                "  --resource-first          - Iterate over resources, then over requests.\n"
                "  --num-threads=<value>     - The number of parallel threads (default: 1, range: 1..*).\n"
                "  --value=<value>           - An 8-bit unsigned decimal number to be used in composing "
                "messages\n"
                "                              sent to the services (default: 97 which is an ASCII code for "
                "'a').\n"
                "  --value-size=<value>      - The number of times to replicate the <value> when composing\n"
                "                              payloads of requests sent to the worker services (default: "
                "1).\n"
                "  --flow-control=<limit>    - If the value is not 0 then it will turn on the flow control\n"
                "                              for the requests processing. In this case a value of the "
                "parameter\n"
                "                              puts a cap on the maximum number of requests being processed "
                "at each\n"
                "                              moment of time (default: 0, range: 0..*).\n"
                "  --silent                  - Do not report any status or error messages, including\n"
                "                              the ones sent via the LSST Logger API.\n"
                "  --xrootd-cdb-threads-max  - The configuration parameter of the XRootD/SSI.\n"
                "  --xrootd-cdb-threads-init - The configuration parameter of the XRootD/SSI.\n"
                "  --memory-cleanup          - Automatically delete XRootD/SSI requests as they finish\n"
                "                              in order to clean the memory allocated by the requests.\n");

        ::fileName = parser.parameter<string>(1);
        ::numRequests = parser.parameter<unsigned int>(2);

        ::serviceProviderLocation = parser.option<string>("service", "localhost:1094");
        ::numResources = parser.option<unsigned int>("num-resources", 1);
        ::resourceFirst = parser.flag("resource-first");
        ::numThreads = parser.option<unsigned int>("num-threads", 1);
        ::value = parser.option<char>("value", 'a');
        ::valueSize = parser.option<unsigned int>("value-size", 1);
        ::flowControlLimit = parser.option<unsigned int>("flow-control", 0);
        ::silent = parser.flag("silent");
        ::xrootdCBThreadsMax = parser.option<int>("xrootd-cdb-threads-max", 0);
        ::xrootdCBThreadsInit = parser.option<int>("xrootd-cdb-threads-init", 0);
        ::memoryCleanup = parser.flag("memory-cleanup");

        if (::valueSize == 0) throw invalid_argument("a value of option <value-size> can't be 0");

    } catch (exception const& ex) {
        cerr << "error: " << ex.what() << endl;
        return 1;
    }
    return ::test();
}
