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
#include "replica/apps/QhttpTestApp.h"

// System headers
#include <atomic>
#include <functional>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <random>
#include <sstream>
#include <stdexcept>
#include <thread>
#include <vector>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "qhttp/MultiPartParser.h"
#include "qhttp/Server.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "util/BlockPost.h"
#include "util/TimeUtils.h"

using namespace std;
using json = nlohmann::json;
namespace qhttp = lsst::qserv::qhttp;
using namespace lsst::qserv;

namespace {
string const description =
        "This application runs an embedded HTTP server 'qhttp' for a purpose of testing"
        " the server's performance, scalability and stability.";

bool const injectDatabaseOptions = false;
bool const boostProtobufVersionCheck = false;
bool const enableServiceProvider = false;

double const millisecondsInSecond = 1000.;
double const KiB = 1024.;
int const minBytes = 1;
int const maxBytes = 1024 * 1024;

/**
 * Read the body of a request.
 * @return The numbef of bytes read (including the newline characters).
 */
uint64_t readBody(qhttp::Request::Ptr const& req) {
    uint64_t numBytes = 0;
    string line;
    while (getline(req->content, line)) numBytes += line.size() + 1;
    return numBytes;
}

/// @return 'YYYY-MM-DD HH:MM:SS.mmm  '
string timestamp() {
    return util::TimeUtils::toDateTimeString(chrono::milliseconds(util::TimeUtils::now())) + "  ";
}

/// @return requestor's IP address as a string
/// @return requestor's IP address as a string
string senderIpAddr(qhttp::Request::Ptr const& req) {
    ostringstream ss;
    ss << req->remoteAddr.address();
    return ss.str();
}

class SimpleRequestProcessor : public qhttp::RequestProcessor {
public:
    explicit SimpleRequestProcessor(qhttp::Response::Ptr response) : qhttp::RequestProcessor(response) {}

    virtual bool onParamValue(qhttp::ContentHeader const& hdr, std::string const& name,
                              std::string_view const& value) {
        cout << "[ Header ]\n"
             << "'" << hdr.str() << "'\n"
             << "[ Param ]\n"
             << "  Name: '" << name << "'\n"
             << "  Value: '" << value << "'" << endl;
        return true;
    }
    virtual bool onFileOpen(qhttp::ContentHeader const& hdr, std::string const& name,
                            std::string const& filename, std::string const& contentType) {
        cout << "[ Header ]\n"
             << "'" << hdr.str() << "'\n"
             << "[ File open ]\n"
             << "  Name: '" << name << "'\n"
             << "  Filename: '" << filename << "'\n"
             << "  Content-type: '" << contentType << "'" << endl;
        return true;
    }
    virtual bool onFileContent(std::string_view const& data) {
        cout << "[ File content: " << data.size() << " bytes ]\n'" << data << "'" << endl;
        return true;
    }
    virtual bool onFileClose() {
        cout << "[ File close ]" << endl;
        return true;
    }
    virtual void onFinished(std::string const& error) {
        cout << "[ Finished ]\n"
             << "  Error: '" << error << "'" << endl;
        if (error.empty()) {
            response->sendStatus(qhttp::STATUS_OK);
        } else {
            response->sendStatus(qhttp::STATUS_INTERNAL_SERVER_ERR);
        }
    }
};

}  // namespace

namespace lsst::qserv::replica {

QhttpTestApp::Ptr QhttpTestApp::create(int argc, char* argv[]) { return Ptr(new QhttpTestApp(argc, argv)); }

QhttpTestApp::QhttpTestApp(int argc, char* argv[])
        : Application(argc, argv, ::description, ::injectDatabaseOptions, ::boostProtobufVersionCheck,
                      ::enableServiceProvider) {
    parser().required("port", "A port number for listening for incoming connections.", _port)
            .option("backlog",
                    "The maximum length of the queue of pending connections to a socket open by the server."
                    " Must be greater than 0.",
                    _backlog)
            .option("num-threads", "The number of the BOOST ASIO threads to run the server.", _numThreads)
            .option("report-interval-ms",
                    "An interval (milliseconds) for reporting the performance counters. Must be greater than "
                    "0.",
                    _reportIntervalMs)
            .flag("progress",
                  "The flag which would turn on periodic progress report on the incoming requests.",
                  _progress)
            .flag("verbose", "The flag which would turn on detailed report on the incoming requests.",
                  _verbose);
}

int QhttpTestApp::runImpl() {
    // A flag for stopping he server
    atomic<bool> stop(false);

    // Counters updated by the requests
    atomic<uint64_t> numRequests(0);
    atomic<uint64_t> numBytesReceived(0);
    atomic<uint64_t> numBytesSent(0);

    // Random number generator is set to generate teh number of characters
    // to be sent as a reply to the correposponding service.
    random_device rd;   // Will be used to obtain a seed for the random number engine
    mt19937 gen(rd());  // Standard mersenne_twister_engine seeded with rd()
    uniform_int_distribution<> distr(minBytes, maxBytes);

    boost::asio::io_service io_service;
    qhttp::Server::Ptr const httpServer = qhttp::Server::create(io_service, _port, _backlog);
    httpServer->addHandlers({{"POST", "/service/receive",
                              [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                  ++numRequests;
                                  if (_verbose)
                                      cout << ::timestamp() << "Request: " << ::senderIpAddr(req)
                                           << "  /service/receive" << endl;
                                  numBytesReceived += readBody(req);
                                  json const reply({{"success", 1}});
                                  resp->send(reply.dump(), "application/json");
                              }},
                             {"POST", "/service/echo",
                              [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                  ++numRequests;
                                  if (_verbose)
                                      cout << ::timestamp() << "Request: " << ::senderIpAddr(req)
                                           << "  /service/echo" << endl;
                                  uint64_t const numBytes = readBody(req);
                                  numBytesReceived += numBytes;
                                  numBytesSent += numBytes;
                                  string const data(numBytes, ' ');
                                  json const reply({{"success", 1}, {"data", data}});
                                  resp->send(reply.dump(), "application/json");
                              }},
                             {"GET", "/service/random",
                              [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                  ++numRequests;
                                  if (_verbose)
                                      cout << ::timestamp() << "Request: " << ::senderIpAddr(req)
                                           << "  /service/random" << endl;
                                  uint64_t const numBytesRandom = distr(gen);
                                  numBytesSent += numBytesRandom;
                                  string const data(numBytesRandom, 'x');
                                  json const reply({{"success", 1}, {"data", data}});
                                  resp->send(reply.dump(), "application/json");
                              }},
                             {"PUT", "/management/stop",
                              [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                  ++numRequests;
                                  if (_verbose)
                                      cout << ::timestamp() << "Request: " << ::senderIpAddr(req)
                                           << "  /management/stop" << endl;
                                  json const reply({{"success", 1}});
                                  resp->send(reply.dump(), "application/json");
                                  httpServer->stop();
                                  stop = true;
                              }}});
    httpServer->addHandlers(
            {{"POST", "/body/dump", [&](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                  ++numRequests;
                  if (_verbose)
                      cout << ::timestamp() << "Request: " << ::senderIpAddr(req) << "  /body/dump" << endl;
                  qhttp::MultiPartParser::parse(req, make_shared<::SimpleRequestProcessor>(resp));
              }}});

    // Make sure the service started before launching any BOOST ASIO threads.
    // This will prevent threads from finishing due to a lack of work to be done.
    httpServer->start();

    // Launch all threads in the pool
    vector<shared_ptr<thread>> threads(_numThreads);
    for (auto&& ptr : threads) {
        ptr = shared_ptr<thread>(new thread([&]() { io_service.run(); }));
    }

    // Begin the monitoring & reporting cycle
    util::BlockPost bp(_reportIntervalMs, _reportIntervalMs + 1);
    while (!stop) {
        uint64_t beginNumRequests = numRequests;
        uint64_t beginNumBytesReceived = numBytesReceived;
        uint64_t beginNumBytesSent = numBytesSent;
        bp.wait(_reportIntervalMs);
        uint64_t const endNumRequests = numRequests;
        uint64_t const endNumBytesReceived = numBytesReceived;
        uint64_t const endNumBytesSent = numBytesSent;
        double const requestsPerSecond =
                (endNumRequests - beginNumRequests) / (_reportIntervalMs / millisecondsInSecond);
        double const kBytesPerSecondReceived = (endNumBytesReceived - beginNumBytesReceived) /
                                               (_reportIntervalMs / millisecondsInSecond) / KiB;
        double const kBytesPerSecondSent =
                (endNumBytesSent - beginNumBytesSent) / (_reportIntervalMs / millisecondsInSecond) / KiB;
        if (_progress) {
            cout << ::timestamp() << "Process: " << setprecision(7) << requestsPerSecond << " Req/s  "
                 << "Receive: " << setprecision(7) << kBytesPerSecondReceived << " KiB/s  "
                 << "Send: " << setprecision(7) << kBytesPerSecondSent << " KiB/s" << endl;
        }
        beginNumRequests = endNumRequests;
    }
    for (auto&& ptr : threads) {
        ptr->join();
    }
    return 0;
}

}  // namespace lsst::qserv::replica
