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
 * @brief test AsyncReq
 */

// System headers
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <thread>

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/AsyncReq.h"
#include "http/Method.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"
#include "qhttp/Status.h"
#include "util/AsyncTimer.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE AsyncReq
#include <boost/test/unit_test.hpp>

using namespace std;
using namespace std::chrono_literals;
namespace asio = boost::asio;
namespace test = boost::test_tools;
using namespace lsst::qserv::http;
namespace qhttp = lsst::qserv::qhttp;
namespace util = lsst::qserv::util;

namespace {

class Server {
public:
    Server(Server const&) = delete;
    Server& operator=(Server const&) = delete;
    explicit Server(uint16_t port = 0) : _io_service(), _server(qhttp::Server::create(_io_service, port)) {}
    ~Server() {
        // The thread won't be available if the server failed to start due to port
        // conflict or some other reason.
        if (_serviceThread != nullptr) {
            _server->stop();
            _io_service.stop();
            _serviceThread->join();
        }
    }
    qhttp::Server::Ptr const& server() const { return _server; }

    void start() {
        _server->start();
        _serviceThread.reset(new thread([this]() {
            asio::io_service::work work(_io_service);
            _io_service.run();
        }));
    }
    uint16_t port() const { return _server->getPort(); }

private:
    asio::io_service _io_service;
    qhttp::Server::Ptr const _server;
    unique_ptr<thread> _serviceThread;
};
}  // namespace

namespace lsst::qserv {

/**
 * The test fixture instantiates a qhttp server, a boost::asio::io_service to run it,
 * manages a thread that runs the io_service, an asynchronous timer to abort tests
 * in cae of lockups, and handles global init and cleanup.
 */
struct AsyncReqFixture {
    AsyncReqFixture() {
        _testAbortTimer = util::AsyncTimer::create(
                io_service, _testExpirationIvalMs, [](auto expirationIvalMs) -> bool {
                    LOGS_ERROR("test exceeded the time budget of " << expirationIvalMs.count() << "ms");
                    std::exit(1);
                });

        // Async applications need to be started before runing BOOST ASIO I/O service
        // to avoid premature termination of the service thread.
        _testAbortTimer->start();
        _serviceThread = make_unique<thread>([this]() { io_service.run(); });
    }
    void createServer(uint16_t port) { httpServer = make_unique<::Server>(port); }
    void createAndStartServer() {
        createServer(0);
        httpServer->start();
    }
    void cancelAbortTimer() { _testAbortTimer->cancel(); }
    ~AsyncReqFixture() { _serviceThread->join(); }

    // BOOST ASIO services.
    asio::io_service io_service;

    // The server is run in the BOOST ASIO thread(s).
    unique_ptr<::Server> httpServer;

    // The request object is used in async callbacks.
    shared_ptr<http::AsyncReq> req;

    // Members that are used for testing the dynamic reconnects.
    vector<http::AsyncReq::HostPort> hostPort;
    size_t hostPortAttempt = 0;

private:
    // The maximum duration of each test case.
    chrono::milliseconds _testExpirationIvalMs = 3s;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<util::AsyncTimer> _testAbortTimer;

    unique_ptr<thread> _serviceThread;
};

BOOST_FIXTURE_TEST_CASE(create, AsyncReqFixture) {
    // Test an ability of the class to correctly parse input parameters.
    LOGS_INFO("create");

    // Callback parameter allows nullptr
    BOOST_REQUIRE_NO_THROW({
        auto const url = "http://127.0.0.1:80/";
        req = http::AsyncReq::create(io_service, nullptr, http::Method::GET, url);
    });

    // HTTPS is not supported
    BOOST_CHECK_THROW(
            {
                auto const url = "https://127.0.0.1:80/";
                req = http::AsyncReq::create(io_service, [](auto const& req) {}, http::Method::GET, url);
            },
            std::invalid_argument);

    cancelAbortTimer();
}

BOOST_FIXTURE_TEST_CASE(simple, AsyncReqFixture) {
    // The simplest test that verifies correct serialization/deserialization
    // of the header and the body in requests and responses.
    LOGS_INFO("simple");

    createAndStartServer();

    auto const method = http::Method::GET;
    string const target = "/simple";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         string const expectedBody = "abcdefg";
                                         BOOST_CHECK_EQUAL(req->version, "HTTP/1.1");
                                         BOOST_CHECK(req->header["Content-Type"].empty());
                                         BOOST_CHECK_EQUAL(req->header["Content-Length"],
                                                           to_string(expectedBody.size()));
                                         BOOST_CHECK_EQUAL(req->header["Header-1"], "A");
                                         BOOST_CHECK_EQUAL(req->header["Header-2"], "B");
                                         string body;
                                         req->content >> body;
                                         BOOST_CHECK_EQUAL(body.size(), expectedBody.size());
                                         BOOST_CHECK_EQUAL(body, expectedBody);
                                         string const emptyBody;
                                         string const contentType = "text/html";
                                         resp->send(emptyBody, contentType);
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    string const data = "abcdefg";
    unordered_map<string, string> const headers = {{"Header-1", "A"}, {"Header-2", "B"}};
    req = http::AsyncReq::create(
            io_service,
            [this](auto const& req) {
                cancelAbortTimer();
                BOOST_CHECK(req->state() == http::AsyncReq::State::FINISHED);
                BOOST_CHECK(req->errorMessage().empty());
                BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"), "0");
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
                BOOST_CHECK_EQUAL(req->responseBodySize(), 0U);
            },
            method, url, data, headers);
    BOOST_CHECK_EQUAL(req->target(), target);
    BOOST_CHECK_EQUAL(req->method(), method);
    req->start();
}

#if 0
// This test is temporary disabled due to changes in the Boost 1.78 (Almalinux 9), where
// the following Beast library's method doesn't seem to have any effect:
// boost::beast::http::response_parser<boost::beast::http::string_body>::body_limit(size_t)
// This isn't critical for Qserv as the below-mentioned status code http::AsyncReq::State::BODY_LIMIT_ERROR
// is not used by the Replication/Ingest system.
// A solution (or a workaround) to this problem will be found later after further investigation.

BOOST_FIXTURE_TEST_CASE(body_limit_error, AsyncReqFixture) {

    // Testing an ability of a request to put a cap on the amount of data expected
    // in the server response's body.
    LOGS_INFO("body_limit_error");

    createAndStartServer();

    size_t const serverResponseBodySize = 1024;
    auto const method = http::Method::PUT;
    string const target = "/return_large_body";
    httpServer->server()->addHandler(
            http::method2string(method), target,
            [serverResponseBodySize](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                string const largeBody(serverResponseBodySize, 'a');
                string const contentType = "text/html";
                resp->send(largeBody, contentType);
            });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = serverResponseBodySize - 1;
    req = http::AsyncReq::create(
            io_service,
            [this, serverResponseBodySize](auto const& req) {
                cancelAbortTimer();
                BOOST_CHECK(req->state() == http::AsyncReq::State::BODY_LIMIT_ERROR);
                BOOST_CHECK(req->errorMessage().empty());
                BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"),
                                  to_string(serverResponseBodySize));
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
                BOOST_CHECK_THROW(req->responseBody(), std::logic_error);
                BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
            },
            method, url, data, headers);

    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), 0U);
    BOOST_CHECK_NO_THROW(req->setMaxResponseBodySize(maxResponseBodySize));
    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), maxResponseBodySize);

    req->start();

    BOOST_CHECK_THROW(req->setMaxResponseBodySize(maxResponseBodySize + 1), std::logic_error);
    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), maxResponseBodySize);
}
#endif

BOOST_FIXTURE_TEST_CASE(expired, AsyncReqFixture) {
    // Testing request expiration due to non-responsive server (which is simulated
    // by introducing a delay into the request handler.)
    LOGS_INFO("expired");

    createAndStartServer();

    auto const method = http::Method::POST;
    string const target = "/delayed_response";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         this_thread::sleep_for(2s);
                                         resp->sendStatus(qhttp::STATUS_OK);
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = 0;
    unsigned int const expirationIvalSec = 1;
    req = http::AsyncReq::create(
            io_service,
            [this](auto const& req) {
                cancelAbortTimer();
                BOOST_CHECK(req->state() == http::AsyncReq::State::EXPIRED);
                BOOST_REQUIRE_NO_THROW(req->errorMessage());
                BOOST_CHECK_THROW(req->responseCode(), std::logic_error);
                BOOST_CHECK_THROW(req->responseHeader(), std::logic_error);
                BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
            },
            method, url, data, headers);

    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), 0U);
    BOOST_CHECK_NO_THROW(req->setMaxResponseBodySize(maxResponseBodySize));
    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), maxResponseBodySize);

    BOOST_CHECK_EQUAL(req->expirationIval(), 0U);
    BOOST_CHECK_NO_THROW(req->setExpirationIval(expirationIvalSec));
    BOOST_CHECK_EQUAL(req->expirationIval(), expirationIvalSec);

    req->start();

    BOOST_CHECK_THROW(req->setMaxResponseBodySize(maxResponseBodySize + 1), std::logic_error);
    BOOST_CHECK_EQUAL(req->maxResponseBodySize(), maxResponseBodySize);

    BOOST_CHECK_THROW(req->setExpirationIval(expirationIvalSec + 1), std::logic_error);
    BOOST_CHECK_EQUAL(req->expirationIval(), expirationIvalSec);
}

BOOST_FIXTURE_TEST_CASE(cancelled, AsyncReqFixture) {
    // Testing request cancelation for the in-flight request.
    LOGS_INFO("cancelled");

    createAndStartServer();

    auto const method = http::Method::DELETE;
    string const target = "/delayed_response_too";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         this_thread::sleep_for(200ms);
                                         resp->sendStatus(qhttp::STATUS_OK);
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    req = http::AsyncReq::create(
            io_service,
            [this](auto const& req) {
                cancelAbortTimer();
                BOOST_CHECK(req->state() == http::AsyncReq::State::CANCELLED);
            },
            method, url);
    req->start();

    // The deadline timer for cancelling the request
    auto const cancelReqTimer =
            util::AsyncTimer::create(io_service, 100ms, [&](auto expirationIvalMs) -> bool {
                BOOST_CHECK(req->cancel());
                return false;
            });
    cancelReqTimer->start();
}

BOOST_FIXTURE_TEST_CASE(cancelled_before_started, AsyncReqFixture) {
    // Testing request cancelation before starting the request.
    LOGS_INFO("cancelled_before_started");

    createAndStartServer();

    auto const method = http::Method::GET;
    string const target = "/quick";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         resp->sendStatus(qhttp::STATUS_OK);
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    req = http::AsyncReq::create(
            io_service,
            [](auto const& req) { BOOST_CHECK(req->state() == http::AsyncReq::State::CANCELLED); }, method,
            url);

    // Cancel right away.
    BOOST_CHECK(req->cancel());
    BOOST_CHECK(req->state() == AsyncReq::State::CANCELLED);
    BOOST_CHECK(!req->cancel());  // since the request was already cancelled

    // It's not allowed to start the cancelled requests
    BOOST_CHECK_THROW(req->start(), std::logic_error);

    cancelAbortTimer();
}

BOOST_FIXTURE_TEST_CASE(delayed_server_start, AsyncReqFixture) {
    // Testing an ability of AsyncReq to wait before the server will start.
    LOGS_INFO("delayed_server_start");

    // Grab the next available port that will be used to configure the REST server
    asio::ip::tcp::socket socket(io_service);
    socket.open(boost::asio::ip::tcp::v4());
    asio::socket_base::reuse_address option(true);
    socket.set_option(option);
    boost::system::error_code ec;
    socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0), ec);
    if (ec) {
        LOGS_INFO("bind failed " << ec);
        std::exit(1);
    }
    uint16_t const port = socket.local_endpoint().port();

    createServer(port);

    auto const method = http::Method::GET;
    string const target = "/redirected_from";
    string const redirectedTarget = "/redirected_to";
    httpServer->server()->addHandler(
            http::method2string(method), target,
            [redirectedTarget](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                resp->headers["Location"] = redirectedTarget;
                resp->sendStatus(qhttp::STATUS_MOVED_PERM);
            });

    // Delay server startup before expiration of the timer
    auto const serverStartDelayTimer =
            util::AsyncTimer::create(io_service, 1s, [this](auto expirationIvalMs) -> bool {
                httpServer->start();
                LOGS_INFO("server started");
                return false;
            });
    serverStartDelayTimer->start();
    LOGS_INFO("server start delay timer started");

    auto const url = "http://127.0.0.1:" + to_string(port) + target;
    req = http::AsyncReq::create(
            io_service,
            [this, redirectedTarget](auto const& req) {
                LOGS_INFO("request finished");
                cancelAbortTimer();
                switch (req->state()) {
                    case http::AsyncReq::State::FINISHED:
                        BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_MOVED_PERM);
                        BOOST_CHECK_EQUAL(req->responseHeader().at("Location"), redirectedTarget);
                        break;
                    case http::AsyncReq::State::CANCELLED:
                        break;
                    default:
                        BOOST_CHECK(false);
                        break;
                }
            },
            method, url);
    LOGS_INFO("request created");
    req->start();
    LOGS_INFO("request started");
}

BOOST_FIXTURE_TEST_CASE(dynamic, AsyncReqFixture) {
    // The test that tests the request configured to allow the dynamic adjustment
    // of the connection parameters for the server.
    LOGS_INFO("dynamic");

    createAndStartServer();

    auto const method = http::Method::GET;
    string const target = "/simple";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         string const emptyBody;
                                         resp->send(emptyBody, "text/html");
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    hostPort = {
            http::AsyncReq::HostPort(),  // invalid connection parameters to be tried on the second iteration
            http::AsyncReq::HostPort{
                    "127.0.0.1", httpServer->port()}  // valid parameters to be used on the third iteration
    };
    hostPortAttempt = 0;
    http::AsyncReq::GetHostPort const getHostPort =
            [this](http::AsyncReq::HostPort const& prev) -> http::AsyncReq::HostPort {
        switch (hostPortAttempt++) {
            case 0:
                // On the first pass we sumulate a problem to get the desired parameters.
                // The client should be able to recover form this situation.
                throw runtime_error("failed to locate the desired connection parameters");
            case 1:
                // This time it's called to fetch the incorrect set of parameters.
                return hostPort[hostPortAttempt - 1];
            case 2:
                // This time it's called to fetch the good set of parameters.
                return hostPort[hostPortAttempt - 1];
            default:
                // Return the previous set in case if the async operation keeps failing.
                return prev;
        }
    };
    req = http::AsyncReq::create(
            io_service,
            [this](auto const& req) {
                cancelAbortTimer();
                // Make sure the host & port info was requested exactly 2 times and the last
                // connection attemnpt succeded yealding the expected response from the server.
                BOOST_CHECK_EQUAL(hostPortAttempt, 2U);
                BOOST_CHECK(req->state() == http::AsyncReq::State::FINISHED);
                BOOST_CHECK(req->errorMessage().empty());
                BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"), "0");
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
                BOOST_CHECK_EQUAL(req->responseBodySize(), 0U);
            },
            method, getHostPort, target);
    req->start();
}

BOOST_FIXTURE_TEST_CASE(wait_current_thread, AsyncReqFixture) {
    // Testing the synchronous wait for completion of requests (waiting in
    // the current thread).
    LOGS_INFO("wait_current_thread");

    createAndStartServer();

    auto const method = http::Method::POST;
    string const target = "/delayed_response";
    httpServer->server()->addHandler(http::method2string(method), target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         this_thread::sleep_for(100ms);
                                         resp->sendStatus(qhttp::STATUS_OK);
                                     });

    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    auto const noCallbackOnFinish = nullptr;
    req = http::AsyncReq::create(io_service, noCallbackOnFinish, method, url);
    req->start();
    LOGS_INFO("request started");
    BOOST_CHECK(req->state() == http::AsyncReq::State::IN_PROGRESS);
    BOOST_CHECK_NO_THROW(req->wait());
    BOOST_CHECK(req->state() == http::AsyncReq::State::FINISHED);
    BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);

    cancelAbortTimer();
}

BOOST_FIXTURE_TEST_CASE(wait_separate_thread, AsyncReqFixture) {
    // Testing the synchronous wait for completion of requests (waiting in
    // a separate thread). Note sending a request to the non-existing service.
    LOGS_INFO("wait_separate_thread");

    createAndStartServer();

    string const target = "/delayed_response";
    httpServer->server()->addHandler("POST", target,
                                     [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                         this_thread::sleep_for(100ms);
                                         resp->sendStatus(qhttp::STATUS_OK);
                                     });
    auto const url = "http://127.0.0.1:" + to_string(httpServer->port()) + target;
    auto const method = http::Method::GET;
    auto const noCallbackOnFinish = nullptr;
    req = http::AsyncReq::create(io_service, noCallbackOnFinish, method, url);
    req->start();

    thread waitForRequestCompletionThread([this]() {
        BOOST_CHECK_NO_THROW(req->wait());
        BOOST_CHECK(req->state() == http::AsyncReq::State::FINISHED);
        BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_NOT_FOUND);
        cancelAbortTimer();
    });
    waitForRequestCompletionThread.join();
}

}  // namespace lsst::qserv
