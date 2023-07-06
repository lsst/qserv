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
 * @brief test HttpAsyncReq
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

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"
#include "qhttp/Status.h"
#include "replica/AsyncTimer.h"
#include "replica/HttpAsyncReq.h"
#include "replica/Mutex.h"

// Boost unit test header
#define BOOST_TEST_MODULE HttpAsyncReq
#include <boost/test/unit_test.hpp>

using namespace std;
namespace asio = boost::asio;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;
namespace qhttp = lsst::qserv::qhttp;
namespace util = lsst::qserv::util;

namespace {

class HttpServer {
public:
    HttpServer(HttpServer const&) = delete;
    HttpServer& operator=(HttpServer const&) = delete;
    explicit HttpServer(uint16_t port = 0)
            : _io_service(), _server(qhttp::Server::create(_io_service, port)) {}
    ~HttpServer() {
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

BOOST_AUTO_TEST_SUITE(Suite)

// Test an ability of the class to correctly parse input parameters.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_create) {
    LOGS_INFO("HttpAsyncReq_create");

    asio::io_service io_service;

    // Callback parameter allows nullptr
    BOOST_REQUIRE_NO_THROW({
        string const url = "http://127.0.0.1:80/";
        string const method = "GET";
        auto req = HttpAsyncReq::create(io_service, nullptr, method, url);
    });

    // HTTPS is not supported
    BOOST_CHECK_THROW(
            {
                string const url = "https://127.0.0.1:80/";
                string const method = "GET";
                auto req = HttpAsyncReq::create(
                        io_service, [](auto const& req) {}, method, url);
            },
            std::invalid_argument);

    // Unknown HTTP method
    BOOST_CHECK_THROW(
            {
                string const url = "http://127.0.0.1:80/";
                string const method = "INVALID";
                auto req = HttpAsyncReq::create(
                        io_service, [](auto const& req) {}, method, url);
            },
            std::invalid_argument);
}

// The simplest test that verifies correct serialization/deserialization
// of the header and the body in requests and responses.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_simple) {
    LOGS_INFO("HttpAsyncReq_simple");

    asio::io_service io_service;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(100), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_simple: test exceeded the time budget of " << expirationIvalMs.count()
                                                                                   << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler(
            "GET", "/simple", [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                string const expectedBody = "abcdefg";
                BOOST_CHECK_EQUAL(req->version, "HTTP/1.1");
                BOOST_CHECK(req->header["Content-Type"].empty());
                BOOST_CHECK_EQUAL(req->header["Content-Length"], to_string(expectedBody.size()));
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
    httpServer.start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/simple";
    string const method = "GET";
    string const data = "abcdefg";
    unordered_map<string, string> const headers = {{"Header-1", "A"}, {"Header-2", "B"}};
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer](auto const& req) {
                testAbortTimer->cancel();
                BOOST_CHECK(req->state() == HttpAsyncReq::State::FINISHED);
                BOOST_CHECK(req->errorMessage().empty());
                BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"), "0");
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
                BOOST_CHECK_EQUAL(req->responseBodySize(), 0U);
            },
            method, url, data, headers);
    BOOST_CHECK_EQUAL(req->url().url(), url);
    BOOST_CHECK_EQUAL(req->method(), method);
    req->start();

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

// Testing an ability of a request to put a cap on the amount of data expected
// in the server response's body.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_body_limit_error) {
    LOGS_INFO("HttpAsyncReq_body_limit_error");

    asio::io_service io_service;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(100), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_body_limit_error: test exceeded the time budget of "
                          << expirationIvalMs.count() << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up and start the server
    ::HttpServer httpServer;
    size_t const serverResponseBodySize = 1024;
    httpServer.server()->addHandler(
            "PUT", "/return_large_body",
            [serverResponseBodySize](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                string const largeBody(serverResponseBodySize, 'a');
                string const contentType = "text/html";
                resp->send(largeBody, contentType);
            });
    httpServer.start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/return_large_body";
    string const method = "PUT";
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = serverResponseBodySize - 1;
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer, serverResponseBodySize](auto const& req) {
                testAbortTimer->cancel();
                BOOST_CHECK(req->state() == HttpAsyncReq::State::BODY_LIMIT_ERROR);
                BOOST_CHECK(req->errorMessage().empty());
                BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_OK);
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"),
                                  to_string(serverResponseBodySize));
                BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
                BOOST_CHECK_THROW(req->responseBody(), std::logic_error);
                BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
            },
            method, url, data, headers, maxResponseBodySize);
    req->start();

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

// Testing request expiration due to non-responsive server (which is simulated
// by introducing a delay into the request handler.)

BOOST_AUTO_TEST_CASE(HttpAsyncReq_expired) {
    LOGS_INFO("HttpAsyncReq_expired");

    asio::io_service io_service;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(3000), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_expired: test exceeded the time budget of "
                          << expirationIvalMs.count() << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("POST", "/delayed_response",
                                    [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                        this_thread::sleep_for(chrono::milliseconds(2500));
                                        resp->sendStatus(qhttp::STATUS_OK);
                                    });
    httpServer.start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/delayed_response";
    string const method = "POST";
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = 0;
    unsigned int const expirationIvalSec = 2;
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer](auto const& req) {
                testAbortTimer->cancel();
                BOOST_CHECK(req->state() == HttpAsyncReq::State::EXPIRED);
                BOOST_REQUIRE_NO_THROW(req->errorMessage());
                BOOST_CHECK_THROW(req->responseCode(), std::logic_error);
                BOOST_CHECK_THROW(req->responseHeader(), std::logic_error);
                BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
            },
            method, url, data, headers, maxResponseBodySize, expirationIvalSec);
    req->start();

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

// Testing request cancelation for the in-flight request.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_cancelled) {
    LOGS_INFO("HttpAsyncReq_cancelled");

    asio::io_service io_service;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(3000), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_simple: test exceeded the time budget of " << expirationIvalMs.count()
                                                                                   << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("DELETE", "/delayed_response_too",
                                    [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                        this_thread::sleep_for(chrono::milliseconds(2000));
                                        resp->sendStatus(qhttp::STATUS_OK);
                                    });
    httpServer.start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/delayed_response_too";
    string const method = "DELETE";
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer](auto const& req) {
                testAbortTimer->cancel();
                BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);
            },
            method, url);
    req->start();

    // The deadline timer for cancelling the request
    auto const cancelReqTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(1000),
                               [&req](auto expirationIvalMs) { BOOST_CHECK(req->cancel()); });
    cancelReqTimer->start();

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

// Testing request cancelation before starting the request.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_cancelled_before_started) {
    LOGS_INFO("HttpAsyncReq_cancelled_before_started");

    asio::io_service io_service;

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(300), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_cancelled_before_started: test exceeded the time budget of "
                          << expirationIvalMs.count() << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("GET", "/quick",
                                    [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                        resp->sendStatus(qhttp::STATUS_OK);
                                    });
    httpServer.start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/quick";
    string const method = "GET";
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer](auto const& req) {
                testAbortTimer->cancel();
                BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);
            },
            method, url);

    // Cancel right away.
    BOOST_CHECK(req->cancel());
    BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);
    BOOST_CHECK(!req->cancel());  // since the request was already cancelled

    // It's not allowed to start the cancelled requests
    BOOST_CHECK_THROW(req->start(), std::logic_error);

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

// Testing an ability of HttpAsyncReq to wait before the server will start.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_delayed_server_start) {
    LOGS_INFO("HttpAsyncReq_delayed_server_start");

    asio::io_service io_service;

    // Grab the next available port that will be used to configure the REST server
    asio::ip::tcp::socket socket(io_service);
    socket.open(boost::asio::ip::tcp::v4());
    asio::socket_base::reuse_address option(true);
    socket.set_option(option);
    boost::system::error_code ec;
    socket.bind(boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), 0), ec);
    if (ec) {
        LOGS_INFO("HttpAsyncReq_delayed_server_start: bind failed " << ec);
        std::exit(1);
    }
    uint16_t const port = socket.local_endpoint().port();
    LOGS_INFO("HttpAsyncReq_delayed_server_start: bind port=" << port);

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    auto const testAbortTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(5000), [](auto expirationIvalMs) {
                LOGS_INFO("HttpAsyncReq_delayed_server_start: test exceeded the time budget of "
                          << expirationIvalMs.count() << " ms");
                std::exit(1);
            });
    testAbortTimer->start();

    // Set up the server on the allocated port. The server start will be delayed by the timer.
    ::HttpServer httpServer(port);
    httpServer.server()->addHandler("GET", "/redirected_from",
                                    [](qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
                                        resp->headers["Location"] = "/redirected_to";
                                        resp->sendStatus(qhttp::STATUS_MOVED_PERM);
                                    });

    // Request object will be created later.
    shared_ptr<HttpAsyncReq> req;

    // Delay server startup before expiration of the timer
    auto const serverStartDelayTimer =
            AsyncTimer::create(io_service, chrono::milliseconds(3000),
                               [&httpServer, &req](auto expirationIvalMs) { httpServer.start(); });
    serverStartDelayTimer->start();

    // Submit a request.
    string const url = "http://127.0.0.1:" + to_string(port) + "/redirected_from";
    string const method = "GET";
    req = HttpAsyncReq::create(
            io_service,
            [testAbortTimer](auto const& req) {
                testAbortTimer->cancel();
                switch (req->state()) {
                    case HttpAsyncReq::State::FINISHED:
                        BOOST_CHECK_EQUAL(req->responseCode(), qhttp::STATUS_MOVED_PERM);
                        BOOST_CHECK_EQUAL(req->responseHeader().at("Location"), "/redirected_to");
                        break;
                    case HttpAsyncReq::State::CANCELLED:
                        break;
                    default:
                        BOOST_CHECK(false);
                        break;
                }
            },
            method, url);
    req->start();

    thread serviceThread([&io_service]() { io_service.run(); });
    serviceThread.join();
}

BOOST_AUTO_TEST_SUITE_END()
