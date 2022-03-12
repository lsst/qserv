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
  * @brief test AsyncTimer
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
#include "replica/AsyncTimer.h"
#include "replica/HttpAsyncReq.h"

// Boost unit test header
#define BOOST_TEST_MODULE HttpAsyncReq
#include <boost/test/unit_test.hpp>

using namespace std;
namespace asio = boost::asio;
namespace test = boost::test_tools;
using namespace lsst::qserv::replica;
namespace qhttp = lsst::qserv::qhttp;

namespace {

class HttpServer {
public:
    HttpServer(HttpServer const&) = delete;
    HttpServer& operator=(HttpServer const&) = delete;
    explicit HttpServer(uint16_t port=0)
    :   _io_service(),
        _server(qhttp::Server::create(_io_service, port)) {
    }
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
        _serviceThread.reset(new thread([this] () {
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
    BOOST_CHECK_THROW({
        string const url = "https://127.0.0.1:80/";
        string const method = "GET";
        auto req = HttpAsyncReq::create(io_service, [](auto const& req) {}, method, url);
    }, std::invalid_argument);

    // Unknown HTTP method
    BOOST_CHECK_THROW({
        string const url = "http://127.0.0.1:80/";
        string const method = "INVALID";
        auto req = HttpAsyncReq::create(io_service, [](auto const& req) {}, method, url);
    }, std::invalid_argument);
}

// The simplest test that verifies correct serialization/deserialization 
// of the header and the body in requests and responses.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_simple) {

    LOGS_INFO("HttpAsyncReq_simple");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 100;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_simple: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("GET", "/simple",
        [] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
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
        })
    ;
    httpServer.start();

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/simple";
    string const method = "GET";
    string const data = "abcdefg";
    unordered_map<string, string> const headers = {
        {"Header-1", "A"},
        {"Header-2", "B"}
    };
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
        io_service,
        [](auto const& req) {
            BOOST_CHECK(req->state() == HttpAsyncReq::State::FINISHED);
            BOOST_CHECK(req->errorMessage().empty());
            BOOST_CHECK_EQUAL(req->responseCode(), 200);
            BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"), "0");
            BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
            BOOST_CHECK_EQUAL(req->responseBodySize(), 0U);
        },
        method, url, data, headers
    );
    BOOST_CHECK_EQUAL(req->url().url(), url);
    BOOST_CHECK_EQUAL(req->method(), method);
    req->start();
    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

// Testing an ability of a request to put a cap on the amount of data expected
// in the server response's body.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_body_limit_error) {

    LOGS_INFO("HttpAsyncReq_body_limit_error");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 100;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_body_limit_error: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up and start the server
    ::HttpServer httpServer;
    size_t const serverResponseBodySize = 1024;
    httpServer.server()->addHandler("PUT", "/return_large_body",
        [serverResponseBodySize] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
            string const largeBody(serverResponseBodySize, 'a');
            string const contentType = "text/html";
            resp->send(largeBody, contentType);
        }
    );
    httpServer.start();

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/return_large_body";
    string const method = "PUT";
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = serverResponseBodySize - 1;
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
        io_service,
        [serverResponseBodySize](auto const& req) {
            BOOST_CHECK(req->state() == HttpAsyncReq::State::BODY_LIMIT_ERROR);
            BOOST_CHECK(req->errorMessage().empty());
            BOOST_CHECK_EQUAL(req->responseCode(), 200);
            BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Length"), to_string(serverResponseBodySize));
            BOOST_CHECK_EQUAL(req->responseHeader().at("Content-Type"), "text/html");
            BOOST_CHECK_THROW(req->responseBody(), std::logic_error);
            BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
        },
        method, url, data, headers, maxResponseBodySize
    );
    req->start();
    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

// Testing request expiration due to non-responsive server (which is simulated
// by introducing a delay into the request handler.)

BOOST_AUTO_TEST_CASE(HttpAsyncReq_expired) {

    LOGS_INFO("HttpAsyncReq_expired");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 3000;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_expired: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("POST", "/delayed_response",
        [] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
            this_thread::sleep_for(chrono::milliseconds(2500));
            resp->sendStatus(200);
        })
    ;
    httpServer.start();

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/delayed_response";
    string const method = "POST";
    string const data;
    unordered_map<string, string> const headers;
    size_t const maxResponseBodySize = 0;
    unsigned int const expirationIvalSec = 2;
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
        io_service,
        [](auto const& req) {
            BOOST_CHECK(req->state() == HttpAsyncReq::State::EXPIRED);
            BOOST_REQUIRE_NO_THROW(req->errorMessage());
            BOOST_CHECK_THROW(req->responseCode(), std::logic_error);
            BOOST_CHECK_THROW(req->responseHeader(), std::logic_error);
            BOOST_CHECK_THROW(req->responseBodySize(), std::logic_error);
        },
        method, url, data, headers, maxResponseBodySize, expirationIvalSec
    );
    req->start();
    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

// Testing request cancelation for the in-flight request.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_cancelled) {

    LOGS_INFO("HttpAsyncReq_cancelled");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 3000;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_cancelled: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("DELETE", "/delayed_response_too",
        [] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
            this_thread::sleep_for(chrono::milliseconds(2000));
            resp->sendStatus(200);
        })
    ;
    httpServer.start();

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/delayed_response_too";
    string const method = "DELETE";
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
        io_service,
        [](auto const& req) {
            BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);
        },
        method, url
    );

    // The deadline timer for cancelling the request
    shared_ptr<AsyncTimer> const cancelReqTimer = AsyncTimer::create();
    unsigned int const cancelReqIvalMs = 1000;
    cancelReqTimer->start(cancelReqIvalMs, [&req] () {
        BOOST_CHECK(req->cancel());
    });

    req->start();
    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

// Testing request cancelation before starting the request.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_cancelled_before_started) {

    LOGS_INFO("HttpAsyncReq_cancelled_before_started");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 3000;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_cancelled_before_started: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up and start the server
    ::HttpServer httpServer;
    httpServer.server()->addHandler("GET", "/quick",
        [] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
            resp->sendStatus(200);
        })
    ;
    httpServer.start();

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(httpServer.port()) + "/quick";
    string const method = "GET";
    shared_ptr<HttpAsyncReq> const req = HttpAsyncReq::create(
        io_service,
        [](auto const& req) {
            BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);
        },
        method, url
    );

    // Cancel right away.
    BOOST_CHECK(req->cancel());
    BOOST_CHECK(req->state() == HttpAsyncReq::State::CANCELLED);

    // It's not allowed to start the cancelled requests
    BOOST_CHECK_THROW(req->start(), std::logic_error);

    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

// Testing an ability of HttpAsyncReq to wait before the server will start on
// some fixed port.

BOOST_AUTO_TEST_CASE(HttpAsyncReq_delayed_server_start) {

    LOGS_INFO("HttpAsyncReq_delayed_server_start");

    // The deadline timer limits the duration of the test to prevent the test from
    // being stuck for longer than expected.
    shared_ptr<AsyncTimer> const testAbortTimer = AsyncTimer::create();
    unsigned int const testAbortIvalMs = 5000;
    testAbortTimer->start(testAbortIvalMs, [testAbortIvalMs] () {
        LOGS_INFO("HttpAsyncReq_delayed_server_start: test exceeded the time budget of " << testAbortIvalMs << " ms");
        std::exit(1);
    });

    // Set up the server on the fixed port. The server start will be delayed by the timer.
    // Since we don't have any guarantee that the port is not in use by some other process,
    // and should this port be not available the timer will cancel the request to allow
    // this test to pass.
    uint16_t const fixedPort = 8080;
    ::HttpServer httpServer(fixedPort);
    httpServer.server()->addHandler("GET", "/redirected_from",
        [] (qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp) {
            resp->headers["Location"] = "/redirected_to";
            resp->sendStatus(301);
        }
    );

    // Request object will be created later. 
    shared_ptr<HttpAsyncReq> req;

    // Delay server startup before expiration of the timer
    shared_ptr<AsyncTimer> const serverStartDelayTimer = AsyncTimer::create();
    unsigned int const serverStartDelayMs = 3000;
    serverStartDelayTimer->start(serverStartDelayMs, [&httpServer, &req] () {
        try {
            httpServer.start();
        } catch (...) {
            LOGS_INFO("HttpAsyncReq_delayed_server_start: failed to start the server due to port conflict");
            if (req != nullptr) {
                req->cancel();
            }
        }
    });

    // Submit a request.
    asio::io_service io_service;
    string const url = "http://127.0.0.1:" + to_string(fixedPort) + "/redirected_from";
    string const method = "GET";
    req = HttpAsyncReq::create(
        io_service,
        [](auto const& req) {
            switch (req->state()) {
                case HttpAsyncReq::State::FINISHED:
                    BOOST_CHECK_EQUAL(req->responseCode(), 301);
                    BOOST_CHECK_EQUAL(req->responseHeader().at("Location"), "/redirected_to");
                    break;
                case HttpAsyncReq::State::CANCELLED:
                    break;
                default:
                    BOOST_CHECK(false);
                    break;
            }
        },
        method, url
    );
    req->start();
    io_service.run();

    // Silence the timer.
    testAbortTimer->cancel();
}

BOOST_AUTO_TEST_SUITE_END()
