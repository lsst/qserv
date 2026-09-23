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

// System headers
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <iterator>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"
#include "curl/curl.h"
#include "nlohmann/json.hpp"

// Qserv headers
#include "http/AsyncReq.h"
#include "qhttp/Response.h"
#include "qhttp/Server.h"
#include "replica/config/Config.h"
#include "replica/config/ConfigSchemaController.h"
#include "replica/config/ConfigTestDataController.h"
#include "replica/requests/http/HttpMessenger.h"
#include "replica/requests/http/HttpMessengerConnector.h"
#include "replica/requests/http/HttpMessengerRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// Boost unit test header
#define BOOST_TEST_MODULE HttpMessenger
#include <boost/test/unit_test.hpp>

namespace asio = boost::asio;
using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace http = lsst::qserv::http;

namespace {

void initMDC() { LOG_MDC("LWP", std::to_string(lsst::log::lwpID())); }

struct HttpMessengerFixture {
    HttpMessengerFixture() {
        // BOOST_REQUIRE_EQUAL(curl_global_init(CURL_GLOBAL_ALL), CURLE_OK);
        string const logLevel = "DEBUG";
        LOG_MDC_INIT(initMDC);
        LOG_CONFIG_PROP(std::string("log4j.rootLogger=") + logLevel +
                        ", CONSOLE\n"
                        "log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\n"
                        "log4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\n"
                        "log4j.appender.CONSOLE.layout.ConversionPattern="
                        "%d{yyyy-MM-ddTHH:mm:ss.SSSZ} LWP %-5X{LWP} %-5p %c{1} %m%n");
        server = lsst::qserv::qhttp::Server::create(ioService, 0);
        server->addHandler("GET", "/json", [](auto const&, auto const& response) {
            response->send(R"({"message":"hello","value":42})", "application/json");
        });
        server->addHandler("GET", "/invalid-json", [](auto const&, auto const& response) {
            response->send("not-json", "application/json");
        });
        server->addHandler("GET", "/delay", [this](auto const&, auto const& response) {
            {
                lock_guard<mutex> lock(delayMutex);
                delayStarted = true;
            }
            delayCondition.notify_all();
            this_thread::sleep_for(chrono::milliseconds(200));
            response->send(R"({"delayed":true})", "application/json");
        });
        server->addHandler("POST", "/echo", [](auto const& request, auto const& response) {
            string body;
            request->content >> body;
            LOGS_INFO("HttpMessengerFixture received POST request with body: " << body);
            // string body((istreambuf_iterator<char>(request->content)), istreambuf_iterator<char>());
            response->send(body, "application/json");
        });
        start();
        config = Config::load(ConfigSchemaController(), ConfigTestDataController::data());
        ConfigWorker worker = config->worker("worker-A");
        worker.httpSvcHost.addr = "127.0.0.1";
        worker.httpSvcPort = server->getPort();
        config->updateWorker(worker);
        config->set<unsigned int>("controller", "http-messenger-num-threads", 1);
        LOGS_INFO("HttpMessengerFixture initialized, server port: " + to_string(server->getPort()));
    }

    HttpMessengerFixture(HttpMessengerFixture const&) = delete;
    HttpMessengerFixture& operator=(HttpMessengerFixture const&) = delete;

    ~HttpMessengerFixture() {
        server->stop();
        ioService.stop();
        for (auto& thread : serviceThreads) {
            if (thread.joinable()) thread.join();
        }
        // curl_global_cleanup();
    }

    void start() {
        server->start();
        for (size_t i = 0; i < 2; ++i) {
            serviceThreads.emplace_back([this]() {
                asio::io_service::work work(ioService);
                ioService.run();
            });
        }
    }

    bool waitForDelayStart(chrono::milliseconds timeout = chrono::seconds(2)) {
        unique_lock<mutex> lock(delayMutex);
        return delayCondition.wait_for(lock, timeout, [this]() { return delayStarted; });
    }

    boost::asio::io_service ioService;
    lsst::qserv::qhttp::Server::Ptr server;
    vector<thread> serviceThreads;
    shared_ptr<Config> config;
    mutex delayMutex;
    condition_variable delayCondition;
    bool delayStarted = false;
};

struct CallbackResult {
    uint64_t id = 0;
    bool success = false;
    string error;
    json response;
};

}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_FIXTURE_TEST_CASE(HttpMessengerRequestGet, HttpMessengerFixture) {
    promise<CallbackResult> callbackPromise;
    auto callbackFuture = callbackPromise.get_future();
    HttpMessengerRequest request(
            71, 5, "/json", lsst::qserv::http::Method::GET, json::object(),
            [&callbackPromise](uint64_t id, bool success, string const& error, json const& response) {
                callbackPromise.set_value({id, success, error, response});
            },
            2000);

    request.process("http://127.0.0.1:" + to_string(server->getPort()));
    BOOST_REQUIRE(callbackFuture.wait_for(chrono::seconds(1)) == future_status::ready);
    CallbackResult const result = callbackFuture.get();
    BOOST_CHECK_EQUAL(result.id, 71U);
    BOOST_CHECK_EQUAL(result.success, true);
    BOOST_CHECK_EQUAL(result.error, string());
    if (result.success) {
        BOOST_CHECK_EQUAL(result.response.at("message").get<string>(), "hello");
        BOOST_CHECK_EQUAL(result.response.at("value").get<int>(), 42);
    }
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerRequestReportsInvalidJson, HttpMessengerFixture) {
    HttpMessengerRequest request(
            72, 0, "/invalid-json", lsst::qserv::http::Method::GET, json::object(),
            [](uint64_t id, bool success, string const& error, json const& response) {
                BOOST_CHECK_EQUAL(id, 72U);
                BOOST_CHECK_EQUAL(success, false);
                BOOST_CHECK_EQUAL(error.empty(), false);
            },
            2000);
    request.process("http://127.0.0.1:" + to_string(server->getPort()));
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerConnectorSendCancelAndStop, HttpMessengerFixture) {
    auto const ids = make_shared<HttpMessengerRequest::IdGenerator>();
    auto connector = HttpMessengerConnector::create(config, "worker-A", ids);
    promise<CallbackResult> firstPromise;
    auto firstFuture = firstPromise.get_future();
    atomic<unsigned int> cancelledCallbackCount{0};

    uint64_t const firstId = connector->send(
            0, "/delay", lsst::qserv::http::Method::GET, json::object(),
            [&firstPromise](uint64_t id, bool success, string const& error, json const& response) {
                firstPromise.set_value({id, success, error, response});
            },
            2000);
    BOOST_REQUIRE(waitForDelayStart());
    BOOST_CHECK(connector->exists(firstId));

    uint64_t const cancelledId = connector->send(
            0, "/json", lsst::qserv::http::Method::GET, json::object(),
            [&cancelledCallbackCount](uint64_t, bool, string const&, json const&) {
                ++cancelledCallbackCount;
            },
            2000);
    BOOST_CHECK(connector->exists(cancelledId));
    BOOST_CHECK_NO_THROW(connector->cancel(cancelledId));
    BOOST_CHECK(!connector->exists(cancelledId));

    BOOST_REQUIRE(firstFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const firstResult = firstFuture.get();
    BOOST_CHECK_EQUAL(firstResult.id, firstId);
    BOOST_CHECK(firstResult.success);
    BOOST_CHECK_EQUAL(cancelledCallbackCount.load(), 0U);

    connector->stop();
    BOOST_CHECK_NO_THROW(connector->stop());
    BOOST_CHECK_THROW(connector->send(
                              0, "/json", lsst::qserv::http::Method::GET, json::object(),
                              [](uint64_t, bool, string const&, json const&) {}, 2000),
                      logic_error);
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerSendPostAndStop, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config);
    promise<CallbackResult> callbackPromise;
    auto callbackFuture = callbackPromise.get_future();
    json const payload = {{"value", "round-trip"}, {"number", 17}};

    uint64_t const id = messenger->send(
            "worker-A", 2, "/echo", lsst::qserv::http::Method::POST, payload,
            [&callbackPromise](uint64_t callbackId, bool success, string const& error, json const& response) {
                LOGS_INFO("HttpMessengerSendPostAndStop callback invoked with id: "
                          << callbackId << ", success: " << success << ", error: " << error
                          << ", payload: " << response);
                callbackPromise.set_value({callbackId, success, error, response});
            },
            2000);

    BOOST_CHECK(id != 0);
    BOOST_REQUIRE(callbackFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const result = callbackFuture.get();
    BOOST_CHECK_EQUAL(result.id, id);
    BOOST_CHECK(result.success);
    BOOST_CHECK_EQUAL(result.response, payload);
    BOOST_CHECK(!messenger->exists("worker-A", id));

    BOOST_CHECK_THROW(messenger->send(
                              "unknown-worker", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
                              [](uint64_t, bool, string const&, json const&) {}, 2000),
                      ConfigUnknownWorker);

    messenger->stop();
    BOOST_CHECK_NO_THROW(messenger->stop());
    BOOST_CHECK_THROW(messenger->send(
                              "worker-A", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
                              [](uint64_t, bool, string const&, json const&) {}, 2000),
                      logic_error);
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerWorkerSurvivesBadResponseAndThrowingCallback, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config);
    promise<CallbackResult> badResponsePromise;
    promise<CallbackResult> goodResponsePromise;
    auto badResponseFuture = badResponsePromise.get_future();
    auto goodResponseFuture = goodResponsePromise.get_future();

    messenger->send(
            "worker-A", 0, "/invalid-json", lsst::qserv::http::Method::GET, json::object(),
            [&badResponsePromise](uint64_t id, bool success, string const& error, json const& response) {
                badResponsePromise.set_value({id, success, error, response});
            },
            2000);
    BOOST_REQUIRE(badResponseFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const badResult = badResponseFuture.get();
    BOOST_CHECK(!badResult.success);
    BOOST_CHECK(!badResult.error.empty());

    messenger->send(
            "worker-A", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
            [](uint64_t, bool, string const&, json const&) {
                throw runtime_error("test callback exception");
            },
            2000);
    uint64_t const finalId = messenger->send(
            "worker-A", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
            [&goodResponsePromise](uint64_t id, bool success, string const& error, json const& response) {
                goodResponsePromise.set_value({id, success, error, response});
            },
            2000);
    BOOST_REQUIRE(goodResponseFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const goodResult = goodResponseFuture.get();
    BOOST_CHECK_EQUAL(goodResult.id, finalId);
    BOOST_CHECK(goodResult.success);
    if (goodResult.success) {
        BOOST_CHECK_EQUAL(goodResult.response.at("message").get<string>(), "hello");
    }
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerStopFromCallbackDoesNotWaitForSiblingCallback, HttpMessengerFixture) {
    ConfigWorker workerB = config->worker("worker-B");
    workerB.httpSvcHost.addr = "127.0.0.1";
    workerB.httpSvcPort = server->getPort();
    config->updateWorker(workerB);

    auto messenger = HttpMessenger::create(config);
    mutex callbackMutex;
    condition_variable callbackCondition;
    bool siblingCallbackStarted = false;
    bool allowSiblingCallbackToReturn = false;
    bool siblingCallbackFinished = false;
    bool siblingCallbackFinishedWhenStopReturned = false;
    promise<void> stopCallbackPromise;
    auto stopCallbackFuture = stopCallbackPromise.get_future();

    messenger->send(
            "worker-B", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
            [&](uint64_t, bool, string const&, json const&) {
                unique_lock<mutex> lock(callbackMutex);
                siblingCallbackStarted = true;
                callbackCondition.notify_all();
                callbackCondition.wait_for(lock, chrono::seconds(2),
                                           [&]() { return allowSiblingCallbackToReturn; });
                siblingCallbackFinished = true;
                callbackCondition.notify_all();
            },
            2000);

    {
        unique_lock<mutex> lock(callbackMutex);
        BOOST_REQUIRE(callbackCondition.wait_for(lock, chrono::seconds(2),
                                                 [&]() { return siblingCallbackStarted; }));
    }

    messenger->send(
            "worker-A", 0, "/json", lsst::qserv::http::Method::GET, json::object(),
            [&](uint64_t, bool, string const&, json const&) {
                messenger->stop();
                {
                    lock_guard<mutex> lock(callbackMutex);
                    siblingCallbackFinishedWhenStopReturned = siblingCallbackFinished;
                    allowSiblingCallbackToReturn = true;
                }
                callbackCondition.notify_all();
                stopCallbackPromise.set_value();
            },
            2000);

    BOOST_REQUIRE(stopCallbackFuture.wait_for(chrono::seconds(1)) == future_status::ready);
    {
        lock_guard<mutex> lock(callbackMutex);
        BOOST_CHECK(!siblingCallbackFinishedWhenStopReturned);
        allowSiblingCallbackToReturn = true;
    }
    callbackCondition.notify_all();
}

BOOST_AUTO_TEST_SUITE_END()
