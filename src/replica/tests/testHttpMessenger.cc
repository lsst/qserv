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
#include <future>
#include <iterator>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

// Third-party headers
#include "boost/asio.hpp"
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

// Boost unit test header
#define BOOST_TEST_MODULE HttpMessenger
#include <boost/test/unit_test.hpp>

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace asio = boost::asio;
namespace http = lsst::qserv::http;

namespace {

struct CallbackResult {
    shared_ptr<http::AsyncReq> request;
    string error;
};

struct HttpMessengerFixture {
    HttpMessengerFixture() {
        server = lsst::qserv::qhttp::Server::create(ioService, 0);
        server->addHandler("GET", "/json", [](auto const&, auto const& response) {
            response->send(R"({"message":"hello","value":42})", "application/json");
        });
        server->addHandler("GET", "/not-json", [](auto const&, auto const& response) {
            response->send("plain response body", "text/plain");
        });
        server->addHandler("GET", "/delay", [this](auto const&, auto const& response) {
            {
                lock_guard<mutex> lock(delayMutex);
                delayStarted = true;
            }
            delayCondition.notify_all();
            this_thread::sleep_for(chrono::milliseconds(300));
            response->send(R"({"delayed":true})", "application/json");
        });
        server->addHandler("POST", "/echo", [](auto const& request, auto const& response) {
            string const body((istreambuf_iterator<char>(request->content)), istreambuf_iterator<char>());
            response->send(body, "application/json");
        });
        server->start();

        auto work = make_shared<asio::io_service::work>(ioService);
        for (size_t i = 0; i < 2; ++i) {
            serviceThreads.emplace_back([this, work]() { ioService.run(); });
        }

        config = Config::load(ConfigSchemaController(), ConfigTestDataController::data());
        config->set<unsigned int>("controller", "num-requests-per-worker", 1);
        for (auto const workerName : {"worker-A", "worker-B"}) {
            ConfigWorker worker = config->worker(workerName);
            worker.httpSvcHost.addr = "127.0.0.1";
            worker.httpSvcPort = server->getPort();
            config->updateWorker(worker);
        }
    }

    HttpMessengerFixture(HttpMessengerFixture const&) = delete;
    HttpMessengerFixture& operator=(HttpMessengerFixture const&) = delete;

    ~HttpMessengerFixture() {
        server->stop();
        ioService.stop();
        for (auto& thread : serviceThreads) {
            if (thread.joinable()) thread.join();
        }
    }

    bool waitForDelayStart(chrono::milliseconds timeout = chrono::seconds(2)) {
        unique_lock<mutex> lock(delayMutex);
        return delayCondition.wait_for(lock, timeout, [this]() { return delayStarted; });
    }

    asio::io_service ioService;
    lsst::qserv::qhttp::Server::Ptr server;
    vector<thread> serviceThreads;
    shared_ptr<Config> config;
    mutex delayMutex;
    condition_variable delayCondition;
    bool delayStarted = false;
};

}  // namespace

BOOST_AUTO_TEST_SUITE(Suite)

BOOST_FIXTURE_TEST_CASE(HttpMessengerGetAndPost, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config, ioService);
    promise<CallbackResult> getPromise;
    promise<CallbackResult> postPromise;
    auto getFuture = getPromise.get_future();
    auto postFuture = postPromise.get_future();
    json const payload = {{"value", "round-trip"}, {"number", 17}};

    BOOST_CHECK_NO_THROW(messenger->send(
            "worker-A", "get-1", 1, "/json", http::Method::GET, json(),
            [&getPromise](shared_ptr<http::AsyncReq> request, string error) {
                getPromise.set_value({std::move(request), std::move(error)});
            },
            2000));
    BOOST_REQUIRE(getFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const getResult = getFuture.get();
    BOOST_REQUIRE(getResult.request != nullptr);
    BOOST_CHECK(getResult.error.empty());
    BOOST_CHECK(getResult.request->state() == http::AsyncReq::State::FINISHED);
    BOOST_CHECK_EQUAL(getResult.request->responseCode(), 200);
    BOOST_CHECK_EQUAL(json::parse(getResult.request->responseBody()).at("message").get<string>(), "hello");
    BOOST_CHECK(!messenger->exists("worker-A", "get-1"));

    messenger->send(
            "worker-A", "post-1", 1, "/echo", http::Method::POST, payload,
            [&postPromise](shared_ptr<http::AsyncReq> request, string error) {
                postPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(postFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const postResult = postFuture.get();
    BOOST_REQUIRE(postResult.request != nullptr);
    BOOST_CHECK(postResult.error.empty());
    BOOST_CHECK_EQUAL(json::parse(postResult.request->responseBody()), payload);
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerRequestReportsAsyncResponseAndDuplicateId, HttpMessengerFixture) {
    auto connector = HttpMessengerConnector::create(config, ioService, "worker-A");
    promise<CallbackResult> callbackPromise;
    auto callbackFuture = callbackPromise.get_future();

    connector->send(
            "request-1", 0, "/delay", http::Method::GET, json(),
            [&callbackPromise](shared_ptr<http::AsyncReq> request, string error) {
                callbackPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(waitForDelayStart());
    BOOST_CHECK(connector->exists("request-1"));
    BOOST_CHECK_THROW(connector->send(
                              "request-1", 0, "/json", http::Method::GET, json(),
                              [](shared_ptr<http::AsyncReq>, string) {}, 2000),
                      logic_error);

    BOOST_REQUIRE(callbackFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const result = callbackFuture.get();
    BOOST_REQUIRE(result.request != nullptr);
    BOOST_CHECK(result.error.empty());
    BOOST_CHECK_EQUAL(result.request->responseBody(), R"({"delayed":true})");
    BOOST_CHECK(!connector->exists("request-1"));

    BOOST_CHECK_THROW(connector->cancel("request-1"), logic_error);
    promise<CallbackResult> reusedIdPromise;
    auto reusedIdFuture = reusedIdPromise.get_future();
    connector->send(
            "request-1", 0, "/json", http::Method::GET, json(),
            [&reusedIdPromise](shared_ptr<http::AsyncReq> request, string error) {
                reusedIdPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(reusedIdFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const reusedIdResult = reusedIdFuture.get();
    BOOST_REQUIRE(reusedIdResult.request != nullptr);
    BOOST_CHECK(reusedIdResult.error.empty());
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerCancelsQueuedRequestAndReportsIt, HttpMessengerFixture) {
    auto connector = HttpMessengerConnector::create(config, ioService, "worker-A");
    promise<CallbackResult> activePromise;
    promise<CallbackResult> queuedPromise;
    auto activeFuture = activePromise.get_future();
    auto queuedFuture = queuedPromise.get_future();

    connector->send(
            "active", 0, "/delay", http::Method::GET, json(),
            [&activePromise](shared_ptr<http::AsyncReq> request, string error) {
                activePromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(waitForDelayStart());

    connector->send(
            "queued", 0, "/json", http::Method::GET, json(),
            [&queuedPromise](shared_ptr<http::AsyncReq> request, string error) {
                queuedPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_CHECK(connector->exists("queued"));
    connector->cancel("queued");

    BOOST_REQUIRE(queuedFuture.wait_for(chrono::seconds(2)) == future_status::ready);
    CallbackResult const queuedResult = queuedFuture.get();
    BOOST_CHECK(queuedResult.request == nullptr);
    BOOST_CHECK(queuedResult.error.empty());
    BOOST_CHECK(!connector->exists("queued"));

    BOOST_REQUIRE(activeFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const activeResult = activeFuture.get();
    BOOST_REQUIRE(activeResult.request != nullptr);
    BOOST_CHECK(activeResult.request->state() == http::AsyncReq::State::FINISHED);
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerCancelActiveRequest, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config, ioService);
    promise<CallbackResult> callbackPromise;
    auto callbackFuture = callbackPromise.get_future();

    messenger->send(
            "worker-A", "cancel-active", 0, "/delay", http::Method::GET, json(),
            [&callbackPromise](shared_ptr<http::AsyncReq> request, string error) {
                callbackPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(waitForDelayStart());
    BOOST_CHECK(messenger->exists("worker-A", "cancel-active"));
    messenger->cancel("worker-A", "cancel-active");

    BOOST_REQUIRE(callbackFuture.wait_for(chrono::seconds(2)) == future_status::ready);
    CallbackResult const result = callbackFuture.get();
    BOOST_REQUIRE(result.request != nullptr);
    BOOST_CHECK(result.request->state() == http::AsyncReq::State::CANCELLED);
    BOOST_CHECK(!messenger->exists("worker-A", "cancel-active"));
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerCancelAllNotifiesQueuedAndActive, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config, ioService);
    promise<CallbackResult> activePromise;
    promise<CallbackResult> queuedPromise;
    auto activeFuture = activePromise.get_future();
    auto queuedFuture = queuedPromise.get_future();

    messenger->send(
            "worker-A", "active", 0, "/delay", http::Method::GET, json(),
            [&activePromise](shared_ptr<http::AsyncReq> request, string error) {
                activePromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(waitForDelayStart());
    messenger->send(
            "worker-A", "queued", 0, "/json", http::Method::GET, json(),
            [&queuedPromise](shared_ptr<http::AsyncReq> request, string error) {
                queuedPromise.set_value({std::move(request), std::move(error)});
            },
            2000);

    messenger->cancelAll();
    BOOST_REQUIRE(activeFuture.wait_for(chrono::seconds(2)) == future_status::ready);
    BOOST_REQUIRE(queuedFuture.wait_for(chrono::seconds(2)) == future_status::ready);
    CallbackResult const activeResult = activeFuture.get();
    CallbackResult const queuedResult = queuedFuture.get();
    BOOST_REQUIRE(activeResult.request != nullptr);
    BOOST_CHECK(activeResult.request->state() == http::AsyncReq::State::CANCELLED);
    BOOST_CHECK(queuedResult.request == nullptr);
    BOOST_CHECK(activeResult.error.empty());
    BOOST_CHECK(queuedResult.error.empty());
    BOOST_CHECK(!messenger->exists("worker-A", "active"));
    BOOST_CHECK(!messenger->exists("worker-A", "queued"));
}

BOOST_FIXTURE_TEST_CASE(HttpMessengerValidationAndCallbackDelivery, HttpMessengerFixture) {
    auto messenger = HttpMessenger::create(config, ioService);
    BOOST_CHECK_THROW(messenger->send(
                              "unknown-worker", "id", 0, "/json", http::Method::GET, json(),
                              [](shared_ptr<http::AsyncReq>, string) {}, 2000),
                      ConfigUnknownWorker);
    BOOST_CHECK_THROW(
            messenger->send("worker-A", "no-callback", 0, "/json", http::Method::GET, json(), {}, 2000),
            invalid_argument);

    promise<CallbackResult> completionPromise;
    auto completionFuture = completionPromise.get_future();
    messenger->send(
            "worker-A", "delivered", 0, "/json", http::Method::GET, json(),
            [&completionPromise](shared_ptr<http::AsyncReq> request, string error) {
                completionPromise.set_value({std::move(request), std::move(error)});
            },
            2000);
    BOOST_REQUIRE(completionFuture.wait_for(chrono::seconds(3)) == future_status::ready);
    CallbackResult const result = completionFuture.get();
    BOOST_REQUIRE(result.request != nullptr);
    BOOST_CHECK(result.error.empty());
    BOOST_CHECK(result.request->state() == http::AsyncReq::State::FINISHED);
    BOOST_CHECK(!messenger->exists("worker-A", "delivered"));
}

BOOST_AUTO_TEST_SUITE_END()