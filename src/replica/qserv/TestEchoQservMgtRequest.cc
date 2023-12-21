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
#include "replica/qserv/TestEchoQservMgtRequest.h"

// Qserv headers
#include "http/Method.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.TestEchoQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<TestEchoQservMgtRequest> TestEchoQservMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName, string const& data,
        TestEchoQservMgtRequest::CallbackType const& onFinish) {
    return shared_ptr<TestEchoQservMgtRequest>(
            new TestEchoQservMgtRequest(serviceProvider, workerName, data, onFinish));
}

TestEchoQservMgtRequest::TestEchoQservMgtRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                 string const& workerName, string const& data,
                                                 TestEchoQservMgtRequest::CallbackType const& onFinish)
        : QservWorkerMgtRequest(serviceProvider, "QSERV_TEST_ECHO", workerName),
          _data(data),
          _onFinish(onFinish) {}

string const& TestEchoQservMgtRequest::dataEcho() const {
    if (not((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error("TestEchoQservMgtRequest::" + string(__func__) +
                          "  no data available in state: " + state2string(state(), extendedState()));
    }
    return _dataEcho;
}

list<pair<string, string>> TestEchoQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("data_length_bytes", to_string(data().size()));
    return result;
}

void TestEchoQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const target = "/echo";
    json const data = json::object({{"data", _data}});
    createHttpReq(lock, http::Method::POST, target, data);
}

QservMgtRequest::ExtendedState TestEchoQservMgtRequest::dataReady(replica::Lock const& lock,
                                                                  json const& data) {
    _dataEcho = data.at("data");
    return QservMgtRequest::ExtendedState::SUCCESS;
}

void TestEchoQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<TestEchoQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
