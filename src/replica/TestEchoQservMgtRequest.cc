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
#include "replica/TestEchoQservMgtRequest.h"

// System headers
#include <set>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.TestEchoQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

TestEchoQservMgtRequest::Ptr TestEchoQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, string const& data,
        TestEchoQservMgtRequest::CallbackType const& onFinish) {
    return TestEchoQservMgtRequest::Ptr(new TestEchoQservMgtRequest(serviceProvider, worker, data, onFinish));
}

TestEchoQservMgtRequest::TestEchoQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                                                 string const& worker, string const& data,
                                                 TestEchoQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_TEST_ECHO", worker), _data(data), _onFinish(onFinish) {}

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

void TestEchoQservMgtRequest::startImpl(replica::Lock const& lock) {
    // Submit the actual request

    auto const request = shared_from_base<TestEchoQservMgtRequest>();

    _qservRequest = wpublish::TestEchoQservRequest::create(
            data(), [request](wpublish::TestEchoQservRequest::Status status, string const& error,
                              string const& data, string const& dataEcho) {
                if (request->state() == State::FINISHED) return;

                replica::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");

                if (request->state() == State::FINISHED) return;

                switch (status) {
                    case wpublish::TestEchoQservRequest::Status::SUCCESS:

                        request->_setData(lock, dataEcho);
                        request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                        break;

                    case wpublish::TestEchoQservRequest::Status::ERROR:

                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                        break;

                    default:
                        throw logic_error("TestEchoQservMgtRequest::" + string(__func__) +
                                          "  unhandled server status: " +
                                          wpublish::TestEchoQservRequest::status2str(status));
                }
            });
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void TestEchoQservMgtRequest::finishImpl(replica::Lock const& lock) {
    switch (extendedState()) {
        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:

            // And if the SSI request is still around then tell it to stop

            if (_qservRequest) {
                bool const cancel = true;
                _qservRequest->Finished(cancel);
            }
            break;

        default:
            break;
    }
}

void TestEchoQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<TestEchoQservMgtRequest>(lock, _onFinish);
}

void TestEchoQservMgtRequest::_setData(replica::Lock const& lock, string const& data) { _dataEcho = data; }

}  // namespace lsst::qserv::replica
