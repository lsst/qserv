/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include <future>
#include <set>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.TestEchoQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

TestEchoQservMgtRequest::Ptr TestEchoQservMgtRequest::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker,
                                        std::string const& data,
                                        TestEchoQservMgtRequest::CallbackType onFinish) {
    return TestEchoQservMgtRequest::Ptr(
        new TestEchoQservMgtRequest(serviceProvider,
                                    io_service,
                                    worker,
                                    data,
                                    onFinish));
}

TestEchoQservMgtRequest::TestEchoQservMgtRequest(
                                ServiceProvider::Ptr const& serviceProvider,
                                boost::asio::io_service& io_service,
                                std::string const& worker,
                                std::string const& data,
                                TestEchoQservMgtRequest::CallbackType onFinish)
    :   QservMgtRequest(serviceProvider,
                        io_service,
                        "QSERV_TEST_ECHO",
                        worker),
        _data(data),
        _onFinish(onFinish) {
}

std::string const& TestEchoQservMgtRequest::dataEcho() const {
    if (not ((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw std::logic_error(
                "TestEchoQservMgtRequest::replicas  replicas aren't available in state: " +
                state2string(state(), extendedState()));
    }
    return _dataEcho;
}

std::map<std::string,std::string> TestEchoQservMgtRequest::extendedPersistentState() const {
    std::map<std::string,std::string> result;
    result["data_length_bytes"] = std::to_string(data().size());
    return result;
}

void TestEchoQservMgtRequest::startImpl(util::Lock const& lock) {

    // Submit the actual request

    auto const request = shared_from_base<TestEchoQservMgtRequest>();

    _qservRequest = wpublish::TestEchoQservRequest::create(
        data(),
        [request] (wpublish::TestEchoQservRequest::Status status,
                   std::string const& error,
                   std::string const& data,
                   std::string const& dataEcho) {

            // IMPORTANT: the final state is required to be tested twice. The first time
            // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
            // their completion while the request termination is in a progress. And the second
            // test is made after acquering the lock to recheck the state in case if it
            // has transitioned while acquering the lock.

            if (request->state() == State::FINISHED) return;
        
            util::Lock lock(request->_mtx, request->context() + "startImpl[callback]");
        
            if (request->state() == State::FINISHED) return;

            switch (status) {

                case wpublish::TestEchoQservRequest::Status::SUCCESS:

                    request->setData(lock, dataEcho);
                    request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                    break;

                case wpublish::TestEchoQservRequest::Status::ERROR:

                    request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;

                default:
                    throw std::logic_error(
                                    "TestEchoQservMgtRequest:  unhandled server status: " +
                                    wpublish::TestEchoQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void TestEchoQservMgtRequest::finishImpl(util::Lock const& lock) {

    if (extendedState() == ExtendedState::CANCELLED) {

        // And if the SSI request is still around then tell it to stop

        if (_qservRequest) {
            bool const cancel = true;
            _qservRequest->Finished(cancel);
        }
    }
    _qservRequest = nullptr;
}

void TestEchoQservMgtRequest::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<TestEchoQservMgtRequest>());
    }
}

void TestEchoQservMgtRequest::setData(util::Lock const& lock,
                                      std::string const& data) {
    _dataEcho = data;
}

}}} // namespace lsst::qserv::replica
