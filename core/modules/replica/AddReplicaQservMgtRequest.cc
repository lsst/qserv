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
#include "replica/AddReplicaQservMgtRequest.h"

// System headers
#include <future>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/LockUtils.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AddReplicaQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

AddReplicaQservMgtRequest::Ptr AddReplicaQservMgtRequest::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker,
                                        unsigned int chunk,
                                        std::vector<std::string> const& databases,
                                        AddReplicaQservMgtRequest::CallbackType onFinish) {
    return AddReplicaQservMgtRequest::Ptr(
        new AddReplicaQservMgtRequest(serviceProvider,
                                      io_service,
                                      worker,
                                      chunk,
                                      databases,
                                      onFinish));
}

AddReplicaQservMgtRequest::AddReplicaQservMgtRequest(
                                ServiceProvider::Ptr const& serviceProvider,
                                boost::asio::io_service& io_service,
                                std::string const& worker,
                                unsigned int chunk,
                                std::vector<std::string> const& databases,
                                AddReplicaQservMgtRequest::CallbackType onFinish)
    :   QservMgtRequest(serviceProvider,
                        io_service,
                        "QSERV_ADD_REPLICA",
                        worker),
        _chunk(chunk),
        _databases(databases),
        _onFinish(onFinish),
        _qservRequest(nullptr) {
}

std::string AddReplicaQservMgtRequest::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databases(),
                              chunk());
}

void AddReplicaQservMgtRequest::startImpl() {

    ASSERT_LOCK(_mtx, context() + "startImpl");

    auto const request = shared_from_base<AddReplicaQservMgtRequest>();

    _qservRequest = wpublish::AddChunkGroupQservRequest::create(
        _chunk,
        _databases,
        [request] (wpublish::ChunkGroupQservRequest::Status status,
                   std::string const& error) {

            switch (status) {
                case wpublish::ChunkGroupQservRequest::Status::SUCCESS:
                    request->finish(QservMgtRequest::ExtendedState::SUCCESS);
                    break;
                case wpublish::ChunkGroupQservRequest::Status::INVALID:
                    request->finish(QservMgtRequest::ExtendedState::SERVER_BAD, error);
                    break;
                case wpublish::ChunkGroupQservRequest::Status::IN_USE:
                    request->finish(QservMgtRequest::ExtendedState::SERVER_IN_USE, error);
                    break;
                case wpublish::ChunkGroupQservRequest::Status::ERROR:
                    request->finish(QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;
                default:
                    throw std::logic_error(
                        "AddReplicaQservMgtRequest:  unhandled server status: " +
                        wpublish::ChunkGroupQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(_worker));
    _service->ProcessRequest(*_qservRequest, resource);
}

void AddReplicaQservMgtRequest::finishImpl() {

    ASSERT_LOCK(_mtx, context() + "finishImpl");

    assertState(State::FINISHED, "AddReplicaQservMgtRequest::finishImpl");

    if (_extendedState == ExtendedState::CANCELLED) {
        // And if the SSI request is still around then tell it to stop
        if (_qservRequest) {
            bool const cancel = true;
            _qservRequest->Finished(cancel);
        }
    }
    _qservRequest = nullptr;
}

void AddReplicaQservMgtRequest::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<AddReplicaQservMgtRequest>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}

}}} // namespace lsst::qserv::replica
