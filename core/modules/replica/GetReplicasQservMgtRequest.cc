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
#include "replica/GetReplicasQservMgtRequest.h"

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

// This macro to appear witin each block which requires thread safety
#define LOCK_GUARD std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetReplicasQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

GetReplicasQservMgtRequest::pointer GetReplicasQservMgtRequest::create(
                                        ServiceProvider::pointer const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker,
                                        std::string const& databaseFamily,
                                        bool inUseOnly,
                                        GetReplicasQservMgtRequest::callback_type onFinish) {
    return GetReplicasQservMgtRequest::pointer(
        new GetReplicasQservMgtRequest(serviceProvider,
                                       io_service,
                                       worker,
                                       databaseFamily,
                                       inUseOnly,
                                       onFinish));
 }

GetReplicasQservMgtRequest::GetReplicasQservMgtRequest(
                                ServiceProvider::pointer const& serviceProvider,
                                boost::asio::io_service& io_service,
                                std::string const& worker,
                                std::string const& databaseFamily,
                                bool inUseOnly,
                                GetReplicasQservMgtRequest::callback_type onFinish)
    :   QservMgtRequest(serviceProvider,
                        io_service,
                        "QSERV:GET_REPLICAS",
                        worker),
        _databaseFamily(databaseFamily),
        _inUseOnly(inUseOnly),
        _onFinish(onFinish),
        _qservRequest(nullptr) {

    if (not _serviceProvider->config()->isKnownDatabaseFamily(_databaseFamily)) {
        throw std::logic_error(
                "GetReplicasQservMgtRequest: unknown database family: " +
                _databaseFamily);
    }
}

QservReplicaCollection const& GetReplicasQservMgtRequest::replicas() const {
    if (not ((_state == State::FINISHED) and (_extendedState == ExtendedState::SUCCESS))) {
        throw std::logic_error(
                "GetReplicasQservMgtRequest::replicas  replicas aren't available in state: " +
                state2string(_state, _extendedState));
    }
    return _replicas;
}

void GetReplicasQservMgtRequest::setReplicas(
        wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {

    // Filter resuls by atabases participating in the family
    std::set<std::string> databases;
    for (auto const& database: _serviceProvider->config()->databases(_databaseFamily)) {
        databases.insert(database);
    }
    _replicas.clear();
    for (auto const& replica: collection) {
        if (databases.count(replica.database)) {
            _replicas.emplace_back(QservReplica{replica.chunk, replica.database, replica.use_count});
        }
    }
}

void GetReplicasQservMgtRequest::startImpl() {

    GetReplicasQservMgtRequest::pointer const& request =
        shared_from_base<GetReplicasQservMgtRequest>();

    _qservRequest = wpublish::GetChunkListQservRequest::create(
        _inUseOnly,
        [request] (wpublish::GetChunkListQservRequest::Status status,
                   std::string const& error,
                   wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {

            switch (status) {
                case wpublish::GetChunkListQservRequest::Status::SUCCESS:
                    request->setReplicas(collection);
                    request->finish(QservMgtRequest::ExtendedState::SUCCESS);
                    break;
                case wpublish::GetChunkListQservRequest::Status::ERROR:
                    request->finish(QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;
                default:
                    throw std::logic_error(
                                    "GetReplicasQservMgtRequest:  unhandled server status: " +
                                    wpublish::GetChunkListQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(_worker));
    _service->ProcessRequest(*_qservRequest, resource);
}

void GetReplicasQservMgtRequest::finishImpl() {

    assertState(State::FINISHED);

    if (_extendedState == ExtendedState::CANCELLED) {
        // And if the SSI request is still around then tell it to stop
        if (_qservRequest) {
            bool const cancel = true;
            _qservRequest->Finished(cancel);
        }
    }
    _qservRequest = nullptr;
}

void GetReplicasQservMgtRequest::notify() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notify");

    // The callback is being made asynchronously in a separate thread
    // to avoid blocking the current thread.

    if (_onFinish) {
        auto self = shared_from_base<GetReplicasQservMgtRequest>();
        std::async(
            std::launch::async,
            [self]() {
                self->_onFinish(self);
            }
        );
    }
}
}}} // namespace lsst::qserv::replica
