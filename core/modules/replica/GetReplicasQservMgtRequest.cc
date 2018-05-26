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
#include "replica/DatabaseMySQL.h"
#include "replica/ServiceProvider.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetReplicasQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

GetReplicasQservMgtRequest::Ptr GetReplicasQservMgtRequest::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        boost::asio::io_service& io_service,
                                        std::string const& worker,
                                        std::string const& databaseFamily,
                                        bool inUseOnly,
                                        GetReplicasQservMgtRequest::CallbackType onFinish) {
    return GetReplicasQservMgtRequest::Ptr(
        new GetReplicasQservMgtRequest(serviceProvider,
                                       io_service,
                                       worker,
                                       databaseFamily,
                                       inUseOnly,
                                       onFinish));
 }

GetReplicasQservMgtRequest::GetReplicasQservMgtRequest(
                                ServiceProvider::Ptr const& serviceProvider,
                                boost::asio::io_service& io_service,
                                std::string const& worker,
                                std::string const& databaseFamily,
                                bool inUseOnly,
                                GetReplicasQservMgtRequest::CallbackType onFinish)
    :   QservMgtRequest(serviceProvider,
                        io_service,
                        "QSERV_GET_REPLICAS",
                        worker),
        _databaseFamily(databaseFamily),
        _inUseOnly(inUseOnly),
        _onFinish(onFinish),
        _qservRequest(nullptr) {
}

QservReplicaCollection const& GetReplicasQservMgtRequest::replicas() const {
    if (not ((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw std::logic_error(
                "GetReplicasQservMgtRequest::replicas  replicas aren't available in state: " +
                state2string(state(), extendedState()));
    }
    return _replicas;
}

std::string GetReplicasQservMgtRequest::extendedPersistentState(SqlGeneratorPtr const& gen) const {
    return gen->sqlPackValues(id(),
                              databaseFamily(),
                              inUseOnly() ? 1 : 0);
}

void GetReplicasQservMgtRequest::setReplicas(
            util::Lock const& lock,
            wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {

    // Filter resuls by databases participating in the family

    std::set<std::string> databases;
    for (auto&& database: serviceProvider()->config()->databases(databaseFamily())) {
        databases.insert(database);
    }
    _replicas.clear();
    for (auto&& replica: collection) {
        if (databases.count(replica.database)) {
            _replicas.emplace_back(QservReplica{replica.chunk,
                                                replica.database,
                                                replica.use_count});
        }
    }
}

void GetReplicasQservMgtRequest::startImpl(util::Lock const& lock) {

    // Check if configuration parameters are valid

    if (not serviceProvider()->config()->isKnownDatabaseFamily(databaseFamily())) {

        LOGS(_log, LOG_LVL_ERROR, context() << "start  ** MISCONFIGURED ** "
             << " database family: '" << databaseFamily() << "'");

        finish(lock, ExtendedState::CONFIG_ERROR);
        return;
    }

    // Submit the actual request

    auto const request = shared_from_base<GetReplicasQservMgtRequest>();

    _qservRequest = wpublish::GetChunkListQservRequest::create(
        inUseOnly(),
        [request] (wpublish::GetChunkListQservRequest::Status status,
                   std::string const& error,
                   wpublish::GetChunkListQservRequest::ChunkCollection const& collection) {

            // IMPORTANT: the final state is required to be tested twice. The first time
            // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
            // their completion while the request termination is in a progress. And the second
            // test is made after acquering the lock to recheck the state in case if it
            // has transitioned while acquering the lock.

            if (request->state() == State::FINISHED) return;
        
            util::Lock lock(request->_mtx, request->context() + "startImpl[callback]");
        
            if (request->state() == State::FINISHED) return;

            switch (status) {

                case wpublish::GetChunkListQservRequest::Status::SUCCESS:

                    request->setReplicas(lock, collection);
                    request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                    break;

                case wpublish::GetChunkListQservRequest::Status::ERROR:

                    request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;

                default:
                    throw std::logic_error(
                                    "GetReplicasQservMgtRequest:  unhandled server status: " +
                                    wpublish::GetChunkListQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void GetReplicasQservMgtRequest::finishImpl(util::Lock const& lock) {

    if (extendedState() == ExtendedState::CANCELLED) {

        // And if the SSI request is still around then tell it to stop

        if (_qservRequest) {
            bool const cancel = true;
            _qservRequest->Finished(cancel);
        }
    }
    _qservRequest = nullptr;
}

void GetReplicasQservMgtRequest::notifyImpl() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "notifyImpl");

    if (_onFinish) {
        _onFinish(shared_from_base<GetReplicasQservMgtRequest>());
    }
}
}}} // namespace lsst::qserv::replica
