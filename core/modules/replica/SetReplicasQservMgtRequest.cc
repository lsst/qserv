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
#include "replica/SetReplicasQservMgtRequest.h"

// System headers
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

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SetReplicasQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

SetReplicasQservMgtRequest::Ptr SetReplicasQservMgtRequest::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        string const& worker,
                                        QservReplicaCollection const& newReplicas,
                                        bool force,
                                        SetReplicasQservMgtRequest::CallbackType const& onFinish) {
    return SetReplicasQservMgtRequest::Ptr(
        new SetReplicasQservMgtRequest(serviceProvider,
                                       worker,
                                       newReplicas,
                                       force,
                                       onFinish));
}


SetReplicasQservMgtRequest::SetReplicasQservMgtRequest(
                                ServiceProvider::Ptr const& serviceProvider,
                                string const& worker,
                                QservReplicaCollection const& newReplicas,
                                bool force,
                                SetReplicasQservMgtRequest::CallbackType const& onFinish)
    :   QservMgtRequest(serviceProvider,
                        "QSERV_SET_REPLICAS",
                        worker),
        _newReplicas(newReplicas),
        _force(force),
        _onFinish(onFinish),
        _qservRequest(nullptr) {
}


QservReplicaCollection const& SetReplicasQservMgtRequest::replicas() const {
    if (not ((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error(
                "SetReplicasQservMgtRequest::" + string(__func__) +
                "  replicas aren't available in state: " + state2string(state(), extendedState()));
    }
    return _replicas;
}


list<pair<string,string>> SetReplicasQservMgtRequest::extendedPersistentState() const {
    list<pair<string,string>> result;
    result.emplace_back("num_replicas", to_string(newReplicas().size()));
    result.emplace_back("force", force() ? "1" : "0");
    return result;
}


void SetReplicasQservMgtRequest::_setReplicas(
            util::Lock const& lock,
            wpublish::SetChunkListQservRequest::ChunkCollection const& collection) {

    _replicas.clear();
    for (auto&& replica: collection) {
        _replicas.push_back(
            QservReplica{
                replica.chunk,
                replica.database,
                replica.use_count
            }
        );
    }
}


void SetReplicasQservMgtRequest::startImpl(util::Lock const& lock) {

    wpublish::SetChunkListQservRequest::ChunkCollection chunks;
    for (auto&& chunkEntry: newReplicas()) {
        chunks.push_back(
            wpublish::SetChunkListQservRequest::Chunk{
                chunkEntry.chunk,
                chunkEntry.database,
                0  /* UNUSED: use_count */
            }
        );
    }
    auto const request = shared_from_base<SetReplicasQservMgtRequest>();

    _qservRequest = wpublish::SetChunkListQservRequest::create(
        chunks,
        force(),
        [request] (wpublish::SetChunkListQservRequest::Status status,
                   string const& error,
                   wpublish::SetChunkListQservRequest::ChunkCollection const& collection) {

            // IMPORTANT: the final state is required to be tested twice. The first time
            // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
            // their completion while the request termination is in a progress. And the second
            // test is made after acquiring the lock to recheck the state in case if it
            // has transitioned while acquiring the lock.

            if (request->state() == State::FINISHED) return;
        
            util::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");
        
            if (request->state() == State::FINISHED) return;

            switch (status) {
                case wpublish::SetChunkListQservRequest::Status::SUCCESS:
                    request->_setReplicas(lock, collection);
                    request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                    break;

                case wpublish::SetChunkListQservRequest::Status::ERROR:
                    request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;

                default:
                    throw logic_error(
                            "SetReplicasQservMgtRequest:: " + string(__func__) +
                            "  unhandled server status: " +
                            wpublish::SetChunkListQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}


void SetReplicasQservMgtRequest::finishImpl(util::Lock const& lock) {

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
    _qservRequest = nullptr;
}


void SetReplicasQservMgtRequest::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<SetReplicasQservMgtRequest>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
