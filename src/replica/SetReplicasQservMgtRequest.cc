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
#include <sstream>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"
#include "util/IterableFormatter.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SetReplicasQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

SetReplicasQservMgtRequest::Ptr SetReplicasQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker,
        QservReplicaCollection const& newReplicas, vector<string> const& databases, bool force,
        SetReplicasQservMgtRequest::CallbackType const& onFinish) {
    return SetReplicasQservMgtRequest::Ptr(
            new SetReplicasQservMgtRequest(serviceProvider, worker, newReplicas, databases, force, onFinish));
}

SetReplicasQservMgtRequest::SetReplicasQservMgtRequest(
        ServiceProvider::Ptr const& serviceProvider, string const& worker,
        QservReplicaCollection const& newReplicas, vector<string> const& databases, bool force,
        SetReplicasQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_SET_REPLICAS", worker),
          _newReplicas(newReplicas),
          _databases(databases),
          _force(force),
          _onFinish(onFinish),
          _qservRequest(nullptr) {}

QservReplicaCollection const& SetReplicasQservMgtRequest::replicas() const {
    if (not((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error("SetReplicasQservMgtRequest::" + string(__func__) +
                          "  replicas aren't available in state: " + state2string(state(), extendedState()));
    }
    return _replicas;
}

list<pair<string, string>> SetReplicasQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("num_replicas", to_string(newReplicas().size()));
    ostringstream databasesStream;
    databasesStream << util::printable(_databases, "", "", " ");
    result.emplace_back("databases", databasesStream.str());
    result.emplace_back("force", bool2str(force()));
    return result;
}

void SetReplicasQservMgtRequest::_setReplicas(
        replica::Lock const& lock, xrdreq::SetChunkListQservRequest::ChunkCollection const& collection) {
    _replicas.clear();
    for (auto&& replica : collection) {
        _replicas.push_back(QservReplica{replica.chunk, replica.database, replica.use_count});
    }
}

void SetReplicasQservMgtRequest::startImpl(replica::Lock const& lock) {
    xrdreq::SetChunkListQservRequest::ChunkCollection chunks;
    for (auto&& chunkEntry : newReplicas()) {
        chunks.push_back(xrdreq::SetChunkListQservRequest::Chunk{
                chunkEntry.chunk, chunkEntry.database, 0 /* UNUSED: use_count */
        });
    }
    auto const request = shared_from_base<SetReplicasQservMgtRequest>();

    _qservRequest = xrdreq::SetChunkListQservRequest::create(
            chunks, _databases, force(),
            [request](proto::WorkerCommandStatus::Code code, string const& error,
                      xrdreq::SetChunkListQservRequest::ChunkCollection const& collection) {
                if (request->state() == State::FINISHED) return;
                replica::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");
                if (request->state() == State::FINISHED) return;

                switch (code) {
                    case proto::WorkerCommandStatus::SUCCESS:
                        request->_setReplicas(lock, collection);
                        request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                        break;
                    case proto::WorkerCommandStatus::ERROR:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                        break;
                    case proto::WorkerCommandStatus::INVALID:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_BAD, error);
                        break;
                    case proto::WorkerCommandStatus::IN_USE:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_CHUNK_IN_USE, error);
                        break;
                    default:
                        throw logic_error(
                                "SetReplicasQservMgtRequest:: " + string(__func__) +
                                "  unhandled server status: " + proto::WorkerCommandStatus_Code_Name(code));
                }
            });
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void SetReplicasQservMgtRequest::finishImpl(replica::Lock const& lock) {
    switch (extendedState()) {
        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:
            if (_qservRequest) _qservRequest->cancel();
            break;
        default:
            break;
    }
}

void SetReplicasQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<SetReplicasQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
