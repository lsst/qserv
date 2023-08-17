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
#include "replica/AddReplicaQservMgtRequest.h"

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AddReplicaQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

AddReplicaQservMgtRequest::Ptr AddReplicaQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, unsigned int chunk,
        vector<string> const& databases, AddReplicaQservMgtRequest::CallbackType const& onFinish) {
    return AddReplicaQservMgtRequest::Ptr(
            new AddReplicaQservMgtRequest(serviceProvider, worker, chunk, databases, onFinish));
}

AddReplicaQservMgtRequest::AddReplicaQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                                                     string const& worker, unsigned int chunk,
                                                     vector<string> const& databases,
                                                     AddReplicaQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_ADD_REPLICA", worker),
          _chunk(chunk),
          _databases(databases),
          _onFinish(onFinish),
          _qservRequest(nullptr) {}

list<pair<string, string>> AddReplicaQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("chunk", to_string(chunk()));
    for (auto&& database : databases()) {
        result.emplace_back("database", database);
    }
    return result;
}

void AddReplicaQservMgtRequest::startImpl(replica::Lock const& lock) {
    auto const request = shared_from_base<AddReplicaQservMgtRequest>();

    _qservRequest = xrdreq::AddChunkGroupQservRequest::create(
            chunk(), databases(), [request](proto::WorkerCommandStatus::Code code, string const& error) {
                if (request->state() == State::FINISHED) return;
                replica::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");
                if (request->state() == State::FINISHED) return;

                switch (code) {
                    case proto::WorkerCommandStatus::SUCCESS:
                        request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                        break;
                    case proto::WorkerCommandStatus::INVALID:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_BAD, error);
                        break;
                    case proto::WorkerCommandStatus::IN_USE:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_CHUNK_IN_USE, error);
                        break;
                    case proto::WorkerCommandStatus::ERROR:
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                        break;
                    default:
                        throw logic_error("AddReplicaQservMgtRequest::" + string(__func__) +
                                          "  unhandled request completion code: " +
                                          proto::WorkerCommandStatus_Code_Name(code));
                }
            });
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void AddReplicaQservMgtRequest::finishImpl(replica::Lock const& lock) {
    switch (extendedState()) {
        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:
            if (_qservRequest) _qservRequest->cancel();
            break;
        default:
            break;
    }
}

void AddReplicaQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    notifyDefaultImpl<AddReplicaQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
