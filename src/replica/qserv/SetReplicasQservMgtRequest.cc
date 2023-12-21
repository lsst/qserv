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
#include "replica/qserv/SetReplicasQservMgtRequest.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "http/Method.h"
#include "replica/config/Configuration.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SetReplicasQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

SetReplicasQservMgtRequest::Ptr SetReplicasQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& workerName,
        QservReplicaCollection const& newReplicas, vector<string> const& databases, bool force,
        SetReplicasQservMgtRequest::CallbackType const& onFinish) {
    return SetReplicasQservMgtRequest::Ptr(new SetReplicasQservMgtRequest(
            serviceProvider, workerName, newReplicas, databases, force, onFinish));
}

SetReplicasQservMgtRequest::SetReplicasQservMgtRequest(
        ServiceProvider::Ptr const& serviceProvider, string const& workerName,
        QservReplicaCollection const& newReplicas, vector<string> const& databases, bool force,
        SetReplicasQservMgtRequest::CallbackType const& onFinish)
        : QservWorkerMgtRequest(serviceProvider, "QSERV_SET_REPLICAS", workerName),
          _newReplicas(newReplicas),
          _databases(databases),
          _force(force),
          _onFinish(onFinish) {}

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
    result.emplace_back("databases", util::String::toString(_databases));
    result.emplace_back("force", replica::bool2str(force()));
    return result;
}

void SetReplicasQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    // Leave replicas that belong to the requested databases only.
    set<string> databaseFilter;
    for (string const& database : _databases) {
        databaseFilter.insert(database);
    }
    json replicas = json::object();
    for (auto&& replica : _newReplicas) {
        if (databaseFilter.contains(replica.database)) {
            replicas[replica.database].push_back(replica.chunk);
        }
    }
    string const target = "/replicas";
    json const data =
            json::object({{"replicas", replicas}, {"force", _force ? 1 : 0}, {"databases", _databases}});
    createHttpReq(lock, http::Method::POST, target, data);
}
QservMgtRequest::ExtendedState SetReplicasQservMgtRequest::dataReady(replica::Lock const& lock,
                                                                     json const& data) {
    _replicas.clear();
    for (auto&& [database, chunks] : data.at("replicas").items()) {
        for (auto&& chunkEntry : chunks) {
            unsigned int const chunk = chunkEntry.at(0);
            unsigned int const useCount = chunkEntry.at(1);
            _replicas.emplace_back(QservReplica{chunk, database, useCount});
        }
    }
    return QservMgtRequest::ExtendedState::SUCCESS;
}

void SetReplicasQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<SetReplicasQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
