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
#include "replica/GetReplicasQservMgtRequest.h"

// System headers
#include <set>
#include <stdexcept>

// Qserv headers
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetReplicasQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

GetReplicasQservMgtRequest::Ptr GetReplicasQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, string const& databaseFamily,
        bool inUseOnly, GetReplicasQservMgtRequest::CallbackType const& onFinish) {
    return GetReplicasQservMgtRequest::Ptr(
            new GetReplicasQservMgtRequest(serviceProvider, worker, databaseFamily, inUseOnly, onFinish));
}

GetReplicasQservMgtRequest::GetReplicasQservMgtRequest(
        ServiceProvider::Ptr const& serviceProvider, string const& worker, string const& databaseFamily,
        bool inUseOnly, GetReplicasQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_GET_REPLICAS", worker),
          _databaseFamily(databaseFamily),
          _inUseOnly(inUseOnly),
          _onFinish(onFinish) {}

QservReplicaCollection const& GetReplicasQservMgtRequest::replicas() const {
    if (!((state() == State::FINISHED) && (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error("GetReplicasQservMgtRequest::" + string(__func__) +
                          "  replicas aren't available in state: " + state2string(state(), extendedState()));
    }
    return _replicas;
}

list<pair<string, string>> GetReplicasQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database_family", _databaseFamily);
    result.emplace_back("in_use_only", replica::bool2str(_inUseOnly));
    return result;
}

void GetReplicasQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const service = "/replicas";
    string query = "?in_use_only=" + string(_inUseOnly ? "1" : "0") + "&databases=";
    for (auto&& database : serviceProvider()->config()->databases(_databaseFamily)) {
        query += database + ",";
    }
    createHttpReq(lock, service, query);
}

QservMgtRequest::ExtendedState GetReplicasQservMgtRequest::dataReady(replica::Lock const& lock,
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

void GetReplicasQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetReplicasQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
