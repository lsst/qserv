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
#include "replica/qserv/RemoveReplicaQservMgtRequest.h"

// Qserv headers
#include "http/Method.h"
#include "replica/util/Common.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.RemoveReplicaQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

RemoveReplicaQservMgtRequest::Ptr RemoveReplicaQservMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName, unsigned int chunk,
        vector<string> const& databases, bool force,
        RemoveReplicaQservMgtRequest::CallbackType const& onFinish) {
    return RemoveReplicaQservMgtRequest::Ptr(
            new RemoveReplicaQservMgtRequest(serviceProvider, workerName, chunk, databases, force, onFinish));
}

RemoveReplicaQservMgtRequest::RemoveReplicaQservMgtRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& workerName, unsigned int chunk,
        vector<string> const& databases, bool force,
        RemoveReplicaQservMgtRequest::CallbackType const& onFinish)
        : QservWorkerMgtRequest(serviceProvider, "QSERV_REMOVE_REPLICA", workerName),
          _chunk(chunk),
          _databases(databases),
          _force(force),
          _onFinish(onFinish) {}

list<pair<string, string>> RemoveReplicaQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    for (auto&& database : databases()) {
        result.emplace_back("database", database);
    }
    result.emplace_back("chunk", to_string(chunk()));
    result.emplace_back("force", replica::bool2str(force()));
    return result;
}

void RemoveReplicaQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const target = "/replica";
    json const data = json::object({{"chunk", _chunk}, {"databases", _databases}, {"force", _force ? 1 : 0}});
    createHttpReq(lock, http::Method::DELETE, target, data);
}

void RemoveReplicaQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<RemoveReplicaQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
