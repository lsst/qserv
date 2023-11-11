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

// Qserv headers
#include "http/Method.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.AddReplicaQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

AddReplicaQservMgtRequest::Ptr AddReplicaQservMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker, unsigned int chunk,
        vector<string> const& databases, AddReplicaQservMgtRequest::CallbackType const& onFinish) {
    return AddReplicaQservMgtRequest::Ptr(
            new AddReplicaQservMgtRequest(serviceProvider, worker, chunk, databases, onFinish));
}

AddReplicaQservMgtRequest::AddReplicaQservMgtRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                     string const& worker, unsigned int chunk,
                                                     vector<string> const& databases,
                                                     AddReplicaQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_ADD_REPLICA", worker),
          _chunk(chunk),
          _databases(databases),
          _onFinish(onFinish) {}

list<pair<string, string>> AddReplicaQservMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("chunk", to_string(chunk()));
    for (auto&& database : databases()) {
        result.emplace_back("database", database);
    }
    return result;
}

void AddReplicaQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const target = "/replica";
    json const data = json::object({{"chunk", _chunk}, {"databases", _databases}});
    createHttpReq(lock, http::Method::POST, target, data);
}

void AddReplicaQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<AddReplicaQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
