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
#include "replica/qserv/GetQueryProgressQservCzarMgtRequest.h"

// Qserv headers
#include "util/String.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetQueryProgressQservCzarMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<GetQueryProgressQservCzarMgtRequest> GetQueryProgressQservCzarMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        vector<QueryId> const& queryIds, unsigned int lastSeconds,
        GetQueryProgressQservCzarMgtRequest::CallbackType const& onFinish) {
    return shared_ptr<GetQueryProgressQservCzarMgtRequest>(new GetQueryProgressQservCzarMgtRequest(
            serviceProvider, czarName, queryIds, lastSeconds, onFinish));
}

GetQueryProgressQservCzarMgtRequest::GetQueryProgressQservCzarMgtRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        vector<QueryId> const& queryIds, unsigned int lastSeconds,
        GetQueryProgressQservCzarMgtRequest::CallbackType const& onFinish)
        : QservCzarMgtRequest(serviceProvider, "QSERV_CZAR_GET_QUERY_PROGRESS", czarName),
          _queryIds(queryIds),
          _lastSeconds(lastSeconds),
          _onFinish(onFinish) {}

void GetQueryProgressQservCzarMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const service = "/query-progress";
    string query;
    query += "?query_ids=" + util::String::toString(_queryIds);
    query += "&last_seconds=" + to_string(_lastSeconds);
    createHttpReq(lock, service, query);
}

void GetQueryProgressQservCzarMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetQueryProgressQservCzarMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
