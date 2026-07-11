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
#include "replica/qserv/PostEventQservCzarMgtRequest.h"

// Qserv headers
#include "http/Method.h"
#include "replica/util/Common.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.PostEventQservCzarMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

PostEventQservCzarMgtRequest::Ptr PostEventQservCzarMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        cconfig::DataManagementEvent const& event,
        PostEventQservCzarMgtRequest::CallbackType const& onFinish) {
    return PostEventQservCzarMgtRequest::Ptr(
            new PostEventQservCzarMgtRequest(serviceProvider, czarName, event, onFinish));
}

PostEventQservCzarMgtRequest::PostEventQservCzarMgtRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        cconfig::DataManagementEvent const& event, PostEventQservCzarMgtRequest::CallbackType const& onFinish)
        : QservCzarMgtRequest(serviceProvider, "QSERV_CZAR_POST_EVENT", czarName),
          _event(event),
          _onFinish(onFinish) {}

list<pair<string, string>> PostEventQservCzarMgtRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("czar", czarName());
    return result;
}

void PostEventQservCzarMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const target = "/event";
    json const data = json::object({{"czar", czarName()}, {"event", _event.toJson()}});
    createHttpReq(lock, http::Method::POST, target, data);
}

void PostEventQservCzarMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<PostEventQservCzarMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
