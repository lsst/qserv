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
#include "replica/GetConfigQservCzarMgtRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetConfigQservCzarMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<GetConfigQservCzarMgtRequest> GetConfigQservCzarMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        GetConfigQservCzarMgtRequest::CallbackType const& onFinish) {
    return shared_ptr<GetConfigQservCzarMgtRequest>(
            new GetConfigQservCzarMgtRequest(serviceProvider, czarName, onFinish));
}

GetConfigQservCzarMgtRequest::GetConfigQservCzarMgtRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& czarName,
        GetConfigQservCzarMgtRequest::CallbackType const& onFinish)
        : QservCzarMgtRequest(serviceProvider, "QSERV_CZAR_GET_CONFIG", czarName), _onFinish(onFinish) {}

void GetConfigQservCzarMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const service = "/config";
    createHttpReq(lock, service);
}

void GetConfigQservCzarMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetConfigQservCzarMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
