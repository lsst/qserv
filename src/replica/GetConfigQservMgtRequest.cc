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
#include "replica/GetConfigQservMgtRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetConfigQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<GetConfigQservMgtRequest> GetConfigQservMgtRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        GetConfigQservMgtRequest::CallbackType const& onFinish) {
    return shared_ptr<GetConfigQservMgtRequest>(
            new GetConfigQservMgtRequest(serviceProvider, worker, onFinish));
}

GetConfigQservMgtRequest::GetConfigQservMgtRequest(shared_ptr<ServiceProvider> const& serviceProvider,
                                                   string const& worker,
                                                   GetConfigQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_GET_DATABASE_STATUS", worker), _onFinish(onFinish) {}

void GetConfigQservMgtRequest::createHttpReqImpl(replica::Lock const& lock) {
    string const service = "/config";
    createHttpReq(lock, service);
}

void GetConfigQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetConfigQservMgtRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
