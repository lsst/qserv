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

// System headers
#include <set>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "proto/worker.pb.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetConfigQservMgtRequest");

}  // namespace

namespace lsst::qserv::replica {

GetConfigQservMgtRequest::Ptr GetConfigQservMgtRequest::create(
        ServiceProvider::Ptr const& serviceProvider, string const& worker,
        GetConfigQservMgtRequest::CallbackType const& onFinish) {
    return GetConfigQservMgtRequest::Ptr(new GetConfigQservMgtRequest(serviceProvider, worker, onFinish));
}

GetConfigQservMgtRequest::GetConfigQservMgtRequest(ServiceProvider::Ptr const& serviceProvider,
                                                   string const& worker,
                                                   GetConfigQservMgtRequest::CallbackType const& onFinish)
        : QservMgtRequest(serviceProvider, "QSERV_GET_DATABASE_STATUS", worker), _onFinish(onFinish) {}

json const& GetConfigQservMgtRequest::info() const {
    if (!((state() == State::FINISHED) && (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error("GetConfigQservMgtRequest::" + string(__func__) +
                          "  no info available in state: " + state2string(state(), extendedState()));
    }
    return _info;
}

void GetConfigQservMgtRequest::startImpl(replica::Lock const& lock) {
    auto const request = shared_from_base<GetConfigQservMgtRequest>();
    _qservRequest = xrdreq::GetConfigQservRequest::create([request](proto::WorkerCommandStatus::Code code,
                                                                    string const& error, string const& info) {
        if (request->state() == State::FINISHED) return;
        replica::Lock const lock(request->_mtx, request->context() + string(__func__) + "[callback]");
        if (request->state() == State::FINISHED) return;

        switch (code) {
            case proto::WorkerCommandStatus::SUCCESS:
                try {
                    request->_setInfo(lock, info);
                    request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                } catch (exception const& ex) {
                    string const msg = "failed to parse worker response, ex: " + string(ex.what());
                    LOGS(_log, LOG_LVL_ERROR, "GetConfigQservMgtRequest::" << __func__ << "  " << msg);
                    request->finish(lock, QservMgtRequest::ExtendedState::SERVER_BAD_RESPONSE, msg);
                }
                break;
            case proto::WorkerCommandStatus::ERROR:
                request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                break;
            default:
                throw logic_error("GetConfigQservMgtRequest::" + string(__func__) +
                                  "  unhandled server status: " + proto::WorkerCommandStatus_Code_Name(code));
        }
    });
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}

void GetConfigQservMgtRequest::finishImpl(replica::Lock const& lock) {
    switch (extendedState()) {
        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:
            if (_qservRequest) _qservRequest->cancel();
            break;
        default:
            break;
    }
}

void GetConfigQservMgtRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_TRACE, context() << __func__);
    notifyDefaultImpl<GetConfigQservMgtRequest>(lock, _onFinish);
}

void GetConfigQservMgtRequest::_setInfo(replica::Lock const& lock, string const& info) {
    _info = json::parse(info);
}

}  // namespace lsst::qserv::replica
