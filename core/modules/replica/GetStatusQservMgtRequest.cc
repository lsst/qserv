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
#include "replica/GetStatusQservMgtRequest.h"

// System headers
#include <set>
#include <stdexcept>

// Third party headers
#include "XrdSsi/XrdSsiProvider.hh"
#include "XrdSsi/XrdSsiService.hh"

// Qserv headers
#include "global/ResourceUnit.h"
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/ServiceProvider.h"

using namespace nlohmann;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.GetStatusQservMgtRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

GetStatusQservMgtRequest::Ptr GetStatusQservMgtRequest::create(
                                        ServiceProvider::Ptr const& serviceProvider,
                                        string const& worker,
                                        GetStatusQservMgtRequest::CallbackType const& onFinish) {
    return GetStatusQservMgtRequest::Ptr(
        new GetStatusQservMgtRequest(serviceProvider,
                                     worker,
                                     onFinish));
}


GetStatusQservMgtRequest::GetStatusQservMgtRequest(
                                ServiceProvider::Ptr const& serviceProvider,
                                string const& worker,
                                GetStatusQservMgtRequest::CallbackType const& onFinish)
    :   QservMgtRequest(serviceProvider,
                        "QSERV_GET_STATUS",
                        worker),
        _onFinish(onFinish) {
}


json const& GetStatusQservMgtRequest::info() const {
    if (not ((state() == State::FINISHED) and (extendedState() == ExtendedState::SUCCESS))) {
        throw logic_error(
                "GetStatusQservMgtRequest::" + string(__func__) + "  no info available in state: " +
                state2string(state(), extendedState()));
    }
    return _info;
}


list<pair<string,string>> GetStatusQservMgtRequest::extendedPersistentState() const {
    list<pair<string,string>> result;
    return result;
}


void GetStatusQservMgtRequest::startImpl(util::Lock const& lock) {

    // Submit the actual request

    auto const request = shared_from_base<GetStatusQservMgtRequest>();

    _qservRequest = wpublish::GetStatusQservRequest::create(
        [request] (wpublish::GetStatusQservRequest::Status status,
                   string const& error,
                   string const& info) {

            // IMPORTANT: the final state is required to be tested twice. The first time
            // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
            // their completion while the request termination is in a progress. And the second
            // test is made after acquiring the lock to recheck the state in case if it
            // has transitioned while acquiring the lock.

            if (request->state() == State::FINISHED) return;
        
            util::Lock lock(request->_mtx, request->context() + string(__func__) + "[callback]");
        
            if (request->state() == State::FINISHED) return;

            switch (status) {

                case wpublish::GetStatusQservRequest::Status::SUCCESS:

                    try {
                        request->_setInfo(lock, info);
                        request->finish(lock, QservMgtRequest::ExtendedState::SUCCESS);
                    } catch(exception const& ex) {
                        string const msg = "failed to parse worker response, ex: " + string(ex.what());
                        LOGS(_log, LOG_LVL_ERROR, "GetStatusQservMgtRequest::" << __func__ << "  " << msg);
                        request->finish(lock, QservMgtRequest::ExtendedState::SERVER_BAD_RESPONSE, msg);
                    }
                    break;

                case wpublish::GetStatusQservRequest::Status::ERROR:

                    request->finish(lock, QservMgtRequest::ExtendedState::SERVER_ERROR, error);
                    break;

                default:
                    throw logic_error(
                            "GetStatusQservMgtRequest::" + string(__func__) +
                            "  unhandled server status: " +
                            wpublish::GetStatusQservRequest::status2str(status));
            }
        }
    );
    XrdSsiResource resource(ResourceUnit::makeWorkerPath(worker()));
    service()->ProcessRequest(*_qservRequest, resource);
}


void GetStatusQservMgtRequest::finishImpl(util::Lock const& lock) {

    switch (extendedState()) {

        case ExtendedState::CANCELLED:
        case ExtendedState::TIMEOUT_EXPIRED:

            // And if the SSI request is still around then tell it to stop

            if (_qservRequest) {
                bool const cancel = true;
                _qservRequest->Finished(cancel);
            }
            break;

        default:
            break;
    }
    _qservRequest = nullptr;
}


void GetStatusQservMgtRequest::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<GetStatusQservMgtRequest>(lock, _onFinish);
}


void GetStatusQservMgtRequest::_setInfo(util::Lock const& lock,
                                        string const& info) {
    _info = json::parse(info);
}

}}} // namespace lsst::qserv::replica
