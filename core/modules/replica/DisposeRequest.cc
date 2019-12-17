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
#include "replica/DisposeRequest.h"

// System headers
#include <functional>
#include <sstream>

// Qserv headers
#include "replica/Messenger.h"
#include "replica/ProtocolBuffer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DisposeRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

DisposeRequestResult::DisposeRequestResult(ProtocolResponseDispose const& message) {
    for (int idx = 0; idx < message.ids_size(); ++idx) {
        auto&& stat = message.ids(idx);
        ids.emplace_back(Status{stat.id(), stat.disposed()});
    }
}


string DisposeRequest::toString(bool extended) const {
    ostringstream oss;
    oss << Request::toString(extended);
    if (extended) {
        oss << "  Disposed requests:\n";
        for (auto&& entry: responseData().ids) {
            if (entry.disposed) {
                oss << "    " << entry.id << "\n";
            }
        }
    }
    return oss.str();
}


DisposeRequest::Ptr DisposeRequest::create(ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::vector<std::string> const& targetIds,
        CallbackType const& onFinish,
        int  priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger) {
    return DisposeRequest::Ptr(new DisposeRequest(serviceProvider,
        io_service,
        worker,
        targetIds,
        onFinish,
        priority,
        keepTracking,
        messenger
    ));
}


DisposeRequest::DisposeRequest(ServiceProvider::Ptr const& serviceProvider,
        boost::asio::io_service& io_service,
        string const& worker,
        std::vector<std::string> const& targetIds,
        CallbackType const& onFinish,
        int  priority,
        bool keepTracking,
        shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         "DISPOSE",
                         worker,
                         priority,
                         keepTracking,
                         false, // allowDuplicate
                         false, // disposeRequired
                         messenger),
        _targetIds(targetIds),
        _onFinish(onFinish) {
}


DisposeRequestResult const& DisposeRequest::responseData() const {
    return _responseData;
}


void DisposeRequest::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__
         << "  worker: " << worker() << " targetIds.size: " << targetIds().size());

    // Serialize the Request message header and the request itself into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_DISPOSE);

    buffer()->serialize(hdr);

    ProtocolRequestDispose message;
    for (auto&& id: targetIds()) {
        message.add_ids(id);
    }
    buffer()->serialize(message);

    _send(lock);
}


void DisposeRequest::_send(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    messenger()->send<ProtocolResponseDispose>(
        worker(),
        id(),
        buffer(),
        // Don't forward the first parameter (request's identifier) of the callback
        // to the response's analyzer. A value of the identifier is already known
        // in a context of the method.
        bind(&DisposeRequest::_analyze, shared_from_base<DisposeRequest>(), _2, _3)
    );
}


void DisposeRequest::_analyze(bool success,
                              ProtocolResponseDispose const& message) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    // This type of request (if delivered to a worker and if a response from
    // the worker is received) is always considered as "successful".

    if (success) {
        _responseData = DisposeRequestResult(message);
    }
    finish(lock, success ? SUCCESS : CLIENT_ERROR);
}


void DisposeRequest::notify(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<DisposeRequest>(lock, _onFinish);
}

}}} // namespace lsst::qserv::replica
