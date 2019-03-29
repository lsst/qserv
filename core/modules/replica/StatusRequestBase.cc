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
#include "replica/StatusRequestBase.h"

// System headers
#include <stdexcept>

// Third party headers
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/ProtocolBuffer.h"
#include "replica/ServiceProvider.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.StatusRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

StatusRequestBase::StatusRequestBase(ServiceProvider::Ptr const& serviceProvider,
                                     boost::asio::io_service& io_service,
                                     char const* requestTypeName,
                                     string const& worker,
                                     string const& targetRequestId,
                                     ProtocolQueuedRequestType targetRequestType,
                                     bool keepTracking,
                                     shared_ptr<Messenger> const& messenger)
    :   RequestMessenger(serviceProvider,
                         io_service,
                         requestTypeName,
                         worker,
                         0,    /* priority */
                         keepTracking,
                         false /* allowDuplicate */,
                         messenger),
        _targetRequestId(targetRequestId),
        _targetRequestType(targetRequestType) {
}


void StatusRequestBase::startImpl(util::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    _sendImpl(lock);
}


void StatusRequestBase::_wait(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Always need to set the interval before launching the timer.

    timer().expires_from_now(boost::posix_time::seconds(timerIvalSec()));
    timer().async_wait(
        boost::bind(
            &StatusRequestBase::_awaken,
            shared_from_base<StatusRequestBase>(),
            boost::asio::placeholders::error
        )
    );
}


void StatusRequestBase::_awaken(boost::system::error_code const& ec) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (isAborted(ec)) return;

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    _sendImpl(lock);
}


void StatusRequestBase::_sendImpl(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    // Serialize the Status message header and the request itself into
    // the network buffer.

    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_STATUS);

    buffer()->serialize(hdr);

    ProtocolRequestStatus message;
    message.set_id(_targetRequestId);
    message.set_queued_type(_targetRequestType);

    buffer()->serialize(message);

    send(lock);
}


void StatusRequestBase::analyze(bool success,
                                ProtocolStatus status) {

    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.

    // IMPORTANT: the final state is required to be tested twice. The first time
    // it's done in order to avoid deadlock on the "in-flight" callbacks reporting
    // their completion while the request termination is in a progress. And the second
    // test is made after acquiring the lock to recheck the state in case if it
    // has transitioned while acquiring the lock.

    if (state() == State::FINISHED) return;

    util::Lock lock(_mtx, context() + __func__);

    if (state() == State::FINISHED) return;

    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    switch (status) {

        case ProtocolStatus::SUCCESS:

            saveReplicaInfo();

            finish(lock, SUCCESS);
            break;

        case ProtocolStatus::QUEUED:
            if (keepTracking()) _wait(lock);
            else                finish(lock, SERVER_QUEUED);
            break;

        case ProtocolStatus::IN_PROGRESS:
            if (keepTracking()) _wait(lock);
            else                finish(lock, SERVER_IN_PROGRESS);
            break;

        case ProtocolStatus::IS_CANCELLING:
            if (keepTracking()) _wait(lock);
            else                finish(lock, SERVER_IS_CANCELLING);
            break;

        case ProtocolStatus::BAD:
            finish(lock, SERVER_BAD);
            break;

        case ProtocolStatus::FAILED:
            finish(lock, SERVER_ERROR);
            break;

        case ProtocolStatus::CANCELLED:
            finish(lock, SERVER_CANCELLED);
            break;

        default:
            throw logic_error(
                    "StatusRequestBase::" + string(__func__) + "  unknown status '"
                    + ProtocolStatus_Name(status) + "' received from server");
    }
}

}}} // namespace lsst::qserv::replica
