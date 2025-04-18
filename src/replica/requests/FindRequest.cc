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
#include "replica/requests/FindRequest.h"

// System headers
#include <functional>
#include <stdexcept>

// Third party headers
#include "boost/date_time/posix_time/posix_time.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/contr/Controller.h"
#include "replica/requests/Messenger.h"
#include "replica/services/DatabaseServices.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/ProtocolBuffer.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::placeholders;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.FindRequest");
bool const allowDuplicateNo = false;
bool const disposeRequired = true;
}  // namespace

namespace lsst::qserv::replica {

FindRequest::Ptr FindRequest::createAndStart(shared_ptr<Controller> const& controller,
                                             string const& workerName, string const& database,
                                             unsigned int chunk, CallbackType const& onFinish, int priority,
                                             bool computeCheckSum, bool keepTracking, string const& jobId,
                                             unsigned int requestExpirationIvalSec) {
    auto ptr = FindRequest::Ptr(new FindRequest(controller, workerName, database, chunk, onFinish, priority,
                                                computeCheckSum, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

FindRequest::FindRequest(shared_ptr<Controller> const& controller, string const& workerName,
                         string const& database, unsigned int chunk, CallbackType const& onFinish,
                         int priority, bool computeCheckSum, bool keepTracking)
        : RequestMessenger(controller, "REPLICA_FIND", workerName, priority, keepTracking, ::allowDuplicateNo,
                           ::disposeRequired),
          _database(database),
          _chunk(chunk),
          _computeCheckSum(computeCheckSum),
          _onFinish(onFinish) {
    controller->serviceProvider()->config()->assertDatabaseIsValid(database);
}

ReplicaInfo const& FindRequest::responseData() const { return _replicaInfo; }

void FindRequest::startImpl(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context() << __func__ << " "
                   << " worker: " << workerName() << " database: " << database() << " chunk: " << chunk()
                   << " computeCheckSum: " << (computeCheckSum() ? "true" : "false"));

    // Serialize the Request message header and the request itself into
    // the network buffer.
    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::QUEUED);
    hdr.set_queued_type(ProtocolQueuedRequestType::REPLICA_FIND);
    hdr.set_timeout(requestExpirationIvalSec());
    hdr.set_priority(priority());
    hdr.set_instance_id(controller()->serviceProvider()->instanceId());
    buffer()->serialize(hdr);

    ProtocolRequestFind message;
    message.set_database(database());
    message.set_chunk(chunk());
    message.set_compute_cs(computeCheckSum());
    buffer()->serialize(message);

    _send(lock);
}

void FindRequest::awaken(boost::system::error_code const& ec) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);

    if (isAborted(ec)) return;
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;

    // Serialize the Status message header and the request itself into
    // the network buffer.
    buffer()->resize();

    ProtocolRequestHeader hdr;
    hdr.set_id(id());
    hdr.set_type(ProtocolRequestHeader::REQUEST);
    hdr.set_management_type(ProtocolManagementRequestType::REQUEST_TRACK);
    hdr.set_instance_id(controller()->serviceProvider()->instanceId());
    buffer()->serialize(hdr);

    ProtocolRequestTrack message;
    message.set_id(id());
    message.set_queued_type(ProtocolQueuedRequestType::REPLICA_FIND);
    buffer()->serialize(message);

    _send(lock);
}

void FindRequest::_send(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    controller()->serviceProvider()->messenger()->send<ProtocolResponseFind>(
            workerName(), id(), priority(), buffer(),
            [self = shared_from_base<FindRequest>()](string const& id, bool success,
                                                     ProtocolResponseFind const& response) {
                self->_analyze(success, response);
            });
}

void FindRequest::_analyze(bool success, ProtocolResponseFind const& message) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "  success=" << (success ? "true" : "false"));

    // This method is called on behalf of an asynchronous callback fired
    // upon a completion of the request within method send() - the only
    // client of analyze(). So, we should take care of proper locking and watch
    // for possible state transition which might occur while the async I/O was
    // still in a progress.
    if (state() == State::FINISHED) return;
    replica::Lock lock(_mtx, context() + __func__);
    if (state() == State::FINISHED) return;
    if (not success) {
        finish(lock, CLIENT_ERROR);
        return;
    }

    // Always use  the latest status reported by the remote server
    setExtendedServerStatus(lock, message.status_ext());

    // Performance counters are updated from either of two sources,
    // depending on the availability of the 'target' performance counters
    // filled in by the 'STATUS' queries. If the later is not available
    // then fallback to the one of the current request.
    if (message.has_target_performance()) {
        mutablePerformance().update(message.target_performance());
    } else {
        mutablePerformance().update(message.performance());
    }

    // Always extract extended data regardless of the completion status
    // reported by the worker service.
    _replicaInfo = ReplicaInfo(&(message.replica_info()));

    // Extract target request type-specific parameters from the response
    if (message.has_request()) {
        _targetRequestParams = FindRequestParams(message.request());
    }
    switch (message.status()) {
        case ProtocolStatus::SUCCESS:
            controller()->serviceProvider()->databaseServices()->saveReplicaInfo(_replicaInfo);
            finish(lock, SUCCESS);
            break;
        case ProtocolStatus::CREATED:
            keepTrackingOrFinish(lock, SERVER_CREATED);
            break;
        case ProtocolStatus::QUEUED:
            keepTrackingOrFinish(lock, SERVER_QUEUED);
            break;
        case ProtocolStatus::IN_PROGRESS:
            keepTrackingOrFinish(lock, SERVER_IN_PROGRESS);
            break;
        case ProtocolStatus::IS_CANCELLING:
            keepTrackingOrFinish(lock, SERVER_IS_CANCELLING);
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
            throw logic_error("FindRequest::" + string(__func__) + " unknown status '" +
                              ProtocolStatus_Name(message.status()) + "' received from server");
    }
}

void FindRequest::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__);
    notifyDefaultImpl<FindRequest>(lock, _onFinish);
}

void FindRequest::savePersistentState(replica::Lock const& lock) {
    controller()->serviceProvider()->databaseServices()->saveState(*this, performance(lock));
}

list<pair<string, string>> FindRequest::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("chunk", to_string(chunk()));
    return result;
}

}  // namespace lsst::qserv::replica
