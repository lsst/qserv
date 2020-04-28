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
#include "replica/WorkerEchoRequest.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "replica/Performance.h"
#include "util/BlockPost.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerEchoRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerEchoRequest::Ptr WorkerEchoRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestEcho const& request) {
    return WorkerEchoRequest::Ptr(new WorkerEchoRequest(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
     ));
}


WorkerEchoRequest::WorkerEchoRequest(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestEcho const& request)
    :   WorkerRequest(
            serviceProvider,
            worker,
            "TEST_ECHO",
            id,
            priority,
            onExpired,
            requestExpirationIvalSec),
        _request(request),
        _delayLeft(request.delay()) {
}


void WorkerEchoRequest::setInfo(ProtocolResponseEcho& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());
    response.set_data(data());

    *(response.mutable_request()) = _request;
}


bool WorkerEchoRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  delay:" << delay() << " _delayLeft:" << _delayLeft);

    util::Lock lock(_mtx, context(__func__));

    switch (status()) {

        case ProtocolStatus::IN_PROGRESS:
            break;

        case ProtocolStatus::IS_CANCELLING:

            // Abort the operation right away

            setStatus(lock, ProtocolStatus::CANCELLED);
            throw WorkerRequestCancelled();

        default:
            throw logic_error(
                    context(__func__) + "  not allowed while in state: " +
                    WorkerRequest::status2string(status()));
    }

    // Block the thread for the random number of milliseconds in the interval
    // below. Then update the amount of time which is still left.

    util::BlockPost blockPost(1000, 2000);

    uint64_t const span = blockPost.wait();
    _delayLeft -= (span < _delayLeft) ? span : _delayLeft;

    // Done if have reached or exceeded the initial delay
    if (0 == _delayLeft) {
        setStatus(lock, ProtocolStatus::SUCCESS);
        return true;
    }
    return false;
}

}}} // namespace lsst::qserv::replica
