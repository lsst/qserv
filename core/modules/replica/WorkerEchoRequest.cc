/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#include "lsst/log/Log.h"
#include "replica/Performance.h"
#include "util/BlockPost.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerEchoRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

WorkerEchoRequest::Ptr WorkerEchoRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                 std::string const& worker,
                                                 std::string const& id,
                                                 int priority,
                                                 std::string const& data,
                                                 uint64_t delay) {
    return WorkerEchoRequest::Ptr(
        new WorkerEchoRequest(serviceProvider,
                              worker,
                              id,
                              priority,
                              data,
                              delay));
}

WorkerEchoRequest::WorkerEchoRequest(ServiceProvider::Ptr const& serviceProvider,
                                     std::string const& worker,
                                     std::string const& id,
                                     int priority,
                                     std::string const& data,
                                     uint64_t delay)
    :   WorkerRequest(serviceProvider,
                      worker,
                      "ECHO",
                      id,
                      priority),
        _data(data),
        _delay(delay),
        _delayLeft(delay) {
}

void WorkerEchoRequest::setInfo(proto::ReplicationResponseEcho& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context() << "setInfo");

    util::Lock lock(_mtx, context() + "setInfo");

    // Return the performance of the target request

    response.set_allocated_target_performance(performance().info());
    response.set_data(data());
}

bool WorkerEchoRequest::execute() {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  delay:" << delay() << " _delayLeft:" << _delayLeft);

    util::Lock lock(_mtx, context() + "execute");

    switch (status()) {

        case STATUS_IN_PROGRESS:
            break;

        case STATUS_IS_CANCELLING:

            // Abort the operation right away

            setStatus(lock, STATUS_CANCELLED);
            throw WorkerRequestCancelled();

        default:
            throw std::logic_error(
                        context() + "execute  not allowed while in state: " +
                        WorkerRequest::status2string(status()));
    }

    // Block the thread for the random number of milliseconds in the interval
    // below. Then update the amount of time which is still left.

    util::BlockPost blockPost(1000, 2000);

    uint64_t const span = blockPost.wait();
    _delayLeft -= (span < _delayLeft) ? span : _delayLeft;

    // Done if have reached or exceeded the initial delay
    if (0 == _delayLeft) {
        setStatus(lock, STATUS_SUCCEEDED);
        return true;
    }
    return false;
}

}}} // namespace lsst::qserv::replica
