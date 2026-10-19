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
#include "replica/util/ServiceStateInfo.h"

// System headers
#include <ostream>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "util/TimeUtils.h"

using namespace std;
using namespace nlohmann;

namespace {

using namespace lsst::qserv::replica;

void dumpRequestInfo(ostream& os, vector<ServiceStateInfo::QueuedRequestInfo> const& requests) {
    for (auto const& r : requests) {
        os << "\n"
           << "    type:     " << protocol::toString(r.type) << "\n"
           << "    id:       " << r.id << "\n"
           << "    priority: " << r.priority << "\n";
    }
}

}  // namespace

namespace lsst::qserv::replica {

void ServiceStateInfo::set(ProtocolServiceResponse const& message) {
    state = protocol::ServiceState(message.service_state());
    startTime = message.start_time();
    numNewRequests = message.num_new_requests();
    numInProgressRequests = message.num_in_progress_requests();
    numFinishedRequests = message.num_finished_requests();
    newRequests.clear();
    for (auto const& request : message.new_requests()) {
        newRequests.emplace_back(protocol::toQueuedRequestType(request.queued_type()), request.id(),
                                 request.priority());
    }
    inProgressRequests.clear();
    for (auto const& request : message.in_progress_requests()) {
        inProgressRequests.emplace_back(protocol::toQueuedRequestType(request.queued_type()), request.id(),
                                        request.priority());
    }
    finishedRequests.clear();
    for (auto const& request : message.finished_requests()) {
        finishedRequests.emplace_back(protocol::toQueuedRequestType(request.queued_type()), request.id(),
                                      request.priority());
    }
}

void ServiceStateInfo::set(json const& info) {
    state = protocol::toServiceState(info.at("service_state").get<string>());
    startTime = info.at("start_time").get<uint64_t>();
    numNewRequests = info.at("num_new_requests").get<size_t>();
    numInProgressRequests = info.at("num_in_progress_requests").get<size_t>();
    numFinishedRequests = info.at("num_finished_requests").get<size_t>();

    newRequests.clear();
    for (auto const& request : info.at("new_requests")) {
        newRequests.emplace_back(protocol::toQueuedRequestType(request.at("queued_type").get<string>()),
                                 request.at("id").get<string>(), request.at("priority").get<int>());
    }

    inProgressRequests.clear();
    for (auto const& request : info.at("in_progress_requests")) {
        inProgressRequests.emplace_back(
                protocol::toQueuedRequestType(request.at("queued_type").get<string>()),
                request.at("id").get<string>(), request.at("priority").get<int>());
    }

    finishedRequests.clear();
    for (auto const& request : info.at("finished_requests")) {
        finishedRequests.emplace_back(protocol::toQueuedRequestType(request.at("queued_type").get<string>()),
                                      request.at("id").get<string>(), request.at("priority").get<int>());
    }
}

ostream& operator<<(ostream& os, ServiceStateInfo const& ss) {
    unsigned int const secondsAgo = (util::TimeUtils::now() - ss.startTime) / 1000.0f;

    os << "ServiceState:\n"
       << "\n  Summary:\n\n"
       << "    service state:              " << protocol::toString(ss.state) << "\n"
       << "    start time [ms]:            " << ss.startTime << " (" << secondsAgo << " seconds ago)\n"
       << "    total new requests:         " << ss.numNewRequests << "\n"
       << "    total in-progress requests: " << ss.numInProgressRequests << "\n"
       << "    total finished requests:    " << ss.numFinishedRequests << "\n";

    os << "\n  New:\n";
    ::dumpRequestInfo(os, ss.newRequests);

    os << "\n  In-Progress:\n";
    ::dumpRequestInfo(os, ss.inProgressRequests);

    os << "\n  Finished:\n";
    ::dumpRequestInfo(os, ss.finishedRequests);

    return os;
}

}  // namespace lsst::qserv::replica
