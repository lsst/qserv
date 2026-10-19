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
#ifndef LSST_QSERV_REPLICA_SERVICESTATEINFO_H
#define LSST_QSERV_REPLICA_SERVICESTATEINFO_H

// System headers
#include <cstddef>
#include <iosfwd>
#include <string>
#include <vector>

// Qserv headers
#include "replica/proto/Protocol.h"

// Third party headers
#include "nlohmann/json.hpp"

// Forward declarations
namespace lsst::qserv::replica {
class ProtocolServiceResponse;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Structure ServiceStateInfo encapsulates various parameters representing the state
 * of the remote request processing service. The parameters are available
 * upon the completion of the service request.
 */
struct ServiceStateInfo {
    protocol::ServiceState state;

    /// When the service started (milliseconds since UNIX Epoch)
    std::uint64_t startTime;

    // Counters for requests known to the service since its last start

    std::size_t numNewRequests;
    std::size_t numInProgressRequests;
    std::size_t numFinishedRequests;

    // Request information may not always be available
    struct QueuedRequestInfo {
        protocol::QueuedRequestType type;
        std::string id;
        int priority;
    };
    std::vector<QueuedRequestInfo> newRequests;
    std::vector<QueuedRequestInfo> inProgressRequests;
    std::vector<QueuedRequestInfo> finishedRequests;

    /// Set parameter values from a protobuf object
    void set(ProtocolServiceResponse const& message);

    // Set parameter values from a JSON object
    void set(nlohmann::json const& j);
};

/// Overloaded streaming operator for type ServiceStateInfo
std::ostream& operator<<(std::ostream& os, const ServiceStateInfo& ss);

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_SERVICESTATEINFO_H
