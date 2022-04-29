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
#include "replica/ServiceManagementRequest.h"

using namespace std;

namespace lsst { namespace qserv { namespace replica {

char const* ServiceSuspendRequestPolicy::requestName() { return "SERVICE_SUSPEND"; }

ProtocolServiceRequestType ServiceSuspendRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_SUSPEND;
}

char const* ServiceResumeRequestPolicy::requestName() { return "SERVICE_RESUME"; }

ProtocolServiceRequestType ServiceResumeRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_RESUME;
}

char const* ServiceStatusRequestPolicy::requestName() { return "SERVICE_STATUS"; }

ProtocolServiceRequestType ServiceStatusRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_STATUS;
}

char const* ServiceRequestsRequestPolicy::requestName() { return "SERVICE_REQUESTS"; }

ProtocolServiceRequestType ServiceRequestsRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_REQUESTS;
}

char const* ServiceDrainRequestPolicy::requestName() { return "SERVICE_DRAIN"; }

ProtocolServiceRequestType ServiceDrainRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_DRAIN;
}

char const* ServiceReconfigRequestPolicy::requestName() { return "SERVICE_RECONFIG"; }

ProtocolServiceRequestType ServiceReconfigRequestPolicy::requestType() {
    return ProtocolServiceRequestType::SERVICE_RECONFIG;
}

}}}  // namespace lsst::qserv::replica
