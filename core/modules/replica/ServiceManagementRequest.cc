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
#include "replica/ServiceManagementRequest.h"

namespace lsst {
namespace qserv {
namespace replica {

char const* ServiceSuspendRequestPolicy::requestName() {
    return "SERVICE_SUSPEND";
}

proto::ReplicationServiceRequestType ServiceSuspendRequestPolicy::requestType () {
    return proto::ReplicationServiceRequestType::SERVICE_SUSPEND;
}

char const* ServiceResumeRequestPolicy::requestName() {
    return "SERVICE_RESUME";
}

proto::ReplicationServiceRequestType ServiceResumeRequestPolicy::requestType() {
    return proto::ReplicationServiceRequestType::SERVICE_RESUME;
}

char const* ServiceStatusRequestPolicy::requestName() {
    return "SERVICE_STATUS";
}

proto::ReplicationServiceRequestType ServiceStatusRequestPolicy::requestType() {
    return proto::ReplicationServiceRequestType::SERVICE_STATUS;
}

char const* ServiceRequestsRequestPolicy::requestName() {
    return "SERVICE_REQUESTS";
}

proto::ReplicationServiceRequestType ServiceRequestsRequestPolicy::requestType() {
    return proto::ReplicationServiceRequestType::SERVICE_REQUESTS;
}

char const* ServiceDrainRequestPolicy::requestName() {
    return "SERVICE_DRAIN";
}

proto::ReplicationServiceRequestType ServiceDrainRequestPolicy::requestType() {
    return proto::ReplicationServiceRequestType::SERVICE_DRAIN;
}

}}} // namespace lsst::qserv::replica
