// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_REPLICA_REQUEST_TYPES_FWD_H
#define LSST_QSERV_REPLICA_REQUEST_TYPES_FWD_H

/// RequestTypesFwd.h declares:
///
/// Forward declarations for smart pointers and calback functions
/// corresponding to specific requests.

// System headers
#include <functional>
#include <memory>

// Qserv headers
#include "replica/Common.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

////////////////////////////////////////////
// Replica creation and deletion requests //
////////////////////////////////////////////

class ReplicationRequestM;
class DeleteRequestM;

using ReplicationRequest = ReplicationRequestM;
using DeleteRequest      = DeleteRequestM;

typedef std::shared_ptr<ReplicationRequest> ReplicationRequest_pointer;
typedef std::shared_ptr<DeleteRequest>      DeleteRequest_pointer;

typedef std::function<void(ReplicationRequest_pointer)> ReplicationRequest_callback_type;
typedef std::function<void(DeleteRequest_pointer)>      DeleteRequest_callback_type;

/////////////////////////////
// Replica lookup requests //
/////////////////////////////

class FindRequestM;
class FindAllRequestM;

using FindRequest    = FindRequestM;
using FindAllRequest = FindAllRequestM;

typedef std::shared_ptr<FindRequest>    FindRequest_pointer;
typedef std::shared_ptr<FindAllRequest> FindAllRequest_pointer;

typedef std::function<void(FindRequest_pointer)>    FindRequest_callback_type;
typedef std::function<void(FindAllRequest_pointer)> FindAllRequest_callback_type;

////////////////////////////////////
// Replication request managememt //
////////////////////////////////////

class StopReplicationRequestPolicy;
class StopDeleteRequestPolicy;
class StopFindRequestPolicy;
class StopFindAllRequestPolicy;

template <typename POLICY> class StopRequestM;

using StopReplicationRequest = StopRequestM<StopReplicationRequestPolicy>;
using StopDeleteRequest      = StopRequestM<StopDeleteRequestPolicy>;
using StopFindRequest        = StopRequestM<StopFindRequestPolicy>;
using StopFindAllRequest     = StopRequestM<StopFindAllRequestPolicy>;

typedef std::shared_ptr<StopReplicationRequest> StopReplicationRequest_pointer;
typedef std::shared_ptr<StopDeleteRequest>      StopDeleteRequest_pointer;
typedef std::shared_ptr<StopFindRequest>        StopFindRequest_pointer;
typedef std::shared_ptr<StopFindAllRequest>     StopFindAllRequest_pointer;

typedef std::function<void(StopReplicationRequest_pointer)> StopReplicationRequest_callback_type;
typedef std::function<void(StopDeleteRequest_pointer)>      StopDeleteRequest_callback_type;
typedef std::function<void(StopFindRequest_pointer)>        StopFindRequest_callback_type;
typedef std::function<void(StopFindAllRequest_pointer)>     StopFindAllRequest_callback_type;

////////////////////////////////////

class StatusReplicationRequestPolicy;
class StatusDeleteRequestPolicy;
class StatusFindRequestPolicy;
class StatusFindAllRequestPolicy;

template <typename POLICY> class StatusRequestM;

using StatusReplicationRequest = StatusRequestM<StatusReplicationRequestPolicy>;
using StatusDeleteRequest      = StatusRequestM<StatusDeleteRequestPolicy>;
using StatusFindRequest        = StatusRequestM<StatusFindRequestPolicy>;
using StatusFindAllRequest     = StatusRequestM<StatusFindAllRequestPolicy>;

typedef std::shared_ptr<StatusReplicationRequest> StatusReplicationRequest_pointer;
typedef std::shared_ptr<StatusDeleteRequest>      StatusDeleteRequest_pointer;
typedef std::shared_ptr<StatusFindRequest>        StatusFindRequest_pointer;
typedef std::shared_ptr <StatusFindAllRequest>     StatusFindAllRequest_pointer;

typedef std::function<void(StatusReplicationRequest_pointer)> StatusReplicationRequest_callback_type;
typedef std::function<void(StatusDeleteRequest_pointer)>      StatusDeleteRequest_callback_type;
typedef std::function<void(StatusFindRequest_pointer)>        StatusFindRequest_callback_type;
typedef std::function<void(StatusFindAllRequest_pointer)>     StatusFindAllRequest_callback_type;


////////////////////////////////////////
// Worker service management requests //
////////////////////////////////////////

class ServiceSuspendRequestPolicy;
class ServiceResumeRequestPolicy;
class ServiceStatusRequestPolicy;
class ServiceRequestsRequestPolicy;
class ServiceDrainRequestPolicy;

template <typename POLICY> class ServiceManagementRequestM;

using ServiceSuspendRequest  = ServiceManagementRequestM<ServiceSuspendRequestPolicy>;
using ServiceResumeRequest   = ServiceManagementRequestM<ServiceResumeRequestPolicy>;
using ServiceStatusRequest   = ServiceManagementRequestM<ServiceStatusRequestPolicy>;
using ServiceRequestsRequest = ServiceManagementRequestM<ServiceRequestsRequestPolicy>;
using ServiceDrainRequest    = ServiceManagementRequestM<ServiceDrainRequestPolicy>;

typedef std::shared_ptr<ServiceSuspendRequest>  ServiceSuspendRequest_pointer;
typedef std::shared_ptr<ServiceResumeRequest>   ServiceResumeRequest_pointer;
typedef std::shared_ptr<ServiceStatusRequest>   ServiceStatusRequest_pointer;
typedef std::shared_ptr<ServiceRequestsRequest> ServiceRequestsRequest_pointer;
typedef std::shared_ptr<ServiceDrainRequest>    ServiceDrainRequest_pointer;

typedef std::function<void(ServiceSuspendRequest_pointer)>  ServiceSuspendRequest_callback_type;
typedef std::function<void(ServiceResumeRequest_pointer)>   ServiceResumeRequest_callback_type;
typedef std::function<void(ServiceStatusRequest_pointer)>   ServiceStatusRequest_callback_type;
typedef std::function<void(ServiceRequestsRequest_pointer)> ServiceRequestsRequest_callback_type;
typedef std::function<void(ServiceDrainRequest_pointer)>    ServiceDrainRequest_callback_type;

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_REQUEST_TYPES_FWD_H
