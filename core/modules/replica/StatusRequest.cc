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
#include "replica/StatusRequest.h"

namespace lsst {
namespace qserv {
namespace replica {

// ---------------------------------------------------
// --------- StatusReplicationRequestPolicy ----------
// ---------------------------------------------------

char const* StatusReplicationRequestPolicy::requestName() {
    return "REQUEST_STATUS:REPLICA_CREATE";
}

proto::ReplicationReplicaRequestType StatusReplicationRequestPolicy::requestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_CREATE;
}

void StatusReplicationRequestPolicy::extractResponseData(
                                        ResponseMessageType const& msg,
                                        ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}
void StatusReplicationRequestPolicy::extractTargetRequestParams(
                                        ResponseMessageType const& msg,
                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ----------------------------------------------
// --------- StatusDeleteRequestPolicy ----------
// ----------------------------------------------

char const* StatusDeleteRequestPolicy::requestName() {
    return "REQUEST_STATUS:REPLICA_DELETE";
}

proto::ReplicationReplicaRequestType StatusDeleteRequestPolicy::requestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_DELETE;
}

void StatusDeleteRequestPolicy::extractResponseData(
                                        ResponseMessageType const& msg,
                                        ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}

void StatusDeleteRequestPolicy::extractTargetRequestParams(
                                        ResponseMessageType const& msg,
                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// --------------------------------------------
// --------- StatusFindRequestPolicy ----------
// --------------------------------------------

char const* StatusFindRequestPolicy::requestName() {
    return "REQUEST_STATUS:REPLICA_FIND";
}

proto::ReplicationReplicaRequestType StatusFindRequestPolicy::requestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_FIND;
}

void StatusFindRequestPolicy::extractResponseData(
                                        ResponseMessageType const& msg,
                                        ResponseDataType& data) {
    data = ReplicaInfo(&(msg.replica_info()));
}

void StatusFindRequestPolicy::extractTargetRequestParams(
                                        ResponseMessageType const& msg,
                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// -----------------------------------------------
// --------- StatusFindAllRequestPolicy ----------
// -----------------------------------------------

char const* StatusFindAllRequestPolicy::requestName() {
    return "REQUEST_STATUS:REPLICA_FIND_ALL";
}

proto::ReplicationReplicaRequestType StatusFindAllRequestPolicy::requestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL;
}

void StatusFindAllRequestPolicy::extractResponseData(
                                        ResponseMessageType const& msg,
                                        ResponseDataType& data) {

    for (int num = msg.replica_info_many_size(), idx = 0; idx < num; ++idx) {
        data.emplace_back(&(msg.replica_info_many(idx)));
    }
}

void StatusFindAllRequestPolicy::extractTargetRequestParams(
                                        ResponseMessageType const& msg,
                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

}}} // namespace lsst::qserv::replica
