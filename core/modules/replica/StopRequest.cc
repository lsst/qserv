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
#include "replica/StopRequest.h"

namespace lsst {
namespace qserv {
namespace replica {

// -------------------------------------------------
// --------- StopReplicationRequestPolicy ----------
// -------------------------------------------------

char const* StopReplicationRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_CREATE";
}

proto::ReplicationReplicaRequestType StopReplicationRequestPolicy::replicaRequestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_CREATE;
}

void StopReplicationRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                       ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}

void StopReplicationRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                              TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// --------------------------------------------
// --------- StopDeleteRequestPolicy ----------
// --------------------------------------------

char const* StopDeleteRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_DELETE";
}

proto::ReplicationReplicaRequestType StopDeleteRequestPolicy::replicaRequestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_DELETE;
}

void StopDeleteRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                  ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}

void StopDeleteRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                         TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ------------------------------------------
// --------- StopFindRequestPolicy ----------
// ------------------------------------------

char const* StopFindRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_FIND";
}

proto::ReplicationReplicaRequestType StopFindRequestPolicy::replicaRequestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_FIND;
}

void StopFindRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                ResponseDataType& data) {

    data = ResponseDataType(&(msg.replica_info()));
}

void StopFindRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                       TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ---------------------------------------------
// --------- StopFindAllRequestPolicy ----------
// ---------------------------------------------

char const* StopFindAllRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_FIND_ALL";
}

proto::ReplicationReplicaRequestType StopFindAllRequestPolicy::replicaRequestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_FIND_ALL;
}

void StopFindAllRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                   ResponseDataType& data) {

    for (int num = msg.replica_info_many_size(), idx = 0; idx < num; ++idx) {
        data.emplace_back(&(msg.replica_info_many(idx)));
    }
}

void StopFindAllRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                          TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ------------------------------------------
// --------- StopEchoRequestPolicy ----------
// ------------------------------------------

char const* StopEchoRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_ECHO";
}

proto::ReplicationReplicaRequestType StopEchoRequestPolicy::replicaRequestType() {
    return proto::ReplicationReplicaRequestType::REPLICA_ECHO;
}

void StopEchoRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                ResponseDataType& data) {
    data = msg.data();
}

void StopEchoRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                       TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

}}} // namespace lsst::qserv::replica
