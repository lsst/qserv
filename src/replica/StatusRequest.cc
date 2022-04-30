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
#include "replica/StatusRequest.h"

// Qserv headers
#include "replica/IndexRequest.h"

using namespace std;

namespace lsst::qserv::replica {

// ---------------------------------------------------
// --------- StatusReplicationRequestPolicy ----------
// ---------------------------------------------------

char const* StatusReplicationRequestPolicy::requestName() { return "REQUEST_STATUS:REPLICA_CREATE"; }

ProtocolQueuedRequestType StatusReplicationRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_CREATE;
}

void StatusReplicationRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                         ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}

void StatusReplicationRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                                TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ----------------------------------------------
// --------- StatusDeleteRequestPolicy ----------
// ----------------------------------------------

char const* StatusDeleteRequestPolicy::requestName() { return "REQUEST_STATUS:REPLICA_DELETE"; }

ProtocolQueuedRequestType StatusDeleteRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_DELETE;
}

void StatusDeleteRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    data = ResponseDataType(&(msg.replica_info()));
}

void StatusDeleteRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                           TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// --------------------------------------------
// --------- StatusFindRequestPolicy ----------
// --------------------------------------------

char const* StatusFindRequestPolicy::requestName() { return "REQUEST_STATUS:REPLICA_FIND"; }

ProtocolQueuedRequestType StatusFindRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_FIND;
}

void StatusFindRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    data = ReplicaInfo(&(msg.replica_info()));
}

void StatusFindRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                         TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// -----------------------------------------------
// --------- StatusFindAllRequestPolicy ----------
// -----------------------------------------------

char const* StatusFindAllRequestPolicy::requestName() { return "REQUEST_STATUS:REPLICA_FIND_ALL"; }

ProtocolQueuedRequestType StatusFindAllRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_FIND_ALL;
}

void StatusFindAllRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    for (int num = msg.replica_info_many_size(), idx = 0; idx < num; ++idx) {
        data.emplace_back(&(msg.replica_info_many(idx)));
    }
}

void StatusFindAllRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                            TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// --------------------------------------------
// --------- StatusEchoRequestPolicy ----------
// --------------------------------------------

char const* StatusEchoRequestPolicy::requestName() { return "REQUEST_STATUS:TEST_ECHO"; }

ProtocolQueuedRequestType StatusEchoRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::TEST_ECHO;
}

void StatusEchoRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    data = msg.data();
}

void StatusEchoRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                         TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// ---------------------------------------------
// --------- StatusIndexRequestPolicy ----------
// ---------------------------------------------

char const* StatusIndexRequestPolicy::requestName() { return "REQUEST_STATUS:INDEX"; }

ProtocolQueuedRequestType StatusIndexRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::INDEX;
}

void StatusIndexRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    data.error = msg.error();
    data.data = msg.data();
}

void StatusIndexRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                          TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

// -------------------------------------------
// --------- StatusSqlRequestPolicy ----------
// -------------------------------------------

char const* StatusSqlRequestPolicy::requestName() { return "REQUEST_STATUS:SQL"; }

ProtocolQueuedRequestType StatusSqlRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::SQL;
}

void StatusSqlRequestPolicy::extractResponseData(ResponseMessageType const& msg, ResponseDataType& data) {
    data.set(msg);
}

void StatusSqlRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

}  // namespace lsst::qserv::replica
