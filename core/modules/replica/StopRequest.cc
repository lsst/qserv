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
#include "replica/StopRequest.h"

// Qserv headers
#include "replica/IndexRequest.h"

using namespace std;

namespace lsst {
namespace qserv {
namespace replica {

// -------------------------------------------------
// --------- StopReplicationRequestPolicy ----------
// -------------------------------------------------

char const* StopReplicationRequestPolicy::requestName() {
    return "REQUEST_STOP:REPLICA_CREATE";
}


ProtocolQueuedRequestType StopReplicationRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_CREATE;
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


ProtocolQueuedRequestType StopDeleteRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_DELETE;
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


ProtocolQueuedRequestType StopFindRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_FIND;
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


ProtocolQueuedRequestType StopFindAllRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::REPLICA_FIND_ALL;
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
    return "REQUEST_STOP:TEST_ECHO";
}


ProtocolQueuedRequestType StopEchoRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::TEST_ECHO;
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


// -------------------------------------------
// --------- StopIndexRequestPolicy ----------
// -------------------------------------------

char const* StopIndexRequestPolicy::requestName() {
    return "REQUEST_STOP:INDEX";
}


ProtocolQueuedRequestType StopIndexRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::TEST_ECHO;
}


void StopIndexRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                                 ResponseDataType& data) {
    data.error = msg.error();
    data.data  = msg.data();
}


void StopIndexRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                        TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}


// -----------------------------------------
// --------- StopSqlRequestPolicy ----------
// -----------------------------------------

char const* StopSqlRequestPolicy::requestName() {
    return "REQUEST_STOP:SQL";
}


ProtocolQueuedRequestType StopSqlRequestPolicy::targetRequestType() {
    return ProtocolQueuedRequestType::SQL;
}


void StopSqlRequestPolicy::extractResponseData(ResponseMessageType const& msg,
                                               ResponseDataType& data) {
    data.set(msg);
}


void StopSqlRequestPolicy::extractTargetRequestParams(ResponseMessageType const& msg,
                                                      TargetRequestParamsType& params) {
    if (msg.has_request()) {
        params = TargetRequestParamsType(msg.request());
    }
}

}}} // namespace lsst::qserv::replica
