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
#include "wpublish/GetStatusQservRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.GetStatusQservRequest");

}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

string GetStatusQservRequest::status2str(Status status) {
    switch (status) {
        case SUCCESS: return "SUCCESS";
        case ERROR:   return "ERROR";
    }
    throw domain_error(
            "GetStatusQservRequest::" + string(__func__) + "  no match for status: " +
            to_string(status));
}


GetStatusQservRequest::Ptr GetStatusQservRequest::create(
                                    GetStatusQservRequest::CallbackType onFinish) {
    return GetStatusQservRequest::Ptr(
        new GetStatusQservRequest(onFinish));
}


GetStatusQservRequest::GetStatusQservRequest(
                                    GetStatusQservRequest::CallbackType onFinish)
    :   _onFinish(onFinish) {
    LOGS(_log, LOG_LVL_DEBUG, "GetStatusQservRequest  ** CONSTRUCTED **");
}


GetStatusQservRequest::~GetStatusQservRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "GetStatusQservRequest  ** DELETED **");
}


void GetStatusQservRequest::onRequest(proto::FrameBuffer& buf) {
    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::GET_STATUS);
    buf.serialize(header);
}


void GetStatusQservRequest::onResponse(proto::FrameBufferView& view) {

    proto::WorkerCommandGetStatusR reply;
    view.parse(reply);

    if (nullptr != _onFinish) {

        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(Status::SUCCESS,
                 string(),
                 reply.info());
    }
}


void GetStatusQservRequest::onError(string const& error) {

    if (nullptr != _onFinish) {

        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(Status::ERROR,
                 error,
                 string());
    }
}

}}} // namespace lsst::qserv::wpublish
