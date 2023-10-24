// -*- LSST-C++ -*-
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
#include "xrdreq/GetConfigQservRequest.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdreq.GetConfigQservRequest");

}  // namespace

namespace lsst::qserv::xrdreq {

GetConfigQservRequest::Ptr GetConfigQservRequest::create(GetConfigQservRequest::CallbackType onFinish) {
    GetConfigQservRequest::Ptr ptr(new GetConfigQservRequest(onFinish));
    ptr->setRefToSelf4keepAlive(ptr);
    return ptr;
}

GetConfigQservRequest::GetConfigQservRequest(GetConfigQservRequest::CallbackType onFinish)
        : _onFinish(onFinish) {
    LOGS(_log, LOG_LVL_TRACE, "GetConfigQservRequest  ** CONSTRUCTED **");
}

GetConfigQservRequest::~GetConfigQservRequest() {
    LOGS(_log, LOG_LVL_TRACE, "GetConfigQservRequest  ** DELETED **");
}

void GetConfigQservRequest::onRequest(proto::FrameBuffer& buf) {
    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::GET_CONFIG);
    buf.serialize(header);
}

void GetConfigQservRequest::onResponse(proto::FrameBufferView& view) {
    proto::WorkerCommandGetDbStatusR reply;
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
        onFinish(proto::WorkerCommandStatus::SUCCESS, string(), reply.info());
    }
}

void GetConfigQservRequest::onError(string const& error) {
    if (nullptr != _onFinish) {
        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(proto::WorkerCommandStatus::ERROR, error, string());
    }
}

}  // namespace lsst::qserv::xrdreq
