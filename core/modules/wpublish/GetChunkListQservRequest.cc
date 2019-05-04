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
#include "wpublish/GetChunkListQservRequest.h"

// System headers
#include <stdexcept>
#include <string>

// LSST headers
#include "lsst/log/Log.h"

using namespace lsst::qserv;
using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.GetChunkListQservRequest");

wpublish::GetChunkListQservRequest::Status translate(proto::WorkerCommandGetChunkListR::Status status) {
    switch (status) {
        case proto::WorkerCommandGetChunkListR::SUCCESS: return wpublish::GetChunkListQservRequest::SUCCESS;
        case proto::WorkerCommandGetChunkListR::ERROR:   return wpublish::GetChunkListQservRequest::ERROR;
    }
    throw domain_error(
            "GetChunkListQservRequest::translate  no match for Protobuf status: " +
            proto::WorkerCommandGetChunkListR_Status_Name(status));
}
}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

string GetChunkListQservRequest::status2str(Status status) {
    switch (status) {
        case SUCCESS: return "SUCCESS";
        case ERROR:   return "ERROR";
    }
    throw domain_error(
            "GetChunkListQservRequest::status2str  no match for status: " +
            to_string(status));
}


GetChunkListQservRequest::Ptr GetChunkListQservRequest::create(
                                    bool inUseOnly,
                                    GetChunkListQservRequest::CallbackType onFinish) {
    return GetChunkListQservRequest::Ptr(new GetChunkListQservRequest(
        inUseOnly,
        onFinish
    ));
}


GetChunkListQservRequest::GetChunkListQservRequest(
                                bool inUseOnly,
                                GetChunkListQservRequest::CallbackType onFinish)
    :   _inUseOnly(inUseOnly),
        _onFinish(onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, "GetChunkListQservRequest  ** CONSTRUCTED **");
}


GetChunkListQservRequest::~GetChunkListQservRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "GetChunkListQservRequest  ** DELETED **");
}


void GetChunkListQservRequest::onRequest(proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::GET_CHUNK_LIST);
    buf.serialize(header);
}


void GetChunkListQservRequest::onResponse(proto::FrameBufferView& view) {

    string const context = "GetChunkListQservRequest  ";

    proto::WorkerCommandGetChunkListR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SERVICE REPLY **  status: "
         << proto::WorkerCommandGetChunkListR_Status_Name(reply.status()));

    ChunkCollection chunks;

    if (reply.status() == proto::WorkerCommandGetChunkListR::SUCCESS) {

        int const num = reply.chunks_size();
        for (int i = 0; i < num; i++) {
            proto::WorkerCommandChunk const& chunkEntry  = reply.chunks(i);
            if (_inUseOnly and not chunkEntry.use_count()) continue;
            Chunk chunk {chunkEntry.chunk(), chunkEntry.db(), chunkEntry.use_count()};
            chunks.push_back(chunk);
        }
        LOGS(_log, LOG_LVL_DEBUG, context << "total chunks: " << num);
    }
    if (nullptr != _onFinish) {

        // Clearing the stored callback after finishing the up-stream notification
        // has two purposes:
        //
        // 1. it guaranties (exactly) one time notification
        // 2. it breaks the up-stream dependency on a caller object if a shared
        //    pointer to the object was mentioned as the lambda-function's closure

        auto onFinish = move(_onFinish);
        _onFinish = nullptr;
        onFinish(::translate(reply.status()),
                 reply.error(),
                 chunks);
    }
}


void GetChunkListQservRequest::onError(string const& error) {

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
                 ChunkCollection());
    }
}

}}} // namespace lsst::qserv::wpublish
