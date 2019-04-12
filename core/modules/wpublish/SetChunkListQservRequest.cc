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
#include "wpublish/SetChunkListQservRequest.h"

// System headers
#include <stdexcept>
#include <string>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace lsst::qserv;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.SetChunkListQservRequest");

wpublish::SetChunkListQservRequest::Status translate(proto::WorkerCommandSetChunkListR::Status status) {
    switch (status) {
        case proto::WorkerCommandSetChunkListR::SUCCESS: return wpublish::SetChunkListQservRequest::SUCCESS;
        case proto::WorkerCommandSetChunkListR::INVALID: return wpublish::SetChunkListQservRequest::INVALID;
        case proto::WorkerCommandSetChunkListR::IN_USE:  return wpublish::SetChunkListQservRequest::IN_USE;
        case proto::WorkerCommandSetChunkListR::ERROR:   return wpublish::SetChunkListQservRequest::ERROR;
    }
    throw domain_error(
            "SetChunkListQservRequest::translate  no match for Protobuf status: " +
            proto::WorkerCommandSetChunkListR_Status_Name(status));
}
}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

string SetChunkListQservRequest::status2str(Status status) {
    switch (status) {
        case SUCCESS: return "SUCCESS";
        case INVALID: return "INVALID";
        case IN_USE:  return "IN_USE";
        case ERROR:   return "ERROR";
    }
    throw domain_error(
            "SetChunkListQservRequest::status2str  no match for status: " +
            to_string(status));
}


SetChunkListQservRequest::Ptr SetChunkListQservRequest::create(
                                SetChunkListQservRequest::ChunkCollection const& chunks,
                                bool force,
                                SetChunkListQservRequest::CallbackType onFinish) {
    return SetChunkListQservRequest::Ptr(new SetChunkListQservRequest(
        chunks,
        force,
        onFinish
    ));
}


SetChunkListQservRequest::SetChunkListQservRequest(
                                SetChunkListQservRequest::ChunkCollection const& chunks,
                                bool force,
                                SetChunkListQservRequest::CallbackType onFinish)
    :   _chunks(chunks),
        _force(force),
        _onFinish(onFinish) {

    LOGS(_log, LOG_LVL_DEBUG, "SetChunkListQservRequest  ** CONSTRUCTED **");
}


SetChunkListQservRequest::~SetChunkListQservRequest() {
    LOGS(_log, LOG_LVL_DEBUG, "SetChunkListQservRequest  ** DELETED **");
}


void SetChunkListQservRequest::onRequest(proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::SET_CHUNK_LIST);
    buf.serialize(header);

    proto::WorkerCommandSetChunkListM message;
    for(auto const& chunkEntry: _chunks) {
        proto::WorkerCommandChunk* ptr = message.add_chunks();
        ptr->set_db(chunkEntry.database);
        ptr->set_chunk(chunkEntry.chunk);
    }
    message.set_force(_force);
    buf.serialize(message);
}


void SetChunkListQservRequest::onResponse(proto::FrameBufferView& view) {

    static string const context = "SetChunkListQservRequest  ";

    proto::WorkerCommandSetChunkListR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SERVICE REPLY **  status: "
         << proto::WorkerCommandSetChunkListR_Status_Name(reply.status()));

    ChunkCollection chunks;

    if (reply.status() == proto::WorkerCommandSetChunkListR::SUCCESS) {
        int const num = reply.chunks_size();
        for (int i = 0; i < num; i++) {
            proto::WorkerCommandChunk const& chunkEntry  = reply.chunks(i);
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


void SetChunkListQservRequest::onError(string const& error) {

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
