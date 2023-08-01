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
#include "xrdreq/SetChunkListQservRequest.h"

// System headers
#include <string>

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.xrdreq.SetChunkListQservRequest");
}  // namespace

namespace lsst::qserv::xrdreq {

SetChunkListQservRequest::Ptr SetChunkListQservRequest::create(
        SetChunkListQservRequest::ChunkCollection const& chunks, vector<string> const& databases, bool force,
        SetChunkListQservRequest::CallbackType onFinish) {
    SetChunkListQservRequest::Ptr ptr(new SetChunkListQservRequest(chunks, databases, force, onFinish));
    ptr->setRefToSelf4keepAlive(ptr);
    return ptr;
}

SetChunkListQservRequest::SetChunkListQservRequest(SetChunkListQservRequest::ChunkCollection const& chunks,
                                                   vector<string> const& databases, bool force,
                                                   SetChunkListQservRequest::CallbackType onFinish)
        : _chunks(chunks), _databases(databases), _force(force), _onFinish(onFinish) {
    LOGS(_log, LOG_LVL_TRACE, "SetChunkListQservRequest  ** CONSTRUCTED **");
}

SetChunkListQservRequest::~SetChunkListQservRequest() {
    LOGS(_log, LOG_LVL_TRACE, "SetChunkListQservRequest  ** DELETED **");
}

void SetChunkListQservRequest::onRequest(proto::FrameBuffer& buf) {
    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::SET_CHUNK_LIST);
    buf.serialize(header);

    proto::WorkerCommandSetChunkListM message;
    for (auto const& chunkEntry : _chunks) {
        proto::WorkerCommandChunk* ptr = message.add_chunks();
        ptr->set_db(chunkEntry.database);
        ptr->set_chunk(chunkEntry.chunk);
    }
    for (auto&& database : _databases) {
        message.add_databases(database);
    }
    message.set_force(_force);
    buf.serialize(message);
}

void SetChunkListQservRequest::onResponse(proto::FrameBufferView& view) {
    static string const context = "SetChunkListQservRequest  ";

    proto::WorkerCommandSetChunkListR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_TRACE,
         context << "** SERVICE REPLY **  status: "
                 << proto::WorkerCommandStatus_Code_Name(reply.status().code()));

    ChunkCollection chunks;

    if (reply.status().code() == proto::WorkerCommandStatus::SUCCESS) {
        int const num = reply.chunks_size();
        for (int i = 0; i < num; i++) {
            proto::WorkerCommandChunk const& chunkEntry = reply.chunks(i);
            Chunk chunk{chunkEntry.chunk(), chunkEntry.db(), chunkEntry.use_count()};
            chunks.push_back(chunk);
        }
        LOGS(_log, LOG_LVL_TRACE, context << "total chunks: " << num);
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
        onFinish(reply.status().code(), reply.status().error(), chunks);
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
        onFinish(proto::WorkerCommandStatus::ERROR, error, ChunkCollection());
    }
}

}  // namespace lsst::qserv::xrdreq
