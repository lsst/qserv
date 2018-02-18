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
#include "wpublish/ReloadChunkListQservRequest.h"

// System headers
#include <string>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ReloadChunkListQservRequest");

}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

ReloadChunkListQservRequest::ReloadChunkListQservRequest (calback_type onFinish)
    :   _onFinish(onFinish){

    LOGS(_log, LOG_LVL_DEBUG, "ReloadChunkListQservRequest  ** CONSTRUCTED **");
}

ReloadChunkListQservRequest::~ReloadChunkListQservRequest () {
    LOGS(_log, LOG_LVL_DEBUG, "ReloadChunkListQservRequest  ** DELETED **");
}

void
ReloadChunkListQservRequest::onRequest (proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(proto::WorkerCommandH::RELOAD_CHUNK_LIST);

    buf.serialize(header);
}

void
ReloadChunkListQservRequest::onResponse (proto::FrameBufferView& view) {

    static std::string const context = "ReloadChunkListQservRequest  ";

    proto::WorkerCommandReloadChunkListR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SERVICE REPLY **  status: "
         << proto::WorkerCommandReloadChunkListR_Status_Name(reply.status()));

    ChunkCollection added;
    ChunkCollection removed;

    if (reply.status() == proto::WorkerCommandReloadChunkListR::SUCCESS) {

        int const numAdded = reply.added_size();
        for (int i = 0; i < numAdded; i++) {
            proto::WorkerCommandReloadChunkListR::Chunk const& chunkEntry  = reply.added(i);
            Chunk chunk {chunkEntry.chunk(), chunkEntry.db()};
            added.push_back(chunk);
        }
        LOGS(_log, LOG_LVL_DEBUG, context << "total chunks added: " << numAdded);

        int const numRemoved = reply.removed_size();
        for (int i = 0; i < numRemoved; i++) {
            proto::WorkerCommandReloadChunkListR::Chunk const& chunkEntry  = reply.removed(i);
            Chunk chunk {chunkEntry.chunk(), chunkEntry.db()};
            removed.push_back(chunk);
        }
        LOGS(_log, LOG_LVL_DEBUG, context << "total chunks removed: " << numRemoved);
    }

    if (_onFinish)
        _onFinish (
            reply.status() == proto::WorkerCommandReloadChunkListR::SUCCESS,
            added,
            removed);
}

}}} // namespace lsst::qserv::wpublish