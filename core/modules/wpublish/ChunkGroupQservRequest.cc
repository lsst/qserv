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
#include "wpublish/ChunkGroupQservRequest.h"

// System headers
#include <stdexcept>
#include <string>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ChunkGroupQservRequest");

using namespace lsst::qserv;

wpublish::ChunkGroupQservRequest::Status translate(proto::WorkerCommandChunkGroupR::Status status) {
    switch (status) {
        case proto::WorkerCommandChunkGroupR::SUCCESS: return wpublish::ChunkGroupQservRequest::SUCCESS;
        case proto::WorkerCommandChunkGroupR::INVALID: return wpublish::ChunkGroupQservRequest::INVALID;
        case proto::WorkerCommandChunkGroupR::IN_USE:  return wpublish::ChunkGroupQservRequest::IN_USE;
        case proto::WorkerCommandChunkGroupR::ERROR:   return wpublish::ChunkGroupQservRequest::ERROR;
    }
    throw std::domain_error(
            "ChunkGroupQservRequest::translate  no match for Protobuf status: " +
            proto::WorkerCommandChunkGroupR_Status_Name(status));
}

}  // namespace

namespace lsst {
namespace qserv {
namespace wpublish {

std::string ChunkGroupQservRequest::status2str(Status status) {
    switch (status) {
        case SUCCESS: return "SUCCESS";
        case INVALID: return "INVALID";
        case IN_USE:  return "IN_USE";
        case ERROR:   return "ERROR";
    }
    throw std::domain_error(
            "ChunkGroupQservRequest::status2str  no match for status: " +
            std::to_string(status));
}

ChunkGroupQservRequest::ChunkGroupQservRequest(bool add,
                                               unsigned int  chunk,
                                               std::vector<std::string> const& databases,
                                               bool force,
                                               CallbackType onFinish)
    :   _add(add),
        _chunk(chunk),
        _databases(databases),
        _force(force),
        _onFinish(onFinish){

    LOGS(_log, LOG_LVL_DEBUG, "ChunkGroupQservRequest[" << (_add ? "add" : "remove")
         << "]   ** CONSTRUCTED **");
}

ChunkGroupQservRequest::~ChunkGroupQservRequest () {
    LOGS(_log, LOG_LVL_DEBUG, "ChunkGroupQservRequest[" << (_add ? "add" : "remove")
         << "]  ** DELETED **");
}

void ChunkGroupQservRequest::onRequest(proto::FrameBuffer& buf) {

    proto::WorkerCommandH header;
    header.set_command(_add ?
                       proto::WorkerCommandH::ADD_CHUNK_GROUP :
                       proto::WorkerCommandH::REMOVE_CHUNK_GROUP);
    buf.serialize(header);

    proto::WorkerCommandChunkGroupM message;
    message.set_chunk(_chunk);
    for (auto const& database: _databases) {
        message.add_dbs(database);
    }
    message.set_force(_force);
    buf.serialize(message);
}

void ChunkGroupQservRequest::onResponse(proto::FrameBufferView& view) {

    proto::WorkerCommandChunkGroupR reply;
    view.parse(reply);

    LOGS(_log, LOG_LVL_DEBUG, "ChunkGroupQservRequest[" << (_add ? "add" : "remove")
         << "** SERVICE REPLY **  status: "
         << proto::WorkerCommandChunkGroupR_Status_Name(reply.status()));

    if (_onFinish) {
        _onFinish(
            ::translate(reply.status()),
            reply.error());
    }
}

void ChunkGroupQservRequest::onError(std::string const& error) {

    if (_onFinish) {
        _onFinish(
            Status::ERROR,
            error);
    }
}

AddChunkGroupQservRequest::Ptr AddChunkGroupQservRequest::create(
                                        unsigned int chunk,
                                        std::vector<std::string> const& databases,
                                        CallbackType onFinish) {
    return AddChunkGroupQservRequest::Ptr(
        new AddChunkGroupQservRequest(chunk,
                                      databases,
                                      onFinish));
}

AddChunkGroupQservRequest::AddChunkGroupQservRequest(
                                    unsigned int chunk,
                                    std::vector<std::string> const& databases,
                                    CallbackType onFinish)
    :   ChunkGroupQservRequest(true,
                               chunk,
                               databases,
                               false,
                               onFinish) {
}

RemoveChunkGroupQservRequest::Ptr RemoveChunkGroupQservRequest::create(
                                            unsigned int chunk,
                                            std::vector<std::string> const& databases,
                                            bool force,
                                            CallbackType onFinish) {
    return RemoveChunkGroupQservRequest::Ptr(
        new RemoveChunkGroupQservRequest(chunk,
                                         databases,
                                         force,
                                         onFinish));
}

RemoveChunkGroupQservRequest::RemoveChunkGroupQservRequest(
                                    unsigned int chunk,
                                    std::vector<std::string> const& databases,
                                    bool force,
                                    CallbackType onFinish)
    :   ChunkGroupQservRequest(false,
                               chunk,
                               databases,
                               force,
                               onFinish) {
}

}}} // namespace lsst::qserv::wpublish
