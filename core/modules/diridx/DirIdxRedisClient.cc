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
#include "diridx/DirIdxRedisClient.h"

// System headers
#include <iostream>
#include <stdexcept>
#include <string>
#include <utility>

// Third-party headers
#include <boost/asio.hpp>
#include <boost/lexical_cast.hpp>
#include <sw/redis++/redis++.h>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers


namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.diridx.DirIdxRedisClient");
}


namespace lsst {
namespace qserv {
namespace diridx {


DirIdxRedisClient::DirIdxRedisClient(std::string const& name, boost::asio::io_context& io_context) :
        _name(name),
        _io_context(io_context)
{
    try {
        _redisCluster = std::make_shared<sw::redis::RedisCluster>("tcp://" + _name);
    } catch (sw::redis::Error const& err) {
        LOGS(_log, LOG_LVL_ERROR, "redis exception: " << err.what() << std::endl);
        throw std::runtime_error(
            "Error connecting to the Global Spatial Index Redis Cluster.");
    }
}


void DirIdxRedisClient::set(unsigned long long objectId, unsigned long chunkId, unsigned short subChunkId,
                             std::function<void (Err)> onComplete) {
    _io_context.dispatch([this, objectId, chunkId, subChunkId, onComplete] () {
        auto idAndField = _getSubIdAndField(objectId);
        auto chunkAndSubchunk = _combineChunkData(chunkId, subChunkId);
        try {
            this->_redisCluster->hset(std::to_string(idAndField.first),
                                      std::to_string(idAndField.second),
                                      chunkAndSubchunk);
        } catch (sw::redis::Error const& err) {
            onComplete(FAIL);
            return;
        }
        onComplete(SUCCESS);
    });
}


ChunkData DirIdxRedisClient::get(unsigned long long objectId) {
    auto idAndField = _getSubIdAndField(objectId);
    // the following line might throw a sw::redis::Error (inherits from std::exception)
    sw::redis::OptionalString chunkData = _redisCluster->hget(std::to_string(idAndField.first), std::to_string(idAndField.second));
    if (not chunkData) {
        LOGS(_log, LOG_LVL_DEBUG, "Could not locate ChunkData for objectId " << objectId);
        return ChunkData();
    }
    auto splitChunkData = _splitChunkData(chunkData.value());
    LOGS(_log, LOG_LVL_DEBUG, "Got " << splitChunkData << " for objectId " << objectId);
    return splitChunkData;
}


ChunkData DirIdxRedisClient::get(std::string id) {
    auto idAndField = _getSubIdAndField(id);
    // the following line might throw a sw::redis::Error (inherits from std::exception)
    sw::redis::OptionalString chunkData = _redisCluster->hget(idAndField.first, idAndField.second);
    if (not chunkData) {
        LOGS(_log, LOG_LVL_DEBUG, "Could not locate ChunkData for objectId " << id);
        return ChunkData();
    }
    auto splitChunkData = _splitChunkData(chunkData.value());
    LOGS(_log, LOG_LVL_DEBUG, "Got " << splitChunkData << " for objectId " << id);
    return splitChunkData;
}


std::pair<unsigned long long, short> DirIdxRedisClient::_getSubIdAndField(unsigned long long objectId) {
    return std::make_pair<unsigned long long, short>(objectId / 100, objectId % 100);
}


std::pair<std::string, std::string> DirIdxRedisClient::_getSubIdAndField(std::string const& id) {
    // split the last 2 characters from the string, or if it's too short return a pair ("0", id)
    if (id.length() < 3) {
        return std::make_pair("0", id);
    }
    return std::make_pair(id.substr(0, id.length()-2), id.substr(id.length()-2, std::string::npos));
}


ChunkData DirIdxRedisClient::_splitChunkData(std::string chunkData) {
    ChunkData chunk;
    auto dotLocation = chunkData.find(".");
    if (std::string::npos == dotLocation) {
        throw std::runtime_error(
            "Received invalid or badly formatted chunk data from secondary index storage: " + chunkData);
    }
    try {
        chunk.setChunkId(boost::lexical_cast<unsigned long>(chunkData.substr(0, dotLocation)));
        chunk.setSubChunkId(boost::lexical_cast<unsigned short>(chunkData.substr(dotLocation+1)));
    } catch (boost::bad_lexical_cast const& err) {
        throw std::runtime_error(
            "Received invalid or badly formatted chunk data from secondary index storage: " + chunkData);
    }
    return chunk;
}


std::string DirIdxRedisClient::_combineChunkData(unsigned long long chunkId, unsigned short subChunkId) {
    return std::to_string(chunkId) + "." + std::to_string(subChunkId);
}


}}} // namespace lsst::qserv::diridx
