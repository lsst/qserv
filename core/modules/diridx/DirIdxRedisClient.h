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

#ifndef LSST_QSERV_DIRIDX_DIRIDXREDISCLIENT_H
#define LSST_QSERV_DIRIDX_DIRIDXREDISCLIENT_H

// System headers
#include <functional>
#include <memory>
#include <string>

// Qserv headers
#include "diridx/ChunkData.h"

// Forward declarations
namespace boost {
namespace asio {
    class io_context;
}}
namespace sw {
namespace redis {
    class RedisCluster;
}}


namespace lsst {
namespace qserv {
namespace diridx {


/**
 * Class DirIdxRedisClient is an interface for getting chunk and subchunk location of object by its
 * objectId.
 *
 * Setting values is handled asynchronously using the io_context passed into the class constructor.
 *
 * Getting values is handled synchronously.
 *
 * If the redis cluster fails an exception (of type sw::redis::Error, which inherits from std::exception)
 * may be thrown. If this happens during a `set` operation the setter thread that the exception was thrown
 * on will enter the caller's catch block and will resume automatically once exception handling completes.
 * See the boost io_context documentation about exceptions for more information.
 *
 * Data Format Inside Redis
 * ========================
 *
 * Object Id
 * ---------
 * The object id is used as the key, and the last 2 digits of the object id number are removed, and those 2
 * digits are used as a field in that slot. For example object id 123456789 will be stored at the hash slot
 * for 1234567, and the field will be 89.
 *
 * Chunk and Subchunk
 * ------------------
 * Chunk and subchunk are the value. They are combined, separated by a dot ('.'). For example, chunk 123456
 * and subchunk 78 will be stored as "123456.78".
 */
class DirIdxRedisClient {
public:

    // Constructor that takes a running io_context allows the caller to establish the threading policy.
    DirIdxRedisClient(std::string const& name, boost::asio::io_context& io_context);

    enum Err { SUCCESS, FAIL };

    /**
     * @brief Set a value in the global spatial index.
     *
     * This call is asynchronous and will return after the set call is queued.
     *
     * @param objectId the object id
     * @param chunkId the chunk id
     * @param subChunkId the subchunk id
     * @param onComplete a function to call once the value has been set in the diridx data store.
     */
    void set(unsigned long long objectId, unsigned long chunkId, unsigned short subChunkId,
             std::function<void (Err)> onComplete);

    /**
     * @brief Get the chunk id and subchunk id for the given objectId
     *
     * @throws sw::redis::Error (which inherits from std::exception) if there is an error setting the value
     *         in redis, may mean the redis cluster is down or IP addresses have changed.
     *
     * @throws std::runtime_error if nonsensical or badly formatted data is recevied from the redis cluster.
     *
     * @param objectId
     * @return ChunkData the chunk id (first) and subchunk id (second)
     */
    ChunkData get(unsigned long long objectId);

    ChunkData get(std::string id);

    DirIdxRedisClient(DirIdxRedisClient const&)=delete;
    DirIdxRedisClient& operator=(DirIdxRedisClient const&)=delete;

    ~DirIdxRedisClient() = default;


private:
    friend class TestAccessor;

    /**
     * @brief For a given objectId get the shortened versions of to use as the hash key and the field offset.
     *
     * Technically this does not need to be part of the public api but is public for unit testing.
     *
     * @param objectId
     * @return std::pair<unsigned long long, short> the shortened object id (first)
     *         and the field offset (second).
     */
    static std::pair<unsigned long long, short> _getSubIdAndField(unsigned long long objectId);

    static std::pair<std::string, std::string> _getSubIdAndField(std::string const& id);

    /**
     * @brief Separate chunk + subchunk data stored together in a string as
     *        "<chunk number>.<subchunk number>" into a pair of integers.
     *
     * @param chunkData
     * @return ChunkData the chunk and subchunk
     */
    static ChunkData _splitChunkData(std::string chunkData);

    /**
     * @brief Combine chunk + subchunk data into a string, separated by a dot as
     *        "<chunk number>.<subchunk number>".
     *
     * @param chunkId
     * @param subchunkId
     * @return std::string
     */
    static std::string _combineChunkData(unsigned long long chunkId, unsigned short subChunkId);

    std::shared_ptr<sw::redis::RedisCluster> _redisCluster;
    std::string _name;
    boost::asio::io_context& _io_context;
};


}}} // namespace lsst::qserv::diridx

#endif // LSST_QSERV_DIRIDX_DIRIDXREDISCLIENT_H
