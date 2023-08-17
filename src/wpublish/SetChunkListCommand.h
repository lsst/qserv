// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
#ifndef LSST_QSERV_WPUBLISH_SET_CHUNK_LIST_COMMAND_H
#define LSST_QSERV_WPUBLISH_SET_CHUNK_LIST_COMMAND_H

// System headers
#include <memory>
#include <set>
#include <string>
#include <vector>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "wbase/WorkerCommand.h"
#include "wpublish/ChunkInventory.h"

// Forward declarations
namespace lsst::qserv {
namespace wbase {
class SendChannel;
}
namespace wpublish {
class ResourceMonitor;
}
}  // namespace lsst::qserv

// This header declarations
namespace lsst::qserv::wpublish {

/**
 * Class SetChunkListCommand sets a new list of chunks
 */
class SetChunkListCommand : public wbase::WorkerCommand {
public:
    /// An abstraction for a chunk to be set
    struct Chunk {
        std::string database;
        unsigned int chunk;
    };

    SetChunkListCommand& operator=(const SetChunkListCommand&) = delete;
    SetChunkListCommand(const SetChunkListCommand&) = delete;
    SetChunkListCommand() = delete;
    virtual ~SetChunkListCommand() override = default;

    /**
     * @param sendChannel      communication channel for reporting results
     * @param chunkInventory   chunks known to the application
     * @param resourceMonitor  counters of resources which are being used
     * @param mySqlConfig      database connection parameters
     * @param chunks           collection of chunks to replace the current one
     * @param databases        limit a scope of the operation to databases of this collection
     * @param force            force chunks removal even if chunks are in use
     */
    SetChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                        std::shared_ptr<ChunkInventory> const& chunkInventory,
                        std::shared_ptr<ResourceMonitor> const& resourceMonitor,
                        mysql::MySqlConfig const& mySqlConfig, std::vector<Chunk> const& chunks,
                        std::vector<std::string> const& databases, bool force);

protected:
    virtual void run() override;

private:
    /**
     * Set the chunk list in the reply
     * @param reply         message to be initialized
     * @param prevExistMap  previous state of the ChunkList
     */
    void _setChunks(proto::WorkerCommandSetChunkListR& reply, ChunkInventory::ExistMap const& prevExistMap);

    // Parameters of the object

    std::shared_ptr<ChunkInventory> const _chunkInventory;
    std::shared_ptr<ResourceMonitor> const _resourceMonitor;
    mysql::MySqlConfig const _mySqlConfig;
    std::vector<Chunk> const _chunks;
    std::set<std::string> _databases;
    bool const _force;
};

}  // namespace lsst::qserv::wpublish

#endif  // LSST_QSERV_WPUBLISH_SET_CHUNK_LIST_COMMAND_H
