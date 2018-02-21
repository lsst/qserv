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
/// RemoveChunkGroupCommand.h
#ifndef LSST_QSERV_PUBLISH_REMOVE_CHUNK_GROUP_COMMAND_H
#define LSST_QSERV_PUBLISH_REMOVE_CHUNK_GROUP_COMMAND_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "proto/worker.pb.h"
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wbase {
class SendChannel;
}}}

namespace lsst {
namespace qserv {
namespace wpublish {

// Forward declarations
class ChunkInventory;
class ResourceMonitor;

/**
  * Class RemoveChunkGroupCommand removes a group of chunks from XRootD
  * and the worker's list of chunks.
  */
class RemoveChunkGroupCommand
    :   public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    RemoveChunkGroupCommand() = delete;
    RemoveChunkGroupCommand& operator=(const RemoveChunkGroupCommand&) = delete;
    RemoveChunkGroupCommand(const RemoveChunkGroupCommand&) = delete;

    /**
     * The normal constructor of the class
     *
     * @param sendChannel     - communication channel for reporting results
     * @param chunkInventory  - chunks known to the application
     * @param resourceMonitor - counters of resources which are being used
     * @param mySqlConfig    - database connection parameters
     * @param chunk           - chunk number
     * @param dbs             - names of databases in the group
     * @param force           - force chunk removal even if this chunk is in use
     */
    RemoveChunkGroupCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                            std::shared_ptr<ChunkInventory>     const& chunkInventory,
                            std::shared_ptr<ResourceMonitor>    const& resourceMonitor,
                            mysql::MySqlConfig                  const& mySqlConfig,
                            int chunk,
                            std::vector<std::string> const& dbs,
                            bool force);

    /// The destructor
    virtual ~RemoveChunkGroupCommand();

    /**
     * Implement the corresponding method of the base class
     *
     * @see WorkerCommand::run()
     */
    void run () override;

private:

    /**
     * Report error condition to the logging stream and reply back to
     * a service caller.
     *
     * @param status  - error status
     * @param message - message to be reported
     */
    void reportError(proto::WorkerCommandChunkGroupR::Status status,
                     std::string const& message);

private:

    std::shared_ptr<ChunkInventory> _chunkInventory;
    std::shared_ptr<ResourceMonitor> _resourceMonitor;
    mysql::MySqlConfig _mySqlConfig;
    int _chunk;
    std::vector<std::string> _dbs;
    bool _force;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_PUBLISH_REMOVE_CHUNK_GROUP_COMMAND_H