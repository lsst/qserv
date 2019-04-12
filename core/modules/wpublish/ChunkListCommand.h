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
/// ChunkListCommand.h
#ifndef LSST_QSERV_WPUBLISH_CHUNK_LIST_COMMAND_H
#define LSST_QSERV_WPUBLISH_CHUNK_LIST_COMMAND_H

// System headers
#include <memory>

// Qserv headers
#include "wbase/WorkerCommand.h"
#include "mysql/MySqlConfig.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wpublish {
    class ChunkInventory;
}}}

// This header declarations
namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class ChunkListCommand is the base class for operations with chunk lists
  */
class ChunkListCommand : public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    ChunkListCommand() = delete;
    ChunkListCommand& operator=(ChunkListCommand const&) = delete;
    ChunkListCommand(ChunkListCommand const&) = delete;

    ~ChunkListCommand() override = default;

protected:

    /**
     * @param sendChannel     communication channel for reporting results
     * @param chunkInventory  transient collection of available chunks to be reloaded (if requested)
     * @param mySqlConfig     database connection parameters
     * @param rebuild         rebuild the list from actual database tables
     * @param reload          reload the list in worker's memory
     */
    ChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                     std::shared_ptr<ChunkInventory> const& chunkInventory,
                     mysql::MySqlConfig const& mySqlConfig,
                     bool rebuild,
                     bool reload);

    void run() override;

private:

    /**
     * Report error condition to the logging stream and reply back to
     * a service caller.
     *
     * @param message  message to be reported
     */
    void _reportError(std::string const& message);

    // Parameters of the object

    std::shared_ptr<ChunkInventory> _chunkInventory;
    mysql::MySqlConfig const        _mySqlConfig;

    bool _rebuild;
    bool _reload;
};

/**
  * Class ReloadChunkListCommand reloads a list of chunks from the database
  */
class ReloadChunkListCommand
    :   public ChunkListCommand {

public:

    // The default construction and copy semantics are prohibited
    ReloadChunkListCommand() = delete;
    ReloadChunkListCommand& operator=(ReloadChunkListCommand const&) = delete;
    ReloadChunkListCommand(ReloadChunkListCommand const&) = delete;

    /**
     * @param sendChannel     communication channel for reporting results
     * @param chunkInventory  transient collection of available chunks to be reloaded (if requested)
     * @param mySqlConfig     database connection parameters
     */
    ReloadChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                           std::shared_ptr<ChunkInventory> const& chunkInventory,
                           mysql::MySqlConfig const& mySqlConfig)
        :   ChunkListCommand(sendChannel,
                             chunkInventory,
                             mySqlConfig,
                             false,
                             true) {
    }

    ~ReloadChunkListCommand() override = default;
};

/**
  * Class RebuildChunkListCommand rebuilds a persistent list of chunks and
  * optionally (if requested) reloads a list of chunks from the database
  * into a transient list.
  */
class RebuildChunkListCommand : public ChunkListCommand {

public:

    // The default construction and copy semantics are prohibited
    RebuildChunkListCommand() = delete;
    RebuildChunkListCommand& operator=( RebuildChunkListCommand const&) = delete;
    RebuildChunkListCommand(RebuildChunkListCommand const&) = delete;

    /**
     * @param sendChannel     communication channel for reporting results
     * @param chunkInventory  transient collection of available chunks to be reloaded (if requested)
     * @param mySqlConfig     database connection parameters
     * @param reload          reload the list in worker's memory
     */
    RebuildChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                            std::shared_ptr<ChunkInventory> const& chunkInventory,
                            mysql::MySqlConfig const& mySqlConfig,
                            bool reload)
        :   ChunkListCommand(sendChannel,
                             chunkInventory,
                             mySqlConfig,
                             true,
                             reload) {
    }

    ~RebuildChunkListCommand() override = default;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_CHUNK_LIST_COMMAND_H