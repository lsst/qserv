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
/// AddChunkGroupCommand.h
#ifndef LSST_QSERV_WCONTROL_ADD_CHUNK_GROUP_COMMAND_H
#define LSST_QSERV_WCONTROL_ADD_CHUNK_GROUP_COMMAND_H

// System headers
#include <memory>
#include <string>
#include <vector>

// Qserv headers
#include "proto/worker.pb.h"
#include "wbase/WorkerCommand.h"
#include "wpublish/ChunkInventory.h"

// Forward declarations


namespace lsst {
namespace qserv {
namespace wcontrol {

/**
  * Class AddChunkGroupCommand reloads a list of chunks from the database
  */
class AddChunkGroupCommand
    :   public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    AddChunkGroupCommand& operator=(const AddChunkGroupCommand&) = delete;
    AddChunkGroupCommand(const AddChunkGroupCommand&) = delete;
    AddChunkGroupCommand() = delete;

    /**
     * The normal constructor of the class
     *
     * @param sendChannel    - communication channel for reporting results
     * @param chunkInventory - chunks known to the application
     * @param chunk          - chunk number
     * @param dbs            - names of databases in the group
     */
    explicit AddChunkGroupCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                  std::shared_ptr<wpublish::ChunkInventory> const& chunkInventory,
                                  int chunk,
                                  std::vector<std::string> const& dbs);

    /// The destructor
    virtual ~AddChunkGroupCommand();

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

    std::shared_ptr<wpublish::ChunkInventory> _chunkInventory;
    int _chunk;
    std::vector<std::string> _dbs;
};

}}} // namespace lsst::qserv::wbase

#endif // LSST_QSERV_WCONTROL_ADD_CHUNK_GROUP_COMMAND_H