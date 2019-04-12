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
/// GetChunkListCommand.h
#ifndef LSST_QSERV_WPUBLISH_GET_CHUNK_LIST_COMMAND_H
#define LSST_QSERV_WPUBLISH_GET_CHUNK_LIST_COMMAND_H

// System headers
#include <memory>

// Qserv headers
#include "wbase/WorkerCommand.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace wpublish {
    class ChunkInventory;
    class ResourceMonitor;
}}}

// This header declarations
namespace lsst {
namespace qserv {
namespace wpublish {

/**
  * Class GetChunkListCommand returns a status of the chunk inventory
  */
class GetChunkListCommand : public wbase::WorkerCommand {

public:

    // The default construction and copy semantics are prohibited
    GetChunkListCommand() = delete;
    GetChunkListCommand& operator=(GetChunkListCommand const&) = delete;
    GetChunkListCommand(GetChunkListCommand const&) = delete;

    ~GetChunkListCommand() override = default;

    /**
     * @param sendChannel     communication channel for reporting results
     * @param chunkInventory  transient collection of available chunks to be reloaded (if requested)
     * @param mySqlConfig     database connection parameters
     */
    GetChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                        std::shared_ptr<ChunkInventory> const& chunkInventory,
                        std::shared_ptr<ResourceMonitor> const& resourceMonitor);

    void run() override;

private:

    // Parameters of the object

    std::shared_ptr<ChunkInventory>  _chunkInventory;
    std::shared_ptr<ResourceMonitor> _resourceMonitor;
};

}}} // namespace lsst::qserv::wpublish

#endif // LSST_QSERV_WPUBLISH_GET_CHUNK_LIST_COMMAND_H