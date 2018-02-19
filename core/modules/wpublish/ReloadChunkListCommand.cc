// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2018 AURA/LSST.
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
#include "wpublish/ReloadChunkListCommand.h"

// System headers
#include <sstream>

// Third-party headers
#include "XrdSsi/XrdSsiCluster.hh"

// LSST headers
#include "lsst/log/Log.h"
#include "proto/worker.pb.h"
#include "wbase/SendChannel.h"
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

/******************************************************************************/
/*                               G l o b a l s                                */
/******************************************************************************/

extern XrdSsiProvider* XrdSsiProviderLookup;


// Qserv headers

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ReloadChunkListCommand");


/// Print the inventory status onto the logging stream
void dumpInventory (lsst::qserv::wpublish::ChunkInventory const& inventory,
                    std::string                           const& context) {
    std::ostringstream os;
    inventory.dbgPrint(os);
    LOGS(_log, LOG_LVL_DEBUG, context << os.str());
}


} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

ReloadChunkListCommand::ReloadChunkListCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                               std::shared_ptr<ChunkInventory> const& chunkInventory,
                                               mysql::MySqlConfig const& mySqlConfig)
    :   wbase::WorkerCommand(sendChannel),

        _chunkInventory(chunkInventory),
        _mySqlConfig   (mySqlConfig) {
}

ReloadChunkListCommand::~ReloadChunkListCommand() {
}

void
ReloadChunkListCommand::reportError(std::string const& message) {

    LOGS(_log, LOG_LVL_ERROR, "ReloadChunkListCommand::run  " << message);

    proto::WorkerCommandUpdateChunkListR reply;

    reply.set_status(proto::WorkerCommandUpdateChunkListR::ERROR);
    reply.set_error (message);

    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

void
ReloadChunkListCommand::run() {

    LOGS(_log, LOG_LVL_DEBUG, "ReloadChunkListCommand::run");

    // Load the new map from the database into a local variable
    ChunkInventory newChunkInventory;
    try {
        xrdsvc::XrdName x;
        newChunkInventory.init(x.getName(), _mySqlConfig);
    } catch (std::exception const& ex) {
        reportError("database operation failed: " + std::string(ex.what()));
        return;
    }
    ::dumpInventory(*_chunkInventory,  "ReloadChunkListCommand::run  _chunkInventory: ");
    ::dumpInventory(newChunkInventory, "ReloadChunkListCommand::run  newChunkInventory: ");
 
    // Compare two maps and worker identifiers to see which resources were
    // were added or removed. Then Update the current map and notify XRootD
    // accordingly.

    ChunkInventory::ExistMap const removedChunks = *_chunkInventory  - newChunkInventory;
    ChunkInventory::ExistMap const addedChunks   = newChunkInventory - *_chunkInventory;

    XrdSsiCluster* clusterManager =
        dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup)->GetClusterManager();

    proto::WorkerCommandUpdateChunkListR reply;
    reply.set_status(proto::WorkerCommandUpdateChunkListR::SUCCESS);

    if (not removedChunks.empty()) {

        for (auto const& entry: removedChunks) {
            std::string const& db = entry.first;

            for (int chunk: entry.second) {
                std::string const resource = "/chk/" + db + "/" + std::to_string(chunk);

                LOGS(_log, LOG_LVL_DEBUG, "ReloadChunkListCommand::run  removing resource: " << resource);

                try {
                    clusterManager->Removed(resource.c_str());  // Notify XRootD/cmsd
                    _chunkInventory->remove(db, chunk);         // Notify QServ
                } catch (std::exception const& ex) {
                    reportError("failed to remove the chunk: " + std::string(ex.what()));
                    return;
                }

                // Notify the caller of this service
                proto::WorkerCommandUpdateChunkListR::Chunk* ptr = reply.add_removed();
                ptr->set_db(db);
                ptr->set_chunk(chunk);
            }
        }
    }
    if (not addedChunks.empty()) {

        for (auto const& entry: addedChunks) {
            std::string const& db = entry.first;

            for (int chunk: entry.second) {
                std::string const resource = "/chk/" + db + "/" + std::to_string(chunk);

                LOGS(_log, LOG_LVL_DEBUG, "ReloadChunkListCommand::run  adding resource: " << resource);

                try {
                    clusterManager->Added(resource.c_str());    // Notify XRootD/cmsd
                    _chunkInventory->add(db, chunk);            // Notify QServ
                } catch (std::exception const& ex) {
                    reportError("failed to add the chunk: " + std::string(ex.what()));
                    return;
                }

                // Notify the caller of this service
                proto::WorkerCommandUpdateChunkListR::Chunk* ptr = reply.add_added();
                ptr->set_db(db);
                ptr->set_chunk(chunk);
            }
        }
    }
    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

}}} // namespace lsst::qserv::wpublish
