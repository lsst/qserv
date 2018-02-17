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
#include "wpublish/RemoveChunkGroupCommand.h"

// System headers

// Third-party headers
#include "XrdSsi/XrdSsiCluster.hh"

// LSST headers
#include "lsst/log/Log.h"
#include "wbase/SendChannel.h"
#include "wpublish/ChunkInventory.h"
#include "wpublish/ResourceMonitor.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

/******************************************************************************/
/*                               G l o b a l s                                */
/******************************************************************************/

extern XrdSsiProvider* XrdSsiProviderLookup;


// Qserv headers

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.RemoveChunkGroupCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

RemoveChunkGroupCommand::RemoveChunkGroupCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 std::shared_ptr<ChunkInventory> const& chunkInventory,
                                                 std::shared_ptr<ResourceMonitor> const& resourceMonitor,
                                                 int chunk,
                                                 std::vector<std::string> const& dbs,
                                                 bool force)
    :   wbase::WorkerCommand(sendChannel),

        _chunkInventory  (chunkInventory),
        _resourceMonitor (resourceMonitor),
        _chunk (chunk),
        _dbs   (dbs),
        _force (force) {
}

RemoveChunkGroupCommand::~RemoveChunkGroupCommand() {
}

void
RemoveChunkGroupCommand::reportError(proto::WorkerCommandChunkGroupR::Status status,
                                     std::string const& message) {

    LOGS(_log, LOG_LVL_ERROR, "RemoveChunkGroupCommand::reportError  " << message);

    proto::WorkerCommandChunkGroupR reply;

    reply.set_status(status);
    reply.set_error (message);

    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

void
RemoveChunkGroupCommand::run() {

    LOGS(_log, LOG_LVL_DEBUG, "RemoveChunkGroupCommand::run");

    if (not _dbs.size()) {
        reportError(proto::WorkerCommandChunkGroupR::INVALID,
                    "the list of database names in the group was found empty");
        return;
    }

    // Make sure none of the chunks in the group is not being used
    // unless in the 'force' mode
    if (not _force) {
        if (resourceMonitor->count(_chunk, _dbs)) {
            reportError(proto::WorkerCommandChunkGroupR::IN_USE,
                        "some chunks of the group are in use");
            return;
        }
    }

    XrdSsiCluster* clusterManager =
        dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup)->GetClusterManager();

    proto::WorkerCommandChunkGroupR reply;
    reply.set_status(proto::WorkerCommandChunkGroupR::SUCCESS);

    for (std::string const& db: _dbs) {

        std::string const resource = "/chk/" + db + "/" + std::to_string(_chunk);

        LOGS(_log, LOG_LVL_DEBUG, "RemoveChunkGroupCommand::run  removing resource: " << resource);

        try {
            clusterManager->Removed(resource.c_str());    // Notify XRootD/cmsd
            _chunkInventory->remove(db, _chunk);          // Notify QServ
        } catch (std::exception const& ex) {
            reportError(proto::WorkerCommandChunkGroupR::ERROR,
                        "failed to remove the chunk: " + std::string(ex.what()));
            return;
        }
    }
    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

}}} // namespace lsst::qserv::wpublish
