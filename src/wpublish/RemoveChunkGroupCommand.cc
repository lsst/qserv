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

// Third-party headers
#include "XrdSsi/XrdSsiCluster.hh"

// Qserv headers
#include "wbase/SendChannel.h"
#include "wcontrol/ResourceMonitor.h"
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

// LSST headers
#include "lsst/log/Log.h"

extern XrdSsiProvider* XrdSsiProviderLookup;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.RemoveChunkGroupCommand");

}  // namespace

namespace lsst::qserv::wpublish {

RemoveChunkGroupCommand::RemoveChunkGroupCommand(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<ChunkInventory> const& chunkInventory,
                                                 shared_ptr<wcontrol::ResourceMonitor> const& resourceMonitor,
                                                 mysql::MySqlConfig const& mySqlConfig, int chunk,
                                                 vector<string> const& dbs, bool force)
        : wbase::WorkerCommand(sendChannel),
          _chunkInventory(chunkInventory),
          _resourceMonitor(resourceMonitor),
          _mySqlConfig(mySqlConfig),
          _chunk(chunk),
          _dbs(dbs),
          _force(force) {}

void RemoveChunkGroupCommand::run() {
    string const context = "RemoveChunkGroupCommand::" + string(__func__) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (_dbs.empty()) {
        reportError<proto::WorkerCommandChunkGroupR>(
                "the list of database names in the group was found empty",
                proto::WorkerCommandStatus::INVALID);
        return;
    }

    // Make sure none of the chunks in the group is not being used
    // unless in the 'force' mode
    if (!_force) {
        if (_resourceMonitor->count(_chunk, _dbs)) {
            reportError<proto::WorkerCommandChunkGroupR>("some chunks of the group are in use",
                                                         proto::WorkerCommandStatus::IN_USE);
            return;
        }
    }

    xrdsvc::SsiProviderServer* providerServer =
            dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup);
    XrdSsiCluster* clusterManager = providerServer->GetClusterManager();

    for (string const& db : _dbs) {
        string const resource = "/chk/" + db + "/" + to_string(_chunk);

        LOGS(_log, LOG_LVL_DEBUG,
             context << "removing the chunk resource: " << resource
                     << " in DataContext=" << clusterManager->DataContext());

        try {
            // Notify XRootD/cmsd and (depending on a mode) modify the provider's copy
            // of the inventory.
            clusterManager->Removed(resource.c_str());
            if (clusterManager->DataContext()) {
                providerServer->GetChunkInventory().remove(db, _chunk);
            }

            // Notify QServ and update the database
            _chunkInventory->remove(db, _chunk, _mySqlConfig);

        } catch (InvalidParamError const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>(ex.what(), proto::WorkerCommandStatus::INVALID);
            return;
        } catch (QueryError const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>(ex.what());
            return;
        } catch (exception const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>("failed to remove the chunk: " + string(ex.what()));
            return;
        }
    }
    if (_resourceMonitor->count(_chunk, _dbs)) {
        // Tell a caller that some of the associated resources are still
        // in use by this worker even though they've been blocked from use for any
        // further requests. It's up to a caller of this service to correctly
        // interpret the effect of the operation based on a presence of the "force"
        // flag in the request.
        reportError<proto::WorkerCommandChunkGroupR>("some chunks of the group are in use",
                                                     proto::WorkerCommandStatus::IN_USE);
        return;
    }
    proto::WorkerCommandChunkGroupR reply;
    reply.mutable_status();
    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SENT **");
}

}  // namespace lsst::qserv::wpublish
