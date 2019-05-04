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
#include "wpublish/ChunkInventory.h"
#include "wpublish/ResourceMonitor.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

// LSST headers
#include "lsst/log/Log.h"

extern XrdSsiProvider* XrdSsiProviderLookup;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.RemoveChunkGroupCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

RemoveChunkGroupCommand::RemoveChunkGroupCommand(shared_ptr<wbase::SendChannel> const& sendChannel,
                                                 shared_ptr<ChunkInventory> const& chunkInventory,
                                                 shared_ptr<ResourceMonitor> const& resourceMonitor,
                                                 mysql::MySqlConfig const& mySqlConfig,
                                                 int chunk,
                                                 vector<string> const& dbs,
                                                 bool force)
    :   wbase::WorkerCommand(sendChannel),
        _chunkInventory(chunkInventory),
        _resourceMonitor(resourceMonitor),
        _mySqlConfig(mySqlConfig),
        _chunk(chunk),
        _dbs(dbs),
        _force(force) {
}


void RemoveChunkGroupCommand::_reportError(proto::WorkerCommandChunkGroupR::Status status,
                                           string const& message) {

    LOGS(_log, LOG_LVL_ERROR, "RemoveChunkGroupCommand::" << __func__ << "  " << message);

    proto::WorkerCommandChunkGroupR reply;

    reply.set_status(status);
    reply.set_error(message);

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    auto streamBuffer = xrdsvc::StreamBuffer::createWithMove(str);
    _sendChannel->sendStream(streamBuffer, true);
}


void RemoveChunkGroupCommand::run() {

    string const context = "RemoveChunkGroupCommand::" + string(__func__) + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _dbs.size()) {
        _reportError(proto::WorkerCommandChunkGroupR::INVALID,
                     "the list of database names in the group was found empty");
        return;
    }

    // Make sure none of the chunks in the group is not being used
    // unless in the 'force' mode
    if (not _force) {
        if (_resourceMonitor->count(_chunk, _dbs)) {
            _reportError(proto::WorkerCommandChunkGroupR::IN_USE,
                         "some chunks of the group are in use");
            return;
        }
    }

    xrdsvc::SsiProviderServer* providerServer = dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup);
    XrdSsiCluster*             clusterManager = providerServer->GetClusterManager();

    for (string const& db: _dbs) {

        string const resource = "/chk/" + db + "/" + to_string(_chunk);

        LOGS(_log, LOG_LVL_DEBUG, context << "removing the chunk resource: "
             << resource << " in DataContext=" << clusterManager->DataContext());

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
            _reportError(proto::WorkerCommandChunkGroupR::INVALID, ex.what());
            return;
        } catch (QueryError const& ex) {
            _reportError(proto::WorkerCommandChunkGroupR::ERROR, ex.what());
            return;
        } catch (exception const& ex) {
            _reportError(proto::WorkerCommandChunkGroupR::ERROR,
                         "failed to remove the chunk: " + string(ex.what()));
            return;
        }
    }

    proto::WorkerCommandChunkGroupR reply;
    if (_resourceMonitor->count(_chunk, _dbs)) {

        // Tell a caller that some of the associated resources are still
        // in use by this worker even though they've been blocked from use for any
        // further requests. It's up to a caller of this service to correctly
        // interpret the effect of the operation based on a presence of the "force"
        // flag in the request.

        reply.set_status(proto::WorkerCommandChunkGroupR::IN_USE);
        reply.set_error("some chunks of the group are in use");

    } else{
        reply.set_status(proto::WorkerCommandChunkGroupR::SUCCESS);
    }

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SENT **");
}

}}} // namespace lsst::qserv::wpublish
