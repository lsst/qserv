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
#include "wpublish/AddChunkGroupCommand.h"

// System headers

// Third-party headers
#include "XrdSsi/XrdSsiCluster.hh"

// LSST headers
#include "lsst/log/Log.h"
#include "wbase/SendChannel.h"
#include "wpublish/ChunkInventory.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

extern XrdSsiProvider* XrdSsiProviderLookup;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.AddChunkGroupCommand");

}  // namespace

namespace lsst::qserv::wpublish {

AddChunkGroupCommand::AddChunkGroupCommand(shared_ptr<wbase::SendChannel> const& sendChannel,
                                           shared_ptr<ChunkInventory> const& chunkInventory,
                                           mysql::MySqlConfig const& mySqlConfig, int chunk,
                                           vector<string> const& databases)
        : wbase::WorkerCommand(sendChannel),
          _chunkInventory(chunkInventory),
          _mySqlConfig(mySqlConfig),
          _chunk(chunk),
          _databases(databases) {}

void AddChunkGroupCommand::run() {
    string const context = "AddChunkGroupCommand::" + string(__func__) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (_databases.empty()) {
        reportError<proto::WorkerCommandChunkGroupR>(
                "the list of database names in the group was found empty",
                proto::WorkerCommandStatus::INVALID);
        return;
    }

    xrdsvc::SsiProviderServer* providerServer =
            dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup);
    XrdSsiCluster* clusterManager = providerServer->GetClusterManager();

    for (auto&& database : _databases) {
        string const resource = "/chk/" + database + "/" + to_string(_chunk);
        LOGS(_log, LOG_LVL_DEBUG,
             context << "  adding the chunk resource: " << resource
                     << " in DataContext=" << clusterManager->DataContext());
        try {
            // Notify XRootD/cmsd and (depending on a mode) modify the provider's copy
            // of the inventory.
            clusterManager->Added(resource.c_str());
            if (clusterManager->DataContext()) {
                providerServer->GetChunkInventory().add(database, _chunk);
            }
            // Notify QServ and update the database
            _chunkInventory->add(database, _chunk, _mySqlConfig);

        } catch (InvalidParamError const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>(ex.what(), proto::WorkerCommandStatus::INVALID);
            return;
        } catch (QueryError const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>(ex.what());
            return;
        } catch (exception const& ex) {
            reportError<proto::WorkerCommandChunkGroupR>("failed to add the chunk: " + string(ex.what()));
            return;
        }
    }
    proto::WorkerCommandChunkGroupR reply;
    reply.mutable_status();
    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SENT **");
}

}  // namespace lsst::qserv::wpublish
