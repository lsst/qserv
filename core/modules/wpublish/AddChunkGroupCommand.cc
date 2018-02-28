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

/******************************************************************************/
/*                               G l o b a l s                                */
/******************************************************************************/

extern XrdSsiProvider* XrdSsiProviderLookup;


// Qserv headers

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.AddChunkGroupCommand");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

AddChunkGroupCommand::AddChunkGroupCommand(std::shared_ptr<wbase::SendChannel> const& sendChannel,
                                           std::shared_ptr<ChunkInventory>     const& chunkInventory,
                                           mysql::MySqlConfig                  const& mySqlConfig,
                                           int chunk,
                                           std::vector<std::string> const& databases)
    :   wbase::WorkerCommand(sendChannel),
        _chunkInventory(chunkInventory),
        _mySqlConfig   (mySqlConfig),
        _chunk(chunk),
        _databases(databases) {
}

AddChunkGroupCommand::~AddChunkGroupCommand() {
}

void
AddChunkGroupCommand::reportError(proto::WorkerCommandChunkGroupR::Status status,
                                  std::string const& message) {

    LOGS(_log, LOG_LVL_ERROR, "AddChunkGroupCommand::reportError  " << message);

    proto::WorkerCommandChunkGroupR reply;

    reply.set_status(status);
    reply.set_error (message);

    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

void
AddChunkGroupCommand::run() {

    LOGS(_log, LOG_LVL_DEBUG, "AddChunkGroupCommand::run");

    if (not _databases.size()) {
        reportError(proto::WorkerCommandChunkGroupR::INVALID,
                    "the list of database names in the group was found empty");
        return;
    }

    xrdsvc::SsiProviderServer* providerServer = dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup);
    XrdSsiCluster*             clusterManager = providerServer->GetClusterManager();

    proto::WorkerCommandChunkGroupR reply;
    reply.set_status(proto::WorkerCommandChunkGroupR::SUCCESS);

    for (std::string const& database: _databases) {

        std::string const resource = "/chk/" + database + "/" + std::to_string(_chunk);

        LOGS(_log, LOG_LVL_DEBUG, "AddChunkGroupCommand::run  adding the chunk resource: "
             << resource << " in DataContext=" << clusterManager->DataContext());

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
            reportError(proto::WorkerCommandChunkGroupR::INVALID, ex.what());
            return;
        } catch (QueryError const& ex) {
            reportError(proto::WorkerCommandChunkGroupR::ERROR, ex.what());
            return;
        } catch (std::exception const& ex) {
            reportError(proto::WorkerCommandChunkGroupR::ERROR,
                        "failed to add the chunk: " + std::string(ex.what()));
            return;
        }
    }
    _frameBuf.serialize(reply);
    _sendChannel->sendStream(_frameBuf.data(), _frameBuf.size(), true);
}

}}} // namespace lsst::qserv::wpublish
