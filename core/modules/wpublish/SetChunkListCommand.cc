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
#include "wpublish/SetChunkListCommand.h"

// Third-party headers
#include "XrdSsi/XrdSsiCluster.hh"

// Qserv headers
#include "wbase/SendChannel.h"
#include "wpublish/ResourceMonitor.h"
#include "xrdsvc/SsiProvider.h"
#include "xrdsvc/XrdName.h"

// LSST headers
#include "lsst/log/Log.h"

extern XrdSsiProvider* XrdSsiProviderLookup;

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.SetChunkListCommand");

} // ann\onymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

SetChunkListCommand::SetChunkListCommand(shared_ptr<wbase::SendChannel> const& sendChannel,
                                         shared_ptr<ChunkInventory> const& chunkInventory,
                                         shared_ptr<ResourceMonitor> const& resourceMonitor,
                                         mysql::MySqlConfig const& mySqlConfig,
                                         vector<SetChunkListCommand::Chunk> const& chunks,
                                         bool force)
    :   wbase::WorkerCommand(sendChannel),
        _chunkInventory(chunkInventory),
        _resourceMonitor(resourceMonitor),
        _mySqlConfig(mySqlConfig),
        _chunks(chunks),
        _force(force) {
}


void SetChunkListCommand::_setChunks(proto::WorkerCommandSetChunkListR& reply,
                                     ChunkInventory::ExistMap const& prevExistMap) {

    for (auto const& entry: prevExistMap) {
        string const& database = entry.first;

        for (int chunk: entry.second) {
            proto::WorkerCommandChunk* ptr = reply.add_chunks();
            ptr->set_db(database);
            ptr->set_chunk(chunk);
            ptr->set_use_count(_resourceMonitor->count(chunk, database));
        }
    }
}


void SetChunkListCommand::_reportError(proto::WorkerCommandSetChunkListR::Status status,
                                       string const& message,
                                       ChunkInventory::ExistMap const& prevExistMap) {

    LOGS(_log, LOG_LVL_ERROR, "SetChunkListCommand::" << __func__ << "  " << message);

    proto::WorkerCommandSetChunkListR reply;

    reply.set_status(status);
    reply.set_error(message);
    _setChunks(reply, prevExistMap);

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    auto streamBuffer = xrdsvc::StreamBuffer::createWithMove(str);
    _sendChannel->sendStream(streamBuffer, true);
}


void SetChunkListCommand::run() {

    string const context = "SetChunkListCommand::" + string(__func__) + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    // Store the current collection of chunks
    ChunkInventory::ExistMap const prevExistMap = _chunkInventory->existMap();

    // Build a temporary object representing a desired chunk list and
    // compare it with the present one.
    ChunkInventory::ExistMap newExistMap;
    for (Chunk const& chunkEntry: _chunks) {
        newExistMap[chunkEntry.database].insert(chunkEntry.chunk);
    }
    ChunkInventory const newChunkInventory(newExistMap,
                                           _chunkInventory->name(),
                                           _chunkInventory->id());

    ChunkInventory::ExistMap const toBeRemovedExistMap =  *_chunkInventory - newChunkInventory;
    ChunkInventory::ExistMap const toBeAddedExistMap   = newChunkInventory -  *_chunkInventory;

    // Make sure none of the chunks in the 'to be removed' group is not being used
    // unless in the 'force' mode
    if (not _force) {
        for (auto const& entry: toBeRemovedExistMap) {
            string const& database = entry.first;
            for (auto chunk: entry.second) {
                if (_resourceMonitor->count(chunk, database)) {
                    _reportError(proto::WorkerCommandSetChunkListR::IN_USE,
                                 "some chunks of the group are in use",
                                prevExistMap);
                    return;
                }
            }
        }
    }

    // Begin making desired adjustments to the current inventory

    xrdsvc::SsiProviderServer* providerServer = dynamic_cast<xrdsvc::SsiProviderServer*>(XrdSsiProviderLookup);
    XrdSsiCluster*             clusterManager = providerServer->GetClusterManager();

    for (auto const& entry: toBeRemovedExistMap) {
        string const& database = entry.first;

        for (auto chunk: entry.second) {

            string const resource = "/chk/" + database + "/" + to_string(chunk);

            LOGS(_log, LOG_LVL_DEBUG, context << "removing the chunk resource: "
                 << resource << " in DataContext=" << clusterManager->DataContext());

            try {
                // Notify XRootD/cmsd and (depending on a mode) modify the provider's copy
                // of the inventory.
                clusterManager->Removed(resource.c_str());
                if (clusterManager->DataContext()) {
                    providerServer->GetChunkInventory().remove(database, chunk);
                }

                // Notify QServ and update the database
                _chunkInventory->remove(database, chunk, _mySqlConfig);

            } catch (InvalidParamError const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::INVALID,
                             ex.what(),
                             prevExistMap);
                return;
            } catch (QueryError const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::ERROR,
                             ex.what(),
                             prevExistMap);
                return;
            } catch (exception const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::ERROR,
                             "failed to remove the chunk: " + string(ex.what()),
                             prevExistMap);
                return;
            }
        }
    }
    for (auto const& entry: toBeAddedExistMap) {
        string const& database = entry.first;

        for (auto chunk: entry.second) {

            string const resource = "/chk/" + database + "/" + to_string(chunk);

            LOGS(_log, LOG_LVL_DEBUG, context << "adding the chunk resource: "
                 << resource << " in DataContext=" << clusterManager->DataContext());

            try {
                // Notify XRootD/cmsd and (depending on a mode) modify the provider's copy
                // of the inventory.
                clusterManager->Added(resource.c_str());
                if (clusterManager->DataContext()) {
                    providerServer->GetChunkInventory().add(database, chunk);
                }

                // Notify QServ and update the database
                _chunkInventory->add(database, chunk, _mySqlConfig);

            } catch (InvalidParamError const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::INVALID,
                             ex.what(),
                             prevExistMap);
                return;
            } catch (QueryError const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::ERROR,
                             ex.what(),
                             prevExistMap);
                return;
            } catch (exception const& ex) {
                _reportError(proto::WorkerCommandSetChunkListR::ERROR,
                             "failed to add the chunk: " + string(ex.what()),
                             prevExistMap);
                return;
            }
        }
    }

    // Send back a reply
    proto::WorkerCommandSetChunkListR reply;
    reply.set_status(proto::WorkerCommandSetChunkListR::SUCCESS);
    _setChunks(reply, prevExistMap);

    _frameBuf.serialize(reply);
    string str(_frameBuf.data(), _frameBuf.size());
    _sendChannel->sendStream(xrdsvc::StreamBuffer::createWithMove(str), true);

    LOGS(_log, LOG_LVL_DEBUG, context << "** SENT **");
}

}}} // namespace lsst::qserv::wpublish
