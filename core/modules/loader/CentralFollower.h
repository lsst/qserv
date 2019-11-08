// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2019 LSST Corporation.
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
 *
 */
#ifndef LSST_QSERV_LOADER_CENTRAL_FOLLOWER_H
#define LSST_QSERV_LOADER_CENTRAL_FOLLOWER_H

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/Central.h"
#include "loader/DoList.h"
#include "loader/Neighbor.h"
#include "loader/ServerTcpBase.h"
#include "loader/WorkerConfig.h"


namespace lsst {
namespace qserv {

namespace proto {
class WorkerKeysInfo;
}

namespace loader {


// &&& class CentralWorkerDoListItem;

/// This class is used a base central class for servers that need to get
/// lists of of worker from the master.
/// CentralFollower provides no service on its own. The derived classes must:
///   call workerInfoReceive(data) to handle LoaderMsg::MAST_WORKER_INFO
///   call getWorkerList()->workerListReceive(data) to handle LoaderMsg::MAST_WORKER_LIST
/// messages sent from the master and call workerInfoReceive() as needed to
/// handle LoaderMsg::MAST_WORKER_INFO.
class CentralFollower : public Central {
public:
    typedef std::pair<CompositeKey, ChunkSubchunk> CompKeyPair;

    /* &&&
    enum SocketStatus {
        VOID0 = 0,
        STARTING1,
        ESTABLISHED2
    };

    enum Direction {
        NONE0 = 0,
        TORIGHT1,
        FROMRIGHT2
    };
    */

    CentralFollower(boost::asio::io_service& ioService,
                    std::string const& hostName_, std::string const& masterHost, int masterPortUdp,
                    int threadPoolSize, int loopSleepTime, int ioThreads, int fPortUdp)
        : Central(ioService, masterHost, masterPortUdp, threadPoolSize, loopSleepTime, ioThreads),
        _hostName(hostName_),
        _udpPort(fPortUdp){
    }

    ~CentralFollower() override;

    /// CentralFollower provides no service on its own. The derived classes must handle
    /// messages sent from the master and call workerInfoReceive() as needed.
    // void startServices() override;
    void startMonitoring() override;

    WWorkerList::Ptr getWorkerList() const { return _wWorkerList; }

    std::string const& getHostName() const { return _hostName; }
    int getUdpPort() const { return _udpPort; }
    virtual int getTcpPort() const { return 0; }

    /////////////////////////////////////////////////////////////////////////////////
    /// Methods to handle messages received from other servers.
    /// 'inMsg' contains information about the originator of a request
    ///         and the type of message.
    /// 'data'  contains the message data.

    /// Receive information about workers from the master.
    bool workerInfoReceive(BufferUdp::Ptr const&  data);

    std::string getOurLogId() const override { return "CentralFollower"; }

    // &&& friend CentralWorkerDoListItem;

protected: // &&& make some or all private again

    /// Real workers need to check this for initial ranges.
    virtual void checkForThisWorkerValues(uint32_t wId, std::string const& ip,
                                          int portUdp, int portTcp, KeyRange& strRange) {};
    /// &&& This function is needed to fill the map. On real workers, CentralWorker
    /// needs to do additional work to set its id.
    void _workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoBuf);

    /// See workerWorkerKeysInfoReq(...)
    // &&& void _workerWorkerKeysInfoReq(LoaderMsg const& inMsg);

    const std::string        _hostName;
    const int                _udpPort;
    // &&& const int                _tcpPort;

    WWorkerList::Ptr _wWorkerList{std::make_shared<WWorkerList>(this)}; ///< Maps of workers.

    /// The DoListItem that makes sure _monitor() is run. &&& needs to ask master for worker map occasionally
    // &&& replace with item to refresh _wWorkerList (see CentralWorker::_startMonitoring)// std::shared_ptr<CentralWorkerDoListItem> _centralWorkerDoListItem;
};





}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_CENTRAL_FOLLOWER_H


