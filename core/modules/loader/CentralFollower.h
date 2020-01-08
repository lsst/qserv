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
#include <atomic>
#include <thread>
#include <vector>

// third party headers
#include "boost/asio.hpp"

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

/// This class is used as a base central class for servers that need to get
/// lists of workers from the master.
/// CentralFollower provides no service on its own. The derived classes must:
///   call workerInfoReceive(data) to handle LoaderMsg::MAST_WORKER_INFO
///   call getWorkerList()->workerListReceive(data) to handle LoaderMsg::MAST_WORKER_LIST
/// messages sent from the master and call workerInfoReceive() as needed to
/// handle LoaderMsg::MAST_WORKER_INFO.
class CentralFollower : public Central {
public:
    typedef std::pair<CompositeKey, ChunkSubchunk> CompKeyPair;

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
    void startMonitoring() override;

    std::string const& getHostName() const { return _hostName; }
    int getUdpPort() const { return _udpPort; }

    /// Only workers have TCP ports.
    virtual int getTcpPort() const { return 0; }

    /// Receive information about workers from the master.
    bool workerInfoReceive(BufferUdp::Ptr const&  data);

    /// Returns a pointer to our worker list.
    WWorkerList::Ptr const& getWorkerList();

    std::string getOurLogId() const override { return "CentralFollower"; }

protected:
    /// Real workers need to check this for initial ranges.
    virtual void checkForThisWorkerValues(uint32_t wId, std::string const& ip,
                                          int portUdp, int portTcp, KeyRange& strRange) {};

private:
    const std::string        _hostName; ///< our host name
    const int                _udpPort;  ///< our UDP port

    /// This function is needed to fill the map. On real workers, CentralWorker
    /// needs to do additional work to set its own id.
    void _workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoBuf);

    /// Maps of workers with their key ranges and network addresses. due to
    /// lazy initialization.
    /// This should be accessed through getWorkerList()
    WWorkerList::Ptr _wWorkerList;
    std::mutex _wListInitMtx; ///< mutex for initialization of _wWorkerList
    std::atomic<bool> _destroy{false};
};

}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_CENTRAL_FOLLOWER_H


