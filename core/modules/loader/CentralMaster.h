// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2018 LSST Corporation.
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
#ifndef LSST_QSERV_LOADER_CENTRALMASTER_H_
#define LSST_QSERV_LOADER_CENTRALMASTER_H_

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/Central.h"



namespace lsst {
namespace qserv {
namespace loader {

class CentralMaster : public Central {
public:
    CentralMaster(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort)
        : Central(ioService, masterHostName, masterPort) {
        _server = std::make_shared<MasterServer>(_ioService, masterHostName, masterPort, this);
    }

    ~CentralMaster() override { _mWorkerList.reset(); }

    void setMaxKeysPerWorker(int val) { _maxKeysPerWorker = val; }
    int getMaxKeysPerWorker() const { return _maxKeysPerWorker; }

    void addWorker(std::string const& ip, int udpPort, int tcpPort); ///< Add a new worker to the system.
    void updateWorkerInfo(uint32_t workerName, NeighborsInfo const& nInfo, StringRange const& strRange);


    MWorkerListItem::Ptr getWorkerNamed(uint32_t name);

    MWorkerList::Ptr getWorkerList() const { return _mWorkerList; }

    void reqWorkerKeysInfo(uint64_t msgId, std::string const& ip, short port,
                          std::string const& ourHostName, short ourPort);

    std::string getOurLogId() override { return "master"; }

    void setWorkerNeighbor(MWorkerListItem::WPtr const& target, int message, uint32_t neighborName);

private:
    void _assignNeighborIfNeeded();
    std::mutex _assignMtx; ///< Protects critical region where worker's can be set to active.

    std::atomic<int> _maxKeysPerWorker{1000};


    MWorkerList::Ptr _mWorkerList{new MWorkerList(this)}; ///< List of workers.

    std::atomic<bool> _firstWorkerRegistered{false}; ///< True when one worker has been activated.
    std::atomic<bool> _addingWorker{false}; ///< True while adding a worker to the end of the list.
                                            // TODO maybe move _addingWorker to MWorkerList.
};

}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRALMASTER_H_
