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
#ifndef LSST_QSERV_LOADER_CENTRAL_H_
#define LSST_QSERV_LOADER_CENTRAL_H_

// system headers
#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <thread>
#include <vector>

// Qserv headers
#include "loader/ClientServer.h"
#include "loader/MasterServer.h"
#include "loader/MWorkerList.h"
#include "loader/WWorkerList.h"
#include "loader/WorkerServer.h"
#include "proto/loader.pb.h"
#include "util/ThreadPool.h"



namespace lsst {
namespace qserv {
namespace loader {

/// &&& TODO add fileId and row to this so it can be checked in _workerKeyInsertReq ?
struct ChunkSubchunk {
    ChunkSubchunk(int chunk_, int subchunk_) : chunk(chunk_), subchunk(subchunk_) {}
    int const chunk;
    int const subchunk;
    friend std::ostream& operator<<(std::ostream& os, ChunkSubchunk csc);
};


/// &&& Initially, just setting this up as Central for the worker, but may work for both Worker and Master.
/// &&& This class is central to loader workers and the master.
/// &&& It is the base class for WorkerCentral and MasterCentral
// This class is 'central' to the execution of the program, and must be around
// until the bitter end. As such, it can be accessed by normal pointers.
class Central {
public:
    Central(boost::asio::io_service& ioService,
            std::string const& masterHostName, int masterPort)
        : _ioService(ioService), _masterHostName(masterHostName), _masterPort(masterPort),
          _checkDoListThread([this](){ _checkDoList(); }){}

    Central() = delete;

    virtual ~Central();

    void run();

    std::string getMasterHostName() const { return _masterHostName; }
    int getMasterPort() const { return _masterPort; }

    uint64_t getNextMsgId() { return _sequence++; }

    int getErrCount() const { return _server->getErrCount(); }

    // WorkerList::Ptr getWorkerList() const { return _workerList; } &&&

    void sendBufferTo(std::string const& host, int port, BufferUdp& sendBuf) {
        _server->sendBufferTo(host, port, sendBuf);
    }

    // Only allow tracked commands on the queue
    void queueCmd(util::CommandTracked::Ptr const& cmd) {
        _queue->queCmd(cmd);
    }

    bool addDoListItem(DoListItem::Ptr const& item) {
        return _doList.addItem(item);
    }

    bool runAndAddDoListItem(DoListItem::Ptr const& item) {
        _doList.runItemNow(item);
        return _doList.addItem(item);
    }


    virtual std::string getOurLogId() { return "baseclass"; }

protected:
    /// Repeatedly check the items on the _doList.
    void _checkDoList();

    boost::asio::io_service& _ioService;

    /// Initialization order is important.
    DoList _doList{*this}; ///< List of items to be checked at regular intervals.

    std::string _masterHostName;    // &&& struct to keep hostName + port (WorkerList.h->NetworkAddress?)
    int _masterPort;
    // WorkerList::Ptr _workerList{new WorkerList(this)}; // &&& May not need to be a pointer. &&&
    
    std::atomic<uint64_t> _sequence{1};

    util::CommandQueue::Ptr _queue{new util::CommandQueue()};
    util::ThreadPool::Ptr _pool{util::ThreadPool::newThreadPool(10, _queue)};

    std::vector<std::thread> _ioServiceThreads; ///< List of asio io threads created by this

    ServerUdpBase::Ptr _server;

    bool _loop{true};
    std::thread _checkDoListThread;
};


#if 0 // &&&
class CentralMaster : public Central {
public:
    CentralMaster(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort)
        : Central(ioService, masterHostName, masterPort) {
        _server = std::make_shared<MasterServer>(_ioService, masterHostName, masterPort, this);
    }

    ~CentralMaster() override { _mWorkerList.reset(); }

    int getMaxKeysPerWorker() const { return _maxKeysPerWorker; }

    void addWorker(std::string const& ip, int port); ///< Add a new worker to the system.
    void updateNeighbors(uint32_t workerName, NeighborsInfo const& nInfo);


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
    std::atomic<bool> _addingWorker{false}; ///< True while adding a worker to the end of the list. &&& This should probably really be handled by states in the list of workers. &&&
};
#endif



}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_H_
