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

/// TODO add fileId and row to this so it can be checked in _workerKeyInsertReq
struct ChunkSubchunk {
    ChunkSubchunk(int chunk_, int subchunk_) : chunk(chunk_), subchunk(subchunk_) {}
    int const chunk;
    int const subchunk;
    friend std::ostream& operator<<(std::ostream& os, ChunkSubchunk csc);
};


/// This class is 'central' to the execution of the program, and must be around
/// until the bitter end. As such, it can be accessed by normal pointers.
/// This class is central to loader workers and the master.
/// It is the base class for CentralWorker, CentralMaster, and CentralClient
class Central {
public:
    Central(boost::asio::io_service& ioService,
            std::string const& masterHostName, int masterPort)
        : _ioService(ioService), _masterAddr(masterHostName, masterPort),
          _checkDoListThread([this](){ _checkDoList(); }){}

    Central() = delete;

    virtual ~Central();

    void run();

    std::string getMasterHostName() const { return _masterAddr.ip; }
    int getMasterPort() const { return _masterAddr.port; }
    NetworkAddress getMasterAddr() { return _masterAddr; }

    uint64_t getNextMsgId() { return _sequence++; }

    int getErrCount() const { return _server->getErrCount(); }

    /// Send the contents of 'sendBuf' to 'host:port'
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

    NetworkAddress _masterAddr; ///< Network address of the master node.
    
    std::atomic<uint64_t> _sequence{1};

    util::CommandQueue::Ptr _queue{new util::CommandQueue()};
    util::ThreadPool::Ptr _pool{util::ThreadPool::newThreadPool(10, _queue)};

    std::vector<std::thread> _ioServiceThreads; ///< List of asio io threads created by this

    ServerUdpBase::Ptr _server;

    bool _loop{true};
    std::thread _checkDoListThread;
};


}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_H_
