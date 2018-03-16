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
#ifndef LSST_QSERV_LOADER_CENTRAL_WORKER_H_
#define LSST_QSERV_LOADER_CENTRAL_WORKER_H_

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


/// Class to describe one of this worker's neighbor.
class Neighbor {
public:
    Neighbor() = default;

    void setAddress(std::string const& hostName, int port) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _address.reset(new NetworkAddress(hostName, port));
    }

    void setAddress(NetworkAddress const& addr) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _address.reset(new NetworkAddress(addr));
    }

    NetworkAddress getAddress() {
        std::lock_guard<std::mutex> lck(_nMtx);
        return *_address;
    }

    void setName(uint32_t name) {
        std::lock_guard<std::mutex> lck(_nMtx);
        if (_name != name) {
            _established = false;
            _address.reset(new NetworkAddress("", -1));
        }
        _name = name;
    }

    uint32_t getName() const { return _name; }

    void setEstablished(bool val) {
        std::lock_guard<std::mutex> lck(_nMtx);
        _established = val;
    }

    bool getEstablished() const { return _established; }

private:
    NetworkAddress::UPtr _address{new NetworkAddress("", -1)};
    uint32_t _name{0}; ///< Name of neighbor, 0 means no neighbor.
    bool _established{false};
    std::mutex _nMtx;
};


class CentralWorker : public Central {
public:
    CentralWorker(boost::asio::io_service& ioService,
                  std::string const& masterHostName, int masterPort,
                  std::string const& hostName,       int port)
        : Central(ioService, masterHostName, masterPort),
          _hostName(hostName), _port(port) {
        _server = std::make_shared<WorkerServer>(_ioService, _hostName, _port, this);
        _monitorWorkers();
    }

    ~CentralWorker() override { _wWorkerList.reset(); }

    WWorkerList::Ptr getWorkerList() const { return _wWorkerList; }

    std::string getHostName() const { return _hostName; }
    int getPort() const { return _port; }

    void registerWithMaster();

    bool workerInfoReceive(BufferUdp::Ptr const&  data); // &&& spelling
    bool workerKeyInsertReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data);
    bool workerKeyInfoReq(LoaderMsg const& inMsg, BufferUdp::Ptr const&  data);
    bool workerWorkerKeysInfoReq(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);
    bool workerWorkerSetRightNeighbor(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);
    bool workerWorkerSetLeftNeighbor(LoaderMsg const& inMsg, BufferUdp::Ptr const& data);

    bool isOurNameInvalid() const {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        return _ourNameInvalid;
    }

    bool setOurName(uint32_t name) {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        if (_ourNameInvalid) {
            _ourName = name;
            _ourNameInvalid = false;
            return true;
        } else {
            /// &&& add error message, check if _ourname matches name
            return false;
        }
    }

    uint32_t getOurName() const {
        std::lock_guard<std::mutex> lck(_ourNameMtx);
        return _ourName;
    }

    /// &&& TODO this is only needed for initial testing and should be deleted.
    std::string getOurLogId() override;

    void testSendBadMessage();

private:
    void _registerWithMaster();
    void _monitorWorkers();

    void _workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoBuf);

    void _workerKeyInsertReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf);
    void _forwardKeyInsertRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                  std::unique_ptr<proto::KeyInfoInsert> const& protoData);

    void _workerKeyInfoReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf);
    void _forwardKeyInfoRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                std::unique_ptr<proto::KeyInfoInsert> const& protoData);

    void _workerWorkerKeysInfoReq(LoaderMsg const& inMsg);

    void _removeOldEntries();

    bool _connectToLeftNeighbor(uint32_t neighborLeftName);


    const std::string _hostName;
    const int         _port;
    WWorkerList::Ptr _wWorkerList{new WWorkerList(this)};

    bool _ourNameInvalid{true}; ///< true until the name has been set by the master.
    uint32_t _ourName; ///< name given to us by the master
    mutable std::mutex _ourNameMtx; ///< protects _ourNameInvalid, _ourName


    StringRange _strRange; ///< range for this worker TODO _range both int and string;
    std::map<std::string, ChunkSubchunk> _directorIdMap;
    std::deque<std::chrono::system_clock::time_point> _recentAdds; ///< track how many keys added recently.
    std::chrono::milliseconds _recent{60 * 1000};
    std::mutex _idMapMtx; ///< protects _strRange, _directorIdMap, _recentAdds

    Neighbor _neighborLeft;
    Neighbor _neighborRight;

};


}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_WORKER_H_

