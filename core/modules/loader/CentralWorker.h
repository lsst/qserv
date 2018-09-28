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
#include "loader/DoList.h"
#include "loader/Neighbor.h"
#include "loader/ServerTcpBase.h"


namespace lsst {
namespace qserv {

namespace proto {
class WorkerKeysInfo;
}

namespace loader {


class CentralWorkerDoListItem;


class CentralWorker : public Central {
public:
    enum SocketStatus {
        VOID0 = 0,
        STARTING1,
        ESTABLISHED2
    };

    CentralWorker(boost::asio::io_service& ioService,
                  std::string const& masterHostName,   int masterPort,
                  std::string const& hostName,         int port,
                  boost::asio::io_context& io_context, int tcpPort);

    ~CentralWorker() override;

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

    friend CentralWorkerDoListItem;

private:
    void _registerWithMaster();
    void _startMonitoring();

    void _monitor();

    void _workerInfoReceive(std::unique_ptr<proto::WorkerListItem>& protoBuf);

    void _workerKeyInsertReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf);
    void _forwardKeyInsertRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                  std::unique_ptr<proto::KeyInfoInsert> const& protoData);

    void _workerKeyInfoReq(LoaderMsg const& inMsg, std::unique_ptr<proto::KeyInfoInsert>& protoBuf);
    void _forwardKeyInfoRequest(WWorkerListItem::Ptr const& target, LoaderMsg const& inMsg,
                                std::unique_ptr<proto::KeyInfoInsert> const& protoData);

    std::unique_ptr<proto::WorkerKeysInfo> _workerKeysInfoBuilder();
    static void _workerKeysInfoExtractor(BufferUdp& data, uint32_t& name, NeighborsInfo& nInfo, StringRange& strRange);

    void _workerWorkerKeysInfoReq(LoaderMsg const& inMsg);

    void _removeOldEntries();

    void _rightConnect();
    void _rightDisconnect();

    bool _connectToLeftNeighbor(uint32_t neighborLeftName);


    const std::string        _hostName;
    const int                _port;
    boost::asio::io_context& _ioContext;
    const int                _tcpPort;

    WWorkerList::Ptr _wWorkerList{new WWorkerList(this)};

    bool _ourNameInvalid{true}; ///< true until the name has been set by the master.
    uint32_t _ourName{0}; ///< name given to us by the master, 0 invalid name.
    mutable std::mutex _ourNameMtx; ///< protects _ourNameInvalid, _ourName


    StringRange _strRange; ///< range for this worker TODO _range both int and string;
    std::map<std::string, ChunkSubchunk> _directorIdMap;
    std::deque<std::chrono::system_clock::time_point> _recentAdds; ///< track how many keys added recently.
    std::chrono::milliseconds _recent{60 * 1000};
    std::mutex _idMapMtx; ///< protects _strRange, _directorIdMap, _recentAdds

    Neighbor _neighborLeft{Neighbor::LEFT};
    Neighbor _neighborRight{Neighbor::RIGHT};
    ServerTcpBase::Ptr _tcpServer; // For our right neighbor to connect to us.

    std::mutex _rightMtx;
    SocketStatus _rightConnectStatus{VOID0};
    std::shared_ptr<tcp::socket>  _rightSocket;

    std::shared_ptr<CentralWorkerDoListItem> _centralWorkerDoListItem;
};


/// This class exists to regularly call the CentralWorker::_monitor() function, which
/// does things like monitor TCP connections.
class CentralWorkerDoListItem : public DoListItem {
public:
    CentralWorkerDoListItem() = delete;
    explicit CentralWorkerDoListItem(CentralWorker* centralWorker) : _centralWorker(centralWorker) {}

    util::CommandTracked::Ptr createCommand() override {
        struct CWMonitorCmd : public util::CommandTracked {
            CWMonitorCmd(CentralWorker* centralW) : centralWorker(centralW) {}
            void action(util::CmdData*) override {
                centralWorker->_monitor();
            }
            CentralWorker* centralWorker;
        };
        util::CommandTracked::Ptr cmd(new CWMonitorCmd(_centralWorker));
        return cmd;
    }

private:
    CentralWorker* _centralWorker;
};


}}} // namespace lsst::qserv::loader


#endif // LSST_QSERV_LOADER_CENTRAL_WORKER_H_

