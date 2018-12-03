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
#ifndef LSST_QSERV_LOADER_WWORKERLIST_H_
#define LSST_QSERV_LOADER_WWORKERLIST_H_

// system headers
#include <atomic>
#include <map>
#include <memory>
#include <mutex>

// Qserv headers
#include "loader/BufferUdp.h"
#include "loader/DoList.h"
#include "loader/NetworkAddress.h"
#include "loader/StringRange.h"


namespace lsst {
namespace qserv {
namespace loader {

class Central;
class CentralWorker;
class CentralMaster;
class LoaderMsg;


/// Standard information for a single worker, IP address, key range, timeouts.
class WWorkerListItem : public std::enable_shared_from_this<WWorkerListItem> {
public:
    using Ptr = std::shared_ptr<WWorkerListItem>;
    using WPtr = std::weak_ptr<WWorkerListItem>;

    static WWorkerListItem::Ptr create(uint32_t name, CentralWorker *central) {
        return WWorkerListItem::Ptr(new WWorkerListItem(name, central));
    }
    static WWorkerListItem::Ptr create(uint32_t name, NetworkAddress const& address, CentralWorker *central) {
        return WWorkerListItem::Ptr(new WWorkerListItem(name, address, central));
    }


    WWorkerListItem() = delete;
    WWorkerListItem(WWorkerListItem const&) = delete;
    WWorkerListItem& operator=(WWorkerListItem const&) = delete;

    virtual ~WWorkerListItem() = default;

    void setUdpAddress(std::string const& ip, int port);
    void setTcpAddress(std::string const& ip, int port);

    /// @Return return the previous range value.
    StringRange setRangeStr(StringRange const& strRange);

    StringRange getRange() const;

    NetworkAddress getAddressUdp() const {
        std::lock_guard<std::mutex> lck(_mtx);
        return *_udpAddress;
    }

    NetworkAddress getAddressTcp() const {
           std::lock_guard<std::mutex> lck(_mtx);
           return *_tcpAddress;
       }

    uint32_t getName() const {
        std::lock_guard<std::mutex> lck(_mtx);
        return _name;
    }

    StringRange getRangeString() const {
        std::lock_guard<std::mutex> lck(_mtx);
        return _range;
    }

    void addDoListItems(Central *central);

    util::CommandTracked::Ptr createCommandWorkerInfoReq(CentralWorker* centralW);

    bool equal(WWorkerListItem &other);

    bool containsKey(std::string const&) const;

    friend std::ostream& operator<<(std::ostream& os, WWorkerListItem const& item);
private:
    WWorkerListItem(uint32_t name, CentralWorker* central) : _name(name), _central(central) {}
    WWorkerListItem(uint32_t name, NetworkAddress const& address, CentralWorker* central)
         : _name(name), _udpAddress(new NetworkAddress(address)), _central(central) {}

    uint32_t _name;
    NetworkAddress::UPtr _udpAddress{new NetworkAddress("",0)}; ///< empty string indicates address is not valid.
    NetworkAddress::UPtr _tcpAddress{new NetworkAddress("",0)}; ///< empty string indicates address is not valid.
    StringRange _range;      ///< min and max range for this worker.
    mutable std::mutex _mtx; ///< protects _name, _address, _range, _workerUpdateNeedsMasterData

    CentralWorker* _central;

    TimeOut _lastContact{std::chrono::minutes(10)};  ///< Last time information was received from this worker

    struct WorkerNeedsMasterData : public DoListItem {
        WorkerNeedsMasterData(WWorkerListItem::Ptr const& wWorkerListItem_, CentralWorker* central_) :
            wWorkerListItem(wWorkerListItem_), central(central_) {}
        WWorkerListItem::WPtr wWorkerListItem;
        CentralWorker* central;
        util::CommandTracked::Ptr createCommand() override;
    };
    DoListItem::Ptr _workerUpdateNeedsMasterData;
};



class WWorkerList : public DoListItem {
public:
    using Ptr = std::shared_ptr<WWorkerList>;

    WWorkerList(CentralWorker* central) : _central(central) {}
    WWorkerList() = delete;
    WWorkerList(WWorkerList const&) = delete;
    WWorkerList& operator=(WWorkerList const&) = delete;

    virtual ~WWorkerList() = default;

    //// Worker only ////////////////////////
    // Receive a list of workers from the master.
    bool workerListReceive(BufferUdp::Ptr const& data);

    bool equal(WWorkerList& other);

    util::CommandTracked::Ptr createCommand() override;
    util::CommandTracked::Ptr createCommandWorker(CentralWorker* centralW);

    ////////////////////////////////////////////
    /// Nearly the same on Worker and Master
    size_t getNameMapSize() {
        std::lock_guard<std::mutex> lck(_mapMtx);
        return _wIdMap.size();
    }
    WWorkerListItem::Ptr getWorkerNamed(uint32_t name) {
        std::lock_guard<std::mutex> lck(_mapMtx);
        auto iter = _wIdMap.find(name);
        if (iter == _wIdMap.end()) { return nullptr; }
        return iter->second;
    }

    void updateEntry(uint32_t name,
                     std::string const& ipUdp, int portUdp, int portTcp,
                     StringRange& strRange);
    WWorkerListItem::Ptr findWorkerForKey(std::string const& key);

    std::string dump() const;

protected:
    void _flagListChange();

    CentralWorker* _central;
    std::map<uint32_t, WWorkerListItem::Ptr> _wIdMap; ///< worker id map
    std::map<NetworkAddress, WWorkerListItem::Ptr> _ipMap;
    std::map<StringRange, WWorkerListItem::Ptr> _rangeMap;
    bool _wListChanged{false}; ///< true if the list has changed
    uint32_t _totalNumberOfWorkers{0}; ///< total number of workers according to the master.
    mutable std::mutex _mapMtx; ///< protects _nameMap, _ipMap, _rangeMap, _wListChanged

    std::atomic<uint32_t> _sequence{1};
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_WWORKERLIST_H_
