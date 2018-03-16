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
#if 0 // &&& Really must make this base class for MWorkerList, WWorkerList,  MWorkerListItem, and  WWorkerListItem
#ifndef LSST_QSERV_LOADER_WORKERLIST_H_
#define LSST_QSERV_LOADER_WORKERLIST_H_

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
#include "loader/WorkerList.h"


namespace lsst {
namespace qserv {
namespace loader {

class Central;
class CentralWorker;
class CentralMaster;
class LoaderMsg;




/// Standard information for a single worker, IP address, key range, timeouts.
//class WorkerListItem : public DoListItem {
class WorkerListItem : public std::enable_shared_from_this<WorkerListItem> {
public:
    using Ptr = std::shared_ptr<WorkerListItem>;
    using WPtr = std::weak_ptr<WorkerListItem>;

    static WorkerListItem::Ptr create(uint32_t name, Central *central) {
        return WorkerListItem::Ptr(new WorkerListItem(name, central));
    }
    static WorkerListItem::Ptr create(uint32_t name, NetworkAddress const& address, Central *central) {
        return WorkerListItem::Ptr(new WorkerListItem(name, address, central));
    }


    WorkerListItem() = delete;
    WorkerListItem(WorkerListItem const&) = delete;
    WorkerListItem& operator=(WorkerListItem const&) = delete;

    virtual ~WorkerListItem() = default;

    NetworkAddress getAddress() const { return _address; }
    uint32_t getName() const { return _name; }

    void addDoListItems(Central *central);

    //util::CommandTracked::Ptr createCommand() override;
    util::CommandTracked::Ptr createCommandWorker(CentralWorker* centralW);
    util::CommandTracked::Ptr createCommandMaster(CentralMaster* centralM);

    friend std::ostream& operator<<(std::ostream& os, WorkerListItem const& item);
private:
    WorkerListItem(uint32_t name, Central* central) : _name(name), _central(central) {}
    WorkerListItem(uint32_t name, NetworkAddress const& address, Central* central)
         : _name(name), _address(address), _central(central) {}

    uint32_t _name;
    NetworkAddress _address{"", 0}; ///< empty string indicates address is not valid.
    TimeOut _lastContact{std::chrono::minutes(10)};  ///< Last time information was received from this worker
    StringRange _range;  ///< min and max range for this worker.

    Central* _central;

    struct WorkerNeedsMasterData : public DoListItem {
        WorkerNeedsMasterData(WorkerListItem::Ptr const& workerListItem_) :  workerListItem(workerListItem_) {}
        WorkerListItem::WPtr workerListItem;
        util::CommandTracked::Ptr createCommand() override;
    };

    DoListItem::Ptr _workerUpdateNeedsMasterData;
};



class WorkerList : public DoListItem {
public:
    using Ptr = std::shared_ptr<WorkerList>;

    WorkerList(Central* central) : _central(central) {}
    WorkerList() = delete;
    WorkerList(WorkerList const&) = delete;
    WorkerList& operator=(WorkerList const&) = delete;

    virtual ~WorkerList() = default;

    ///// Master only //////////////////////
    // Returns pointer to new item if an item was created.
    WorkerListItem::Ptr addWorker(std::string const& ip, short port);

    // Returns true of message could be parsed and a send will be attempted.
    bool sendListTo(uint64_t msgId, std::string const& ip, short port,
                    std::string const& outHostName, short ourPort);


    //// Worker only ////////////////////////
    // Receive a list of workers from the master.
    bool workerListReceive(BufferUdp::Ptr const& data);

    util::CommandTracked::Ptr createCommand() override;
    util::CommandTracked::Ptr createCommandWorker(CentralWorker* centralW);
    util::CommandTracked::Ptr createCommandMaster(CentralMaster* centralM);

protected:
    void _flagListChange();

    Central* _central;
    std::map<uint32_t, WorkerListItem::Ptr> _nameMap;
    std::map<NetworkAddress, WorkerListItem::Ptr> _ipMap;
    bool _wListChanged{false}; ///< true if the list has changed
    BufferUdp::Ptr _stateListData; ///< message
    uint32_t _totalNumberOfWorkers{0}; ///< total number of workers according to the master.
    std::mutex _mapMtx; ///< protects _nameMap, _ipMap, _wListChanged

    std::atomic<uint32_t> _sequence{1};
};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_WORKERLIST_H_
#endif
