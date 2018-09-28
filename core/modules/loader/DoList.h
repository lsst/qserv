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
#ifndef LSST_QSERV_LOADER_DOLIST_H_
#define LSST_QSERV_LOADER_DOLIST_H_

// system headers
#include <chrono>
#include <list>

// Qserv headers
#include "util/ThreadPool.h"

namespace lsst {
namespace qserv {
namespace loader {


class Central;


class TimeOut {
public:
    using TimePoint = std::chrono::system_clock::time_point;
    using Clock = std::chrono::system_clock;

    TimeOut(std::chrono::milliseconds timeOut) : _timeOut(timeOut) {}

    bool due() { return due(Clock::now()); }
    bool due(TimePoint now) {
        auto triggerDiff = std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastTrigger);
        return (triggerDiff > _timeOut);
    }

    void triggered() { return triggered(Clock::now()); }
    void triggered(TimePoint now) {
        _lastTrigger = now;
    }

    std::chrono::milliseconds timeLeft(TimePoint now) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(now - _lastTrigger);
    }

    void setTimeOut(std::chrono::milliseconds timeOut) { _timeOut = timeOut; }
    std::chrono::milliseconds getTimeOut() const { return _timeOut; }
private:
    // How much time since lastTrigger needs to pass before triggering.
    std::chrono::milliseconds _timeOut; // default, 15 minutes
    TimePoint _lastTrigger{std::chrono::seconds(0)};
};


/// Children of this class must be created with shared pointers.
class DoListItem : public std::enable_shared_from_this<DoListItem> {
public:
    using Ptr = std::shared_ptr<DoListItem>;

    DoListItem() = default;

    DoListItem(DoListItem const&) = delete;
    DoListItem& operator=(DoListItem const&) = delete;

    virtual ~DoListItem() = default;

    util::CommandTracked::Ptr runIfNeeded(TimeOut::TimePoint now) {
        std::lock_guard<std::mutex> lock(_mtx);
        if (_command == nullptr) {
            if (_isOneShotDone()) { return nullptr; }
            if ((_needInfo || _timeOut.due(now)) && _timeRequest.due(now)) {
                _timeRequest.triggered();
                    _command = createCommand();
                return _command;
            }
        } else if (_command->isFinished()) {
            _command.reset(); // open the door for the command to be sent again
        }
        return nullptr;
    }

    bool isAlreadyOnList() { return _addedToList; }

    /// Returns original value of _addedToList.
    bool setAddedToList(bool value) {
        return _addedToList.exchange(value);
    }

    bool removeFromList() {
        std::lock_guard<std::mutex> lock(_mtx);
        return (_isOneShotDone() || _remove);
    }

    /// The info has been updated, so no need to ask for it for a while.
    void infoReceived() {
        std::lock_guard<std::mutex> lock(_mtx);
        _needInfo = false;
        _timeOut.triggered();
    }

    void setNeedInfo() {
        std::lock_guard<std::mutex> lock(_mtx);
        _needInfo = true;
    }

    DoListItem::Ptr getDoListItemPtr() {
        return shared_from_this();
    }

    virtual util::CommandTracked::Ptr createCommand()=0;

protected:
    std::atomic<bool>    _addedToList{false}; ///< True when added to a DoList

    bool    _oneShot{false}; ///< True if after the needed information is gathered, this item can be dropped.
    bool    _needInfo{true}; ///< True if information is needed.
    bool    _remove{false}; ///< set to true if this item should no longer be checked.
    TimeOut _timeOut{std::chrono::minutes(15)};
    TimeOut _timeRequest{std::chrono::seconds(5)}; ///< Rate limiter, no more than 1 message every 5 seconds
    util::CommandTracked::Ptr _command;
    std::mutex _mtx; ///< protects _timeOut, _timeRequest, _command, _oneShot, _needInfo

private:
    /// Lock _mtx before calling.
    bool _isOneShotDone() {
        return (!_needInfo && _oneShot);
    }
};


/// A list of things that need to be done with timers.
/// Everything on the list is checked, if it's timer has expired, it is queued and the timer reset.
/// If it is a single use item, it is deleted after completion.
class DoList {
public:
    DoList(Central& central) : _central(central) {}
    DoList() = delete;
    DoList(DoList const&) = delete;
    DoList& operator=(DoList const&) = delete;

    ~DoList() = default;

    void checkList();
    bool addItem(DoListItem::Ptr const& item) {
        if (item == nullptr) return false;
        if (item->isAlreadyOnList()) return false; // fast atomic test
        {
            std::lock_guard<std::mutex> lock(_addListMtx);
            // Need to make sure this wasn't added before the mutex got locked.
            if (not item->setAddedToList(true)) {
                _addList.push_back(item);
                return true;
            }
        }
        return false;
    }

    void runItemNow(DoListItem::Ptr const& item);


private:
    std::list<DoListItem::Ptr> _list;
    std::mutex _listMtx; ///< Protects _list (lock this one first)

    std::list<DoListItem::Ptr> _addList;
    std::mutex _addListMtx; ///< Protects _addList (lock this one second)

    Central& _central;
};


}}} // namespace lsst:qserv:loader


#endif
