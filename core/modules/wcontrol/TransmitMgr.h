// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2020 LSST Corporation.
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
 */

#ifndef LSST_QSERV_WCONTROL_TRANSMITMGR_H
#define LSST_QSERV_WCONTROL_TRANSMITMGR_H

// System headers
#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <map>

// Qserv headers
#include "global/intTypes.h"


namespace lsst {
namespace qserv {
namespace wcontrol {

class TransmitLock;
class QidMgr;

class LockCount {
public:
    LockCount() = default;

    friend QidMgr;
private:
    int _take(); /// @return _totalCount.
    int _release(); /// @return _totalCount.

    std::atomic<int> _totalCount{0};
    std::atomic<int> _count{0};
    std::atomic<int> _maxCount{1};
    std::mutex _lMtx;
    std::condition_variable _lCv;
};


/// Limit the number of transmitting tasks sharing the same query id number.
class QidMgr {
public:
    QidMgr() = default;
    QidMgr(QidMgr const&) = delete;
    QidMgr& operator=(QidMgr const&) = delete;
    virtual ~QidMgr() = default;

    friend class TransmitLock;

private:
    void _take(QueryId const& qid);
    void _release(QueryId const& qid);

    std::mutex _mapMtx;
    std::map<QueryId, LockCount> _qidLocks;
};


/// A way to limit the number of concurrent
/// transmits. 'interactive queries' are not blocked.
/// New tasks cannot transmit to the czar until the number of jobs
/// currently transmitting data drops below maxAlreadyTran
/// Priority is given to finish tasks that have already started transmitting.
/// TODO:
///    -- The czar these are being sent to should be taken into consideration
///       as the limit should really be per czar.
///    -- maybe change at runtime.
class TransmitMgr {
public:
    using Ptr = std::shared_ptr<TransmitMgr>;

    TransmitMgr(int maxTransmits)
        : _maxTransmits(maxTransmits) {
        assert(_maxTransmits > 0);
    }

    TransmitMgr() = delete;
    TransmitMgr(TransmitMgr const&) = delete;
    TransmitMgr& operator=(TransmitMgr const&) = delete;
    virtual ~TransmitMgr() = default;

    int getTotalCount() { return _totalCount; }
    int getTransmitCount() { return _transmitCount; }

    virtual std::ostream& dump(std::ostream &os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream &out, TransmitMgr const& mgr);

    friend class TransmitLock;

private:
    void _take(bool interactive);

    void _release(bool interactive);

    std::atomic<int> _totalCount{0};
    std::atomic<int> _transmitCount{0};
    int const _maxTransmits;
    std::mutex _mtx;
    std::condition_variable _tCv;

    QidMgr _qidMgr;
};


/// RAII class to support TransmitMgr
class TransmitLock {
public:
    using Ptr = std::shared_ptr<TransmitLock>;
    TransmitLock(TransmitMgr& transmitMgr, bool interactive, QueryId const qid)
      : _transmitMgr(transmitMgr), _interactive(interactive), _qid(qid) {
        _transmitMgr._qidMgr._take(_qid);
        _transmitMgr._take(_interactive);
    }

    TransmitLock() = delete;
    TransmitLock(TransmitLock const&) = delete;
    TransmitLock& operator=(TransmitLock const&) = delete;

    ~TransmitLock() {
        _transmitMgr._release(_interactive);
        _transmitMgr._qidMgr._release(_qid);
    }

private:
    TransmitMgr& _transmitMgr;
    bool const _interactive;
    QueryId const _qid;
};


}}} // namespace lsst::qserv::wcontrol

#endif // LSST_QSERV_WCONTROL_TRANSMITMGR_H
