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
#include <map>
#include <mutex>
#include <map>

// Qserv headers
#include "global/intTypes.h"

namespace lsst::qserv::wcontrol {

class TransmitLock;
class QidMgr;

/// Limit the number of transmitting tasks sharing the same query id number.
class QidMgr {
public:
    QidMgr() = delete;
    QidMgr(int maxTransmits, int maxPerQid) : _maxTransmits(maxTransmits), _maxPerQid(maxPerQid) {
        _setMaxCount(0);
    }
    QidMgr(QidMgr const&) = delete;
    QidMgr& operator=(QidMgr const&) = delete;
    virtual ~QidMgr() = default;

    friend class TransmitLock;

private:
    /// Set _maxCount according to members and uniqueQidCounts
    /// _mapMtx must be held before calling this function (aside from constructor).
    void _setMaxCount(int uniqueQidCount);

    void _take(QueryId const& qid);
    void _release(QueryId const& qid);

    int const _maxTransmits;  ///< Maximum number of transmits per czar connection

    ///< Absolute maximum number of Transmits per unique QID + czarID
    int const _maxPerQid;
    int _prevUniqueQidCount = -1;  ///< previous number of unique QID's, invalid value to start.
    std::atomic<int> _maxCount{1};
    std::mutex _mapMtx;

    class LockCount {
    public:
        LockCount() = default;
        LockCount(LockCount const&) = delete;
        LockCount& operator=(LockCount const&) = delete;
        ~LockCount() = default;

        int take();     /// @return _totalCount.
        int release();  /// @return _totalCount.
        std::atomic<int> lcTotalCount{0};
        std::atomic<int> lcCount{0};
        std::atomic<int> lcMaxCount{1};
        std::mutex lcMtx;
        std::condition_variable lcCv;
    };

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

    TransmitMgr(int maxTransmits, int maxPerQid) : _maxTransmits(maxTransmits), _maxPerQid(maxPerQid) {
        assert(_maxTransmits > 0);
    }

    TransmitMgr() = delete;
    TransmitMgr(TransmitMgr const&) = delete;
    TransmitMgr& operator=(TransmitMgr const&) = delete;
    virtual ~TransmitMgr() = default;

    int getTotalCount() { return _totalCount; }
    int getTransmitCount() { return _transmitCount; }

    /// Class methods, that have already locked '_mtx', should call 'dumpBase'.
    std::ostream& dump(std::ostream& os) const;

    /// This will try to lock 'TransmitMgr::_mtx'.
    friend std::ostream& operator<<(std::ostream& out, TransmitMgr const& mgr);

    friend class TransmitLock;

protected:
    /// _mtx must be locked before calling this function.
    /// Dump the contents of the class to a string for logging.
    virtual std::ostream& dumpBase(std::ostream& os) const;

    /// _mtx must be locked before calling this function.
    std::string dump() const;

private:
    void _take(bool interactive);

    void _release(bool interactive);

    std::atomic<int> _totalCount{0};
    std::atomic<int> _transmitCount{0};
    int const _maxTransmits;
    int const _maxPerQid;
    std::mutex _mtx;
    std::condition_variable _tCv;

    QidMgr _qidMgr{_maxTransmits, _maxPerQid};
};

/// RAII class to support TransmitMgr
class TransmitLock {
public:
    using Ptr = std::shared_ptr<TransmitLock>;
    TransmitLock(TransmitMgr& transmitMgr, bool interactive, QueryId const qid)
            : _transmitMgr(transmitMgr), _interactive(interactive), _qid(qid) {
        //_transmitMgr._qidMgr._take(_qid);
        _transmitMgr._take(_interactive);
    }

    TransmitLock() = delete;
    TransmitLock(TransmitLock const&) = delete;
    TransmitLock& operator=(TransmitLock const&) = delete;

    ~TransmitLock() {
        _transmitMgr._release(_interactive);
        //_transmitMgr._qidMgr._release(_qid);
    }

private:
    TransmitMgr& _transmitMgr;
    bool const _interactive;
    QueryId const _qid;
};

}  // namespace lsst::qserv::wcontrol

#endif  // LSST_QSERV_WCONTROL_TRANSMITMGR_H
