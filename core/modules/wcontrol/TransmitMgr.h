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

// Qserv headers
#include "qmeta/types.h"


namespace lsst {
namespace qserv {
namespace wcontrol {

class TransmitLock;

/// This class is used to limit the number of concurrent transmits.
/// 'interactive queries' are not blocked.
/// New tasks cannot transmit to a czar until the number of tasks
/// currently transmitting data to that czar drops below _maxAlreadyTran
/// Priority is given to tasks that have already started transmitting
/// in the hope of finishing them as soon as possible, which frees up
/// resources on the related czar.
class TransmitMgr {
public:
    using Ptr = std::shared_ptr<TransmitMgr>;

    TransmitMgr(int maxTransmits, int maxAlreadyTran)
        : _maxTransmits(maxTransmits),  _maxAlreadyTran(maxAlreadyTran) {
        assert(_maxTransmits >= 1);
        assert(_maxAlreadyTran >= 1);
        assert(_maxTransmits >= _maxAlreadyTran);
    }
    TransmitMgr() = delete;
    TransmitMgr(TransmitMgr const&) = delete;
    TransmitMgr& operator=(TransmitMgr const&) = delete;
    virtual ~TransmitMgr() = default;

    int getTotalCount(qmeta::CzarId czarId) const;
    int getTransmitCount(qmeta::CzarId czarId) const;
    int getAlreadyTransCount(qmeta::CzarId czarId) const;

    /// Class methods, that have already locked '_mtx', should call 'dumpBase'.
    std::ostream& dump(std::ostream &os) const;

    /// This will try to lock 'TransmitMgr::_mtx'.
    friend std::ostream& operator<<(std::ostream &out, TransmitMgr const& mgr);

    friend class TransmitLock;

protected:
    /// _mtx must be locked before calling this function.
    /// Dump the contents of the class to a string for logging.
    virtual std::ostream& dumpBase(std::ostream &os) const;

    /// _mtx must be locked before calling this function.
    std::string dump() const;

private:
    void _take(bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId);

    void _release(bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId);

    /// This class is used to store transmit information for a czar
    class TransmitInfo {
    public:
        TransmitInfo() = default;
        TransmitInfo(TransmitInfo const&) = default;
        TransmitInfo& operator=(TransmitInfo const&) = default;
    private:
        friend class TransmitMgr;
        int _totalCount = 0;
        int _transmitCount = 0;
        int _alreadyTransCount = 0;
        int _takeCalls = 0; ///< current number of calls to _take.
    };

    int const _maxTransmits;
    int const _maxAlreadyTran;
    mutable std::mutex _mtx;
    std::condition_variable _tCv;
    std::map<qmeta::CzarId, TransmitInfo> _czarTransmitMap; ///< map of information per czar.
};


/// RAII class to support TransmitMgr
class TransmitLock {
public:
    TransmitLock(TransmitMgr& transmitMgr, bool interactive, bool alreadyTransmitting, qmeta::CzarId czarId)
      : _transmitMgr(transmitMgr), _interactive(interactive),
        _alreadyTransmitting(alreadyTransmitting), _czarId(czarId) {
        _transmitMgr._take(_interactive, _alreadyTransmitting, czarId);
    }
    TransmitLock() = delete;
    TransmitLock(TransmitLock const&) = delete;
    TransmitLock& operator=(TransmitLock const&) = delete;

    ~TransmitLock() {
        _transmitMgr._release(_interactive, _alreadyTransmitting, _czarId);
    }

private:
    TransmitMgr& _transmitMgr;
    bool _interactive;
    bool _alreadyTransmitting;
    qmeta::CzarId _czarId;
};


}}} // namespace lsst::qserv::wcontrol

#endif // LSST_QSERV_WCONTROL_TRANSMITMGR_H
