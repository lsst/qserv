<<<<<<< HEAD
<<<<<<< HEAD
=======
/*
 * TransmitMgr.h
 *
 *  Created on: Feb 18, 2020
 *      Author: jgates
 */

#ifndef CORE_MODULES_WDB_TRANSMITMGR_H_
#define CORE_MODULES_WDB_TRANSMITMGR_H_





#endif /* CORE_MODULES_WDB_TRANSMITMGR_H_ */


>>>>>>> Added code to examine memory double free and limit the number of active transmitions.
=======
>>>>>>> Removed extraneous #ifdef and comments.
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

#ifndef LSST_QSERV_WDB_TRANSMITMGR_H
#define LSST_QSERV_WDB_TRANSMITMGR_H

// System headers
#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <mutex>

// Qserv headers


namespace lsst {
namespace qserv {
namespace wdb {

class TransmitLock;

/// Currently, a quick and dirty way to limit the number of concurrent
/// transmits. 'interactive queries' are not blocked.
/// New tasks cannot transmit to the czar until the number of jobs
/// currently transmitting data drops below maxAlreadyTran
/// Priority is given to finish tasks that have already started transmitting.
/// This class is meant to improve transmission efficiency, and
/// it is possible for _alreadyTransCount to go above normal
/// limits due to a race condition. Fixing the race condition may well
/// slow things down more than the occasional breach of the
/// _alreadyTransCount limit.
/// TODO:
///    -- The czar these are being sent to should be taken into consideration
///    -- maxTransmits set via config, maybe change at runtime.
class TransmitMgr {
public:
    TransmitMgr(int maxTransmits, int maxAlreadyTran)
        : _maxTransmits(maxTransmits),  _maxAlreadyTran(maxAlreadyTran) {
        assert(_maxTransmits > 1);
        assert(_maxAlreadyTran > 1);
    }
    // &&& delete default constructor, copy constructor, and such

    int getTotalCount() { return _totalCount; }
    int getTransmitCount() { return _transmitCount; }
    int getAlreadyTransCount() { return _alreadyTransCount; }

    friend class TransmitLock;

private:
    void _take(bool interactive, bool alreadyTransmitting);

    void _release(bool interactive, bool alreadyTransmitting);

    std::atomic<int> _totalCount{0};
    std::atomic<int> _transmitCount{0};
    std::atomic<int> _alreadyTransCount{0};
    int _maxTransmits;
    int _maxAlreadyTran;
    std::mutex _mtx;
    std::condition_variable _tCv;
};


/// RAII class to support TransmitMgr
class TransmitLock {
public:
    TransmitLock(TransmitMgr& transmitMgr, bool interactive, bool alreadyTransmitting)
      : _transmitMgr(transmitMgr), _interactive(interactive),
        _alreadyTransmitting(alreadyTransmitting) {
        _transmitMgr._take(_interactive, _alreadyTransmitting);
    }
    // &&& delete default constructor and such

    ~TransmitLock() {
        _transmitMgr._release(_interactive, _alreadyTransmitting);
    }

private:
    TransmitMgr& _transmitMgr;
    bool _interactive;
    bool _alreadyTransmitting;
};


}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_TRANSMITMGR_H
