/*
 * TransmitMgr.h
 *
 *  Created on: Feb 18, 2020
 *      Author: jgates
 */

#ifndef CORE_MODULES_WDB_TRANSMITMGR_H_
#define CORE_MODULES_WDB_TRANSMITMGR_H_





#endif /* CORE_MODULES_WDB_TRANSMITMGR_H_ */


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
 /**
  * @file
  *
  * @brief QueryAction instances perform single-shot query execution with the
  * result reflected in the db state or returned via a SendChannel. Works with
  * new XrdSsi API.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
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
/// TODO:
///    -- Priority should be given to finish tasks that have already started transmitting
///    -- RAII class should be created for take() and release().
///    -- maxTransmits set via config, maybe change at runtime.
class TransmitMgr {
public:
    TransmitMgr(unsigned int maxTransmits) : _maxTransmits(maxTransmits)  {}
    // &&& delete default constructor, copy constructor, and such

    unsigned int getTransmitCount() { return _transmitCount; }

    friend class TransmitLock;

private:
    void _take(bool interactive) {
        if (not interactive) {
            std::unique_lock<std::mutex> uLock(_mtx);
            _tCv.wait(uLock, [this](){ return _transmitCount < _maxTransmits; });
        }
        ++_transmitCount;
    }

    void _release() {
        --_transmitCount;
        _tCv.notify_one();
    }

    std::atomic<unsigned int> _transmitCount{0};
    unsigned int _maxTransmits;
    std::mutex _mtx;
    std::condition_variable _tCv;
};


/// RAII class to support TransmitMgr
class TransmitLock {
public:
    TransmitLock(TransmitMgr& transmitMgr, bool interactive) : _transmitMgr(transmitMgr) {
        _transmitMgr._take(interactive);
    }
    // &&& delete default constructor and such

    ~TransmitLock() {
        _transmitMgr._release();
    }

private:
    TransmitMgr& _transmitMgr;
};


}}} // namespace lsst::qserv::wdb

#endif // LSST_QSERV_WDB_TRANSMITMGR_H
