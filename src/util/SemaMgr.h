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

#ifndef LSST_QSERV_UTIL_SEMAMGR_H
#define LSST_QSERV_UTIL_SEMAMGR_H

// System headers
#include <assert.h>
#include <atomic>
#include <condition_variable>
#include <mutex>

// Qserv headers

namespace lsst::qserv::util {

/// This class (with SemaLock) is used to limit the number of simultaneous
/// connections to MySQL for merging the results.
class SemaMgr {
public:
    using Ptr = std::shared_ptr<SemaMgr>;
    explicit SemaMgr(int max) : _max(max) { assert(_max >= 1); }
    SemaMgr() = delete;
    SemaMgr(SemaMgr const&) = delete;
    SemaMgr& operator=(SemaMgr const&) = delete;
    virtual ~SemaMgr() = default;

    int getTotalCount() { return _totalCount; }
    int getUsedCount() { return _usedCount; }
    int setMax(int max) {
        int oldMax = _max;
        if (max < 1) max = 1;
        _max = max;
        if (_max > oldMax) _tCv.notify_all();
        return max;
    }

    virtual std::ostream& dump(std::ostream& os) const;
    std::string dump() const;
    friend std::ostream& operator<<(std::ostream& out, SemaMgr const& semaMgr);

    friend class SemaLock;

private:
    void _take() {
        ++_totalCount;
        std::unique_lock<std::mutex> uLock(_mtx);
        _tCv.wait(uLock, [this]() { return _usedCount < _max; });
        ++_usedCount;
    }

    void _release() {
        --_totalCount;
        --_usedCount;
        _tCv.notify_one();
    }

    std::atomic<int> _totalCount{0};
    std::atomic<int> _usedCount{0};
    std::atomic<int> _max;
    std::mutex _mtx;
    std::condition_variable _tCv;
};

/// RAII class to support SemaMgr
class SemaLock {
public:
    explicit SemaLock(SemaMgr& semaMgr) : _semaMgr(semaMgr) { _semaMgr._take(); }
    SemaLock() = delete;
    SemaLock(SemaLock const&) = delete;
    SemaLock& operator=(SemaLock const&) = delete;

    ~SemaLock() { _semaMgr._release(); }

private:
    SemaMgr& _semaMgr;
};

}  // namespace lsst::qserv::util

#endif  // LSST_QSERV_UTIL_SEMAMGR_H
