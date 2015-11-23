// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2013 LSST Corporation.
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
#ifndef LSST_QSERV_WSCHED_CHUNKSTATE_H
#define LSST_QSERV_WSCHED_CHUNKSTATE_H
 /**
  *
  * @brief ChunkState is a way to track which chunks are being scanned
  * and which are cached.
  *
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <iosfwd>
#include <set>


namespace lsst {
namespace qserv {
namespace wsched {

/// This class tracks current scans for a ChunkDisk.
class ChunkState {
public:
    typedef std::set<int> IntSet;

    ChunkState() {}

    void addScan(int chunkId) {
        _scan.insert(chunkId);
        _last = chunkId;
    }

    /// @Return true if the chunkId was erased from _scan.
    bool markComplete(int chunkId) {
        return _scan.erase(chunkId) > 0;
    }

    bool isScan(int chunkId) const {
        return _scan.end() != _scan.find(chunkId);
    }
    bool empty() const {
        return _scan.empty();
    }

    bool hasScan() const {
        return !_scan.empty();
    }

    int lastScan() const {
        return _last;
    }

    friend std::ostream& operator<<(std::ostream& os, ChunkState const& cs);

private:
    IntSet _scan;
    int _last{-1};
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_CHUNKSTATE_H

