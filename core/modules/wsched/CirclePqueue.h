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
#ifndef LSST_QSERV_WSCHED_CIRCLEPQUEUE_H
#define LSST_QSERV_WSCHED_CIRCLEPQUEUE_H
 /**
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <set>
#include <queue>

// Third-party headers
#include <boost/scoped_ptr.hpp>

namespace lsst {
namespace qserv {
namespace wsched {

/// CirclePqueue is a circular queue that provides key-ordered
/// removal. Elements are removed in monotonically-increasing key
/// order, until there are no more elements with "higher-valued" keys,
/// at which point removal continues with the "lowest-valued" key. It
/// does this by maintaining two priority queues, both min-key-sorted,
/// and a key cursor. The cursor stores the split point for the two queues.
template <class T, class Less, class GetPos>
class CirclePqueue {
public:
    /// Invert Less so we have a minheap rather than a maxheap.
    struct Greater {
        inline bool operator()(T const& a, T const& b) { return less(b, a); }
        Less less;
    };
    typedef std::priority_queue<T, std::vector<T>, Greater> Queue;
    typedef typename GetPos::value_type Pos;
    //CirclePqueue() {}

    void insert(T const& t, bool equalOk=true) {
        Pos tPos = _getPos(t);
        if(_pos) {
            if(tPos < *_pos) { _pending.push(t); }
            else {
                // Can we add more to the current?
                if((tPos == *_pos) && equalOk) { _active.push(t); }
                else { _pending.push(t); }
            }
        } else {
            _pos.reset(new Pos(tPos));
            _active.push(t);
        }
    }
    T const& front() const { return _active.top(); }

    void pop_front() {
        assert(!_active.empty());
        _active.pop();
        if(_active.empty() && !_pending.empty()) {
            // Swap
            std::swap(_active, _pending);
            //_logger->debug("ChunkDisk active-pending swap");
        }
    }
    size_t size() const { return _active.size() + _pending.size(); }
    size_t empty() const { return _active.empty() && _pending.empty(); }

private:
    GetPos _getPos;
    Less _less;

    Queue _active;
    Queue _pending;
    boost::scoped_ptr<Pos> _pos;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_CIRCLEPQUEUE_H
