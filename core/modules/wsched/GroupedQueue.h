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
#ifndef LSST_QSERV_WSCHED_GROUPEDQUEUE_H
#define LSST_QSERV_WSCHED_GROUPEDQUEUE_H
 /**
  * @author Daniel L. Wang, SLAC
  */

// System headers
#include <deque>

// Third-party headers
#include "boost/shared_ptr.hpp"

namespace lsst {
namespace qserv {
namespace wsched {

/// GroupedQueue is a queue of elements grouped by a key
/// value. New elements are inserted by finding the right-most element
/// that shares its key with the new element and inserting the new
/// value after the found element. If no key-sharing element is found,
/// the new element is placed at the back.
///
/// The intent is that GroupedQueue behaves just like a queue, except
/// that new elements can go earlier if they have friends already in
/// line. This is used to handle "interactive" queries in a roughly
/// FIFO ordering, with opportunistic reuse of chunk I/O when
/// possible. Because of line-jumping, there is a chance for
/// starvation if these queries are not interactive. Starvation is
/// mitigated by a parameter which limits the size of the group of
/// "friends".
///
/// deque is chosen as the underlying data structure. A deque of lists
/// of elements was considered, in order to eliminate the O(n)
/// insertion penalty, but is probably unnecessary, because n should
/// be small.
///
/// Note: Because STL deque::insert always inserts *before* an
/// element, and we want new elements to be logically inserted after
/// the last element of their group, the deque operates reversed:
/// i.e., _deque.front() is the back of the queue and _deque.back() is
/// the front of the queue.
template <class T, class KeyEqual>
class GroupedQueue {
public:
    typedef std::deque<T> Deque;

    /// @param maxClique maximum group size
    ///                  0=no limit, 1=singletons only
    explicit GroupedQueue(int maxClique=0)
        : _maxClique(maxClique) {}

    /// @return number removed
    template <class F>
    int removeIf(F f) {
        int numErased = 0;
        typename Deque::iterator i = _deque.begin();
        typename Deque::iterator e = _deque.end();
        while(i != e) {
            if(f(*i)) {
                ++numErased;
                i = _deque.erase(i);
            } else { // no match, continue
                ++i;
            }
        }
        return numErased;
    }
    void insert(T const& t) {
        if(_maxClique == 1) { // Never join friends
            _deque.push_front(t);
            return;
        }
        typename Deque::iterator i = _deque.begin();
        typename Deque::const_iterator e = _deque.end();
        for(; i != e; ++i) {
            if(_eq(t, *i)) { // Found friend(s)
                if(_maxClique > 1) { // Clique restriction?
                    typename Deque::iterator j = i+1;
                    for(int space=_maxClique-1; space > 0; --space, ++j) {
                        if(!_eq(t, *j)) { // No match; there is space
                            _deque.insert(i, t);
                            return;
                        }
                    }
                    // Friends found, but clique too big, don't join.
                    break;
                } else { // No clique restriction; join.
                    _deque.insert(i, t);
                    return;
                }
            }
        }
        // Either no friends found, or clique was too big.
        _deque.push_front(t); // Place at the logical back.
    }
    T& front() { return _deque.back(); }
    T const& front() const { return _deque.back(); }
    void pop_front() { _deque.pop_back(); }
    size_t size() const { return _deque.size(); }
    size_t empty() const { return _deque.empty(); }
private:
    KeyEqual _eq;
    Deque _deque;
    int _maxClique;
};

}}} // namespace lsst::qserv::wsched

#endif // LSST_QSERV_WSCHED_GROUPEDQUEUE_H
