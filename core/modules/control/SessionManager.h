// -*- LSST-C++ -*-

/*
 * LSST Data Management System
 * Copyright 2008, 2009, 2010 LSST Corporation.
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


// SessionManager -- tracks sessions that the frontend dispatches out.
// A "session" maps to a user-issued query, which the frontend should
// break apart into many chunk queries.

// Implementation notes:
// * The session manager reuses ids like a coat check system with lots
//   of tags.
// * If you store objects, you probably want to store shared_ptrs.
// * The session manager makes a copy of the Value that is stored.
//
#ifndef LSST_QSERV_CONTROL_SESSIONMANAGER_H
#define LSST_QSERV_CONTROL_SESSIONMANAGER_H
// Includes
#include <boost/thread.hpp> // for mutex primitives

namespace lsst {
namespace qserv {
namespace control {

template <typename Value>
class SessionManager {
public:
    SessionManager() :_idLimit(200000000), _nextId(1) {}

    int newSession(Value const& v) {
        boost::lock_guard<boost::mutex> g(_mutex);
        int id = _getNextId();
        _map[id] = v;
        return id;
    }

    Value& getSession(int id) {
        boost::lock_guard<boost::mutex> g(_mutex);
        return _map[id];
    }

    void discardSession(int id) {
        boost::lock_guard<boost::mutex> g(_mutex);
        MapIterator i = _map.find(id);
        if(i != _map.end()) {
            _map.erase(i);
        }
    }

private:
    typedef std::map<int, Value> Map;
    typedef typename Map::iterator MapIterator;

    int _getNextId() {
        int goodId = _nextId++; // Dispense the next id.
        while(true) {
            if(_nextId < _idLimit) { // Still within limit?
                MapIterator i = _map.find(_nextId);
                if(i == _map.end()) { // Not already assigned?
                    break;
                }
            }
            ++_nextId;
        }
        assert(goodId != _nextId); // Should have found *new* nextId.
        return goodId;
    }

    boost::mutex _mutex;
    Map _map;
    int const _idLimit; // explicit arbitrary numerical id limit.
    int _nextId;
};

}}} // namespace lsst::qserv::control

#endif // LSST_QSERV_CONTROL_SESSIONMANAGER_H
