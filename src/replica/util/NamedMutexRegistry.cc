

/*
 * LSST Data Management System
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

// Class header
#include "replica/util/NamedMutexRegistry.h"

// System headers
#include <cassert>
#include <stdexcept>

using namespace std;

namespace lsst::qserv::replica {

shared_ptr<replica::Mutex> NamedMutexRegistry::get(string const& name) {
    string const context = "NamedMutexRegistry::" + string(__func__) + " ";
    if (name.empty()) {
        throw invalid_argument(context + "the name of a mutex can't be empty.");
    }
    replica::Lock const lock(_registryAccessMtx, "NamedMutexRegistry(" + name + ")");
    shared_ptr<replica::Mutex> mtx;
    auto const itr = _registry.find(name);
    if (itr != _registry.end()) {
        mtx = itr->second;
    } else {
        auto const insertItr = _registry.insert(make_pair(name, make_shared<replica::Mutex>()));
        if (!insertItr.second) {
            throw runtime_error(context + "duplicate key: " + name + ".");
        }
        mtx = insertItr.first->second;
    }

    // All we can guarantee here is that one copy of the pointer is stored in the registry,
    // and the other one is in the above-initialized instance of the shared pointer to be
    // returned back to a client upon completion of this method.
    assert(mtx.use_count() > 1);

    // Garbage collect all unused mutex objects except the above found/created mutex.
    // The algorithm is based on a guarantee (C++11 standard) that the reference counter of
    // the shared pointer is an atomic object and it's thread safe. Therefore a reference
    // counter that is less than 2 would guarantee that no other reference to the pointed
    // object exists beyond a scope of the registry.
    for (auto it = _registry.begin(); it != _registry.end();) {
        if ((it->second.use_count() < 2) && (it->second->id() != mtx->id())) {
            it = _registry.erase(it);
        } else {
            ++it;
        }
    }
    return mtx;
}

size_t NamedMutexRegistry::size() const {
    replica::Lock const lock(_registryAccessMtx, "NamedMutexRegistry::" + string(__func__));
    return _registry.size();
}

}  // namespace lsst::qserv::replica
