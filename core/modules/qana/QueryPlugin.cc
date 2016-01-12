// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2015 AURA/LSST.
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

/**
  * @file
  *
  * @brief Implementation of the factory lookup code for QueryPlugins.
  *
  * @author Daniel L. Wang, SLAC
  */

// Parent class
#include "qana/QueryPlugin.h"

// System headers
#include <map>
#include <mutex>

// Qserv headers
#include "qana/PluginNotFoundError.h"

namespace { // File-scope helpers

struct Registry {
    typedef std::map<std::string, lsst::qserv::qana::QueryPlugin::FactoryPtr> FactoryMap;

    std::mutex mutex;
    FactoryMap map;
};

Registry& registry() {
    static Registry instance;
    return instance;
}

} // anonymous namespace

namespace lsst {
namespace qserv {
namespace qana {

QueryPlugin::Ptr QueryPlugin::newInstance(std::string const& name) {
    Registry& r = registry();
    std::lock_guard<std::mutex> guard(r.mutex);
    Registry::FactoryMap::iterator e = r.map.find(name);
    if (e == r.map.end()) {
        throw PluginNotFoundError(name);
    } else {
        return e->second->newInstance();
    }
}

void QueryPlugin::registerClass(QueryPlugin::FactoryPtr f) {
    Registry& r = registry();
    std::lock_guard<std::mutex> guard(r.mutex);
    if (f) {
        r.map[f->getName()] = f;
    }
}

}}} // namespace lsst::qserv::qana
