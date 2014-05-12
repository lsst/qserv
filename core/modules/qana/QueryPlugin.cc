/*
 * LSST Data Management System
 * Copyright 2012 LSST Corporation.
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
  * @file QueryMapping.cc
  *
  * @brief Implementation of the factory lookup code for QueryPlugins.
  *
  * @author Daniel L. Wang, SLAC
  */

#include "qana/QueryPlugin.h"

// System headers
#include <map>

// Third-party headers
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
//#include <boost/thread/call_once.hpp>

// Local headers
#include "qana/PluginNotFoundError.h"

namespace lsst {
namespace qserv {
namespace qana {

namespace { // File-scope helpers
    typedef std::map<std::string, lsst::qserv::qana::QueryPlugin::FactoryPtr> FactoryMap;

//boost::once_flag factoryMapFlag = BOOST_ONCE_INIT;
static boost::mutex factoryMapMutex;

// Static local member
FactoryMap& factoryMap() {
    static FactoryMap instance;
    return instance;
}

} // anonymous namespace

QueryPlugin::Ptr
QueryPlugin::newInstance(std::string const& name) {
    boost::lock_guard<boost::mutex> guard(factoryMapMutex);

    FactoryMap::iterator e = factoryMap().find(name);
    if(e == factoryMap().end()) { // No plugin.
        throw PluginNotFoundError(name);
    } else {
        return e->second->newInstance();
    }
}

void
QueryPlugin::registerClass(QueryPlugin::FactoryPtr f) {
    boost::lock_guard<boost::mutex> guard(factoryMapMutex);
    if(!f.get()) return;
    factoryMap()[f->getName()] = f;
}

}}} // namespace lsst::qserv::qana
