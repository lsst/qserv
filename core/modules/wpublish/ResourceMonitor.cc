// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2012-2018 AURA/LSST.
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
#include "wpublish/ResourceMonitor.h"

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "global/ResourceUnit.h"


// This macro to appear witin each block which requires thread safety

#define LOCK_GUARD \
std::lock_guard<std::mutex> lock(_mtx)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.wpublish.ResourceMonitor");

} // annonymous namespace

namespace lsst {
namespace qserv {
namespace wpublish {

void
ResourceMonitor::add (std::string const& resource) {

    LOCK_GUARD;

    _resourceCounter[resource]++;
}

void
ResourceMonitor::remove (std::string const& resource) {

    LOCK_GUARD;

    if (not _resourceCounter.count(resource)) return;
    if (not --(_resourceCounter[resource])) _resourceCounter.erase(resource);
}

unsigned int
ResourceMonitor::count (std::string const& resource) const {

    LOCK_GUARD;

    return _resourceCounter.count(resource) ? _resourceCounter.at(resource) : 0;
}

unsigned int
ResourceMonitor::count (int chunk,
                        std::string const& db) const {
    return count(ResourceUnit::makePath(chunk, db));
}

unsigned int
ResourceMonitor::count (int chunk,
                        std::vector<std::string> const& dbs) const {
    unsigned int result = 0;
    for (std::string const& db: dbs) result += count(chunk, db);
    return result;
}

ResourceMonitor::ResourceCounter
ResourceMonitor::resourceCounter () const {

    // Make a copy of the map while under protection of the lock guard.

    LOCK_GUARD;

    ResourceCounter result = _resourceCounter;
    
    return result;
}

}}} // namespace lsst::qserv::wpublish