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
#include "wcontrol/ResourceMonitor.h"

// Qserv headers
#include "global/ResourceUnit.h"

using namespace std;
using namespace nlohmann;

namespace lsst::qserv::wcontrol {

void ResourceMonitor::increment(string const& resource) {
    lock_guard<mutex> lock(_mtx);
    _resourceCounter[resource]++;
}

void ResourceMonitor::decrement(string const& resource) {
    lock_guard<mutex> lock(_mtx);
    if (not _resourceCounter.count(resource)) return;
    if (not --(_resourceCounter[resource])) _resourceCounter.erase(resource);
}

unsigned int ResourceMonitor::count(string const& resource) const {
    lock_guard<mutex> lock(_mtx);
    return _resourceCounter.count(resource) ? _resourceCounter.at(resource) : 0;
}

unsigned int ResourceMonitor::count(int chunk, string const& databaseName) const {
    return count(ResourceUnit::makePath(chunk, databaseName));
}

unsigned int ResourceMonitor::count(int chunk, vector<string> const& databaseNames) const {
    unsigned int result = 0;
    for (string const& database : databaseNames) {
        result += count(chunk, database);
    }
    return result;
}

json ResourceMonitor::statusToJson() const {
    lock_guard<mutex> lock(_mtx);
    json result = json::array();
    for (auto&& [resource, counter] : _resourceCounter) {
        result.push_back({resource, counter});
    }
    return result;
}

}  // namespace lsst::qserv::wcontrol