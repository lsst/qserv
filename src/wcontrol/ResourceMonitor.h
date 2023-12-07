// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2011-2018 LSST Corporation.
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
#ifndef LSST_QSERV_WCONTROL_RESOURCEMONITOR_H
#define LSST_QSERV_WCONTROL_RESOURCEMONITOR_H

// System headers
#include <map>
#include <mutex>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

namespace lsst::qserv::wcontrol {

/**
 * Class ResourceMonitor is a thread-safe implementation for a counter of resources
 * which are in use at any given moment by the application.
 */
class ResourceMonitor {
public:
    /// The ResourceCounter type definition maps resources to the number of uses
    /// of each resource.
    using ResourceCounter = std::map<std::string, unsigned int>;

    ResourceMonitor& operator=(ResourceMonitor const&) = delete;
    ResourceMonitor(ResourceMonitor const&) = delete;
    ResourceMonitor() = default;
    ~ResourceMonitor() = default;

    /**
     * Increment resource usage counter by 1.
     * @param resource The name of a resource affected by the operation.
     */
    void increment(std::string const& resource);

    /**
     * Decrement resource usage counter by 1.
     * @param resource The name of a resource affected by the operation.
     */
    void decrement(std::string const& resource);

    /**
     * @param resource The name of a resource.
     * @return The counter of resource uses (by resource name).
     */
    unsigned int count(std::string const& resource) const;

    /**
     * @param chunk The chunk number.
     * @param databaseName The name of a database.
     * @return The counter of resource uses (by database name and chunk number).
     */
    unsigned int count(int chunk, std::string const& databaseName) const;

    /**
     * The method will returns  a sum of counters for all uses of the chunk
     * across all databases.
     * @param chunk The chunk number.
     * @param databaseNames The names of databases.
     * @return The counter of a group of related resources uses.
     */
    unsigned int count(int chunk, std::vector<std::string> const& databaseNames) const;

    /// @return The JSON representation of the object's status for the monitoring.
    nlohmann::json statusToJson() const;

private:
    ResourceCounter _resourceCounter;  ///< Number of uses for each resource.
    mutable std::mutex _mtx;           ///< Mutex for thread safe implementation of the public API.
};

}  // namespace lsst::qserv::wcontrol

#endif  // LSST_QSERV_WCONTROL_RESOURCEMONITOR_H