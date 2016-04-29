// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2008-2015 LSST Corporation.
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

#ifndef LSST_QSERV_WCONFIG_WORKERCONFIG_H
#define LSST_QSERV_WCONFIG_WORKERCONFIG_H

// System headers
#include <string>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

namespace lsst {
namespace qserv {

namespace wconfig {

/// The Config object provides a thin abstraction layer to shield code from
/// the details of how the qserv worker is configured.
class WorkerConfig {
public:
    WorkerConfig(util::ConfigStore const& ConfigStore);


    /* Get groupScheduler group size
     * set from configuration file
     */
    const unsigned int getThreadPoolSize() const {
        return _threadPoolSize;
    }

    const unsigned int getMaxGroupSize() const {
        return _maxGroupSize;
    }

    /* Get max thread reserve */
    const unsigned int getMaxReserveFast() const {
        return _maxReserveFast;
    }

    const unsigned int getMaxReserveMed() const {
        return _maxReserveMed;
    }

    const unsigned int getMaxReserveSlow() const {
        return _maxReserveSlow;
    }

    const std::string& getMemManClass() const {
        return _memManClass;
    }

    const std::string& getMemManLocation() const {
        return _memManLocation;
    }

    const uint64_t getMemManSizeMb() const {
        return _memManSizeMb;
    }

    const mysql::MySqlConfig& getMySqlConfig() const {
        return _mySqlConfig;
    }

    /* Get fast shared scan priority */
    const unsigned int getPriorityFast() const {
        return _priorityFast;
    }

    /* Get medium shared scan priority */
    const unsigned int getPriorityMed() const {
        return _priorityMed;
    }

    /* Get slow shared scan priority */
    const unsigned int getPrioritySlow() const {
        return _prioritySlow;
    }

    /** Overload output operator for current class
     *
     * @param out
     * @param workerConfig
     * @return an output stream
     */
    friend std::ostream& operator<<(std::ostream &out, WorkerConfig const& workerConfig);

private:
    mysql::MySqlConfig const _mySqlConfig;

    std::string const _memManClass;
    uint64_t const _memManSizeMb;
    std::string const _memManLocation;

    unsigned int const _threadPoolSize;
    unsigned int const _maxGroupSize;

    unsigned int const _prioritySlow;
    unsigned int const _priorityMed;
    unsigned int const _priorityFast;

    unsigned int const _maxReserveSlow;
    unsigned int const _maxReserveMed;
    unsigned int const _maxReserveFast;
};

}}} // namespace qserv::core::wconfig

#endif // LSST_QSERV_WCONFIG_WORKERCONFIG_H
