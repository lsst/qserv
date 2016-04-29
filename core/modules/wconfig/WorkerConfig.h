// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2016 LSST Corporation.
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
#include <cstdint>
#include <string>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

namespace lsst {
namespace qserv {
namespace wconfig {

/**
 *  Provide all configuration parameters for a Qserv worker instance
 *
 *  Parse an INI configuration file, identify required parameters and ignore
 *  others, analyze and store them inside private member variables, use default
 *  values for missing parameters, provide accessor for each of these variable.
 *  This class hide configuration complexity
 *  from other part of the code. All private member variables are related to INI
 *  parameters and are immutables.
 *
 */
class WorkerConfig {
public:

    /**
     *  Create WorkerConfig instance from a INI configuration file
     *
     *  @param configFileName: path to worker INI configuration file
     */
    explicit WorkerConfig(std::string configFileName)
        : WorkerConfig(util::ConfigStore(configFileName)) {
    }

    WorkerConfig(WorkerConfig const&) = delete;
    WorkerConfig& operator=(WorkerConfig const&) = delete;

    /* Get thread pool size for shared scans
     *
     * @return thread pool size for shared scans
     */
    unsigned int getThreadPoolSize() const {
        return _threadPoolSize;
    }

    /* Get maximum number of task accepted in a group queue
     *
     * @return maximum number of task accepted in a group queue
     */
    unsigned int getMaxGroupSize() const {
        return _maxGroupSize;
    }

    /* Get max thread reserve for fast shared scan
     *
     * @return max thread reserve for fast shared scan
     */
    unsigned int getMaxReserveFast() const {
        return _maxReserveFast;
    }

    /* Get max thread reserve for medium shared scan
     *
     * @return max thread reserve for medium shared scan
     */
    unsigned int getMaxReserveMed() const {
        return _maxReserveMed;
    }

    /* Get max thread reserve for slow shared scan
     *
     * @return max thread reserve for slow shared scan
     */
    unsigned int getMaxReserveSlow() const {
        return _maxReserveSlow;
    }

    /* Get selected memory management implementation
     *
     * @return class name implementing selected memory management
     */
    std::string const& getMemManClass() const {
        return _memManClass;
    }

    /* Get path to directory where the Memory Manager database resides
     *
     * @return path to directory where the Memory Manager database resides
     */
    std::string const& getMemManLocation() const {
        return _memManLocation;
    }

    /* Get maximum amount of memory that can be used by Memory Manager
     *
     * @return maximum amount of memory that can be used by Memory Manager
     */
    uint64_t getMemManSizeMb() const {
        return _memManSizeMb;
    }

    /* Get MySQL configuration for worker MySQL instance
     *
     * @return a structure containing MySQL parameters
     */
    mysql::MySqlConfig const& getMySqlConfig() const {
        return _mySqlConfig;
    }

    /* Get fast shared scan priority
     *
     * @return fast shared scan priority
     */
    unsigned int const getPriorityFast() const {
        return _priorityFast;
    }

    /* Get medium shared scan priority
     *
     * @return medium shared scan priority
     */
    unsigned int const getPriorityMed() const {
        return _priorityMed;
    }

    /* Get slow shared scan priority
     *
     * @return slow shared scan priority
     */
    unsigned int const getPrioritySlow() const {
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

    WorkerConfig(util::ConfigStore const& configStore);

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
