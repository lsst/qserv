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
#include <memory>
#include <mutex>
#include <string>

// Third party headers
#include <nlohmann/json.hpp>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"

namespace lsst::qserv::wconfig {

/**
 * Provide all configuration parameters for a Qserv worker instance.
 * Parse an INI configuration file, identify required parameters and ignore
 * others, analyze and store them inside private member variables, use default
 * values for missing parameters, provide accessor for each of these variable.
 * This class hides configuration complexity
 * from other part of the code. All private member variables are related to INI
 * parameters and are immutables.
 *
 * @note the class has a thread-safe API.
 */
class WorkerConfig {
public:
    /**
     * The enumeration type representing available methods for pulling query results
     * from workers.
     * @note The default method, if none was found in the configuration, would be SSI.
     */
    enum class ResultDeliveryProtocol : int {
        SSI = 0,    ///< Pull data from the SSI stream (default)
        XROOT = 1,  ///< Use XROOTD file protocol
        HTTP = 2    ///< Use HTTP protocol
    };

    /// @return the string representation of the protocol
    /// @throw std::invalid_argument if the protocol is unknown
    static std::string protocol2str(ResultDeliveryProtocol const& p);

    /**
     * Create an instance of WorkerConfig and if a configuration file is provided then
     * load parameters from the file. Otherwise create an object with default values
     * of the parameters.
     * @note One has to call this method at least once before trying to obtain
     *   a pointer of the instance by calling 'instnce()'. The method 'create()'
     *   can be called many times. A new instance would be created each time and
     *   stored witin the class.
     * @param configFileName - (optional) path to worker INI configuration file
     * @return the shared pointer to the configuration object
     */
    static std::shared_ptr<WorkerConfig> create(std::string const& configFileName = std::string());

    /**
     * Get a pointer to an instance that was created by a last call to
     * the method 'create'.
     * @return the shared pointer to the configuration object
     * @throws std::logic_error when attempting to call the bethod before creating an instance.
     */
    static std::shared_ptr<WorkerConfig> instance();

    WorkerConfig(WorkerConfig const&) = delete;
    WorkerConfig& operator=(WorkerConfig const&) = delete;

    /// @return thread pool size for shared scans
    unsigned int getThreadPoolSize() const { return _threadPoolSize; }

    /// @return maximum number of threads the pool can have in existence at any given time
    unsigned int getMaxPoolThreads() const { return _maxPoolThreads; }

    /// @return required number of tasks for table in a chunk for the average to be valid
    unsigned int getRequiredTasksCompleted() const { return _requiredTasksCompleted; }

    /// @return maximum number of tasks that can be booted from a single user query
    unsigned int getMaxTasksBootedPerUserQuery() const { return _maxTasksBootedPerUserQuery; }

    /// @return maximum time for a user query to complete  all tasks on the fast scan
    unsigned int getScanMaxMinutesFast() const { return _scanMaxMinutesFast; }

    /// @return maximum time for a user query to complete all tasks on the medium scan
    unsigned int getScanMaxMinutesMed() const { return _scanMaxMinutesMed; }

    /// @return maximum time for a user query to complete all tasks on the slow scan
    unsigned int getScanMaxMinutesSlow() const { return _scanMaxMinutesSlow; }

    /// @return maximum time for a user query to complete all tasks on the snail scan
    unsigned int getScanMaxMinutesSnail() const { return _scanMaxMinutesSnail; }

    /// @return maximum number of task accepted in a group queue
    unsigned int getMaxGroupSize() const { return _maxGroupSize; }

    /// @return max thread reserve for fast shared scan
    unsigned int getMaxReserveFast() const { return _maxReserveFast; }

    /// @return max thread reserve for medium shared scan
    unsigned int getMaxReserveMed() const { return _maxReserveMed; }

    /// @return max thread reserve for slow shared scan
    unsigned int getMaxReserveSlow() const { return _maxReserveSlow; }

    /// @return max thread reserve for snail shared scan
    unsigned int getMaxReserveSnail() const { return _maxReserveSnail; }

    /// @return class name implementing selected memory management
    std::string const& getMemManClass() const { return _memManClass; }

    /// @return path to directory where the Memory Manager database resides
    std::string const& getMemManLocation() const { return _memManLocation; }

    /// @return maximum amount of memory that can be used by Memory Manager
    uint64_t getMemManSizeMb() const { return _memManSizeMb; }

    /// @return a configuration for worker MySQL instance.
    mysql::MySqlConfig const& getMySqlConfig() const { return _mySqlConfig; }

    /// @return fast shared scan priority
    unsigned int getPriorityFast() const { return _priorityFast; }

    /// @return medium shared scan priority
    unsigned int getPriorityMed() const { return _priorityMed; }

    /// @return slow shared scan priority
    unsigned int getPrioritySlow() const { return _prioritySlow; }

    /// @return slow shared scan priority
    unsigned int getPrioritySnail() const { return _prioritySnail; }

    /// @return maximum concurrent chunks for fast shared scan
    unsigned int getMaxActiveChunksFast() const { return _maxActiveChunksFast; }

    /// @return maximum concurrent chunks for medium shared scan
    unsigned int getMaxActiveChunksMed() const { return _maxActiveChunksMed; }

    /// @return maximum concurrent chunks for slow shared scan
    unsigned int getMaxActiveChunksSlow() const { return _maxActiveChunksSlow; }

    /// @return maximum concurrent chunks for snail shared scan
    unsigned int getMaxActiveChunksSnail() const { return _maxActiveChunksSnail; }

    /// @return the maximum number of SQL connections for tasks
    unsigned int getMaxSqlConnections() const { return _maxSqlConnections; }

    /// @return the number of SQL connections reserved for interactive tasks
    unsigned int getReservedInteractiveSqlConnections() const { return _ReservedInteractiveSqlConnections; }

    /// @return the maximum number of gigabytes that can be used by StreamBuffers
    unsigned int getBufferMaxTotalGB() const { return _bufferMaxTotalGB; }

    /// @return the maximum number of concurrent transmits to a czar
    unsigned int getMaxTransmits() const { return _maxTransmits; }

    int getMaxPerQid() const { return _maxPerQid; }

    /// @return the name of a folder where query results will be stored
    std::string const& resultsDirname() const { return _resultsDirname; }

    /// @return the port number of the worker XROOTD service for serving result files
    uint16_t resultsXrootdPort() const { return _resultsXrootdPort; }

    /// @return the number of the BOOST ASIO threads for servicing HTGTP requests
    size_t resultsNumHttpThreads() const { return _resultsNumHttpThreads; }

    /// @return the result delivery method
    ResultDeliveryProtocol resultDeliveryProtocol() const { return _resultDeliveryProtocol; }

    /// @return 'true' if result files (if any) left after the previous run of the worker
    /// had to be deleted from the corresponding folder.
    bool resultsCleanUpOnStart() const { return _resultsCleanUpOnStart; }

    /// @return the JSON representation of the configuration parameters.
    /// @note The object has two collections of the parameters: 'input' - for
    /// parameters that were proided to the construction of the class, and
    /// 'actual' - for parameters that were expected (and set in the transient
    /// state). These collection may not be the same. For example, some older
    /// parameters may be phased out while still being present in the configuration
    /// files. Or, new actual parameters (with some reasonable defaults) might be
    /// introduced while not being set in the configuration file.
    nlohmann::json toJson() const { return _jsonConfig; }

    /**
     * Dump the configuration object onto the output stream.
     * @param out - the output stream object
     * @param workerConfig - worker configuration object
     * @return the output stream object
     */
    friend std::ostream& operator<<(std::ostream& out, WorkerConfig const& workerConfig);

private:
    /// Initialize parameters with default values
    WorkerConfig();

    /// Initialize parameters from the configuration store
    /// @param configStore
    WorkerConfig(util::ConfigStore const& configStore);

    /// This method is called by both c-tors to populate the JSON configuration with actual
    /// parameters of the object.
    /// @param coll The name of a collection to be populated.
    void _populateJsonConfig(std::string const& coll);

    /// This mutex protects the static member _instance.
    static std::mutex _mtxOnInstance;

    /// The configuratoon object created by the last call to the method 'test'.
    static std::shared_ptr<WorkerConfig> _instance;

    nlohmann::json _jsonConfig;  ///< JSON-ified initial configuration

    mysql::MySqlConfig _mySqlConfig;

    std::string const _memManClass;
    uint64_t const _memManSizeMb;
    std::string const _memManLocation;

    unsigned int const _threadPoolSize;
    unsigned int const _maxPoolThreads;
    unsigned int const _maxGroupSize;
    unsigned int const _requiredTasksCompleted;

    unsigned int const _prioritySlow;
    unsigned int const _prioritySnail;
    unsigned int const _priorityMed;
    unsigned int const _priorityFast;

    unsigned int const _maxReserveSlow;
    unsigned int const _maxReserveSnail;
    unsigned int const _maxReserveMed;
    unsigned int const _maxReserveFast;

    unsigned int const _maxActiveChunksSlow;
    unsigned int const _maxActiveChunksSnail;
    unsigned int const _maxActiveChunksMed;
    unsigned int const _maxActiveChunksFast;

    unsigned int const _scanMaxMinutesFast;
    unsigned int const _scanMaxMinutesMed;
    unsigned int const _scanMaxMinutesSlow;
    unsigned int const _scanMaxMinutesSnail;
    unsigned int const _maxTasksBootedPerUserQuery;

    unsigned int const _maxSqlConnections;
    unsigned int const _ReservedInteractiveSqlConnections;
    unsigned int const _bufferMaxTotalGB;
    unsigned int const _maxTransmits;
    int const _maxPerQid;
    std::string const _resultsDirname;
    uint16_t const _resultsXrootdPort;
    size_t const _resultsNumHttpThreads;
    ResultDeliveryProtocol const _resultDeliveryProtocol;
    bool const _resultsCleanUpOnStart;
};

}  // namespace lsst::qserv::wconfig

#endif  // LSST_QSERV_WCONFIG_WORKERCONFIG_H
