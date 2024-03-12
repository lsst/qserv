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
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <sstream>

// Third party headers
#include <nlohmann/json.hpp>

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "util/ConfigStore.h"
#include "util/Issue.h"

namespace lsst::qserv::wconfig {

/// Class for handling configuration exceptions.
class ConfigException : public util::Issue {
public:
    ConfigException(Context const& ctx, std::string const& msg) : util::Issue(ctx, msg) {}
};

class ConfigVal {
public:
    using Ptr = std::shared_ptr<ConfigVal>;

    ConfigVal() = delete;
    ConfigVal(std::string const& section, std::string const& name, bool required)
            : _section(section), _name(name), _required(required) {}
    virtual ~ConfigVal() = default;

    ConfigVal(ConfigVal const&) = delete;
    ConfigVal& operator=(ConfigVal const&) = delete;

    std::string getSection() const { return _section; }
    std::string getName() const { return _name; }
    std::string getSectionName() const { return _section + "." + _name; }

    /// Return true if the value is required to be from a file.
    bool isRequired() const { return _required; }

    /// Calling this indicates this value should not be shown to users or put in logs.
    void setHidden() { _hidden = true; }

    /// Return true if this value should not be seen by users or logs.
    bool isHidden() const { return _hidden; }

    void setValSetFromFile(bool setFromFile) { _valSetFromFile = setFromFile; }
    bool isValSetFromFile() const { return _valSetFromFile; }

    /// All child classes should be able to return a valid string version of their value,
    /// but this function will hide values of `_hidden` `ConfigVal`.
    /// If the string value of something that is hidden is needed, dynamic cast and use
    /// to_string(configVal->getVal)).
    virtual std::string getValStr() const = 0;

private:
    std::string const _section;
    std::string const _name;
    bool const _required;          ///< &&& doc
    bool _hidden = false;          ///< &&& doc
    std::string _strFromCfg;       ///< Original string read from the configuration file.
    bool _valSetFromFile = false;  ///< set to true if value was set from configuration file.

protected:
    /// &&& doc
    void logValSet(std::string const& msg = std::string());
};

/// &&& doc
template <typename T>
class ConfigValT : public ConfigVal {
public:
    static std::shared_ptr<ConfigValT<T>> create(std::string const& section, std::string const& name,
                                                 bool required, T defVal) {
        return std::shared_ptr<ConfigValT<T>>(new ConfigValT<T>(section, name, required, defVal));
    }

    virtual ~ConfigValT() = default;

    /// Return the string value of non-hidden config values. (setH
    std::string getValStr() const override {
        std::stringstream os;
        if (!isHidden()) {
            os << _val;
        } else {
            os << "*****";
        }
        return os.str();
    }
    T const& getVal() const { return _val; }
    T const& getDefVal() const { return _defVal; }
    void setVal(T val) {
        _val = val;
        logValSet();
    }

private:
    ConfigValT(std::string const& section, std::string const& name, bool required, T defVal)
            : ConfigVal(section, name, required), _val(defVal), _defVal(defVal) {}
    T _val;
    T _defVal;
};

using ConfigValTInt = ConfigValT<int64_t>;
using ConfigValTIntPtr = std::shared_ptr<ConfigValTInt>;
using ConfigValTUInt = ConfigValT<uint64_t>;
using ConfigValTUIntPtr = std::shared_ptr<ConfigValTUInt>;
using ConfigValTDbl = ConfigValT<double>;
using ConfigValTDblPtr = std::shared_ptr<ConfigValTDbl>;  // &&& unused?
using ConfigValTStr = ConfigValT<std::string>;
using ConfigValTStrPtr = std::shared_ptr<ConfigValTStr>;

class ConfigValResultDeliveryProtocol : public ConfigVal {
public:
    using CvrdpPtr = std::shared_ptr<ConfigValResultDeliveryProtocol>;
    enum TEnum {
        HTTP = 0,  ///< Use HTTP protocol
        XROOT = 1  ///< Use XROOTD file protocol
    };

    ConfigValResultDeliveryProtocol() = delete;
    virtual ~ConfigValResultDeliveryProtocol() = default;

    static CvrdpPtr create(std::string const& section, std::string const& name, bool required,
                           std::string const& defVal) {
        return CvrdpPtr(new ConfigValResultDeliveryProtocol(section, name, required, defVal));
    }

    /// &&& doc
    static TEnum parse(std::string const& str);

    /// &&& doc
    static std::string toString(TEnum protocol);

    /// Return the string value of non-hidden config values. (setH
    std::string getValStr() const override {
        if (!isHidden()) {
            return toString(_val);
        }
        return "*****";
    }

    TEnum getVal() const { return _val; }
    TEnum getDefVal() const { return _defVal; }

    void setVal(TEnum val) {
        _val = val;
        logValSet();
    }

private:
    ConfigValResultDeliveryProtocol(std::string const& section, std::string const& name, bool required,
                                    std::string const& defVal)
            : ConfigVal(section, name, required), _val(parse(defVal)), _defVal(_val) {}
    TEnum _val;
    TEnum _defVal;
};

class ConfigValMap {
public:
    using NameMap = std::map<std::string, ConfigVal::Ptr>;  ///< key is ConfigVal::_name
    using SectionMap = std::map<std::string, NameMap>;      ///< key is ConfigVal::_section

    /// &&& doc
    void addEntry(ConfigVal::Ptr const& newVal);

    /// &&& doc
    ConfigVal::Ptr getEntry(std::string const& section, std::string const& name);

    /// &&& doc
    /// @throws ConfigException if there are problems.
    void readConfigStore(util::ConfigStore const& configStore);

    /// &&& doc
    std::tuple<bool, std::string> checkRequired() const;

    /// &&& doc
    void populateJson(nlohmann::json& js, std::string const& coll);  // &&& add const

private:
    SectionMap _sectionMap;
};

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
    /// Create an instance of WorkerConfig and if a configuration file is provided then
    /// load parameters from the file. Otherwise create an object with default values
    /// of the parameters.
    /// @note One has to call this method at least once before trying to obtain
    ///  a pointer of the instance by calling 'instnce()'. The method 'create()'
    ///  can be called many times. A new instance would be created each time and
    ///  stored witin the class.
    /// @param configFileName - (optional) path to worker INI configuration file
    /// @return the shared pointer to the configuration object
    static std::shared_ptr<WorkerConfig> create(std::string const& configFileName = std::string());

    /// Get a pointer to an instance that was created by a last call to
    /// the method 'create'.
    /// @return the shared pointer to the configuration object
    /// @throws std::logic_error when attempting to call the bethod before creating an instance.
    static std::shared_ptr<WorkerConfig> instance();

    WorkerConfig(WorkerConfig const&) = delete;
    WorkerConfig& operator=(WorkerConfig const&) = delete;

    /// @return thread pool size for shared scans
    unsigned int getThreadPoolSize() const { return _threadPoolSize->getVal(); }

    /// @return maximum number of threads the pool can have in existence at any given time
    unsigned int getMaxPoolThreads() const { return _maxPoolThreads->getVal(); }

    /// @return required number of tasks for table in a chunk for the average to be valid
    unsigned int getRequiredTasksCompleted() const { return _requiredTasksCompleted->getVal(); }

    /// @return maximum number of tasks that can be booted from a single user query
    unsigned int getMaxTasksBootedPerUserQuery() const { return _maxTasksBootedPerUserQuery->getVal(); }

    /// @return maximum number of tasks that can be booted from a single user query
    unsigned int getMaxConcurrentBootedTasks() const { return _maxConcurrentBootedTasks->getVal(); }

    /// @return maximum time for a user query to complete  all tasks on the fast scan
    unsigned int getScanMaxMinutesFast() const { return _scanMaxMinutesFast->getVal(); }

    /// @return maximum time for a user query to complete all tasks on the medium scan
    unsigned int getScanMaxMinutesMed() const { return _scanMaxMinutesMed->getVal(); }

    /// @return maximum time for a user query to complete all tasks on the slow scan
    unsigned int getScanMaxMinutesSlow() const { return _scanMaxMinutesSlow->getVal(); }

    /// @return maximum time for a user query to complete all tasks on the snail scan
    unsigned int getScanMaxMinutesSnail() const { return _scanMaxMinutesSnail->getVal(); }

    /// @return maximum number of task accepted in a group queue
    unsigned int getMaxGroupSize() const { return _maxGroupSize->getVal(); }

    /// @return max thread reserve for fast shared scan
    unsigned int getMaxReserveFast() const { return _maxReserveFast->getVal(); }

    /// @return max thread reserve for medium shared scan
    unsigned int getMaxReserveMed() const { return _maxReserveMed->getVal(); }

    /// @return max thread reserve for slow shared scan
    unsigned int getMaxReserveSlow() const { return _maxReserveSlow->getVal(); }

    /// @return max thread reserve for snail shared scan
    unsigned int getMaxReserveSnail() const { return _maxReserveSnail->getVal(); }

    /// @return class name implementing selected memory management
    std::string const getMemManClass() const { return _memManClass->getVal(); }

    /// @return path to directory where the Memory Manager database resides
    std::string const getMemManLocation() const { return _memManLocation->getVal(); }

    /// @return maximum amount of memory that can be used by Memory Manager
    uint64_t getMemManSizeMb() const { return _memManSizeMb->getVal(); }

    /// @return a configuration for worker MySQL instance.
    mysql::MySqlConfig const& getMySqlConfig() const { return _mySqlConfig; }

    /// @return fast shared scan priority
    unsigned int getPriorityFast() const { return _priorityFast->getVal(); }

    /// @return medium shared scan priority
    unsigned int getPriorityMed() const { return _priorityMed->getVal(); }

    /// @return slow shared scan priority
    unsigned int getPrioritySlow() const { return _prioritySlow->getVal(); }

    /// @return slow shared scan priority
    unsigned int getPrioritySnail() const { return _prioritySnail->getVal(); }

    /// @return maximum concurrent chunks for fast shared scan
    unsigned int getMaxActiveChunksFast() const { return _maxActiveChunksFast->getVal(); }

    /// @return maximum concurrent chunks for medium shared scan
    unsigned int getMaxActiveChunksMed() const { return _maxActiveChunksMed->getVal(); }

    /// @return maximum concurrent chunks for slow shared scan
    unsigned int getMaxActiveChunksSlow() const { return _maxActiveChunksSlow->getVal(); }

    /// @return maximum concurrent chunks for snail shared scan
    unsigned int getMaxActiveChunksSnail() const { return _maxActiveChunksSnail->getVal(); }

    /// @return the maximum number of SQL connections for tasks
    unsigned int getMaxSqlConnections() const { return _maxSqlConnections->getVal(); }

    /// @return the number of SQL connections reserved for interactive tasks
    unsigned int getReservedInteractiveSqlConnections() const {
        return _ReservedInteractiveSqlConnections->getVal();
    }

    /// @return the maximum number of gigabytes that can be used by StreamBuffers
    unsigned int getBufferMaxTotalGB() const { return _bufferMaxTotalGB->getVal(); }

    /// @return the maximum number of concurrent transmits to a czar
    unsigned int getMaxTransmits() const { return _maxTransmits->getVal(); }

    int getMaxPerQid() const { return _maxPerQid->getVal(); }

    /// @return the name of a folder where query results will be stored
    std::string const resultsDirname() const { return _resultsDirname->getVal(); }

    /// @return the port number of the worker XROOTD service for serving result files
    uint16_t resultsXrootdPort() const { return _resultsXrootdPort->getVal(); }

    /// @return the number of the BOOST ASIO threads for servicing HTGTP requests
    size_t resultsNumHttpThreads() const { return _resultsNumHttpThreads->getVal(); }

    /// @return the result delivery method
    ConfigValResultDeliveryProtocol::TEnum resultDeliveryProtocol() const {
        return _resultDeliveryProtocol->getVal();
    }

    /// @return 'true' if result files (if any) left after the previous run of the worker
    /// had to be deleted from the corresponding folder.
    bool resultsCleanUpOnStart() const { return _resultsCleanUpOnStart->getVal(); }

    // Parameters of the worker management service

    std::string const& replicationInstanceId() const { return _replicationInstanceId->getVal(); }
    std::string const& replicationAuthKey() const { return _replicationAuthKey->getVal(); }
    std::string const& replicationAdminAuthKey() const { return _replicationAdminAuthKey->getVal(); }
    std::string const& replicationRegistryHost() const { return _replicationRegistryHost->getVal(); }
    uint16_t replicationRegistryPort() const { return _replicationRegistryPort->getVal(); }
    unsigned int replicationRegistryHearbeatIvalSec() const {
        return _replicationRegistryHearbeatIvalSec->getVal();
    }
    uint16_t replicationHttpPort() const { return _replicationHttpPort->getVal(); }
    size_t replicationNumHttpThreads() const { return _replicationNumHttpThreads->getVal(); }

    /// The actual port number is set at run time after starting the service on
    /// the dynamically allocated port (in case when the port number was set
    /// to 0 in the initial configuration).
    /// @param port The actual port number.
    void setReplicationHttpPort(uint16_t port);

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
    void _populateJsonConfigNew(std::string const& coll);
    void _populateJsonConfig(std::string const& coll);  //&&& del

    /// This mutex protects the static member _instance.
    static std::mutex _mtxOnInstance;

    /// The configuratoon object created by the last call to the method 'test'.
    static std::shared_ptr<WorkerConfig> _instance;

    nlohmann::json _jsonConfig;  ///< JSON-ified configuration

    mysql::MySqlConfig _mySqlConfig;

    ConfigValMap _configValMap;  ///< Map of all configuration entries

    /// &&& doc
    /// @throws ConfigException if the value is invalid or already in the map.
    ConfigVal::Ptr _addToMapBase(ConfigVal::Ptr const& configVal) {
        _configValMap.addEntry(configVal);
        return configVal;
    }

    ConfigValTIntPtr _addToMapInt(std::string const& section, std::string const& name, bool required,
                                  int defVal) {
        auto ptr = ConfigValTInt::create(section, name, required, defVal);
        _addToMapBase(ptr);
        return ptr;
    }
    ConfigValTUIntPtr _addToMapUInt(std::string const& section, std::string const& name, bool required,
                                    unsigned int defVal) {
        auto ptr = ConfigValTUInt::create(section, name, required, defVal);
        _addToMapBase(ptr);
        return ptr;
    }
    ConfigValTDblPtr _addToMapDbl(std::string const& section, std::string const& name, bool required,
                                  double defVal) {
        auto ptr = ConfigValTDbl::create(section, name, required, defVal);
        _addToMapBase(ptr);
        return ptr;
    }
    ConfigValTStrPtr _addToMapStr(std::string const& section, std::string const& name, bool required,
                                  std::string const& defVal) {
        auto ptr = ConfigValTStr::create(section, name, required, defVal);
        _addToMapBase(ptr);
        return ptr;
    }

    ConfigValResultDeliveryProtocol::CvrdpPtr _addToMapCvrdp(std::string const& section,
                                                             std::string const& name, bool required,
                                                             std::string const& defVal) {
        auto ptr = ConfigValResultDeliveryProtocol::create(section, name, required, defVal);
        _addToMapBase(ptr);
        return ptr;
    }

    bool const Required = true;  //&&& ???
    bool const NotReq = false;   //&&& ???

    ConfigValTStrPtr _memManClass = _addToMapStr("memman", "class", NotReq, "MemManReal");
    ConfigValTUIntPtr _memManSizeMb = _addToMapUInt("memman", "memory", NotReq, 1000);
    ConfigValTStrPtr _memManLocation = _addToMapStr("memman", "location", Required, "/qserv/data/mysql");
    ConfigValTUIntPtr _threadPoolSize = _addToMapUInt("scheduler", "thread_pool_size", NotReq, 0);
    ConfigValTUIntPtr _maxPoolThreads = _addToMapUInt("scheduler", "max_pool_threads", NotReq, 5000);
    ConfigValTUIntPtr _maxGroupSize = _addToMapUInt("scheduler", "group_size", NotReq, 1);
    ConfigValTUIntPtr _requiredTasksCompleted =
            _addToMapUInt("scheduler", "required_tasks_completed", NotReq, 25);
    ConfigValTUIntPtr _prioritySlow = _addToMapUInt("scheduler", "priority_slow", NotReq, 2);
    ConfigValTUIntPtr _prioritySnail = _addToMapUInt("scheduler", "priority_snail", NotReq, 1);
    ConfigValTUIntPtr _priorityMed = _addToMapUInt("scheduler", "priority_med", NotReq, 3);
    ConfigValTUIntPtr _priorityFast = _addToMapUInt("scheduler", "priority_fast", NotReq, 4);
    ConfigValTUIntPtr _maxReserveSlow = _addToMapUInt("scheduler", "reserve_slow", NotReq, 2);
    ConfigValTUIntPtr _maxReserveSnail = _addToMapUInt("scheduler", "reserve_snail", NotReq, 2);
    ConfigValTUIntPtr _maxReserveMed = _addToMapUInt("scheduler", "reserve_med", NotReq, 2);
    ConfigValTUIntPtr _maxReserveFast = _addToMapUInt("scheduler", "reserve_fast", NotReq, 2);
    ConfigValTUIntPtr _maxActiveChunksSlow = _addToMapUInt("scheduler", "maxactivechunks_slow", NotReq, 2);
    ConfigValTUIntPtr _maxActiveChunksSnail = _addToMapUInt("scheduler", "maxactivechunks_snail", NotReq, 1);
    ConfigValTUIntPtr _maxActiveChunksMed = _addToMapUInt("scheduler", "maxactivechunks_med", NotReq, 4);
    ConfigValTUIntPtr _maxActiveChunksFast = _addToMapUInt("scheduler", "maxactivechunks_fast", NotReq, 4);
    ConfigValTUIntPtr _scanMaxMinutesFast = _addToMapUInt("scheduler", "scanmaxminutes_fast", NotReq, 60);
    ConfigValTUIntPtr _scanMaxMinutesMed = _addToMapUInt("scheduler", "scanmaxminutes_med", NotReq, 60 * 8);
    ConfigValTUIntPtr _scanMaxMinutesSlow =
            _addToMapUInt("scheduler", "scanmaxminutes_slow", NotReq, 60 * 12);
    ConfigValTUIntPtr _scanMaxMinutesSnail =
            _addToMapUInt("scheduler", "scanmaxminutes_snail", NotReq, 60 * 24);
    ConfigValTUIntPtr _maxTasksBootedPerUserQuery =
            _addToMapUInt("scheduler", "maxtasksbootedperuserquery", NotReq, 5);
    ConfigValTUIntPtr _maxConcurrentBootedTasks =
            _addToMapUInt("scheduler", "maxconcurrentbootedtasks", NotReq, 25);
    ConfigValTUIntPtr _maxSqlConnections = _addToMapUInt("sqlconnections", "maxsqlconn", NotReq, 800);
    ConfigValTUIntPtr _ReservedInteractiveSqlConnections =
            _addToMapUInt("sqlconnections", "reservedinteractivesqlconn", NotReq, 50);
    ConfigValTUIntPtr _bufferMaxTotalGB = _addToMapUInt("transmit", "buffermaxtotalgb", NotReq, 41);
    ConfigValTUIntPtr _maxTransmits = _addToMapUInt("transmit", "maxtransmits", NotReq, 40);
    ConfigValTIntPtr _maxPerQid = _addToMapInt("transmit", "maxperqid", NotReq, 3);
    ConfigValTStrPtr _resultsDirname = _addToMapStr("results", "dirname", NotReq, "/qserv/data/results");
    ConfigValTUIntPtr _resultsXrootdPort = _addToMapUInt("results", "xrootd_port", NotReq, 1094);
    ConfigValTUIntPtr _resultsNumHttpThreads = _addToMapUInt("results", "num_http_threads", NotReq, 1);
    ConfigValResultDeliveryProtocol::CvrdpPtr _resultDeliveryProtocol =
            _addToMapCvrdp("results", "protocol", NotReq, "HTTP");
    ConfigValTIntPtr _resultsCleanUpOnStart =
            _addToMapInt("results", "clean_up_on_start", NotReq, 1);  //&&& boolean
    ConfigValTStrPtr _replicationInstanceId = _addToMapStr("replication", "instance_id", NotReq, "");
    ConfigValTStrPtr _replicationAuthKey = _addToMapStr("replication", "auth_key", NotReq, "");

    ConfigValTStrPtr _replicationAdminAuthKey = _addToMapStr("replication", "admin_auth_key", NotReq, "");

    ConfigValTStrPtr _replicationRegistryHost = _addToMapStr("replication", "registry_host", Required, "");
    ConfigValTUIntPtr _replicationRegistryPort = _addToMapUInt("replication", "registry_port", Required, 0);
    ConfigValTUIntPtr _replicationRegistryHearbeatIvalSec =
            _addToMapUInt("replication", "registry_heartbeat_ival_sec", NotReq, 1);
    ConfigValTUIntPtr _replicationHttpPort = _addToMapUInt("replication", "http_port", Required, 0);
    ConfigValTUIntPtr _replicationNumHttpThreads =
            _addToMapUInt("replication", "num_http_threads", NotReq, 2);
    ConfigValTUIntPtr _mysqlPort = _addToMapUInt("mysql", "port", NotReq, 4048);
    ConfigValTStrPtr _mysqlSocket = _addToMapStr("mysql", "socket", NotReq, "");
    ConfigValTStrPtr _mysqlUsername = _addToMapStr("mysql", "username", Required, "qsmaster");
    ConfigValTStrPtr _mysqlPassword = _addToMapStr("mysql", "password", Required, "not_the_password");

    ConfigValTStrPtr _mysqlHostname = _addToMapStr("mysql", "hostname", Required, "none");
};

}  // namespace lsst::qserv::wconfig

#endif  // LSST_QSERV_WCONFIG_WORKERCONFIG_H
