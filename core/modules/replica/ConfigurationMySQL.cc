/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#include "replica/ConfigurationMySQL.h"

// System headers
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationMySQL");

using namespace lsst::qserv::replica;

template <typename T>
bool tryParameter(database::mysql::Row& row,
                  std::string const&    desiredCategory,
                  std::string const&    desiredParam,
                  T&                    value) {

    std::string category;
    row.get("category", category);
    if (desiredCategory != category) { return false; }

    std::string param;
    row.get("param", param);
    if (desiredParam != param) { return false; }

    row.get("value", value);
    return true;
}

template <typename T>
void readMandatoryParameter(database::mysql::Row& row,
                            std::string const&    name,
                            T&                    value) {
    if (not row.get(name, value)) {
        throw std::runtime_error(
                "ConfigurationMySQL::readMandatoryParameter()  the field '" + name +
                "' is not allowed to be NULL");
    }
}

template <typename T>
void readOptionalParameter(database::mysql::Row& row,
                           std::string const&    name,
                           T&                    value,
                           T const&              defaultValue) {
    if (not row.get(name, value)) {
        value = defaultValue;
    }
}
} // namespace

namespace lsst {
namespace qserv {
namespace replica {

ConfigurationMySQL::ConfigurationMySQL(database::mysql::ConnectionParams const& connectionParams)
    :   Configuration(),
        _connectionParams(connectionParams) {

    loadConfiguration();
}

std::string ConfigurationMySQL::configUrl() const {
    return  _databaseTechnology + ":" + _connectionParams.toString();
}

WorkerInfo const& ConfigurationMySQL::disableWorker(std::string const& name) {

    std::string const context = "ConfigurationMySQL::disableWorker  ";

    LOGS(_log, LOG_LVL_DEBUG, context << name);

    // This will also throw an exception if the worker is unknown
    WorkerInfo const& info = workerInfo(name);
    if (info.isEnabled) {

        database::mysql::Connection::pointer conn;
        try {
    
            // First update the database
            conn = database::mysql::Connection::open(_connectionParams);
            conn->begin();
            conn->executeSimpleUpdateQuery(
                "config_worker",
                conn->sqlEqual("name", name),
                std::make_pair("is_enabled",  0));
            conn->commit();
    
            // Then update the transient state (note this change will be also be)
            // seen via the above obtainer reference to the worker description.
            _workerInfo[name].isEnabled = false;
    
        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << ex.what());
            if (conn and conn->inTransaction()) {
                conn->commit();
            }
        }
    }
    return info;
}

void ConfigurationMySQL::deleteWorker(std::string const& name) {

    std::string const context = "ConfigurationMySQL::deleteWorker  ";

    LOGS(_log, LOG_LVL_DEBUG, context << name);

    // This will also throw an exception if the worker is unknown
    WorkerInfo const& info = workerInfo(name);

    database::mysql::Connection::pointer conn;
    try {

        // First update the database
        conn = database::mysql::Connection::open(_connectionParams);
        conn->begin();
        conn->execute ("DELETE FROM config_worker WHERE " + conn->sqlEqual("name", info.name));
        conn->commit();

        // Then update the transient state 
        _workerInfo.erase(info.name);

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << ex.what());
        if (conn and conn->inTransaction()) {
            conn->commit();
        }
    }
}

void ConfigurationMySQL::loadConfiguration() {

    std::string const context = "ConfigurationMySQL::loadConfiguration  ";

    // The common parameters (if any defined) of the workers will be intialize
    // from table 'config' and be used as defaults when reading worker-specific
    // configurations from table 'config_worker'
    uint16_t    commonWorkerSvcPort = Configuration::defaultWorkerSvcPort;
    uint16_t    commonWorkerFsPort  = Configuration::defaultWorkerFsPort;
    std::string commonWorkerDataDir = Configuration::defaultDataDir;

    // Open and keep a database connection for loading other parameters
    // from there.
    database::mysql::Connection::pointer const conn =
        database::mysql::Connection::open(_connectionParams);

    database::mysql::Row row;

    // Read the common parameters and defaults shared by all components
    // of the replication system. The table also provides default values
    // for some critical parameters of the worker-side services.

    conn->execute("SELECT * FROM " + conn->sqlId("config"));

    while (conn->next(row)) {
        
        ::tryParameter(row, "common", "request_buf_size_bytes",     _requestBufferSizeBytes) or
        ::tryParameter(row, "common", "request_retry_interval_sec", _retryTimeoutSec) or

        ::tryParameter(row, "controller", "http_server_port",    _controllerHttpPort) or
        ::tryParameter(row, "controller", "http_server_threads", _controllerHttpThreads) or
        ::tryParameter(row, "controller", "request_timeout_sec", _controllerRequestTimeoutSec) or

        ::tryParameter(row, "worker", "technology",                 _workerTechnology) or
        ::tryParameter(row, "worker", "num_svc_processing_threads", _workerNumProcessingThreads) or
        ::tryParameter(row, "worker", "num_fs_processing_threads",  _workerNumFsProcessingThreads) or
        ::tryParameter(row, "worker", "fs_buf_size_bytes",          _workerFsBufferSizeBytes) or
        ::tryParameter(row, "worker", "svc_port",                   commonWorkerSvcPort)  or
        ::tryParameter(row, "worker", "fs_port",                    commonWorkerFsPort) or
        ::tryParameter(row, "worker", "data_dir",                   commonWorkerDataDir);
    }
    
    // Read worker-specific configurations and construct WorkerInfo.
    // Use the above retreived common parameters as defaults where applies

    conn->execute("SELECT * FROM " + conn->sqlId ("config_worker"));

    while (conn->next(row)) {
        WorkerInfo info;
        ::readMandatoryParameter(row, "name",         info.name);
        ::readMandatoryParameter(row, "is_enabled",   info.isEnabled);
        ::readMandatoryParameter(row, "is_read_only", info.isReadOnly);
        ::readMandatoryParameter(row, "svc_host",     info.svcHost);
        ::readOptionalParameter( row, "svc_port",     info.svcPort, commonWorkerSvcPort);
        ::readMandatoryParameter(row, "fs_host",      info.fsHost);
        ::readOptionalParameter( row, "fs_port",      info.fsPort,  commonWorkerFsPort);
        ::readOptionalParameter( row, "data_dir",     info.dataDir, commonWorkerDataDir);

        Configuration::translateDataDir(info.dataDir, info.name);

        _workerInfo[info.name] = info;
    }
    // Read database family-specific configurations and construct _replicationLevel

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database_family"));

    while (conn->next(row)) {

        std::string family;
        ::readMandatoryParameter(row, "name", family);
        ::readMandatoryParameter(row, "min_replication_level", _replicationLevel[family]);
    }

    // Read database-specific configurations and construct DatabaseInfo.

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database"));

    while (conn->next(row)) {

        std::string database;
        ::readMandatoryParameter(row, "database", database);
        _databaseInfo[database].name = database;

        ::readMandatoryParameter(row, "family_name", _databaseInfo[database].family);
    }

    // Read database-specific table definitions and extend the corresponding DatabaseInfo.

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database_table"));

    while (conn->next(row)) {

        std::string database;
        ::readMandatoryParameter(row, "database", database);

        std::string table;
        ::readMandatoryParameter(row, "table", table);
        
        bool isPartitioned;
        ::readMandatoryParameter(row, "is_partitioned", isPartitioned);
        
        if (isPartitioned) { _databaseInfo[database].partitionedTables.push_back(table); }
        else               { _databaseInfo[database].regularTables    .push_back(table); }
    }

    // Values of these parameters are predetermined by the connection
    // parameters passed into this object

    _databaseTechnology = "mysql";
    _databaseHost       = _connectionParams.host;
    _databasePort       = _connectionParams.port;
    _databaseUser       = _connectionParams.user;
    _databasePassword   = _connectionParams.password;
    _databaseName       = _connectionParams.database;

    dumpIntoLogger();
}

}}} // namespace lsst::qserv::replica