/*
 * LSST Data Management System
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
#include <algorithm>
#include <iterator>
#include <stdexcept>
#include <sstream>
#include <vector>

// Qserv headers
#include "replica/ChunkNumber.h"

using namespace std;
using namespace lsst::qserv::replica;

namespace {

template <typename T>
bool tryParameter(database::mysql::Row& row,
                  string const& desiredCategory,
                  string const& desiredParam,
                  T& value) {

    string category;
    row.get("category", category);
    if (desiredCategory != category) return false;

    string param;
    row.get("param", param);
    if (desiredParam != param) return false;

    row.get("value", value);
    return true;
}


template <typename T>
void readMandatoryParameter(database::mysql::Row& row,
                            string const& name,
                            T& value) {
    if (not row.get(name, value)) {
        throw runtime_error(
                "ConfigurationMySQL::" + string(__func__) + "  the field '" + name +
                "' is not allowed to be NULL");
    }
}


template <typename T>
void readOptionalParameter(database::mysql::Row& row,
                           string const& name,
                           T& value,
                           T const& defaultValue) {
    if (not row.get(name, value)) {
        value = defaultValue;
    }
}


template<class T>
void configInsert(ostream& os,
                  string const& category,
                  string const& param,
                  T const& val) {
    os << "INSERT INTO `config` VALUES ('" << category << "', '" << param << "', '" << val << "');\n";
}

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

string ConfigurationMySQL::dump2init(Configuration::Ptr const& config) {

    using namespace std;

    if (config == nullptr) {
        throw invalid_argument(
                "ConfigurationMySQL::" + string(__func__) + "  the configuration can't be empty");
    }
    ostringstream str;

    ::configInsert(str, "common",     "request_buf_size_bytes",     config->requestBufferSizeBytes());
    ::configInsert(str, "common",     "request_retry_interval_sec", config->retryTimeoutSec());
    ::configInsert(str, "controller", "num_threads",                config->controllerThreads());
    ::configInsert(str, "controller", "http_server_port",           config->controllerHttpPort());
    ::configInsert(str, "controller", "http_server_threads",        config->controllerHttpThreads());
    ::configInsert(str, "controller", "request_timeout_sec",        config->controllerRequestTimeoutSec());
    ::configInsert(str, "controller", "job_timeout_sec",            config->jobTimeoutSec());
    ::configInsert(str, "controller", "job_heartbeat_sec",          config->jobHeartbeatTimeoutSec());
    ::configInsert(str, "database",   "services_pool_size",         config->databaseServicesPoolSize());
    ::configInsert(str, "xrootd",     "auto_notify",                config->xrootdAutoNotify() ? 1 : 0);
    ::configInsert(str, "xrootd",     "host",                       config->xrootdHost());
    ::configInsert(str, "xrootd",     "port",                       config->xrootdPort());
    ::configInsert(str, "xrootd",     "request_timeout_sec",        config->xrootdTimeoutSec());
    ::configInsert(str, "worker",     "technology",                 config->workerTechnology());
    ::configInsert(str, "worker",     "num_svc_processing_threads", config->workerNumProcessingThreads());
    ::configInsert(str, "worker",     "num_fs_processing_threads",  config->fsNumProcessingThreads());
    ::configInsert(str, "worker",     "fs_buf_size_bytes",          config->workerFsBufferSizeBytes());
    ::configInsert(str, "worker",     "svc_host",                   defaultWorkerSvcHost);
    ::configInsert(str, "worker",     "svc_port",                   defaultWorkerSvcPort);
    ::configInsert(str, "worker",     "fs_host",                    defaultWorkerFsHost);
    ::configInsert(str, "worker",     "fs_port",                    defaultWorkerFsPort);
    ::configInsert(str, "worker",     "data_dir",                   defaultDataDir);
    ::configInsert(str, "worker",     "db_host",                    defaultWorkerDbHost);
    ::configInsert(str, "worker",     "db_port",                    defaultWorkerDbPort);
    ::configInsert(str, "worker",     "db_user",                    defaultWorkerDbUser);

    for (auto&& worker: config->allWorkers()) {
        auto&& info = config->workerInfo(worker);
        str << "INSERT INTO `config_worker` VALUES ("
            << "'" << info.name << "',"
            <<        (info.isEnabled  ? 1 : 0) << ","
            <<        (info.isReadOnly ? 1 : 0) << ","
            << "'" <<  info.svcHost << "',"
            <<         info.svcPort << ","
            << "'" <<  info.fsHost  << "',"
            <<         info.fsPort  << ","
            << "'" <<  info.dataDir << "',"
            << "'" <<  info.dbHost  << "',"
            <<         info.dbPort  << ","
            << "'" <<  info.dbUser  << "'"
            << ");\n";
    }
    for (auto&& family: config->databaseFamilies()) {
        auto&& familyInfo = config->databaseFamilyInfo(family);

        str << "INSERT INTO `config_database_family` VALUES ("
            << "'" << familyInfo.name << "',"
            <<        familyInfo.replicationLevel << ","
            <<        familyInfo.numStripes << ","
            <<        familyInfo.numSubStripes
            << ");\n";

        for (auto&& database: config->databases(familyInfo.name)) {
            auto&& databaseInfo = config->databaseInfo(database);

            str << "INSERT INTO `config_database` VALUES ("
                << "'" << databaseInfo.name << "','" << databaseInfo.family << "');\n";

            for (auto&& table: databaseInfo.partitionedTables) {
                str << "INSERT INTO `config_database_table` VALUES ("
                    << "'" << databaseInfo.name << "','" << table << "',1);\n";
            }
            for (auto&& table: databaseInfo.regularTables) {
                str << "INSERT INTO `config_database_table` VALUES ("
                    << "'" << databaseInfo.name << "','" << table << "',0);\n";
            }
        }
    }
    return str.str();
}


ConfigurationMySQL::ConfigurationMySQL(database::mysql::ConnectionParams const& connectionParams)
    :   Configuration(),
        _connectionParams(connectionParams),
        _log(LOG_GET("lsst.qserv.replica.ConfigurationMySQL")) {

    _loadConfiguration();
}


string ConfigurationMySQL::prefix() const {
    return _databaseTechnology;
}


string ConfigurationMySQL::configUrl() const {
    return _connectionParams.toString();
}


void ConfigurationMySQL::addWorker(WorkerInfo const& info) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << info.name);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database

        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&info](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "config_worker",
                     info.name,
                     info.isEnabled ? 1 : 0,
                     info.isReadOnly ? 1 : 0,
                     info.svcHost,
                     info.svcPort,
                     info.fsHost,
                     info.fsPort,
                     info.dataDir
                );
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        _workerInfo[info.name] = info;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << ex.what());
        throw;
    }
}


void ConfigurationMySQL::deleteWorker(string const& name) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database

        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name](decltype(handler.conn) conn) {
                conn->begin();
                conn->execute("DELETE FROM config_worker WHERE " + conn->sqlEqual("name", name));
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        _workerInfo.erase(itr);

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << ex.what());
        throw;
    }
}


WorkerInfo ConfigurationMySQL::disableWorker(string const& name,
                                             bool disable) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name
         << " disable=" << (disable ? "true" : "false"));

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database state

        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,disable](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("is_enabled", disable ? 0 : 1));
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.isEnabled = not disable;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerReadOnly(string const& name,
                                                 bool readOnly) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name
         << " readOnly=" << (readOnly ? "true" : "false"));

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database state

        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,readOnly](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("is_read_only", readOnly ? 1 : 0));
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.isReadOnly = readOnly;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerSvcHost(string const& name,
                                                string const& host) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " host=" << host);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,&host](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("svc_host", host));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.svcHost = host;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerSvcPort(string const& name,
                                                uint16_t port) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " port=" << port);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,port](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("svc_port", port));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.svcPort = port;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerFsHost(string const& name,
                                               string const& host) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " host=" << host);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,&host](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("fs_host", host));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.fsHost = host;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerFsPort(string const& name,
                                               uint16_t port) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " port=" << port);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,port](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("fs_port", port));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);
    
        auto itr = safeFindWorker(lock, name, context_);
        itr->second.fsPort = port;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerDataDir(string const& name,
                                                string const& dataDir) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " dataDir=" << dataDir);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,&dataDir](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("data_dir", dataDir));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.dataDir = dataDir;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerDbHost(std::string const& name,
                                               std::string const& host) {
    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " host=" << host);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,&host](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("db_host", host));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.dbHost = host;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerDbPort(std::string const& name,
                                               uint16_t port) {
    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " port=" << port);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,port](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("db_port", port));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);
    
        auto itr = safeFindWorker(lock, name, context_);
        itr->second.dbPort = port;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


WorkerInfo ConfigurationMySQL::setWorkerDbUser(std::string const& name,
                                               std::string const& user)  {
    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name=" << name << " user=" << user);

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name,&user](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", name),
                    make_pair("db_user", user));
                conn->commit();
            }
        );

        // Then update the transient state 

        util::Lock lock(_mtx, context_);

        auto itr = safeFindWorker(lock, name, context_);
        itr->second.dbUser = user;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
    return workerInfo(name);
}


DatabaseFamilyInfo ConfigurationMySQL::addDatabaseFamily(DatabaseFamilyInfo const& info) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  familyInfo: " << info);
    
    if (info.name.empty()) {
        throw invalid_argument(context_ + "  the family name can't be empty");
    }
    if (info.replicationLevel == 0) {
        throw invalid_argument(context_ + "  the replication level can't be 0");
    }
    if (info.numStripes == 0) {
        throw invalid_argument(context_ + "  the number of stripes level can't be 0");
    }
    if (info.numSubStripes == 0) {
        throw invalid_argument(context_ + "  the number of sub-stripes level can't be 0");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&info](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "config_database_family",
                    info.name,
                    info.replicationLevel,
                    info.numStripes,
                    info.numSubStripes
                );
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        _databaseFamilyInfo[info.name] = DatabaseFamilyInfo{
            info.name,
            info.replicationLevel,
            info.numStripes,
            info.numSubStripes,
            make_shared<ChunkNumberQservValidator>(
                static_cast<int32_t>(info.numStripes),
                static_cast<int32_t>(info.numSubStripes))
        };
        return _databaseFamilyInfo[info.name];

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


void ConfigurationMySQL::deleteDatabaseFamily(string const& name) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name: " << name);

    if (name.empty()) {
        throw invalid_argument(context_ + "  the family name can't be empty");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name](decltype(handler.conn) conn) {
                conn->begin();
                conn->execute(
                    "DELETE FROM " + conn->sqlId("config_database_family") +
                    "  WHERE  "    + conn->sqlEqual("name", name)
                );
                conn->commit();
            }
        );

        // Then update the transient state
        //
        // NOTE: when updating the transient state do not check if the family is still there
        // because the transient state may not be consistent with the persistent one.

        util::Lock lock(_mtx, context_);

        _databaseFamilyInfo.erase(name);

        // Find and delete the relevant databases
        for(auto itr = _databaseInfo.begin(); itr != _databaseInfo.end();) {
            if (itr->second.family == name) {
                itr = _databaseInfo.erase(itr);     // the iterator now points past the erased element
            } else {
                ++itr;
            }
        }

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


DatabaseInfo ConfigurationMySQL::addDatabase(DatabaseInfo const& info) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  familyInfo: " << info);
    
    if (info.name.empty()) {
        throw invalid_argument(context_ + "  the database name can't be empty");
    }
    if (info.family.empty()) {
        throw invalid_argument(context_ + "  the family name can't be empty");
    }
    if (not isKnownDatabaseFamily(info.family)) {
        throw invalid_argument(context_ + "  unknown database family: '" + info.family + "'");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&info](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "config_database",
                    info.name,
                    info.family
                );
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        _databaseInfo[info.name] = DatabaseInfo{
            info.name,
            info.family,
            {},
            {}
        };
        return _databaseInfo[info.name];

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


void ConfigurationMySQL::deleteDatabase(string const& name) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  name: " << name);

    if (name.empty()) {
        throw invalid_argument(context_ + "  the database name can't be empty");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&name](decltype(handler.conn) conn) {
                conn->begin();
                conn->execute(
                    "DELETE FROM " + conn->sqlId("config_database") +
                    "  WHERE  "    + conn->sqlEqual("database", name)
                );
                conn->commit();
            }
        );

        // Then update the transient state
        //
        // NOTE: when updating the transient state do not check if the database is still there
        // because the transient state may not be consistent with the persistent one.

        util::Lock lock(_mtx, context_);

        _databaseInfo.erase(name);

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


DatabaseInfo ConfigurationMySQL::addTable(string const& database,
                                          string const& table,
                                          bool isPartitioned) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  database: " << database
         << " table: " << table << " isPartitioned: " << (isPartitioned ? "true" : "false"));

    if (database.empty()) {
        throw invalid_argument(context_ + "  the database name can't be empty");
    }
    if (table.empty()) {
        throw invalid_argument(context_ + "  the table name can't be empty");
    }
    if (not isKnownDatabase(database)) {
        throw invalid_argument(context_ + "  unknown database");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&database,&table,isPartitioned](decltype(handler.conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "config_database_table",
                    database,
                    table,
                    isPartitioned
                );
                conn->commit();
            }
        );

        // Then update the transient state

        util::Lock lock(_mtx, context_);

        auto& info = _databaseInfo[database];
        if (isPartitioned) {
            info.partitionedTables.push_back(table);
        } else {
            info.regularTables.push_back(table);
        }
        return info;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


DatabaseInfo ConfigurationMySQL::deleteTable(string const& database,
                                             string const& table) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  database: " << database
         << " table: " << table);

    if (database.empty()) {
        throw invalid_argument(context_ + "  the database name can't be empty");
    }
    if (table.empty()) {
        throw invalid_argument(context_ + "  the table name can't be empty");
    }
    if (not isKnownDatabase(database)) {
        throw invalid_argument(context_ + "  unknown database");
    }

    database::mysql::ConnectionHandler handler;
    try {

        // First update the database
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&database,&table](decltype(handler.conn) conn) {
                conn->begin();
                conn->execute(
                    "DELETE FROM " + conn->sqlId("config_database_table") +
                    "  WHERE  "    + conn->sqlEqual("database", database) +
                    "    AND "     + conn->sqlEqual("table",    table)
                );
                conn->commit();
            }
        );

        // Then update the transient state
        //
        // NOTE: when updating the transient state do not check if the database is still there
        // because the transient state may not be consistent with the persistent one.

        util::Lock lock(_mtx, context_);

        auto& info = _databaseInfo[database];

        auto pTableItr = find(info.partitionedTables.cbegin(),
                              info.partitionedTables.cend(),
                              table);
        if (pTableItr != info.partitionedTables.cend()) {
            info.partitionedTables.erase(pTableItr);
        }
        auto rTableItr = find(info.regularTables.cbegin(),
                              info.regularTables.cend(),
                              table);
        if (rTableItr != info.regularTables.cend()) {
            info.regularTables.erase(rTableItr);
        }
        return info;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


void ConfigurationMySQL::_loadConfiguration() {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_);

    util::Lock lock(_mtx, context_);

    database::mysql::ConnectionHandler handler;
    try {
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [this, &lock](decltype(handler.conn) conn) {
                this->_loadConfigurationImpl(lock, conn);
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}


void ConfigurationMySQL::_loadConfigurationImpl(util::Lock const& lock,
                                                database::mysql::Connection::Ptr const& conn) {

    // The common parameters (if any defined) of the workers will be initialize
    // from table 'config' and be used as defaults when reading worker-specific
    // configurations from table 'config_worker'

    uint16_t commonWorkerSvcPort = Configuration::defaultWorkerSvcPort;
    uint16_t commonWorkerFsPort  = Configuration::defaultWorkerFsPort;
    string   commonWorkerDataDir = Configuration::defaultDataDir;
    uint16_t commonWorkerDbPort  = Configuration::defaultWorkerDbPort;
    string   commonWorkerDbUser  = Configuration::defaultWorkerDbUser;

    database::mysql::Row row;

    // Read the common parameters and defaults shared by all components
    // of the replication system. The table also provides default values
    // for some critical parameters of the worker-side services.

    conn->execute("SELECT * FROM " + conn->sqlId("config"));

    while (conn->next(row)) {

        ::tryParameter(row, "common", "request_buf_size_bytes",     _requestBufferSizeBytes) or
        ::tryParameter(row, "common", "request_retry_interval_sec", _retryTimeoutSec) or

        ::tryParameter(row, "controller", "num_threads",         _controllerThreads) or
        ::tryParameter(row, "controller", "http_server_port",    _controllerHttpPort) or
        ::tryParameter(row, "controller", "http_server_threads", _controllerHttpThreads) or
        ::tryParameter(row, "controller", "request_timeout_sec", _controllerRequestTimeoutSec) or
        ::tryParameter(row, "controller", "job_timeout_sec",     _jobTimeoutSec) or
        ::tryParameter(row, "controller", "job_heartbeat_sec",   _jobHeartbeatTimeoutSec) or

        ::tryParameter(row, "database", "services_pool_size", _databaseServicesPoolSize) or

        ::tryParameter(row, "database", "qserv_master_host",     _qservMasterDatabaseHost) or
        ::tryParameter(row, "database", "qserv_master_port",     _qservMasterDatabasePort) or
        ::tryParameter(row, "database", "qserv_master_user",     _qservMasterDatabaseUser) or
        ::tryParameter(row, "database", "qserv_master_password", _qservMasterDatabasePassword) or
        ::tryParameter(row, "database", "qserv_master_name",     _qservMasterDatabaseName) or

        ::tryParameter(row, "database", "qserv_master_services_pool_size", _qservMasterDatabaseServicesPoolSize) or

        ::tryParameter(row, "xrootd", "auto_notify",         _xrootdAutoNotify) or
        ::tryParameter(row, "xrootd", "host",                _xrootdHost) or
        ::tryParameter(row, "xrootd", "port",                _xrootdPort) or
        ::tryParameter(row, "xrootd", "request_timeout_sec", _xrootdTimeoutSec) or

        ::tryParameter(row, "worker", "technology",                 _workerTechnology) or
        ::tryParameter(row, "worker", "num_svc_processing_threads", _workerNumProcessingThreads) or
        ::tryParameter(row, "worker", "num_fs_processing_threads",  _fsNumProcessingThreads) or
        ::tryParameter(row, "worker", "fs_buf_size_bytes",          _workerFsBufferSizeBytes) or
        ::tryParameter(row, "worker", "svc_port",                   commonWorkerSvcPort)  or
        ::tryParameter(row, "worker", "fs_port",                    commonWorkerFsPort) or
        ::tryParameter(row, "worker", "data_dir",                   commonWorkerDataDir) or
        ::tryParameter(row, "worker", "db_port",                    commonWorkerDbPort);
        ::tryParameter(row, "worker", "db_user",                    commonWorkerDbUser);
    }

    // Read worker-specific configurations and construct WorkerInfo.
    // Use the above retrieved common parameters as defaults where applies

    conn->execute("SELECT * FROM " + conn->sqlId ("config_worker"));

    while (conn->next(row)) {
        WorkerInfo info;
        ::readMandatoryParameter(row, "name",         info.name);
        ::readMandatoryParameter(row, "is_enabled",   info.isEnabled);
        ::readMandatoryParameter(row, "is_read_only", info.isReadOnly);
        ::readMandatoryParameter(row, "svc_host",     info.svcHost);
        ::readOptionalParameter( row, "svc_port",     info.svcPort,    commonWorkerSvcPort);
        ::readMandatoryParameter(row, "fs_host",      info.fsHost);
        ::readOptionalParameter( row, "fs_port",      info.fsPort,     commonWorkerFsPort);
        ::readOptionalParameter( row, "data_dir",     info.dataDir,    commonWorkerDataDir);
        ::readMandatoryParameter(row, "db_host",      info.dbHost);
        ::readOptionalParameter( row, "db_port",      info.dbPort,     commonWorkerDbPort);
        ::readOptionalParameter( row, "db_user",      info.dbUser,     commonWorkerDbUser);

        Configuration::translateDataDir(info.dataDir, info.name);

        _workerInfo[info.name] = info;
    }

    // Read database family-specific configurations and construct DatabaseFamilyInfo

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database_family"));

    while (conn->next(row)) {

        string name;

        ::readMandatoryParameter(row, "name", name);
        _databaseFamilyInfo[name].name = name;

        ::readMandatoryParameter(row, "min_replication_level", _databaseFamilyInfo[name].replicationLevel);
        ::readMandatoryParameter(row, "num_stripes",           _databaseFamilyInfo[name].numStripes);
        ::readMandatoryParameter(row, "num_sub_stripes",       _databaseFamilyInfo[name].numSubStripes);

        _databaseFamilyInfo[name].chunkNumberValidator =
            make_shared<ChunkNumberQservValidator>(
                    static_cast<int32_t>(_databaseFamilyInfo[name].numStripes),
                    static_cast<int32_t>(_databaseFamilyInfo[name].numSubStripes));
    }

    // Read database-specific configurations and construct DatabaseInfo.

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database"));

    while (conn->next(row)) {

        string database;
        ::readMandatoryParameter(row, "database", database);
        _databaseInfo[database].name = database;

        ::readMandatoryParameter(row, "family_name", _databaseInfo[database].family);
    }

    // Read database-specific table definitions and extend the corresponding DatabaseInfo.

    conn->execute("SELECT * FROM " + conn->sqlId ("config_database_table"));

    while (conn->next(row)) {

        string database;
        ::readMandatoryParameter(row, "database", database);

        string table;
        ::readMandatoryParameter(row, "table", table);

        bool isPartitioned;
        ::readMandatoryParameter(row, "is_partitioned", isPartitioned);

        if (isPartitioned) _databaseInfo[database].partitionedTables.push_back(table);
        else               _databaseInfo[database].regularTables.push_back(table);
    }

    // Values of these parameters are predetermined by the connection
    // parameters passed into this object

    _databaseTechnology = "mysql";
    _databaseHost       = _connectionParams.host;
    _databasePort       = _connectionParams.port;
    _databaseUser       = _connectionParams.user;
    _databaseName       = _connectionParams.database;

    dumpIntoLogger();
}


void ConfigurationMySQL::_setImp(string const& category,
                                 string const& param,
                                 SetValueExprFunc const& setValueExprFunc,
                                 function<void()> const& onSuccess) {

    string const context_ = context() + __func__;

    LOGS(_log, LOG_LVL_DEBUG, context_ << "  category: " << category << " param: " << param);

    database::mysql::ConnectionHandler handler;
    try {
        handler.conn = database::mysql::Connection::open(_connectionParams);
        handler.conn->execute(
            [&category,&param,&setValueExprFunc](decltype(handler.conn) conn) {
                ostringstream query;
                query << "UPDATE  " << conn->sqlId("config")
                      << "  SET   " << setValueExprFunc(conn)
                      << "  WHERE " << conn->sqlEqual("category", category)
                      << "    AND " << conn->sqlEqual("param", param);
                conn->begin();
                conn->execute(query.str());
                conn->commit();
            }
        );

        util::Lock lock(_mtx, context_);
        onSuccess();

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "MySQL error: " << ex.what());
        throw;
    }
}

}}} // namespace lsst::qserv::replica
