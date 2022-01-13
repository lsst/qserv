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
#include "replica/Configuration.h"

// System headers
#include <chrono>
#include <thread>

// Qserv headers
#include "replica/ConfigParserJSON.h"
#include "replica/ConfigParserMySQL.h"
#include "replica/DatabaseMySQL.h"
#include "util/Timer.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using namespace std::chrono_literals;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

/**
 * @param connectionUrl The connection URL.
 * @param database The optional name of a database to replace the one defined in the url.
 * @return The MySQL connection descriptor.
 */
database::mysql::ConnectionParams connectionParams(string const& connectionUrl, string const& database) {
    database::mysql::ConnectionParams params = database::mysql::ConnectionParams::parse(connectionUrl);
    if (!database.empty()) params.database = database;
    return params;
}
} // namespace

namespace lsst {
namespace qserv {
namespace replica {


// These (static) data members are allowed to be changed, and they are set
// globally for an application (process).
bool         Configuration::_databaseAllowReconnect = true;
unsigned int Configuration::_databaseConnectTimeoutSec = 3600;
unsigned int Configuration::_databaseMaxReconnects = 1;
unsigned int Configuration::_databaseTransactionTimeoutSec = 3600;
bool         Configuration::_schemaUpgradeWait = true;
unsigned int Configuration::_schemaUpgradeWaitTimeoutSec = 3600;
string       Configuration::_qservCzarDbUrl = "mysql://qsmaster@localhost:3306/qservMeta";
string       Configuration::_qservWorkerDbUrl = "mysql://qsmaster@localhost:3306/qservw_worker";
util::Mutex  Configuration::_classMtx;


// ---------------
// The static API.
// ---------------

void Configuration::setQservCzarDbUrl(string const& url) {
    if (url.empty()) {
        throw invalid_argument("Configuration::" + string(__func__) + "  empty string is not allowed.");
    }
    util::Lock const lock(_classMtx, _context(__func__));
    _qservCzarDbUrl = url;
}


string Configuration::qservCzarDbUrl() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _qservCzarDbUrl;
}


database::mysql::ConnectionParams Configuration::qservCzarDbParams(string const& database) {
    util::Lock const lock(_classMtx, _context(__func__));
    return connectionParams(_qservCzarDbUrl, database);
}


void Configuration::setQservWorkerDbUrl(string const& url) {
    if (url.empty()) {
        throw invalid_argument("Configuration::" + string(__func__) + "  empty string is not allowed.");
    }
    util::Lock const lock(_classMtx, _context(__func__));
    _qservWorkerDbUrl = url;
}


string Configuration::qservWorkerDbUrl() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _qservWorkerDbUrl;
}


database::mysql::ConnectionParams Configuration::qservWorkerDbParams(string const& database) {
    util::Lock const lock(_classMtx, _context(__func__));
    return connectionParams(_qservWorkerDbUrl, database);
}


void Configuration::setDatabaseAllowReconnect(bool value) {
    util::Lock const lock(_classMtx, _context(__func__));
    _databaseAllowReconnect = value;
}


bool Configuration::databaseAllowReconnect() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _databaseAllowReconnect;
}


void Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    util::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseConnectTimeoutSec = value;
}


unsigned int Configuration::databaseConnectTimeoutSec() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _databaseConnectTimeoutSec;
}


void Configuration::setDatabaseMaxReconnects(unsigned int value) {
    util::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseMaxReconnects = value;
}


unsigned int Configuration::databaseMaxReconnects() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _databaseMaxReconnects;
}


void Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    util::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseTransactionTimeoutSec = value;
}


unsigned int Configuration::databaseTransactionTimeoutSec() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _databaseTransactionTimeoutSec;
}


bool Configuration::schemaUpgradeWait() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _schemaUpgradeWait;
}


void Configuration::setSchemaUpgradeWait(bool value) {
    util::Lock const lock(_classMtx, _context(__func__));
    _schemaUpgradeWait = value;
}


unsigned int Configuration::schemaUpgradeWaitTimeoutSec() {
    util::Lock const lock(_classMtx, _context(__func__));
    return _schemaUpgradeWaitTimeoutSec;
}


void Configuration::setSchemaUpgradeWaitTimeoutSec(unsigned int value) {
    util::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _schemaUpgradeWaitTimeoutSec = value;
}


Configuration::Ptr Configuration::load(string const& configUrl) {
    Ptr const ptr(new Configuration());
    util::Lock const lock(ptr->_mtx, _context(__func__));
    bool const reset = false;
    ptr->_load(lock, configUrl, reset);
    return ptr;
}


Configuration::Ptr Configuration::load(json const& obj) {
    Ptr const ptr(new Configuration());
    util::Lock const lock(ptr->_mtx, _context(__func__));
    bool const reset = false;
    ptr->_load(lock, obj, reset);
    return ptr;
}


string Configuration::_context(string const& func) {
    return "CONFIG  " + func;
}


// -----------------
// The instance API.
// -----------------

Configuration::Configuration()
    :   _data(ConfigurationSchema::defaultConfigData()) {
}


void Configuration::reload() {
    util::Lock const lock(_mtx, _context(__func__));
    if (!_configUrl.empty()) {
        bool const reset = true;
        _load(lock, _configUrl, reset);
    }
}


void Configuration::reload(string const& configUrl) {
    util::Lock const lock(_mtx, _context(__func__));
    bool const reset = true;
    _load(lock, configUrl, reset);
}


void Configuration::reload(json const& obj) {
    util::Lock const lock(_mtx, _context(__func__));
    bool const reset = true;
    _load(lock, obj, reset);
}


string Configuration::configUrl(bool showPassword) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (_connectionPtr == nullptr) return string();
    return _connectionParams.toString(showPassword);
}


map<string, set<string>> Configuration::parameters() const {
    return ConfigurationSchema::parameters();
}


string Configuration::getAsString(string const& category, string const& param) const {
    util::Lock const lock(_mtx, _context(__func__));
    return ConfigurationSchema::json2string(
            _context(__func__) + " category: '" + category + "' param: '" + param + "' ",
            _get(lock, category, param));
}


void Configuration::setFromString(string const& category, string const& param,
                                  string const& val, bool updatePersistentState) {
    json obj;
    {
        util::Lock const lock(_mtx, _context(__func__));
        obj = _get(lock, category, param);
    }
    if (obj.is_string()) {
        Configuration::set<string>(category, param, val, updatePersistentState);
    } else if (obj.is_number_unsigned()) {
        Configuration::set<uint64_t>(category, param, stoull(val), updatePersistentState);
    } else if (obj.is_number_integer()) {
        Configuration::set<int64_t>(category, param, stoll(val), updatePersistentState);
    } else if (obj.is_number_float()) {
        Configuration::set<double>(category, param, stod(val), updatePersistentState);
    } else {
        throw invalid_argument(
                _context(__func__) + " unsupported data type of category: '" + category + "' param: '" + param
                + "' value: " + val + "'.");
    }
}


void Configuration::_load(util::Lock const& lock, json const& obj, bool reset) {

    if (reset) {
        _data = ConfigurationSchema::defaultConfigData();
        _workers.clear();
    }
    _configUrl = string();
    _connectionPtr = nullptr;

    // Validate and update configuration parameters.
    // Catch exceptions for error reporting.
    ConfigParserJSON parser(_data, _workers, _databaseFamilies, _databases);
    parser.parse(obj);

    bool const showPassword = false;
    LOGS(_log, LOG_LVL_DEBUG, _context() << _toJson(lock, showPassword).dump());
}


void Configuration::_load(util::Lock const& lock, string const& configUrl, bool reset) {

    if (reset) {
        _data = ConfigurationSchema::defaultConfigData();
        _workers.clear();
    }
    _configUrl = configUrl;

    // When initializing the connection object use the current defaults for the relevant
    // fields that are missing in the connection string. After that update the database
    // info in the configuration to match values of the parameters that were parsed
    // in the connection string.
    _connectionParams = database::mysql::ConnectionParams::parse(
            configUrl,
            _get(lock, "database", "host").get<string>(),
            _get(lock, "database", "port").get<uint16_t>(),
            _get(lock, "database", "user").get<string>(),
            _get(lock, "database", "password").get<string>()
    );
    _data["database"]["host"] = _connectionParams.host;
    _data["database"]["port"] = _connectionParams.port;
    _data["database"]["user"] = _connectionParams.user;
    _data["database"]["password"] = _connectionParams.password;
    _data["database"]["name"] = _connectionParams.database;

    // The schema upgrade timer is used for limiting a duration of time when
    // tracking (if enabled) the schema upgrade. The timeout includes
    // the connect (or reconnect) time.
    util::Timer schemaUpgradeTimer;
    schemaUpgradeTimer.start();

    // Read data, validate and update configuration parameters.
    _connectionPtr = database::mysql::Connection::open(_connectionParams);
    while (true) {
        try {
            _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
                ConfigParserMySQL parser(conn, _data, _workers, _databaseFamilies, _databases);
                parser.parse();
            });
            break;
        } catch (ConfigVersionMismatch const& ex) {
            if (Configuration::schemaUpgradeWait()) {
                if (ex.version > ex.requiredVersion) {
                    LOGS(_log, LOG_LVL_ERROR, _context() << "Database schema version is newer than"
                        << " the one required by the application, ex: " << ex.what());
                        throw;
                }
                schemaUpgradeTimer.stop();
                if (schemaUpgradeTimer.getElapsed() > Configuration::schemaUpgradeWaitTimeoutSec()) {
                    LOGS(_log, LOG_LVL_ERROR, _context() << "The maximum duration of time ("
                        << Configuration::schemaUpgradeWaitTimeoutSec() << " seconds) has expired"
                        << " while waiting for the database schema upgrade. The schema version is still older than"
                        << " the one required by the application, ex: " << ex.what());
                    throw;
                } else {
                    LOGS(_log, LOG_LVL_WARN, _context() << "Database schema version is still older than the one"
                        << " required by the application after " << schemaUpgradeTimer.getElapsed()
                        << " seconds of waiting for the schema upgrade, ex: " << ex.what());
                }
            } else {
                LOGS(_log, LOG_LVL_ERROR, _context() << ex.what());
                throw;
            }
        }
        std::this_thread::sleep_for(5000ms);
    }
    bool const showPassword = false;
    LOGS(_log, LOG_LVL_DEBUG, _context() << _toJson(lock, showPassword).dump());
}


vector<string> Configuration::workers(bool isEnabled, bool isReadOnly) const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _workers) {
        string const& name = itr.first;
        WorkerInfo const& info = itr.second;
        if (isEnabled) {
            if (info.isEnabled && (isReadOnly == info.isReadOnly)) {
                names.push_back(name);
            }
        } else {
            if (!info.isEnabled) {
                names.push_back(name);
            }
        }
    }
    return names;
}


vector<string> Configuration::allWorkers() const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _workers) {
        string const& name = itr.first;
        names.push_back(name);
    }
    return names;
}


vector<string> Configuration::databaseFamilies() const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _databaseFamilies) {
        string const& name = itr.first;
        names.push_back(name);
    }
    return names;
}


bool Configuration::isKnownDatabaseFamily(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (name.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    return _databaseFamilies.count(name) != 0;
}


DatabaseFamilyInfo Configuration::databaseFamilyInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _databaseFamilyInfo(lock, name);
}


DatabaseFamilyInfo Configuration::addDatabaseFamily(DatabaseFamilyInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    if (info.name.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    if (_databaseFamilies.find(info.name) != _databaseFamilies.end()) {
        throw invalid_argument(_context(__func__) + " the family '" + info.name + "' already exists.");
    }
    string errors;
    if (info.replicationLevel == 0) errors += " replicationLevel(0)";
    if (info.numStripes       == 0) errors += " numStripes(0)";
    if (info.numSubStripes    == 0) errors += " numSubStripes(0)";
    if (info.overlap          <= 0) errors += " overlap(<=0)";
    if (!errors.empty()) throw invalid_argument(_context(__func__) + errors);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->executeInsertQuery("config_database_family",
                                    info.name, info.replicationLevel,
                                    info.numStripes, info.numSubStripes, info.overlap);
        });
    }
    _databaseFamilies[info.name] = info;
    return info;
}


void Configuration::deleteDatabaseFamily(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    DatabaseFamilyInfo& familyInfo = _databaseFamilyInfo(lock, name);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database_family") +
                          " WHERE " + conn->sqlEqual("name", familyInfo.name));
        });
    }
    // In order to maintain consistency of the persistent state also delete all
    // dependent databases.
    // NOTE: if using MySQL-based persistent backend the removal of the dependent 
    //       tables from MySQL happens automatically since it's enforced by the PK/FK
    //       relationship between the corresponding tables.
    vector<string> databasesToBeRemoved;
    for (auto&& itr: _databases) {
        string const& database = itr.first;
        DatabaseInfo const& info = itr.second;
        if (info.family == familyInfo.name) {
            databasesToBeRemoved.push_back(database);
        }
    }
    for (string const& database: databasesToBeRemoved) {
        _databases.erase(database);
    }
    _databaseFamilies.erase(familyInfo.name);
}


size_t Configuration::replicationLevel(string const& family) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _databaseFamilyInfo(lock, family).replicationLevel;
}


vector<string> Configuration::databases(string const& family, bool allDatabases, bool isPublished) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (!family.empty()) {
        if (_databaseFamilies.find(family) == _databaseFamilies.cend()) {
            throw invalid_argument(_context(__func__) + " no such family '" + family + "'.");
        }
    }
    vector<string> names;
    for (auto&& itr: _databases) {
        string const& name = itr.first;
        DatabaseInfo const& info = itr.second;
        if (!family.empty() && (family != info.family)) {
            continue;
        }
        if (!allDatabases) {
            if (isPublished != info.isPublished) continue;  
        }
        names.push_back(name);
    }
    return names;
}


void Configuration::assertDatabaseIsValid(string const& name) {
    if (!isKnownDatabase(name)) {
        throw invalid_argument(_context(__func__) + " database name is not valid: " + name);
    }
}


bool Configuration::isKnownDatabase(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (name.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    return _databases.count(name) != 0;
}


DatabaseInfo Configuration::databaseInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _databaseInfo(lock, name);
}


DatabaseInfo Configuration::addDatabase(string const& database, std::string const& family) {
    util::Lock const lock(_mtx, _context(__func__));
    if (database.empty()) {
        throw invalid_argument(_context(__func__) + " the database name can't be empty.");
    }
    auto itr = _databases.find(database);
    if (itr != _databases.end()) {
        throw invalid_argument(_context(__func__) + " the database '" + database + "' already exists.");
    }
    // This will throw an exception if the family isn't valid
    _databaseFamilyInfo(lock, family);

    // When a new database is being added only these fields are considered.
    DatabaseInfo info;
    info.name = database;
    info.family = family;
    info.isPublished = false;   // the new database can't be published at this time

    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&info](decltype(_connectionPtr) conn) {
            conn->executeInsertQuery("config_database",
                                     info.name,
                                     info.family,
                                     info.isPublished ? 1 : 0);
        });
    }
    _databases[info.name] = info;
    return info;
}


DatabaseInfo Configuration::publishDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    bool const publish = true;
    return _publishDatabase(lock, name, publish);
}


DatabaseInfo Configuration::unPublishDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    bool const publish = false;
    return _publishDatabase(lock, name, publish);
}


void Configuration::deleteDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& info = _databaseInfo(lock, name);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database") +
                          " WHERE " + conn->sqlEqual("database", info.name));
        });
    }
    _databases.erase(info.name);
}


DatabaseInfo Configuration::addTable(
        string const& database, string const& table, bool isPartitioned, list<SqlColDef> const& columns,
        bool isDirectorTable, string const& directorTable, string const& directorTableKey,
        string const& latitudeColName, string const& longitudeColName) {

    util::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& databaseInfo = _databaseInfo(lock, database);
    databaseInfo.addTable(table, columns, isPartitioned,
                          isDirectorTable, directorTable, directorTableKey,
                          latitudeColName, longitudeColName);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->executeInsertQuery("config_database_table",
                                     database, table, isPartitioned,
                                     directorTable, directorTableKey, latitudeColName, longitudeColName);
            int colPosition = 0;
            for (auto&& coldef: columns) {
                conn->executeInsertQuery("config_database_table_schema",
                                         database, table, colPosition++,  // column position
                                         coldef.name, coldef.type);
            }
        });
    }
    return databaseInfo;
}


DatabaseInfo Configuration::deleteTable(string const& database, string const& table) {
    util::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& databaseInfo = _databaseInfo(lock, database);
    databaseInfo.removeTable(table);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database_table") +
                          " WHERE " + conn->sqlEqual("database", databaseInfo.name) +
                          " AND " + conn->sqlEqual("table", table));
        });
    }
    return databaseInfo;
}


void Configuration::assertWorkerIsValid(string const& name) {
    if (!isKnownWorker(name)) {
        throw invalid_argument(_context(__func__) + " worker name is not valid: " + name);
    }
}


void Configuration::assertWorkersAreDifferent(string const& firstName,
                                              string const& secondName) {
    assertWorkerIsValid(firstName);
    assertWorkerIsValid(secondName);

    if (firstName == secondName) {
        throw invalid_argument(_context(__func__) + " worker names are the same: " + firstName);
    }
}


bool Configuration::isKnownWorker(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _workers.count(name) != 0;
}


WorkerInfo Configuration::workerInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    auto const itr = _workers.find(name);
    if (itr != _workers.cend()) return itr->second;
    throw invalid_argument(_context(__func__) +" unknown worker '" + name + "'.");
}


WorkerInfo Configuration::addWorker(WorkerInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    if (_workers.find(info.name) == _workers.cend()) return _updateWorker(lock, info);
    throw invalid_argument(_context(__func__) +" worker '" + info.name + "' already exists.");
}


void Configuration::deleteWorker(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    auto itr = _workers.find(name);
    if (itr == _workers.end()) {
        throw invalid_argument(_context(__func__) +" unknown worker '" + name + "'.");
    }
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_worker") +
                          " WHERE " + conn->sqlEqual("name", name));
        });
    }
    _workers.erase(itr);
}


WorkerInfo Configuration::disableWorker(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    auto itr = _workers.find(name);
    if (itr == _workers.end()) {
        throw invalid_argument(_context(__func__) +" unknown worker '" + name + "'.");
    }
    WorkerInfo& info = itr->second;
    if (info.isEnabled) {
        if (_connectionPtr != nullptr) {
            _connectionPtr->executeInOwnTransaction([&info](decltype(_connectionPtr) conn) {
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", info.name),
                    make_pair("is_enabled", 0)
                );
            });
        }
        info.isEnabled = false;
    }
    return info;
}


WorkerInfo Configuration::updateWorker(WorkerInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    if (_workers.find(info.name) != _workers.end()) return _updateWorker(lock, info);
    throw invalid_argument(_context(__func__) +" unknown worker '" + info.name + "'.");
}


json Configuration::toJson(bool showPassword) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _toJson(lock, showPassword);
}


json Configuration::_toJson(util::Lock const& lock, bool showPassword) const {
    json data;
    data["general"] = _data;
    json& workersJson = data["workers"];
    for (auto&& itr: _workers) {
        WorkerInfo const& info = itr.second;
        workersJson.push_back(info.toJson());
    }
    json& databaseFamilies = data["database_families"];
    for (auto&& itr: _databaseFamilies) {
        DatabaseFamilyInfo const& info = itr.second;
        databaseFamilies.push_back(info.toJson());
    }
    json& databases = data["databases"];
    for (auto&& itr: _databases) {
        DatabaseInfo const& info = itr.second;
        databases.push_back(info.toJson());
    }
    return data;
}

json const& Configuration::_get(
        util::Lock const& lock, string const& category, string const& param) const
{
    json::json_pointer const pointer("/" + category + "/" + param);
    if (!_data.contains(pointer)) {
        throw invalid_argument(
                _context(__func__) + " no such parameter for category: '" + category
                + "', param: '" + param + "'");
    }
    return _data.at(pointer);
}


json& Configuration::_get(
        util::Lock const& lock, string const& category, string const& param)
{
    return _data[json::json_pointer("/" + category + "/" + param)];
}


void Configuration::_set(
        string const& category, string const& param, string const& value) const
{
    _connectionPtr->executeInsertOrUpdate(
        [&](decltype(_connectionPtr) conn) {
            conn->executeInsertQuery("config", category, param, value);
        },
        [&](decltype(_connectionPtr) conn) {
            conn->executeSimpleUpdateQuery(
                "config",
                conn->sqlEqual("category", category) + " AND " + conn->sqlEqual("param", param),
                make_pair("value", value)
            );
        }
    );
}


WorkerInfo Configuration::_updateWorker(util::Lock const& lock, WorkerInfo const& inputInfo) {

    // Make sure all required fields are set in the input worker descriptor.
    // The only required fields are the name of the worker and the host name of
    // of the Replication service. If the host names for other services will be
    // found missing then the host name of the Replication service will be assumed.
    if (inputInfo.name.empty() || inputInfo.svcHost.empty()) {
       throw invalid_argument(
                _context(__func__) + " incomplete definition of the worker: '" + inputInfo.name +"'.");
    }

    // Make an updated worker descriptor with optional fields completed using
    // existing defaults for workers. This step is required to ensure user-provided
    // descriptors are complete before making any changes to the Configuration's
    // transient or persistent states.
    WorkerInfo const completeInfo = WorkerInfo(inputInfo, _data.at("worker_defaults"));

    // Make sure no host/port conflicts would exist in the transient configuration after
    // adding/updating the worker. This step is not needed for the MySQL-based persistent
    // backend due to the schema-enforced uniqueness constraints.
    std::set<pair<string, uint16_t>> hostPort;
    for (auto&& itr: _workers) {
        string const& worker = itr.first;
        WorkerInfo const& info = itr.second;
        // Skip the target worker from the test to avoid comparing it to itself.
        if (worker == completeInfo.name) continue;
        hostPort.insert(make_pair(info.svcHost, info.svcPort));
        hostPort.insert(make_pair(info.fsHost, info.fsPort));
        hostPort.insert(make_pair(info.loaderHost, info.loaderPort));
        hostPort.insert(make_pair(info.exporterHost, info.exporterPort));
        hostPort.insert(make_pair(info.httpLoaderHost, info.httpLoaderPort));
    }
    bool const portConflictFound =
            !hostPort.insert(make_pair(completeInfo.svcHost, completeInfo.svcPort)).second ||
            !hostPort.insert(make_pair(completeInfo.fsHost, completeInfo.fsPort)).second ||
            !hostPort.insert(make_pair(completeInfo.loaderHost, completeInfo.loaderPort)).second ||
            !hostPort.insert(make_pair(completeInfo.exporterHost, completeInfo.exporterPort)).second ||
            !hostPort.insert(make_pair(completeInfo.httpLoaderHost, completeInfo.httpLoaderPort)).second;
    if (portConflictFound) {
        throw invalid_argument(
                _context(__func__) + " port conflict detected either between the updated worker: '"
                + completeInfo.name + "' and one of the existing workers or within the updated worker itself.");
    }

    // Update the persistent state.
    bool const update = _workers.count(completeInfo.name) != 0;
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            if (update) {
                conn->executeSimpleUpdateQuery(
                    "config_worker",
                    conn->sqlEqual("name", completeInfo.name),
                    make_pair("is_enabled", completeInfo.isEnabled),
                    make_pair("is_read_only", completeInfo.isReadOnly),
                    make_pair("svc_host", completeInfo.svcHost),
                    make_pair("svc_port", completeInfo.svcPort),
                    make_pair("fs_host", completeInfo.fsHost),
                    make_pair("fs_port", completeInfo.fsPort),
                    make_pair("data_dir", completeInfo.dataDir),
                    make_pair("loader_host", completeInfo.loaderHost),
                    make_pair("loader_port", completeInfo.loaderPort),
                    make_pair("loader_tmp_dir", completeInfo.loaderTmpDir),
                    make_pair("exporter_host", completeInfo.exporterHost),
                    make_pair("exporter_port", completeInfo.exporterPort),
                    make_pair("exporter_tmp_dir", completeInfo.exporterTmpDir),
                    make_pair("http_loader_host", completeInfo.httpLoaderHost),
                    make_pair("http_loader_port", completeInfo.httpLoaderPort),
                    make_pair("http_loader_tmp_dir", completeInfo.httpLoaderTmpDir)
                );
            } else {
                conn->executeInsertQuery(
                    "config_worker",
                    completeInfo.name,
                    completeInfo.isEnabled,
                    completeInfo.isReadOnly,
                    completeInfo.svcHost,
                    completeInfo.svcPort,
                    completeInfo.fsHost,
                    completeInfo.fsPort,
                    completeInfo.dataDir,
                    completeInfo.loaderHost,
                    completeInfo.loaderPort,
                    completeInfo.loaderTmpDir,
                    completeInfo.exporterHost,
                    completeInfo.exporterPort,
                    completeInfo.exporterTmpDir,
                    completeInfo.httpLoaderHost,
                    completeInfo.httpLoaderPort,
                    completeInfo.httpLoaderTmpDir
                );
            }
        });
    }
    _workers[completeInfo.name] = completeInfo;
    return completeInfo;
}


DatabaseFamilyInfo& Configuration::_databaseFamilyInfo(util::Lock const& lock, string const& name) {
    if (name.empty()) throw invalid_argument(_context(__func__) + " the database family name is empty.");
    auto const itr = _databaseFamilies.find(name);
    if (itr != _databaseFamilies.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " no such database family '" + name + "'.");
}


DatabaseInfo& Configuration::_databaseInfo(util::Lock const& lock, string const& name) {
    if (name.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    auto const itr = _databases.find(name);
    if (itr != _databases.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " no such database '" + name + "'.");
}


DatabaseInfo& Configuration::_publishDatabase(util::Lock const& lock, string const& name, bool publish) {
    DatabaseInfo& databseInfo = _databaseInfo(lock, name);
    if (publish && databseInfo.isPublished) {
        throw logic_error(_context(__func__) + " database '" + databseInfo.name +"' is already published.");
    } else if (!publish && !databseInfo.isPublished) {
        throw logic_error(_context(__func__) + " database '" + databseInfo.name +"' is not published.");
    }
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->executeSimpleUpdateQuery(
                "config_database",
                conn->sqlEqual("database", databseInfo.name),
                make_pair("is_published", publish ? 1 : 0));
        });
    }
    databseInfo.isPublished = publish;
    return databseInfo;
}

}}} // namespace lsst::qserv::replica
