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
#include "replica/config/Configuration.h"

// System headers
#include <algorithm>
#include <chrono>
#include <thread>

// Qserv headers
#include "replica/config/ConfigParserJSON.h"
#include "replica/config/ConfigParserMySQL.h"
#include "replica/mysql/DatabaseMySQLExceptions.h"
#include "replica/mysql/DatabaseMySQLGenerator.h"
#include "replica/util/Performance.h"
#include "util/Timer.h"
#include "util/TimeUtils.h"

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
}  // namespace

namespace lsst::qserv::replica {

// These (static) data members are allowed to be changed, and they are set
// globally for an application (process).
bool Configuration::_databaseAllowReconnect = true;
unsigned int Configuration::_databaseConnectTimeoutSec = 3600;
unsigned int Configuration::_databaseMaxReconnects = 1;
unsigned int Configuration::_databaseTransactionTimeoutSec = 3600;
bool Configuration::_schemaUpgradeWait = true;
unsigned int Configuration::_schemaUpgradeWaitTimeoutSec = 3600;
string Configuration::_qservCzarDbUrl = "mysql://qsmaster@localhost:3306/qservMeta";
string Configuration::_qservWorkerDbUrl = "mysql://qsmaster@localhost:3306/qservw_worker";
replica::Mutex Configuration::_classMtx;

// ---------------
// The static API.
// ---------------

void Configuration::setQservCzarDbUrl(string const& url) {
    if (url.empty()) {
        throw invalid_argument("Configuration::" + string(__func__) + "  empty string is not allowed.");
    }
    replica::Lock const lock(_classMtx, _context(__func__));
    _qservCzarDbUrl = url;
}

string Configuration::qservCzarDbUrl() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _qservCzarDbUrl;
}

database::mysql::ConnectionParams Configuration::qservCzarDbParams(string const& database) {
    replica::Lock const lock(_classMtx, _context(__func__));
    return connectionParams(_qservCzarDbUrl, database);
}

void Configuration::setQservWorkerDbUrl(string const& url) {
    if (url.empty()) {
        throw invalid_argument("Configuration::" + string(__func__) + "  empty string is not allowed.");
    }
    replica::Lock const lock(_classMtx, _context(__func__));
    _qservWorkerDbUrl = url;
}

string Configuration::qservWorkerDbUrl() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _qservWorkerDbUrl;
}

database::mysql::ConnectionParams Configuration::qservWorkerDbParams(string const& database) {
    replica::Lock const lock(_classMtx, _context(__func__));
    return connectionParams(_qservWorkerDbUrl, database);
}

void Configuration::setDatabaseAllowReconnect(bool value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    _databaseAllowReconnect = value;
}

bool Configuration::databaseAllowReconnect() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _databaseAllowReconnect;
}

void Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseConnectTimeoutSec = value;
}

unsigned int Configuration::databaseConnectTimeoutSec() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _databaseConnectTimeoutSec;
}

void Configuration::setDatabaseMaxReconnects(unsigned int value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseMaxReconnects = value;
}

unsigned int Configuration::databaseMaxReconnects() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _databaseMaxReconnects;
}

void Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _databaseTransactionTimeoutSec = value;
}

unsigned int Configuration::databaseTransactionTimeoutSec() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _databaseTransactionTimeoutSec;
}

bool Configuration::schemaUpgradeWait() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _schemaUpgradeWait;
}

void Configuration::setSchemaUpgradeWait(bool value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    _schemaUpgradeWait = value;
}

unsigned int Configuration::schemaUpgradeWaitTimeoutSec() {
    replica::Lock const lock(_classMtx, _context(__func__));
    return _schemaUpgradeWaitTimeoutSec;
}

void Configuration::setSchemaUpgradeWaitTimeoutSec(unsigned int value) {
    replica::Lock const lock(_classMtx, _context(__func__));
    if (0 == value) {
        throw invalid_argument("Configuration::" + string(__func__) + "  0 is not allowed.");
    }
    _schemaUpgradeWaitTimeoutSec = value;
}

Configuration::Ptr Configuration::load(string const& configUrl) {
    Ptr const ptr(new Configuration());
    replica::Lock const lock(ptr->_mtx, _context(__func__));
    bool const reset = false;
    ptr->_load(lock, configUrl, reset);
    return ptr;
}

Configuration::Ptr Configuration::load(json const& obj) {
    Ptr const ptr(new Configuration());
    replica::Lock const lock(ptr->_mtx, _context(__func__));
    bool const reset = false;
    ptr->_load(lock, obj, reset);
    return ptr;
}

string Configuration::_context(string const& func) { return "CONFIG  " + func; }

// -----------------
// The instance API.
// -----------------

Configuration::Configuration() : _data(ConfigurationSchema::defaultConfigData()) {}

void Configuration::reload() {
    replica::Lock const lock(_mtx, _context(__func__));
    if (!_configUrl.empty()) {
        bool const reset = true;
        _load(lock, _configUrl, reset);
    }
}

void Configuration::reload(string const& configUrl) {
    replica::Lock const lock(_mtx, _context(__func__));
    bool const reset = true;
    _load(lock, configUrl, reset);
}

void Configuration::reload(json const& obj) {
    replica::Lock const lock(_mtx, _context(__func__));
    bool const reset = true;
    _load(lock, obj, reset);
}

string Configuration::configUrl(bool showPassword) const {
    replica::Lock const lock(_mtx, _context(__func__));
    if (_connectionPtr == nullptr) return string();
    return _connectionParams.toString(showPassword);
}

map<string, set<string>> Configuration::parameters() const { return ConfigurationSchema::parameters(); }

string Configuration::getAsString(string const& category, string const& param) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return ConfigurationSchema::json2string(
            _context(__func__) + " category: '" + category + "' param: '" + param + "' ",
            _get(lock, category, param));
}

void Configuration::setFromString(string const& category, string const& param, string const& val) {
    json obj;
    {
        replica::Lock const lock(_mtx, _context(__func__));
        obj = _get(lock, category, param);
    }
    if (obj.is_string()) {
        Configuration::set<string>(category, param, val);
    } else if (obj.is_number_unsigned()) {
        Configuration::set<uint64_t>(category, param, stoull(val));
    } else if (obj.is_number_integer()) {
        Configuration::set<int64_t>(category, param, stoll(val));
    } else if (obj.is_number_float()) {
        Configuration::set<double>(category, param, stod(val));
    } else {
        throw invalid_argument(_context(__func__) + " unsupported data type of category: '" + category +
                               "' param: '" + param + "' value: " + val + "'.");
    }
}

void Configuration::_load(replica::Lock const& lock, json const& obj, bool reset) {
    if (reset) {
        _workers.clear();
        _databaseFamilies.clear();
        _databases.clear();
        _czars.clear();
    }
    _configUrl = string();
    _connectionPtr = nullptr;

    // Validate and update configuration parameters.
    // Catch exceptions for error reporting.
    ConfigParserJSON parser(_data, _workers, _databaseFamilies, _databases, _czars);
    parser.parse(obj);

    bool const showPassword = false;
    LOGS(_log, LOG_LVL_DEBUG, _context() << _toJson(lock, showPassword).dump());
}

void Configuration::_load(replica::Lock const& lock, string const& configUrl, bool reset) {
    if (reset) {
        _workers.clear();
        _databaseFamilies.clear();
        _databases.clear();
        _czars.clear();
    }
    _configUrl = configUrl;

    // When initializing the connection object use the current defaults for the relevant
    // fields that are missing in the connection string. After that update the database
    // info in the configuration to match values of the parameters that were parsed
    // in the connection string.
    _connectionParams = database::mysql::ConnectionParams::parse(
            configUrl, _get(lock, "database", "host").get<string>(),
            _get(lock, "database", "port").get<uint16_t>(), _get(lock, "database", "user").get<string>(),
            _get(lock, "database", "password").get<string>());
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
    _g = database::mysql::QueryGenerator(_connectionPtr);
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
                    LOGS(_log, LOG_LVL_ERROR,
                         _context() << "Database schema version is newer than"
                                    << " the one required by the application, ex: " << ex.what());
                    throw;
                }
                schemaUpgradeTimer.stop();
                if (schemaUpgradeTimer.getElapsed() > Configuration::schemaUpgradeWaitTimeoutSec()) {
                    LOGS(_log, LOG_LVL_ERROR,
                         _context() << "The maximum duration of time ("
                                    << Configuration::schemaUpgradeWaitTimeoutSec() << " seconds) has expired"
                                    << " while waiting for the database schema upgrade. The schema version "
                                       "is still older than"
                                    << " the one required by the application, ex: " << ex.what());
                    throw;
                } else {
                    LOGS(_log, LOG_LVL_WARN,
                         _context() << "Database schema version is still older than the one"
                                    << " required by the application after "
                                    << schemaUpgradeTimer.getElapsed()
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
    replica::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& [name, worker] : _workers) {
        if (isEnabled) {
            if (worker.isEnabled && (isReadOnly == worker.isReadOnly)) {
                names.push_back(name);
            }
        } else {
            if (!worker.isEnabled) {
                names.push_back(name);
            }
        }
    }
    return names;
}

size_t Configuration::numWorkers(bool isEnabled, bool isReadOnly) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _numWorkers(lock, isEnabled, isReadOnly);
}

size_t Configuration::_numWorkers(replica::Lock const& lock, bool isEnabled, bool isReadOnly) const {
    size_t result = 0;
    for (auto&& [name, worker] : _workers) {
        if (isEnabled) {
            if (worker.isEnabled && (isReadOnly == worker.isReadOnly)) result++;
        } else {
            if (!worker.isEnabled) result++;
        }
    }
    return result;
}

vector<string> Configuration::allWorkers() const {
    replica::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& [name, worker] : _workers) {
        names.push_back(name);
    }
    return names;
}

vector<string> Configuration::databaseFamilies() const {
    replica::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& [name, family] : _databaseFamilies) {
        names.push_back(name);
    }
    return names;
}

bool Configuration::isKnownDatabaseFamily(string const& familyName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    if (familyName.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    return _databaseFamilies.count(familyName) != 0;
}

DatabaseFamilyInfo Configuration::databaseFamilyInfo(string const& familyName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _databaseFamilyInfo(lock, familyName);
}

DatabaseFamilyInfo Configuration::addDatabaseFamily(DatabaseFamilyInfo const& family) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (family.name.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    if (_databaseFamilies.find(family.name) != _databaseFamilies.end()) {
        throw invalid_argument(_context(__func__) + " the family '" + family.name + "' already exists.");
    }
    string errors;
    if (family.replicationLevel == 0) errors += " replicationLevel(0)";
    if (family.numStripes == 0) errors += " numStripes(0)";
    if (family.numSubStripes == 0) errors += " numSubStripes(0)";
    if (family.overlap <= 0) errors += " overlap(<=0)";
    if (!errors.empty()) throw invalid_argument(_context(__func__) + errors);
    if (_updatePersistentState(lock)) {
        string const query = _g.insert("config_database_family", family.name, family.replicationLevel,
                                       family.numStripes, family.numSubStripes, family.overlap);
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    _databaseFamilies[family.name] = family;
    return family;
}

void Configuration::deleteDatabaseFamily(string const& familyName) {
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseFamilyInfo& family = _databaseFamilyInfo(lock, familyName);
    if (_updatePersistentState(lock)) {
        string const query = _g.delete_("config_database_family") + _g.where(_g.eq("name", family.name));
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    // In order to maintain consistency of the persistent state also delete all
    // dependent databases.
    // NOTE: if using MySQL-based persistent backend the removal of the dependent
    //       tables from MySQL happens automatically since it's enforced by the PK/FK
    //       relationship between the corresponding tables.
    vector<string> databasesToBeRemoved;
    for (auto&& [name, database] : _databases) {
        if (database.family == family.name) {
            databasesToBeRemoved.push_back(name);
        }
    }
    for (string const& databaseName : databasesToBeRemoved) {
        _databases.erase(databaseName);
    }
    _databaseFamilies.erase(family.name);
}

size_t Configuration::replicationLevel(string const& familyName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _databaseFamilyInfo(lock, familyName).replicationLevel;
}

size_t Configuration::effectiveReplicationLevel(string const& familyName, size_t desiredReplicationLevel,
                                                bool workerIsEnabled, bool workerIsReadOnly) const {
    // IMPORTANT: Obtain a value of the hard limit before acquiring the lock
    // on the mutex in order to avoid a deadlock.
    size_t const hardLimit = this->get<size_t>("controller", "max-repl-level");
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseFamilyInfo const& family = _databaseFamilyInfo(lock, familyName);
    size_t const adjustedReplicationLevel =
            desiredReplicationLevel == 0 ? family.replicationLevel : desiredReplicationLevel;
    return std::min(
            {adjustedReplicationLevel, hardLimit, _numWorkers(lock, workerIsEnabled, workerIsReadOnly)});
}

void Configuration::setReplicationLevel(string const& familyName, size_t newReplicationLevel) {
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseFamilyInfo& family = _databaseFamilyInfo(lock, familyName);
    if (newReplicationLevel == 0) {
        throw invalid_argument(_context(__func__) + " replication level must be greater than 0.");
    }
    if (_updatePersistentState(lock)) {
        string const query =
                _g.update("config_database_family", make_pair("min_replication_level", newReplicationLevel)) +
                _g.where(_g.eq("name", family.name));
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    family.replicationLevel = newReplicationLevel;
}

vector<string> Configuration::databases(string const& familyName, bool allDatabases, bool isPublished) const {
    replica::Lock const lock(_mtx, _context(__func__));
    if (!familyName.empty()) {
        if (_databaseFamilies.find(familyName) == _databaseFamilies.cend()) {
            throw invalid_argument(_context(__func__) + " no such family '" + familyName + "'.");
        }
    }
    vector<string> names;
    for (auto&& [name, database] : _databases) {
        if (!familyName.empty() && (familyName != database.family)) {
            continue;
        }
        if (!allDatabases) {
            if (isPublished != database.isPublished) continue;
        }
        names.push_back(name);
    }
    return names;
}

void Configuration::assertDatabaseIsValid(string const& databaseName) {
    if (!isKnownDatabase(databaseName)) {
        throw invalid_argument(_context(__func__) + " database name is not valid: " + databaseName);
    }
}

bool Configuration::isKnownDatabase(string const& databaseName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    if (databaseName.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    return _databases.count(databaseName) != 0;
}

DatabaseInfo Configuration::databaseInfo(string const& databaseName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _databaseInfo(lock, databaseName);
}

DatabaseInfo Configuration::addDatabase(string const& databaseName, std::string const& familyName) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (databaseName.empty()) {
        throw invalid_argument(_context(__func__) + " the database name can't be empty.");
    }
    auto itr = _databases.find(databaseName);
    if (itr != _databases.end()) {
        throw invalid_argument(_context(__func__) + " the database '" + databaseName + "' already exists.");
    }
    // This will throw an exception if the family isn't valid
    _databaseFamilyInfo(lock, familyName);

    // Create a new empty database.
    DatabaseInfo const database = DatabaseInfo::create(databaseName, familyName);
    if (_updatePersistentState(lock)) {
        string const query =
                _g.insert("config_database", database.name, database.family, database.isPublished ? 1 : 0,
                          database.createTime, database.publishTime);
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    _databases[database.name] = database;
    return database;
}

DatabaseInfo Configuration::publishDatabase(string const& databaseName) {
    replica::Lock const lock(_mtx, _context(__func__));
    bool const publish = true;
    return _publishDatabase(lock, databaseName, publish);
}

DatabaseInfo Configuration::unPublishDatabase(string const& databaseName) {
    replica::Lock const lock(_mtx, _context(__func__));
    bool const publish = false;
    return _publishDatabase(lock, databaseName, publish);
}

void Configuration::deleteDatabase(string const& databaseName) {
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& database = _databaseInfo(lock, databaseName);
    if (_updatePersistentState(lock)) {
        string const query = _g.delete_("config_database") + _g.where(_g.eq("database", database.name));
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    _databases.erase(database.name);
}

DatabaseInfo Configuration::addTable(TableInfo const& table_) {
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& database = _databaseInfo(lock, table_.database);
    if (database.isPublished) {
        throw invalid_argument(_context(__func__) +
                               " adding tables to the published databases isn't allowed.");
    }
    // Make sure the input is sanitized & validated before attempting to register
    // the new table in the persistent store. After that the table could be also
    // registered in the transient state.
    bool const sanitize = true;
    TableInfo const table = database.validate(_databases, table_, sanitize);
    if (_updatePersistentState(lock)) {
        vector<string> queries;
        string const query = _g.insert(
                "config_database_table", table.database, table.name, table.isPartitioned,
                table.directorTable.databaseTableName(), table.directorTable.primaryKeyColumn(),
                table.directorTable2.databaseTableName(), table.directorTable2.primaryKeyColumn(),
                table.flagColName, table.angSep, table.uniquePrimaryKey ? 1 : 0, table.latitudeColName,
                table.longitudeColName, table.isPublished ? 1 : 0, table.createTime, table.publishTime);
        queries.emplace_back(query);
        int colPosition = 0;
        for (auto&& column : table.columns) {
            string const query = _g.insert("config_database_table_schema", table.database, table.name,
                                           colPosition++, column.name, column.type);
            queries.emplace_back(query);
        }
        _connectionPtr->executeInOwnTransaction([&queries](decltype(_connectionPtr) conn) {
            for (auto&& query : queries) {
                conn->execute(query);
            }
        });
    }
    bool const validate = false;
    database.addTable(_databases, table, validate);
    return database;
}

DatabaseInfo Configuration::deleteTable(string const& databaseName, string const& tableName) {
    replica::Lock const lock(_mtx, _context(__func__));
    DatabaseInfo& database = _databaseInfo(lock, databaseName);
    database.removeTable(tableName);
    if (_updatePersistentState(lock)) {
        string const query = _g.delete_("config_database_table") +
                             _g.where(_g.eq("database", database.name), _g.eq("table", tableName));
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    return database;
}

void Configuration::assertWorkerIsValid(string const& workerName) {
    if (!isKnownWorker(workerName)) {
        throw invalid_argument(_context(__func__) + " worker name is not valid: " + workerName);
    }
}

void Configuration::assertWorkersAreDifferent(string const& workerOneName, string const& workerTwoName) {
    assertWorkerIsValid(workerOneName);
    assertWorkerIsValid(workerTwoName);
    if (workerOneName == workerTwoName) {
        throw invalid_argument(_context(__func__) + " worker names are the same: " + workerOneName);
    }
}

bool Configuration::isKnownWorker(string const& workerName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _workers.count(workerName) != 0;
}

ConfigWorker Configuration::worker(string const& workerName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    auto const itr = _workers.find(workerName);
    if (itr != _workers.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " unknown worker '" + workerName + "'.");
}

ConfigWorker Configuration::addWorker(ConfigWorker const& worker) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (_workers.find(worker.name) == _workers.cend()) return _updateWorker(lock, worker);
    throw invalid_argument(_context(__func__) + " worker '" + worker.name + "' already exists.");
}

void Configuration::deleteWorker(string const& workerName) {
    replica::Lock const lock(_mtx, _context(__func__));
    auto itr = _workers.find(workerName);
    if (itr == _workers.end()) {
        throw invalid_argument(_context(__func__) + " unknown worker '" + workerName + "'.");
    }
    if (_updatePersistentState(lock)) {
        string const query = _g.delete_("config_worker") + _g.where(_g.eq("name", workerName));
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }
    _workers.erase(itr);
}

ConfigWorker Configuration::disableWorker(string const& workerName) {
    replica::Lock const lock(_mtx, _context(__func__));
    auto itr = _workers.find(workerName);
    if (itr == _workers.end()) {
        throw invalid_argument(_context(__func__) + " unknown worker '" + workerName + "'.");
    }
    ConfigWorker& worker = itr->second;
    if (worker.isEnabled) {
        if (_updatePersistentState(lock)) {
            string const query = _g.update("config_worker", make_pair("is_enabled", 0)) +
                                 _g.where(_g.eq("name", worker.name));
            _connectionPtr->executeInOwnTransaction(
                    [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
        }
        worker.isEnabled = false;
    }
    return worker;
}

ConfigWorker Configuration::updateWorker(ConfigWorker const& worker) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (_workers.find(worker.name) != _workers.end()) return _updateWorker(lock, worker);
    throw invalid_argument(_context(__func__) + " unknown worker '" + worker.name + "'.");
}

vector<std::string> Configuration::allCzars() const {
    replica::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& [name, czar] : _czars) {
        names.push_back(name);
    }
    return names;
}

size_t Configuration::numCzars() const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _czars.size();
}

bool Configuration::isKnownCzar(string const& czarName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _czars.count(czarName) != 0;
}

ConfigCzar Configuration::czar(string const& czarName) const {
    replica::Lock const lock(_mtx, _context(__func__));
    auto const itr = _czars.find(czarName);
    if (itr != _czars.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " unknown Czar '" + czarName + "'.");
}

ConfigCzar Configuration::addCzar(ConfigCzar const& czar) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (czar.name.empty()) {
        throw invalid_argument(_context(__func__) + " Czar name was not provided.");
    }
    if (_czars.find(czar.name) == _czars.cend()) {
        _czars[czar.name] = czar;
        return czar;
    }
    throw invalid_argument(_context(__func__) + " Czar '" + czar.name + "' already exists.");
}

void Configuration::deleteCzar(string const& czarName) {
    replica::Lock const lock(_mtx, _context(__func__));
    auto itr = _czars.find(czarName);
    if (itr == _czars.end()) {
        throw invalid_argument(_context(__func__) + " unknown Czar '" + czarName + "'.");
    }
    _czars.erase(itr);
}

ConfigCzar Configuration::updateCzar(ConfigCzar const& czar) {
    replica::Lock const lock(_mtx, _context(__func__));
    if (_czars.find(czar.name) != _czars.end()) {
        if (czar.name.empty()) {
            throw invalid_argument(_context(__func__) + " Czar name was not provided.");
        }
        _czars[czar.name] = czar;
        return czar;
    }
    throw invalid_argument(_context(__func__) + " unknown Czar '" + czar.name + "'.");
}

map<qmeta::CzarId, string> Configuration::czarIds() const {
    map<qmeta::CzarId, string> ids;
    replica::Lock const lock(_mtx, _context(__func__));
    for (auto&& [name, czar] : _czars) {
        ids[czar.id] = name;
    }
    return ids;
}

json Configuration::toJson(bool showPassword) const {
    replica::Lock const lock(_mtx, _context(__func__));
    return _toJson(lock, showPassword);
}

json Configuration::_toJson(replica::Lock const& lock, bool showPassword) const {
    json data;
    data["general"] = _data;
    json& workersJson = data["workers"];
    for (auto&& [name, worker] : _workers) {
        workersJson.push_back(worker.toJson());
    }
    json& databaseFamilies = data["database_families"];
    for (auto&& [name, family] : _databaseFamilies) {
        databaseFamilies.push_back(family.toJson());
    }
    json& databases = data["databases"];
    for (auto&& [name, database] : _databases) {
        databases.push_back(database.toJson());
    }
    json& czarsJson = data["czars"];
    for (auto&& [name, czar] : _czars) {
        czarsJson.push_back(czar.toJson());
    }
    return data;
}

json const& Configuration::_get(replica::Lock const& lock, string const& category,
                                string const& param) const {
    json::json_pointer const pointer("/" + category + "/" + param);
    if (!_data.contains(pointer)) {
        throw invalid_argument(_context(__func__) + " no such parameter for category: '" + category +
                               "', param: '" + param + "'");
    }
    return _data.at(pointer);
}

json& Configuration::_get(replica::Lock const& lock, string const& category, string const& param) {
    return _data[json::json_pointer("/" + category + "/" + param)];
}

ConfigWorker Configuration::_updateWorker(replica::Lock const& lock, ConfigWorker const& worker) {
    if (worker.name.empty()) {
        throw invalid_argument(_context(__func__) + " worker name can't be empty.");
    }

    // Update a subset of parameters in the persistent state.
    bool const update = _workers.count(worker.name) != 0;
    if (_updatePersistentState(lock)) {
        string query;
        if (update) {
            query = _g.update("config_worker", make_pair("is_enabled", worker.isEnabled),
                              make_pair("is_read_only", worker.isReadOnly)) +
                    _g.where(_g.eq("name", worker.name));
        } else {
            query = _g.insert("config_worker", worker.name, worker.isEnabled, worker.isReadOnly);
        }
        _connectionPtr->executeInOwnTransaction(
                [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
    }

    // Update all parameters in the transient state.
    _workers[worker.name] = worker;
    return worker;
}

DatabaseFamilyInfo& Configuration::_databaseFamilyInfo(replica::Lock const& lock, string const& familyName) {
    if (familyName.empty())
        throw invalid_argument(_context(__func__) + " the database family name is empty.");
    auto const itr = _databaseFamilies.find(familyName);
    if (itr != _databaseFamilies.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " no such database family '" + familyName + "'.");
}

DatabaseInfo& Configuration::_databaseInfo(replica::Lock const& lock, string const& databaseName) {
    if (databaseName.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    auto const itr = _databases.find(databaseName);
    if (itr != _databases.cend()) return itr->second;
    throw invalid_argument(_context(__func__) + " no such database '" + databaseName + "'.");
}

DatabaseInfo& Configuration::_publishDatabase(replica::Lock const& lock, string const& databaseName,
                                              bool publish) {
    DatabaseInfo& database = _databaseInfo(lock, databaseName);
    if (publish && database.isPublished) {
        throw logic_error(_context(__func__) + " database '" + database.name + "' is already published.");
    } else if (!publish && !database.isPublished) {
        throw logic_error(_context(__func__) + " database '" + database.name + "' is not published.");
    }
    if (publish) {
        uint64_t const publishTime = util::TimeUtils::now();
        // Firstly, publish all tables that were not published.
        for (auto const& tableName : database.tables()) {
            TableInfo& table = database.findTable(tableName);
            if (!table.isPublished) {
                if (_updatePersistentState(lock)) {
                    string const query =
                            _g.update("config_database_table", make_pair("is_published", 1),
                                      make_pair("publish_time", publishTime)) +
                            _g.where(_g.eq("database", database.name), _g.eq("table", table.name));
                    _connectionPtr->executeInOwnTransaction(
                            [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
                }
                table.isPublished = true;
                table.publishTime = publishTime;
            }
        }
        // Then publish the database.
        if (_updatePersistentState(lock)) {
            string const query = _g.update("config_database", make_pair("is_published", 1),
                                           make_pair("publish_time", publishTime)) +
                                 _g.where(_g.eq("database", database.name));
            _connectionPtr->executeInOwnTransaction(
                    [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
        }
        database.isPublished = true;
        database.publishTime = publishTime;
    } else {
        // Do not unpublish individual tables. The operation only affects
        // the general status of the database to allow adding more tables.
        if (_updatePersistentState(lock)) {
            string const query = _g.update("config_database", make_pair("is_published", 0)) +
                                 _g.where(_g.eq("database", database.name));
            _connectionPtr->executeInOwnTransaction(
                    [&query](decltype(_connectionPtr) conn) { conn->execute(query); });
        }
        database.isPublished = false;
    }
    return database;
}

}  // namespace lsst::qserv::replica
