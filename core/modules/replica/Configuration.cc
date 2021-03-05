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
#include <iostream>

// Qserv headers
#include "replica/ChunkNumber.h"
#include "replica/DatabaseMySQL.h"
#include "util/IterableFormatter.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace util = lsst::qserv::util;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.Configuration");

bool columnInSchema(string const& colName, list<SqlColDef> const& columns) {
    return columns.end() != find_if(
            columns.begin(), columns.end(),
            [&] (SqlColDef const& coldef) { return coldef.name == colName; });
}

/**
 * The class for parsing and loading a configuration stored in a JSON object.
 */
class ConfigParserJSON {
public:
    ConfigParserJSON() = delete;
    ConfigParserJSON(ConfigParserJSON const&) = delete;
    ConfigParserJSON& operator=(ConfigParserJSON const&) = delete;

    ConfigParserJSON(string const& context) : _context(context) {}

    /**
     * Parse the input object's content, validate it, and amend it to the output data object.
     * @param data The output object to be modified.
     * @param obj The input object to be parsed.
     */
    void parse(json& data, json const& obj) {
        if (!obj.is_object()) throw invalid_argument(_context + "a JSON object is required.");

        // Validate and amend configuration parameters.
        //
        // IMPORTANT: Note an order in which the parameter groups are being evaluated.
        //   This order guarantees data consistency based on the dependency between
        //   the parameters of the groups. For instance, worker definitions in the group 
        //   'workers' are processed after processing the default parameters of
        //   the workers in group 'worker_defaults'. Also, the database definitions in the group
        //   'databases' will be processed after processing database families in the group
        //   'database_families' so that the database's family name would be validated
        //   against names of the known families.
        //
        // OTHER NOTES:
        //   - The ordering approach also allows to process incomplete input configurations,
        //   or inject configuration options in more than one object.
        //   - Unknown groups of parameters as well as unknown parameters will be ignored.
        //   - The last insert always wins.

        // Make sure the input configuration's version (if any was presented in the input object)
        // matches the one required by the application.
        if ((obj.count("meta") != 0) && (obj.at("meta").count("version") != 0)) {
            int const version = obj.at("meta").at("version").get<int>();
            if (version != ConfigurationSchema::version) {
                throw ConfigVersionMismatch(version, ConfigurationSchema::version);
            }
        }

        // Now parse the groups.
        vector<string> const groups = {
            "common",
            "controller",
            "database",
            "xrootd",
            "worker",
            "worker_defaults",
            "workers",
            "database_families",
            "databases"
        };
        for (auto&& group: groups) {

            // Skip missing groups.
            if (obj.count(group) == 0) continue;

            json const& in = obj.at(group);
            json& out = data[group];

            if (group == "common") {
                _parseValue<size_t>(out, in, group, "request_buf_size_bytes");
                _parseValue<unsigned int>(out, in, group, "request_retry_interval_sec");
            } else if (group == "controller") {
                _parseValue<size_t>(out, in, group, "num_threads");
                _parseValue<unsigned int>(out, in, group, "request_timeout_sec");
                _parseValue<unsigned int>(out, in, group, "job_timeout_sec");
                _parseValue<unsigned int>(out, in, group, "job_heartbeat_sec");
                _parseValue<size_t>(out, in, group, "http_server_threads");
                _parseValue<uint16_t>(out, in, group, "http_server_port");
                _parseValue<string>(out, in, group, "empty_chunks_dir");
            } else if (group == "database") {
                _parseValue<size_t>(out, in, group, "services_pool_size");
                _parseValue<string>(out, in, group, "host");
                _parseValue<uint16_t>(out, in, group, "port");
                _parseValue<string>(out, in, group, "user");
                _parseValue<string>(out, in, group, "password");
                _parseValue<string>(out, in, group, "name");
                _parseValue<size_t>(out, in, group, "qserv_master_services_pool_size");
                _parseValue<string>(out, in, group, "qserv_master_host");
                _parseValue<uint16_t>(out, in, group, "qserv_master_port");
                _parseValue<string>(out, in, group, "qserv_master_user");
                _parseValue<string>(out, in, group, "qserv_master_name");
                _parseValue<string>(out, in, group, "qserv_master_tmp_dir");
            } else if (group == "xrootd") {
                _parseValue<unsigned int>(out, in, group, "auto_notify");
                _parseValue<unsigned int>(out, in, group, "request_timeout_sec");
                _parseValue<string>(out, in, group, "host");
                _parseValue<uint16_t>(out, in, group, "port");
            } else if (group == "worker") {
                _parseValue<string>(out, in, group, "technology");
                _parseValue<size_t>(out, in, group, "num_svc_processing_threads");
                _parseValue<size_t>(out, in, group, "num_fs_processing_threads");
                _parseValue<size_t>(out, in, group, "fs_buf_size_bytes");
                _parseValue<size_t>(out, in, group, "num_loader_processing_threads");
                _parseValue<size_t>(out, in, group, "num_exporter_processing_threads");
                _parseValue<size_t>(out, in, group, "num_http_loader_processing_threads");
            } else if (group == "worker_defaults") {
                _parseValue<uint16_t>(out, in, group, "svc_port");
                _parseValue<uint16_t>(out, in, group, "fs_port");
                _parseValue<string>(out, in, group, "data_dir");
                _parseValue<uint16_t>(out, in, group, "db_port");
                _parseValue<string>(out, in, group, "db_user");
                _parseValue<uint16_t>(out, in, group, "loader_port");
                _parseValue<string>(out, in, group, "loader_tmp_dir");
                _parseValue<uint16_t>(out, in, group, "exporter_port");
                _parseValue<string>(out, in, group, "exporter_tmp_dir");
                _parseValue<uint16_t>(out, in, group, "http_loader_port");
                _parseValue<string>(out, in, group, "http_loader_tmp_dir");
            } else if (group == "workers") {
                for (auto&& workerItr: in.items()) {
                    string const& worker = workerItr.key();
                    json const& inWorker = workerItr.value();
                    // Use this constructor to validate the schema and to fill in the missing (optional)
                    // parameters. If it won't throw then the input description is correct and can be placed
                    // into the output object. Using defaults is needed to ensure the worker entry is
                    // complete before storying in the transient state. Note that users of the API may rely
                    // on the default values of some parameters of workers.
                    WorkerInfo const info(inWorker, data.at("worker_defaults"));
                    if (worker != info.name) {
                        throw invalid_argument(
                                _context + "inconsistent definition for worker: " + worker + " in JSON object: "
                                + inWorker.dump());
                    }
                    // Reister the validated/postprocessed JSON w/ missing fields correction
                    out[worker] = info.toJson();
                }
            } else if (group == "database_families") {
                for (auto&& familyItr: in.items()) {
                    string const& family = familyItr.key();
                    json const& inFamily = familyItr.value();
                    // Use this constructor to validate the schema. If it won't throw then
                    // the input description is correct and can be placed into the output object.
                    DatabaseFamilyInfo const info(inFamily);
                    if (family != info.name) {
                        throw invalid_argument(
                                _context + "inconsistent definition for database family: " + family + " in JSON object: "
                                + inFamily.dump());
                    }
                    // Reister the validated/postprocessed JSON w/ missing fields correction
                    out[family] = info.toJson();
                }
            } else if (group == "databases") {
                for (auto&& databaseItr: in.items()) {
                    string const& database = databaseItr.key();
                    json const& inDatabase = databaseItr.value();
                    // Use this constructor to validate the schema. If it won't throw then
                    // the input description is correct and can be placed into the output object.
                    // Note that the c-tor expects a collection of the database families to ensure
                    // an existing family name was provided in the input spec.
                    DatabaseInfo const info(inDatabase, data.at("database_families"));
                    if (database != info.name) {
                        throw invalid_argument(
                                _context + "inconsistent definition for database: " + database + " in JSON object: "
                                + inDatabase.dump());
                    }
                    // Reister the validated/postprocessed JSON w/ missing fields correction
                    out[database] = info.toJson();
                }
            }
        }
    }
private:
    /**
     * check if a value of the proposed parameter exists at the source object, and if
     * so then parse it into a value of the required (template parameter 'T') type and
     * update the parameter's value at the destination dictionary.
     * @param dest The destination object.
     * @param source The source object.
     * @param category The name of the parameter's category.
     * @param param The name of the parameter to test.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.
     */
    template <typename T>
    void _parseValue(json& dest, json const& source, string const& category, string const& param) {
        if (source.count(param) != 0) {
            T const val = source.at(param).get<T>();
            // Sanitize the value to ensure it matches schema requirements.
            dest[param] = ConfigurationSchema::emptyAllowed(category, param) ? val :
                    detail::TypeConversionTrait<T>::validate(
                            _context + " category='" + category + "' param='" + param + "' ", val);
        }
    }

    // Input parameters
    string const _context;
};

/**
 * The class for parsing and loading the persistet configuration stored in MySQL.
 */
class ConfigParserMySQL {
public:
    ConfigParserMySQL() = delete;
    ConfigParserMySQL(ConfigParserMySQL const&) = delete;
    ConfigParserMySQL& operator=(ConfigParserMySQL const&) = delete;

    ConfigParserMySQL(string const& context,
                      database::mysql::Connection::Ptr const& conn,
                      json& data)
        :   _context(context), _conn(conn), _data(data) {
    }

    /**
     * Parse and load everything.
     * @param data The transient configuration object to be populated/amended.
     * @throws std::runtime_error If a required field has NULL.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.

     */
    void parse() {
        // At the first step check the version number.
        int const version = _parseVersion();
        if (version != ConfigurationSchema::version) {
            throw ConfigVersionMismatch(version, ConfigurationSchema::version);
        }

        // Read and amend to the transient state the general parameters and defaults
        // shared by all components of the Replication system. The table also provides
        // default values for some critical parameters of the worker-side services.
        _parseGeneral();

        // Parse groupped parameters
        _data["workers"] = _parseWorkers();
        _data["database_families"] = _parseDatabaseFamilies();
        _data["databases"] = _parseDatabases();
    }

private:
    /// @return The version number.
    /// @throws std::runtime_error If a required field has NULL.
    int _parseVersion() {
        string const colname = "value";
        int version = 0;
        try {
            bool const versionWasFound = _conn->executeSingleValueSelect(
                    "SELECT " + _conn->sqlId(colname) + " FROM " + _conn->sqlId("config")
                    + " WHERE " + _conn->sqlEqual("category", "meta")
                     + " AND "  + _conn->sqlEqual("param", "version"), colname, version);
            if (!versionWasFound) {
                version = 0;
            }
        } catch (database::mysql::EmptyResultSetError const&) {
            // Version number was missing in the configuration database.
            version = 0;
        } catch (exception const& ex) {
            throw runtime_error(
                    _context + " failed to locate version number in the configuration database, ex: "
                    + string(ex.what()));
        }
        return version;
    }

    /**
     * Inspect the current row from the MySQL result set to see if the desired parameter
     * is found in there, and i fso then extract its value and put into the corresponding
     * location of the transent configuration state.
     * @return 'true' if the parameter was found and extracted.
     * @throws std::runtime_error If a required field has NULL.
     * @throws std::invalid_argument If the parameter's value didn't pass the validation.
     */
    template <typename T>
    bool _tryParameter(string const& desiredCategory, string const& desiredParam) {
        string category;
        _row.get("category", category);
        if (desiredCategory != category) return false;

        string param;
        _row.get("param", param);
        if (desiredParam != param) return false;

        T value;
        if (!_row.get("value", value)) {
            throw runtime_error(
                    _context + " NULL is not allowed for category='" + category + "' param='" + param + "'.");
        }
        // Sanitize the value to ensure it matches schema requirements.
        _data[desiredCategory][desiredParam] = 
                ConfigurationSchema::emptyAllowed(category, param) ? value :
                        detail::TypeConversionTrait<T>::validate(
                                _context + " category='" + category + "' param='" + param + "' ", value);
        return true;
    }

    /// Popalate JSON objects representing general parameters.
    void _parseGeneral() {
        _defaults = json::object();
        _conn->execute("SELECT * FROM " + _conn->sqlId("config"));
        while (_conn->next(_row)) {
            _tryParameter<size_t>(      "common", "request_buf_size_bytes") ||
            _tryParameter<unsigned int>("common", "request_retry_interval_sec") ||
            _tryParameter<size_t>(      "controller", "num_threads") ||
            _tryParameter<unsigned int>("controller", "request_timeout_sec") ||
            _tryParameter<unsigned int>("controller", "job_timeout_sec") ||
            _tryParameter<unsigned int>("controller", "job_heartbeat_sec") ||
            _tryParameter<size_t>(      "controller", "http_server_threads") ||
            _tryParameter<uint16_t>(    "controller", "http_server_port") ||
            _tryParameter<string>(      "controller", "empty_chunks_dir") ||
            _tryParameter<size_t>(      "database", "services_pool_size") ||
            _tryParameter<string>(      "database", "qserv_master_host") ||
            _tryParameter<uint16_t>(    "database", "qserv_master_port") ||
            _tryParameter<string>(      "database", "qserv_master_user") ||
            _tryParameter<string>(      "database", "qserv_master_name") ||
            _tryParameter<size_t>(      "database", "qserv_master_services_pool_size") ||
            _tryParameter<string>(      "database", "qserv_master_tmp_dir") ||
            _tryParameter<unsigned int>("xrootd", "auto_notify") ||
            _tryParameter<string>(      "xrootd", "host") ||
            _tryParameter<uint16_t>(    "xrootd", "port") ||
            _tryParameter<unsigned int>("xrootd", "request_timeout_sec") ||
            _tryParameter<string>(      "worker", "technology") ||
            _tryParameter<size_t>(      "worker", "num_svc_processing_threads") ||
            _tryParameter<size_t>(      "worker", "num_fs_processing_threads") ||
            _tryParameter<size_t>(      "worker", "fs_buf_size_bytes") ||
            _tryParameter<size_t>(      "worker", "num_loader_processing_threads") ||
            _tryParameter<size_t>(      "worker", "num_exporter_processing_threads") ||
            _tryParameter<size_t>(      "worker", "num_http_loader_processing_threads") ||
            _tryParameter<uint16_t>(    "worker_defaults", "svc_port") ||
            _tryParameter<uint16_t>(    "worker_defaults", "fs_port") ||
            _tryParameter<string>(      "worker_defaults", "data_dir") ||
            _tryParameter<uint16_t>(    "worker_defaults", "db_port") ||
            _tryParameter<string>(      "worker_defaults", "db_user") ||
            _tryParameter<uint16_t>(    "worker_defaults", "loader_port") ||
            _tryParameter<string>(      "worker_defaults", "loader_tmp_dir") ||
            _tryParameter<uint16_t>(    "worker_defaults", "exporter_port") ||
            _tryParameter<string>(      "worker_defaults", "exporter_tmp_dir") ||
            _tryParameter<uint16_t>(    "worker_defaults", "http_loader_port") ||
            _tryParameter<string>(      "worker_defaults", "http_loader_tmp_dir");
        }
    }

    /// @return A JSON object representing a collection of workers.
    json _parseWorkers() {
        _defaults = _data.at("worker_defaults");
        json resultJson = json::object();
        bool const isOptional = true;
        _conn->execute("SELECT * FROM " + _conn->sqlId("config_worker"));
        while (_conn->next(_row)) {
            _objJson = json::object();
            _parseParam<string>("name");
            _parseParam<int>("is_enabled");
            _parseParam<int>("is_read_only" );
            _parseParam<string>("svc_host");
            _parseParam<uint16_t>("svc_port", isOptional);
            _parseParam<string>("fs_host");
            _parseParam<uint16_t>("fs_port", isOptional);
            _parseParam<string>("data_dir", isOptional);
            _parseParam<string>("db_host");
            _parseParam<uint16_t>("db_port", isOptional);
            _parseParam<string>("db_user", isOptional);
            _parseParam<string>("loader_host");
            _parseParam<uint16_t>("loader_port", isOptional);
            _parseParam<string>("loader_tmp_dir", isOptional);
            _parseParam<string>("exporter_host");
            _parseParam<uint16_t>("exporter_port", isOptional);
            _parseParam<string>("exporter_tmp_dir", isOptional);
            _parseParam<string>("http_loader_host");
            _parseParam<uint16_t>("http_loader_port", isOptional);
            _parseParam<string>("http_loader_tmp_dir", isOptional);
            resultJson[_objJson.at("name").get<string>()] = _objJson;
        }
        return resultJson;
    }

    /// @return A JSON object representing a collection of database families.
    json _parseDatabaseFamilies() {
        _defaults = json::object();
        json resultJson = json::object();
        _conn->execute("SELECT * FROM " + _conn->sqlId("config_database_family"));
        while (_conn->next(_row)) {
            _objJson = json::object();
            _parseParam<string>("name");
            _parseParam<unsigned int>("min_replication_level");
            _parseParam<unsigned int>("num_stripes");
            _parseParam<unsigned int>("num_sub_stripes");
            _parseParam<double>("overlap");
            resultJson[_objJson.at("name").get<string>()] = _objJson;
        }
        return resultJson;
    }

    json _parseDatabases() {
        _defaults = json::object();
        json databasesJson = json::object();
        _conn->execute("SELECT * FROM " + _conn->sqlId("config_database"));
        while (_conn->next(_row)) {
            _objJson = json::object();
            _parseParam<string>("database");
            _parseParam<string>("family_name");
            _parseParam<int>("is_published");
            _parseParam<string>("chunk_id_key");
            _parseParam<string>("sub_chunk_id_key");
            _objJson["director_table"] = string();
            _objJson["director_key"] = string();
            databasesJson[_objJson.at("database").get<string>()] = _objJson;
        }
        // Read database-specific table definitions and extend the corresponding database entries.
        _conn->execute("SELECT * FROM " + _conn->sqlId("config_database_table"));
        while (_conn->next(_row)) {
            _objJson = json::object();
            string const database = _parseParam<string>("database");
            string const table = _parseParam<string>("table");
            if (_parseParam<int>("is_partitioned") != 0) {
                if (_parseParam<int>("is_director") != 0) {
                    databasesJson[database]["director_table"] = table;
                    databasesJson[database]["director_key"] = _parseParam<string>("director_key");
                }
                databasesJson[database]["tables"][table] = json::object({
                    {"name", table},
                    {"is_partitioned", 1},
                    {"latitude_key",  _parseParam<string>("latitude_key")},
                    {"longitude_key", _parseParam<string>("longitude_key")}
                });
            } else {
                databasesJson[database]["tables"][table] = json::object({
                    {"name", table},
                    {"is_partitioned", 0}
                });
            }
        }

        // Read schema for each table (if available)
        for (auto&& databaseItr: databasesJson.items()) {
            string const& database = databaseItr.key();
            json& databaseJson = databaseItr.value();
            for (auto&& tableItr: databaseJson["tables"].items()) {
                string const& table = tableItr.key();
                json& tableJson = tableItr.value();
                json& columnsJson = tableJson["columns"] = json::array();
                _conn->execute(
                    "SELECT "     + _conn->sqlId("col_name") + "," + _conn->sqlId("col_type") +
                    "  FROM "     + _conn->sqlId("config_database_table_schema") +
                    "  WHERE "    + _conn->sqlEqual("database", database) +
                    "    AND "    + _conn->sqlEqual("table", table) +
                    "  ORDER BY " + _conn->sqlId("col_position") + " ASC");
                while (_conn->next(_row)) {
                    _objJson = json::object();
                    columnsJson.push_back({
                        {"name", _parseParam<string>("col_name")},
                        {"type", _parseParam<string>("col_type")}
                    });
                }
            }
        }
        return databasesJson;
    }

    /**
     * Parse a value of the specified parameter and set it in the currnet object.
     * Use the default for the optional parameters if NULL was encountered.
     * @param name The name of a parameters,
     * @param isOptional The flag indicating of the parameters is no mandatory.
     * @return The parsed value.
     * @throws runtime_error If NULL was encountered while parsing the mandatory parameter.
     */
    template <typename T>
    T _parseParam(string const& name, bool isOptional=false) {
        T value;
        if (!_row.get(name, value)) {
            if (isOptional) {
                value = _defaults.at(name).get<T>();
            } else {
                throw runtime_error(
                        _context + " the spec field '" + name + "' is not allowed to be NULL");
            }
        }
        _objJson[name] = value;
        return value;
    }

    // Input parameters
    string const _context;
    database::mysql::Connection::Ptr const _conn;
    json& _data;

    // Is initialized with optional defaults when parsing specific groups of objects. 
    json _defaults;

    // The current context of the parser is set before parsing the next row with object info.
    database::mysql::Row _row;  // input data
    json _objJson;              // object description to be filled from the row
};

} // namespace

namespace lsst {
namespace qserv {
namespace replica {

// ------------------------------------------------------------------------------
// Instance API of class WorkerInfo
// ------------------------------------------------------------------------------

WorkerInfo::WorkerInfo(json const& obj, json const& defaults) {
    string const context = "WorkerInfo::WorkerInfo(json,defaults): ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    // Amend missing attributes from a collection of the defaults if needed.
    try {
        name       = obj.at("name").get<string>();
        isEnabled  = obj.at("is_enabled").get<int>() != 0;
        isReadOnly = obj.at("is_read_only").get<int>() != 0;

        svcHost =  obj.at("svc_host").get<string>();
        svcPort = (obj.count("svc_port") != 0 ? obj : defaults).at("svc_port").get<uint16_t>();

        fsHost  =  obj.count("fs_host")  != 0 ? obj.at("fs_host").get<string>() : svcHost;
        fsPort  = (obj.count("fs_port")  != 0 ? obj : defaults).at("fs_port").get<uint16_t>();
        dataDir = (obj.count("data_dir") != 0 ? obj : defaults).at("data_dir").get<string>();

        dbHost =  obj.count("db_host") != 0 ? obj.at("db_host").get<string>() : svcHost;
        dbPort = (obj.count("db_port") != 0 ? obj : defaults).at("db_port").get<uint16_t>();
        dbUser = (obj.count("db_user") != 0 ? obj : defaults).at("db_user").get<string>();

        loaderHost   =  obj.count("loader_host")    != 0 ? obj.at("loader_host").get<string>() : svcHost;
        loaderPort   = (obj.count("loader_port")    != 0 ? obj : defaults).at("loader_port").get<uint16_t>();
        loaderTmpDir = (obj.count("loader_tmp_dir") != 0 ? obj : defaults).at("loader_tmp_dir").get<string>();

        exporterHost   =  obj.count("exporter_host")    != 0 ? obj.at("exporter_host").get<string>() : svcHost;
        exporterPort   = (obj.count("exporter_port")    != 0 ? obj : defaults).at("exporter_port").get<uint16_t>();
        exporterTmpDir = (obj.count("exporter_tmp_dir") != 0 ? obj : defaults).at("exporter_tmp_dir").get<string>();

        httpLoaderHost   =  obj.count("http_loader_host")    != 0 ? obj.at("http_loader_host").get<string>() : svcHost;
        httpLoaderPort   = (obj.count("http_loader_port")    != 0 ? obj : defaults).at("http_loader_port").get<uint16_t>();
        httpLoaderTmpDir = (obj.count("http_loader_tmp_dir") != 0 ? obj : defaults).at("http_loader_tmp_dir").get<string>();

    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}


WorkerInfo::WorkerInfo(WorkerInfo const& info, json const& defaults) {
    string const context = "WorkerInfo::WorkerInfo(info,defaults): ";
    if (info.name.empty()) {
        throw invalid_argument(context + "the input name of a worker.");
    }
    if (info.svcHost.empty()) {
        throw invalid_argument(context + "the input name of a host for the Replication service is empty.");
    }
    if (!defaults.is_object()) {
        throw invalid_argument(context + "a JSON object with worker defaults is required.");
    }
    // Amend missing attributes from a collection of the defaults if needed.
    try {
        name       = info.name;
        isEnabled  = info.isEnabled;
        isReadOnly = info.isReadOnly;

        svcHost = info.svcHost;
        svcPort = info.svcPort != 0 ? info.svcPort : defaults.at("svc_port").get<uint16_t>();

        fsHost  = !info.fsHost.empty()  ? info.fsHost  : info.svcHost;
        fsPort  = info.fsPort != 0      ? info.fsPort  : defaults.at("fs_port").get<uint16_t>();
        dataDir = !info.dataDir.empty() ? info.dataDir : defaults.at("data_dir").get<string>();

        dbHost  = !info.dbHost.empty() ? info.dbHost  : info.svcHost;
        dbPort  = info.dbPort != 0     ? info.dbPort : defaults.at("db_port").get<uint16_t>();
        dbUser =  !info.dbUser.empty() ? info.dbUser : defaults.at("db_user").get<string>();

        loaderHost   = !info.loaderHost.empty()   ? info.loaderHost  : info.svcHost;
        loaderPort   = info.loaderPort != 0       ? info.loaderPort   : defaults.at("loader_port").get<uint16_t>();
        loaderTmpDir = !info.loaderTmpDir.empty() ? info.loaderTmpDir : defaults.at("loader_tmp_dir").get<string>();

        exporterHost   = !info.exporterHost.empty()   ? info.exporterHost   : info.svcHost;
        exporterPort   = info.exporterPort != 0       ? info.exporterPort   : defaults.at("exporter_port").get<uint16_t>();
        exporterTmpDir = !info.exporterTmpDir.empty() ? info.exporterTmpDir : defaults.at("exporter_tmp_dir").get<string>();

        httpLoaderHost   = !info.httpLoaderHost.empty()   ? info.httpLoaderHost   : info.svcHost;
        httpLoaderPort   = info.httpLoaderPort != 0       ? info.httpLoaderPort   : defaults.at("http_loader_port").get<uint16_t>();
        httpLoaderTmpDir = !info.httpLoaderTmpDir.empty() ? info.httpLoaderTmpDir : defaults.at("http_loader_tmp_dir").get<string>();

    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
}


json WorkerInfo::toJson() const {
    json infoJson;
    infoJson["name"] = name;
    infoJson["is_enabled"] = isEnabled  ? 1 : 0;
    infoJson["is_read_only"] = isReadOnly ? 1 : 0;
    infoJson["svc_host"] = svcHost;
    infoJson["svc_port"] = svcPort;
    infoJson["fs_host"] = fsHost;
    infoJson["fs_port"] = fsPort;
    infoJson["data_dir"] = dataDir;
    infoJson["db_host"] = dbHost;
    infoJson["db_port"] = dbPort;
    infoJson["db_user"] = dbUser;
    infoJson["loader_host"] = loaderHost;
    infoJson["loader_port"] = loaderPort;
    infoJson["loader_tmp_dir"] = loaderTmpDir;
    infoJson["exporter_host"] = exporterHost;
    infoJson["exporter_port"] = exporterPort;
    infoJson["exporter_tmp_dir"] = exporterTmpDir;
    infoJson["http_loader_host"] = httpLoaderHost;
    infoJson["http_loader_port"] = httpLoaderPort;
    infoJson["http_loader_tmp_dir"] = httpLoaderTmpDir;
    return infoJson;
}


ostream& operator <<(ostream& os, WorkerInfo const& info) {
    os  << "WorkerInfo: " << info.toJson().dump();
    return os;
}


// ------------------------------------------------------------------------------
// Instance API of class DatabaseInfo
// ------------------------------------------------------------------------------

DatabaseInfo::DatabaseInfo(json const& obj, json const& families) {
    string const context = "DatabaseInfo::DatabaseInfo(json): ";
    if (obj.empty()) return;    // the default construction
    if (!obj.is_object()) throw invalid_argument(context + "a JSON object is required.");
    try {
        name = obj.at("database").get<string>();
        family = obj.at("family_name").get<string>();
        // Family validation is allowed to be explicitly disabled by passing the 'null'
        // object instead of the dictionary. The 'null' object is constructed by passing
        // 'nullptr' in place of 'families' or using the default constructor of the class
        // 'json::json()'.
        if (!families.is_null() && (families.count(family) == 0)) {
            throw invalid_argument(
                    context + "the family name '" + family + "' specified in the JSON object is unknown.");
        }
        isPublished  = obj.at("is_published").get<int>() != 0;
        if (obj.count("tables") != 0) {
            for (auto&& itr: obj.at("tables").items()) {
                string const& table = itr.key();
                json const& tableJson = itr.value();
                if (table != tableJson.at("name").get<string>()) {
                    throw invalid_argument(
                            context + "the table name '" + table + "' found in a dictionary of the database's " + name 
                            + "' tables is not consistent within the table's name '" + tableJson.at("name").get<string>()
                            + "' within the JSON object representing the table.");
                }
                bool const isPartitioned = tableJson.at("is_partitioned").get<int>() != 0;
                if (isPartitioned) {
                    partitionedTables.push_back(table);
                    latitudeColName[table]  = tableJson.at("latitude_key").get<string>();
                    longitudeColName[table] = tableJson.at("longitude_key").get<string>();
                } else {
                    regularTables.push_back(table);
                    latitudeColName[table]  = string();
                    longitudeColName[table] = string();
                }
            }
        }
        if (obj.count("columns") != 0) {
            for (auto&& itr: obj.at("columns").items()) {
                auto&& table = itr.key();
                auto&& columnsJson = itr.value();
                list<SqlColDef>& tableColumns = columns[table];
                for (auto&& coldefJson: columnsJson) {
                    tableColumns.push_back(
                        SqlColDef(coldefJson.at("name").get<string>(), coldefJson.at("type").get<string>()));
                }
            }
        }
        directorTable     = obj.at("director_table").get<string>();
        directorTableKey  = obj.at("director_key").get<string>();
        chunkIdColName    = obj.at("chunk_id_key").get<string>();
        subChunkIdColName = obj.at("sub_chunk_id_key").get<string>();
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }

}

json DatabaseInfo::toJson() const {
    json infoJson;
    infoJson["database"] = name;
    infoJson["family_name"] = family;
    infoJson["is_published"] = isPublished ? 1 : 0;
    infoJson["tables"] = json::object();
    for (auto&& name: partitionedTables) {
        infoJson["tables"][name] = json::object({
            {"name", name},
            {"is_partitioned", 1},
            {"latitude_key", latitudeColName.at(name)},
            {"longitude_key", longitudeColName.at(name)}
        });
    }
    for (auto&& name: regularTables) {
        infoJson["tables"][name] = json::object({
            {"name", name},
            {"is_partitioned", 0},
            {"latitude_key", ""},
            {"longitude_key", ""}
        });
    }
    for (auto&& columnsEntry: columns) {
        string const& table = columnsEntry.first;
        auto const& coldefs = columnsEntry.second;
        json coldefsJson;
        for (auto&& coldef: coldefs) {
            json coldefJson;
            coldefJson["name"] = coldef.name;
            coldefJson["type"] = coldef.type;
            coldefsJson.push_back(coldefJson);
        }
        infoJson["columns"][table] = coldefsJson;
    }
    infoJson["director_table"] = directorTable;
    infoJson["director_key"] = directorTableKey;
    infoJson["chunk_id_key"] = chunkIdColName;
    infoJson["sub_chunk_id_key"] = subChunkIdColName;
    return infoJson;
}


string DatabaseInfo::schema4css(string const& table) const {
    string schema;
    for (auto const& coldef: columns.at(table)) {
        schema += string(schema.empty() ? "(" : ", ") + "`" + coldef.name + "` " + coldef.type;
    }
    schema += ")";
    return schema;
}


bool DatabaseInfo::isPartitioned(string const& table) const {
    if (partitionedTables.end() != find(partitionedTables.begin(), partitionedTables.end(), table))  return true;
    if (regularTables.end() != find(regularTables.begin(), regularTables.end(), table)) return false;
    throw invalid_argument(
            "DatabaseInfo::" + string(__func__) +
            "no such table '" + table + "' found in database '" + name + "'");
}


bool DatabaseInfo::isDirector(string const& table) const {
    // This test will also ensure the table is known. Otherwise, an exception
    // will be thrown.
    if (not isPartitioned(table)) return false;
    return table == directorTable;
}


ostream& operator <<(ostream& os, DatabaseInfo const& info) {
    os  << "DatabaseInfo: " << info.toJson().dump();
    return os;
}


// ------------------------------------------------------------------------------
// Instance API of class DatabaseFamilyInfo
// ------------------------------------------------------------------------------

DatabaseFamilyInfo::DatabaseFamilyInfo(json const& obj) {
    string const context = "DatabaseFamilyInfo::DatabaseFamilyInfo(json): ";
    if (obj.empty()) return;
    if (!obj.is_object()) {
        throw invalid_argument(context + "a JSON object is required.");
    }
    try {
        name = obj.at("name").get<string>();
        replicationLevel = obj.at("min_replication_level").get<size_t>();
        numStripes = obj.at("num_stripes").get<unsigned int>();
        numSubStripes = obj.at("num_sub_stripes").get<unsigned int>();
        overlap = obj.at("overlap").get<double>();
    } catch (exception const& ex) {
        throw invalid_argument(context + "the JSON object is not valid, ex: " + string(ex.what()));
    }
    chunkNumberValidator = make_shared<ChunkNumberQservValidator>(
            static_cast<int32_t>(numStripes),
            static_cast<int32_t>(numSubStripes));
}


json DatabaseFamilyInfo::toJson() const {
    json infoJson;
    infoJson["name"] = name;
    infoJson["min_replication_level"] = replicationLevel;
    infoJson["num_stripes"] = numStripes;
    infoJson["num_sub_stripes"] = numSubStripes;
    infoJson["overlap"] = overlap;
    return infoJson;
}


ostream& operator <<(ostream& os, DatabaseFamilyInfo const& info) {
    os  << "DatabaseFamilyInfo: " << info.toJson().dump();
    return os;
}


// ------------------------------------------------------------------------------
// Static API of class Configuration
// ------------------------------------------------------------------------------

// These (static) data members are allowed to be changed, and they are set
// globally for an application (process).
bool         Configuration::_databaseAllowReconnect = true;
unsigned int Configuration::_databaseConnectTimeoutSec = 3600;
unsigned int Configuration::_databaseMaxReconnects = 1;
unsigned int Configuration::_databaseTransactionTimeoutSec = 3600;
string       Configuration::_qservMasterDatabasePassword = "";
string       Configuration::_qservWorkerDatabasePassword = "";
bool         Configuration::_xrootdAllowReconnect = true;
unsigned int Configuration::_xrootdConnectTimeoutSec = 3600;


string Configuration::setQservMasterDatabasePassword(string const& newPassword) {
    string result = newPassword;
    swap(result, _qservMasterDatabasePassword);
    return result;
}


string Configuration::setQservWorkerDatabasePassword(string const& newPassword) {
    string result = newPassword;
    swap(result, _qservWorkerDatabasePassword);
    return result;
}


bool Configuration::setDatabaseAllowReconnect(bool value) {
    swap(value, _databaseAllowReconnect);
    return value;
}


unsigned int Configuration::setDatabaseConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseConnectTimeoutSec);
    return value;
}


unsigned int Configuration::setDatabaseMaxReconnects(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseMaxReconnects);
    return value;
}


unsigned int Configuration::setDatabaseTransactionTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _databaseTransactionTimeoutSec);
    return value;
}


bool Configuration::setXrootdAllowReconnect(bool value) {
    swap(value, _xrootdAllowReconnect);
    return value;

}


unsigned int Configuration::setXrootdConnectTimeoutSec(unsigned int value) {
    if (0 == value) {
        throw invalid_argument(
                "Configuration::" + string(__func__) + "  0 is not allowed as a value");
    }
    swap(value, _xrootdConnectTimeoutSec);
    return value;

}


Configuration::Ptr Configuration::load(string const& configUrl, bool autoMigrateSchema) {
    bool const reset = false;
    try {
        Ptr const ptr(new Configuration());
        util::Lock const lock(ptr->_mtx, _context(__func__));
        ptr->_load(lock, configUrl, reset);
        return ptr;
    } catch (ConfigVersionMismatch const& ex) {
        if (autoMigrateSchema && (ex.version < ex.requiredVersion)) {
            LOGS(_log, LOG_LVL_WARN, _context() << "schema version " << ex.version
                    << " detected in " << configUrl << " doesn't match the required version "
                    << ex.requiredVersion << ". Trying schema upgrade now.");
        } else {
            throw;
        }
    }
    Ptr const ptr(new Configuration());
    util::Lock const lock(ptr->_mtx, _context(__func__));
    ConfigurationSchema::upgrade(configUrl);
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
    return "CONFIG   " + func;
}


// ------------------------------------------------------------------------------
// Instance API of class Configuration
// ------------------------------------------------------------------------------

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

    if (reset) _data = ConfigurationSchema::defaultConfigData();
    _configUrl = string();
    _connectionPtr = nullptr;

    // Validate and amend configuration parameters.
    // Catch exceptions for error reporting.
    ConfigParserJSON parser(_context("_load[JSON]"));
    parser.parse(_data, obj);

    bool const showPassword = false;
    LOGS(_log, LOG_LVL_DEBUG, _context() << _toJson(lock, showPassword).dump());
}


void Configuration::_load(util::Lock const& lock, string const& configUrl, bool reset) {

    if (reset) _data = ConfigurationSchema::defaultConfigData();
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

    // Read data, validate and amend configuration parameters.
    _connectionPtr = database::mysql::Connection::open(_connectionParams);
    _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
        ConfigParserMySQL parser(_context("_load[MySQL]"), conn, _data);
        parser.parse();
    });

    bool const showPassword = false;
    LOGS(_log, LOG_LVL_DEBUG, _context() << _toJson(lock, showPassword).dump());
}


vector<string> Configuration::workers(bool isEnabled, bool isReadOnly) const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _data.at("workers").items()) {
        string const& name = itr.key();
        json const& obj = itr.value();
        bool const workerIsEnabled = obj.at("is_enabled").get<int>() != 0;
        bool const workerIsReadOnly = obj.at("is_read_only").get<int>() != 0;
        if (isEnabled) {
            if (workerIsEnabled && (isReadOnly == workerIsReadOnly)) {
                names.push_back(name);
            }
        } else {
            if (!workerIsEnabled) {
                names.push_back(name);
            }
        }
    }
    return names;
}


vector<string> Configuration::allWorkers() const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _data.at("workers").items()) {
        string const& name = itr.key();
        names.push_back(name);
    }
    return names;
}


vector<string> Configuration::databaseFamilies() const {
    util::Lock const lock(_mtx, _context(__func__));
    vector<string> names;
    for (auto&& itr: _data.at("database_families").items()) {
        string const& name = itr.key();
        names.push_back(name);
    }
    return names;
}


bool Configuration::isKnownDatabaseFamily(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (name.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    return _data.at("database_families").count(name) != 0;
}


DatabaseFamilyInfo Configuration::databaseFamilyInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return DatabaseFamilyInfo(_databaseFamily(lock, name));
}


DatabaseFamilyInfo Configuration::addDatabaseFamily(DatabaseFamilyInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertDatabaseFamilyExists(lock, info.name, false);
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
    _data["database_families"][info.name] = info.toJson();
    return info;
}


void Configuration::deleteDatabaseFamily(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertDatabaseFamilyExists(lock, name, true);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database_family") +
                          " WHERE " + conn->sqlEqual("name", name));
        });
    }
    // In order to maintain consistency of the persistent state also delete all
    // dependent databases.
    // NOTE: if using MySQL-based persistent backend the removal of the dependent 
    //       tables from MySQL happens automatically since it's enforced by the PK/FK
    //       relationship between the corresponding tables.
    vector<string> databasesToBeRemoved;
    for (auto&& itr: _data.at("databases").items()) {
        string const& database = itr.key();
        json const& databaseJson = itr.value();
        if (databaseJson.at("family_name").get<string>() == name) {
            databasesToBeRemoved.push_back(database);
        }
    }
    for (string const& database: databasesToBeRemoved) {
        _data["databases"].erase(database);
    }
    _data["database_families"].erase(name);
}


size_t Configuration::replicationLevel(string const& family) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _databaseFamily(lock, family).at("min_replication_level").get<size_t>();
}


vector<string> Configuration::databases(
        string const& family, bool allDatabases, bool isPublished) const
{
    util::Lock const lock(_mtx, _context(__func__));
    if (!family.empty()) _assertDatabaseFamilyExists(lock, family, true);
    vector<string> names;
    for (auto&& itr: _data.at("databases").items()) {
        string const& name = itr.key();
        json const& obj = itr.value();
        if (!family.empty() && (family != obj.at("family_name").get<string>())) {
            continue;
        }
        if (!allDatabases) {
            bool const databaseIsPublished = obj.at("is_published").get<int>() != 0;
            if (isPublished != databaseIsPublished) continue;  
        }
        names.push_back(name);
    }
    return names;
}


bool Configuration::isKnownDatabase(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    if (name.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    return _data.at("databases").count(name) != 0;
}


DatabaseInfo Configuration::databaseInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return DatabaseInfo(_database(lock, name));
}


DatabaseInfo Configuration::addDatabase(DatabaseInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertDatabaseFamilyExists(lock, info.family, true);
    _assertDatabaseExists(lock, info.name, false);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            auto const isNotPublished = 0;  // the new database can't be published at this time
            conn->executeInsertQuery("config_database", info.name, info.family, isNotPublished,
                                     info.chunkIdColName, info.subChunkIdColName);
        });
    }
    _data["databases"][info.name] = info.toJson();
    return DatabaseInfo(_data.at("databases").at(info.name));
}


DatabaseInfo Configuration::publishDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    json& obj = _database(lock, name);
    if (obj.at("is_published").get<int>() != 0) {
        throw logic_error(_context(__func__) + " database '" + name +"' is already published.");
    }
    int const isPublishedFlag = 1;
    return _publishDatabase(lock, obj, name, isPublishedFlag);
}


DatabaseInfo Configuration::unPublishDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    json& obj = _database(lock, name);
    if (obj.at("is_published").get<int>() == 0) {
        throw logic_error(_context(__func__) + " database '" + name +"' is not published.");
    }
    int const isPublishedFlag = 0;
    return _publishDatabase(lock, obj, name, isPublishedFlag);
}


void Configuration::deleteDatabase(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertDatabaseExists(lock, name, true);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database") +
                          " WHERE " + conn->sqlEqual("database", name));
        });
    }
    _data["databases"].erase(name);
}


DatabaseInfo Configuration::addTable(
        string const& database, string const& table, bool isPartitioned, list<SqlColDef> const& columns,
        bool isDirectorTable, string const& directorTableKey, string const& chunkIdColName,
        string const& subChunkIdColName, string const& latitudeColName, string const& longitudeColName)
{
    util::Lock const lock(_mtx, _context(__func__));
    _validateTableParameters(lock, database, table, isPartitioned,
                             columns, isDirectorTable,
                             directorTableKey, chunkIdColName,
                             subChunkIdColName, latitudeColName,
                             longitudeColName);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->executeInsertQuery("config_database_table",
                                     database, table, isPartitioned, isDirectorTable,
                                     directorTableKey, latitudeColName, longitudeColName);
            int colPosition = 0;
            for (auto&& coldef: columns) {
                conn->executeInsertQuery("config_database_table_schema",
                                         database, table, colPosition++,  // column position
                                         coldef.name, coldef.type);
            }
            if (isPartitioned) {
                conn->executeSimpleUpdateQuery("config_database",
                                               conn->sqlEqual("database", database),
                                               make_pair("chunk_id_key", chunkIdColName),
                                               make_pair("sub_chunk_id_key", subChunkIdColName));
            }
        });
    }
    json& databaseJson = _database(lock, database);
    if (isDirectorTable) {
        databaseJson["director_table"] = table;
        databaseJson["director_key"] = directorTableKey;
        databaseJson["chunk_id_key"] = chunkIdColName;
        databaseJson["sub_chunk_id_key"] = subChunkIdColName;
    }
    auto tableJson = json::object({
        {"name", table},
        {"is_partitioned", isPartitioned ? 1 : 0}
    });
    if (isPartitioned) {
        tableJson["latitude_key"] = latitudeColName;
        tableJson["longitude_key"] = longitudeColName;
    }
    json& columnsJson = tableJson["columns"];
    columnsJson = json::array();
    for (auto&& coldef: columns) {
        columnsJson.push_back(json::object({{"name", coldef.name}, {"type", coldef.type}}));
    }
    databaseJson["tables"][table] = tableJson;
    return DatabaseInfo(_database(lock, database));
}


DatabaseInfo Configuration::deleteTable(string const& database, string const& table) {
    util::Lock const lock(_mtx, _context(__func__));
    json& databaseJson = _database(lock, database);
    _assertTableExists(lock, databaseJson, table, true);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_database_table") +
                          " WHERE " + conn->sqlEqual("database", database) +
                          " AND " + conn->sqlEqual("table", table));
        });
    }
    databaseJson["tables"].erase(table);
    if (databaseJson.at("director_table").get<string>() == table) {
        // These attributes are set for the director table only.
        databaseJson["director_table"] = "";
        databaseJson["director_key"] = "";
        databaseJson["chunk_id_key"] = "";
        databaseJson["sub_chunk_id_key"] = "";
    }
    return DatabaseInfo(databaseJson);
}


bool Configuration::isKnownWorker(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _data.at("workers").count(name) != 0;
}


WorkerInfo Configuration::workerInfo(string const& name) const {
    util::Lock const lock(_mtx, _context(__func__));
    return WorkerInfo(_worker(lock, name));
}


WorkerInfo Configuration::addWorker(WorkerInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertWorkerExists(lock, info.name, false);
    return _updateWorker(lock, info);
}


void Configuration::deleteWorker(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertWorkerExists(lock, name, true);
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->execute("DELETE FROM " + conn->sqlId("config_worker") +
                          " WHERE " + conn->sqlEqual("name", name));
        });
    }
    _data["workers"].erase(name);
}


WorkerInfo Configuration::disableWorker(string const& name) {
    util::Lock const lock(_mtx, _context(__func__));
    json& workerJson = _worker(lock, name);
    if (workerJson.at("is_enabled") == 0) {
        throw logic_error(_context(__func__) + " worker: '" + name +"' is already disabled.");
    }
    workerJson["is_enabled"] = 0;
    return _updateWorker(lock, WorkerInfo(workerJson));
}


WorkerInfo Configuration::updateWorker(WorkerInfo const& info) {
    util::Lock const lock(_mtx, _context(__func__));
    _assertWorkerExists(lock, info.name, true);
    return _updateWorker(lock, info);
}


json Configuration::toJson(bool showPassword) const {
    util::Lock const lock(_mtx, _context(__func__));
    return _toJson(lock, showPassword);
}


json Configuration::_toJson(util::Lock const& lock, bool showPassword) const {
    return _data;
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
    _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
        conn->executeSimpleUpdateQuery(
            "config",
            conn->sqlEqual("category", category) + " AND " + conn->sqlEqual("param", param),
            make_pair("value", value));
    });

}


WorkerInfo Configuration::_updateWorker(util::Lock const& lock, WorkerInfo const& info) {

    // Make sure all required fields are set in the input worker descriptor.
    // The only required fields are the name of the worker and the host name of
    // of the Replication service. If the host names for other services will be
    // found missing then the host name of the Replication service will be assumed.
    if (info.name.empty() || info.svcHost.empty()) {
       throw invalid_argument(
                _context(__func__) + " incomplete definition of the worker: '" + info.name +"'.");
    }

    // Make an updated worker descriptor with optional fields completed using
    // existing defaults for workers. This step is required to ensure user-provided
    // descriptors are complete before making any changes to the Configuration's
    // transient or persistent states.
    WorkerInfo const completeInfo = WorkerInfo(info, _data.at("worker_defaults"));

    // Make sure no port conflict exists between services of the updated worker.

    // Make sure no host/port conflicts would exist in the transient configuration after
    // adding/updating the worker. This step is not needed for the MySQL-based persistent
    // backend due to the schema-enforced uniqueness constraints.
    std::set<pair<string, uint16_t>> hostPort;
    for (auto&& itr: _data["workers"].items()) {
        string const& worker = itr.key();
        json const& obj = itr.value();
        // Skip the target worker from the test to avoid comparing it with itself.
        if (worker == completeInfo.name) continue;
        hostPort.insert(make_pair(obj.at("svc_host").get<string>(), obj.at("svc_port").get<uint16_t>()));
        hostPort.insert(make_pair(obj.at("fs_host").get<string>(), obj.at("fs_port").get<uint16_t>()));
        hostPort.insert(make_pair(obj.at("db_host").get<string>(), obj.at("db_port").get<uint16_t>()));
        hostPort.insert(make_pair(obj.at("loader_host").get<string>(), obj.at("loader_port").get<uint16_t>()));
        hostPort.insert(make_pair(obj.at("exporter_host").get<string>(), obj.at("exporter_port").get<uint16_t>()));
        hostPort.insert(make_pair(obj.at("http_loader_host").get<string>(), obj.at("http_loader_port").get<uint16_t>()));
    }
    bool const portConflictFound =
            !hostPort.insert(make_pair(completeInfo.svcHost, completeInfo.svcPort)).second ||
            !hostPort.insert(make_pair(completeInfo.fsHost, completeInfo.fsPort)).second ||
            !hostPort.insert(make_pair(completeInfo.dbHost, completeInfo.dbPort)).second ||
            !hostPort.insert(make_pair(completeInfo.loaderHost, completeInfo.loaderPort)).second ||
            !hostPort.insert(make_pair(completeInfo.exporterHost, completeInfo.exporterPort)).second ||
            !hostPort.insert(make_pair(completeInfo.httpLoaderHost, completeInfo.httpLoaderPort)).second;
    if (portConflictFound) {
        LOGS(_log, LOG_LVL_ERROR, _context() << " port/host conflict in worker: " << completeInfo);
        throw invalid_argument(
                _context(__func__) + " port conflict detected either between the updated worker: '"
                + completeInfo.name + "' and one of the existing workers or within the updated worker itself.");
    }

    // Update the persistent state.
    bool const update = _data.at("workers").count(completeInfo.name) != 0;
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
                    make_pair("db_host", completeInfo.dbHost),
                    make_pair("db_port", completeInfo.dbPort),
                    make_pair("db_user", completeInfo.dbUser),
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
                    completeInfo.dbHost,
                    completeInfo.dbPort,
                    completeInfo.dbUser,
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

    // Update the transient state.
    _data["workers"][completeInfo.name] = completeInfo.toJson();
    return completeInfo;
}


void Configuration::_assertDatabaseFamilyExists(
        util::Lock const &lock, string const& name, bool mustExist) const
{
    if (name.empty()) throw invalid_argument(_context(__func__) + " the family name is empty.");
    bool const exists = _data.at("database_families").count(name) != 0;
    if (exists != mustExist) {
        throw invalid_argument(
                _context(__func__) + " the database family: '" + name + "' " +
                string(exists ? "already exists." : " doesn't exists."));
    }
}


json const& Configuration::_databaseFamily(util::Lock const &lock, string const& name) const {
    _assertDatabaseFamilyExists(lock, name, true);
    return _data.at("database_families").at(name);
}


json& Configuration::_databaseFamily(util::Lock const &lock, string const& name) {
    _assertDatabaseFamilyExists(lock, name, true);
    return _data["database_families"][name];
}


void Configuration::_assertDatabaseExists(
        util::Lock const& lock, string const& name, bool mustExist) const
{
    if (name.empty()) throw invalid_argument(_context(__func__) + " the database name is empty.");
    bool const exists = _data.at("databases").count(name) != 0;
    if (exists != mustExist) {
        throw invalid_argument(
                _context(__func__) + " the database: '" + name + "' " +
                string(exists ? "already exists." : " doesn't exists."));
    }
}


json const& Configuration::_database(util::Lock const& lock, string const& name) const {
    _assertDatabaseExists(lock, name, true);
    return _data.at("databases").at(name);
}


json& Configuration::_database(util::Lock const& lock, string const& name) {
    _assertDatabaseExists(lock, name, true);
    return _data["databases"][name];
}


DatabaseInfo Configuration::_publishDatabase(
        util::Lock const& lock, json& obj, string const& name, int isPublishedFlag)
{
    if (_connectionPtr != nullptr) {
        _connectionPtr->executeInOwnTransaction([&](decltype(_connectionPtr) conn) {
            conn->executeSimpleUpdateQuery(
                "config_database",
                conn->sqlEqual("database", name),
                make_pair("is_published", isPublishedFlag));
        });
    }
    obj["is_published"] = isPublishedFlag;
    return DatabaseInfo(obj);
}


void Configuration::_assertTableExists(
        util::Lock const& lock, json const& databaseJson, string const& table, bool mustExist) const
{
    if (table.empty()) throw invalid_argument(_context(__func__) + " the table name is empty");
    string const database = databaseJson["database"].get<string>();
    bool const exists = databaseJson.at("tables").count(table) != 0;
    if (exists != mustExist) {
        throw invalid_argument(
                _context(__func__) + " the table: '" + table + "' "
                + string(exists ? "already exists" : " doesn't exists")
                + " in the database: '" + database + "'.");
    }
}


void Configuration::_validateTableParameters(
        util::Lock const& lock, string const& database, string const& table, bool isPartitioned,
        list<SqlColDef> const& columns, bool isDirectorTable, string const& directorTableKey,
        string const& chunkIdColName, string const& subChunkIdColName, string const& latitudeColName,
        string const& longitudeColName) const
{
    json const& databaseJson = _database(lock, database);
    _assertTableExists(lock, databaseJson, table, false);

    // Validate flags and column names
    if (isPartitioned) {
        if (isDirectorTable) {
            string const directorTable = databaseJson.count("director_table") ?
                    databaseJson.at("director_table").get<string>() : string();
            if (!directorTable.empty()) {
                throw invalid_argument(
                        _context(__func__) + " another table '" + directorTable +
                        "' was already claimed as the 'director' table.");
            }
            if (directorTableKey.empty()) {
                throw invalid_argument(
                        _context(__func__) + " a valid column name must be provided"
                        " for the 'director' table");
            }
            if (!columnInSchema(directorTableKey, columns)) {
                throw invalid_argument(
                        _context(__func__) + " a value of parameter 'directorTableKey'"
                        " provided for the 'director' table '" + table + "' doesn't match any column"
                        " in the table schema");
            }
            if (!latitudeColName.empty()) {
                if (!columnInSchema(latitudeColName, columns)) {
                    throw invalid_argument(
                            _context(__func__) + " a value '" + latitudeColName + "' of parameter 'latitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");
                }
            }
            if (!longitudeColName.empty()) {
                if (!columnInSchema(longitudeColName, columns)) {
                    throw invalid_argument(
                            _context(__func__) + " a value '" + longitudeColName + "' of parameter 'longitudeColName'"
                            " provided for the partitioned table '" + table + "' doesn't match any column"
                            " in the table schema");
                }
            }
        }
        map<string,string> const colDefs = {
            {"chunkIdColName",    chunkIdColName},
            {"subChunkIdColName", subChunkIdColName}
        };
        for (auto&& entry: colDefs) {
            string const& role = entry.first;
            string const& colName = entry.second;
            if (colName.empty()) {
                throw invalid_argument(
                        _context(__func__) + " a valid column name must be provided"
                        " for the '" + role + "' parameter of the partitioned table");
            }
            if (!columnInSchema(colName, columns)) {
                throw invalid_argument(
                        _context(__func__) + " no matching column found in the provided"
                        " schema for name '" + colName + " as required by parameter '" + role +
                        "' of the partitioned table: '" + table + "'");
            }
        }
    } else {
        if (isDirectorTable) {
            throw invalid_argument(_context(__func__) + " regular tables can't be the 'director' ones");
        }
    }
}


void Configuration::_assertWorkerExists(
        util::Lock const& lock, string const& name, bool mustExist) const
{
    if (name.empty()) throw invalid_argument(_context(__func__) + " the worker name is empty.");
    bool const exists = _data.at("workers").count(name) != 0;
    if (exists != mustExist) {
        throw invalid_argument(
                _context(__func__) + " the worker: '" + name + "' " +
                string(exists ? "already exists." : " doesn't exists."));
    }
}


json const& Configuration::_worker(util::Lock const& lock, string const& name) const {
    _assertWorkerExists(lock, name, true);
    return _data.at("workers").at(name);
}


json& Configuration::_worker(util::Lock const& lock, string const& name) {
    _assertWorkerExists(lock, name, true);
    return _data["workers"][name];
}

}}} // namespace lsst::qserv::replica
