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
#include "replica/ConfigurationSchema.h"

// System headers
#include <algorithm>
#include <stdexcept>

// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/ConfigurationExceptions.h"
#include "replica/DatabaseMySQL.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.ConfigurationSchema");

/**
 * Generate a properly quoted string for an identifier using a valid MySQL connection
 * (if provided) or an explicitly specified quotation string.
 */
string sqlId(string id,
             std::shared_ptr<database::mysql::Connection> const& conn,
             string quote) {
    if (conn == nullptr) return quote + id + quote;
    return conn->sqlId(id);
}

/**
 * Generate a properly quoted string for an identifier using a valid MySQL connection
 * (if provided) or an explicitly specified quotation string.
 */
string sqlValue(string val,
                std::shared_ptr<database::mysql::Connection> const& conn,
                string quote) {
    if (conn == nullptr) return quote + val + quote;
    return conn->sqlValue(val);
}

/// @return Table definition in the schema.
json const& tableDef(string const& context, json const& schema, string const& table) {
    for (auto&& tableJson: schema) {
        if (table == tableJson.at("table").get<string>()) return tableJson;
    }
    throw invalid_argument(context + "no definition exists for the table '" + table + "'.");
}

/// @return Column definition in the schema.
json const& columnDef(string const& context, json const& schema, string const& table, string const& column) {
    for (json const& columnJson: tableDef(context, schema, table).at("columns")) {
        if (column == columnJson.at("COLUMN_NAME").get<string>()) return columnJson;
    }
    throw invalid_argument(
            context + "no definition exists for the column '" + column + "' in the table '" + table + "'.");
}

/**
 * @return A collection of column names found in the information schema for
 *   for the specified database and table. The collection is ordered by
 *   the ordinal positions of columns in the table.
 */
vector<string> tableColumns(database::mysql::Connection::Ptr const& conn, string const& database,
                            string const& table) {
    vector<string> columns;
    string const columnName = "COLUMN_NAME";
    conn->executeInOwnTransaction([&](decltype(conn) conn) {
        columns = conn->executeAllValuesSelect<string>(
            "SELECT "    + conn->sqlId(columnName) +
            " FROM "     + conn->sqlId("information_schema", "COLUMNS") +
            " WHERE "    + conn->sqlEqual("TABLE_SCHEMA", database) +
            " AND  "     + conn->sqlEqual("TABLE_NAME", table) +
            " ORDER BY " + conn->sqlId("ORDINAL_POSITION"),
            columnName
        );
    });
    return columns;
}

/**
 * @return 'true' if the specified column exists in the table definition.
 */
bool tableColumnExists(database::mysql::Connection::Ptr const& conn, string const& database,
                       string const& table, string const& column) {
    vector<string> const columns = tableColumns(conn, database, table);
    return columns.cend() != find(columns.cbegin(), columns.cend(), column);
}

template <typename T>
T _attributeValue(json const& schemaJson, string const& category, string const& param,
                  string const& attr, T const& defaultValue) {
    if (schemaJson.count(category)) {
        json const& categoryJson = schemaJson.at(category);
        if (categoryJson.count(param)) {
            json const& paramJson = categoryJson.at(param);
            if (paramJson.count(attr)) return paramJson.at(attr).get<T>();
        }
    }
    return defaultValue;
}
} // namespace

namespace lsst {
namespace qserv {
namespace replica {

int const ConfigurationSchema::version = 1;

/**
 * The schema definition is nested dictionary in which the top-level key reprsents
 * the so called "categories" of parameters.
 *
 * There are two kinds of the categories:
 *  - The "general" categories hosting single value parameters. Values of these attributes
 *    are obtained and modified using the Configuration API's methods 'get<T>` and 'set<T>`.
 *  - The "group" categories, where each such category is an array of objects mapping to
 *    C++ classes representing the corresponding high-level abstractions in the Configuration
 *    API, such 'WorkerInfo', 'DatabaseFamilyInfo', and 'DatabaseInfo'.
 *
 * All parameters of the "general" categories have two mandatory attributes:
 *  - The attribute "description" contains the documentation string explaining the attribute
 *  - The attribute "default" holds the default value of the attribute. The value's type depends
 *    on the attribute's role, and once it's defined here it's enforced through the rest of
 *    the implementation. For instance, the type can't be changed via the method 'Configuration::set<T>'.
 *
 * Some general parameters are also allowed to have the optional attributes:
 *   - The attribute "read-only" set to 1 would indicate that the parameter's state
 *     can't be changed via method 'Configuration::set<T>'.
 *   - The attribute "empty-allowed" set to 1 would relax parameter value's validation
 *     by method 'Configuration::set<T>' to allow 0 for numeric types and the empty string
 *     fr strings.
 *   - The attribute "security-context" if set to 1 would indicate to the API user that
 *     the paameter has some the security-sensitive context (passwords, authorization keys,
 *     etc.). Parameters possesing this attribute are supposed to be used with care by
 *     the dependent automation tools to avoid exposing sensitive information in log files,
 *     reports, etc.
 */
json const ConfigurationSchema::_schemaJson = json::object({
    {"meta", {
        {"version", {
            {"description",
                "The current version of the configuration. Must be greater than 0."},
            {"read-only", 1},
            {"default", ConfigurationSchema::version}
        }}
    }},
    {"common", {
        {"request_buf_size_bytes", {
            {"description",
                "The default buffer size for network communications. Must be greater than 0."},
            {"default", 131072}
        }},
        {"request_retry_interval_sec", {
            {"description",
                "The default retry timeout for network communications. Must be greater than 0."},
            {"default", 1}
        }}
    }},
    {"controller", {
        {"num_threads", {
            {"description",
                "The number of threads managed by BOOST ASIO. Must be greater than 0."},
            {"default", 2}
        }},
        {"request_timeout_sec", {
            {"description",
                "The default timeout for completing worker requests. Must be greater than 0."},
            {"default", 600}
        }},
        {"job_timeout_sec", {
            {"description",
                "The default timeout for completing jobs. Must be greater than 0."},
            {"default", 600}
        }},
        {"job_heartbeat_sec", {
            {"description",
                "The heartbeat interval for jobs. A value of 0 disables heartbeats."},
            {"empty-allowed", 1},
            {"default", 0}
        }},
        {"http_server_threads", {
            {"description",
                "The number of threads managed by BOOST ASIO for the HTTP server. Must be greater than 0."},
            {"default", 2}
        }},
        {"http_server_port", {
            {"description",
                "The port number for the controller's HTTP server. Must be greater than 0."},
            {"default", 25081}
        }},
        {"empty_chunks_dir", {
            {"description",
                "A path to a folder where Qserv master stores its empty chunk lists. Must be non-empty."},
            {"default", "/qserv/data/qserv"}
        }}
    }},
    {"database", {
        {"services_pool_size", {
            {"description", "The pool size at the client database services connector."},
            {"default", 2}
        }},
        {"host", {
            {"description",
                "The host name of the MySQL server where the Replication system maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service as it's"
                " set up at the startup time of the Replication/Ingest system."},
            {"read-only", 1},
            {"default", "localhost"}
        }},
        {"port", {
            {"description",
                "The port number of the MySQL server where the Replication maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service as it's"
                " set up at the startup time of the Replication/Ingest system."},
            {"read-only", 1},
            {"default", 3306}
        }},
        {"user", {
            {"description",
                "The MySQL user account of a service where the Replication system maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service as it's"
                " set up at the startup time of the Replication/Ingest system."},
            {"read-only", 1},
            {"default", "qsreplica"}
        }},
        {"password", {
            {"description",
                "A password for the MySQL account where the Replication system maintains its persistent state"},
            {"read-only", 1},
            {"security-context", 1},
            {"empty-allowed", 1},
            {"default", ""}
        }},
        {"name", {
            {"description",
                "The name of a MySQL database for a service where the Replication system maintains its"
                " persistent state. Note that this parameter can't be updated through the Configuration"
                "  service as it's set up at the startup time of the Replication/Ingest system."},
            {"read-only", 1},
            {"default", "qservReplica"}
        }},
        {"qserv_master_services_pool_size", {
            {"description",
                "The pool size at the client database services connector for the Qserv Master database."},
            {"default", 2}
        }},
        {"qserv_master_host", {
            {"description",
                "The host name of the MySQL server where Qserv 'czar' maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service."},
            {"default", "localhost"}
        }},
        {"qserv_master_port", {
            {"description",
                "The port number of the MySQL server where Qserv 'czar' maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service."},
            {"default", 3306}
        }},
        {"qserv_master_user", {
            {"description",
                "The MySQL user account of a service where Qserv 'czar' maintains its persistent state."
                " Note that this parameter can't be updated through the Configuration service."},
            {"default", "qsmaster"}
        }},
        {"qserv_master_name", {
            {"description",
                "The name of the default MySQL database of a service where Qserv 'czar' maintains its"
                " persistent state. Note that this parameter can't be updated through the Configuration service."},
            {"default", lsst::qserv::SEC_INDEX_DB}
        }},
        {"qserv_master_tmp_dir", {
            {"description",
                "The temporary folder for exchanging data with the Qserv 'czar' database service."},
            {"default", "/qserv/data/ingest"}
        }}
    }},
    {"xrootd", {
        {"auto_notify", {
            {"description",
                "Automatically notify Qserv on changes in replica disposition."},
            {"empty-allowed", 1},
            {"default", 1}
        }},
        {"request_timeout_sec", {
            {"description",
                "The default timeout for communications with Qserv over XRootD/SSI."},
            {"default", 180}
        }},
        {"host", {
            {"description",
                "The service location (the host name or an IP address) of XRootD/SSI for"
                " communications with Qserv."},
            {"default", "localhost"}
        }},
        {"port", {
            {"description",
                "A port number for the XRootD/SSI service needed for communications with Qserv."},
            {"default", 1094}
        }}
    }},
    {"worker", {
        {"technology", {
            {"description",
                "The name of a technology for implementing replica management requests at workers."},
            {"default", "FS"}
        }},
        {"num_svc_processing_threads", {
            {"description",
                "The number of request processing threads in each Replication worker service."},
            {"default", 2}
        }},
        {"num_fs_processing_threads", {
            {"description",
                "The number of request processing threads in each Replication worker's file service."},
            {"default", 2}
        }},
        {"fs_buf_size_bytes", {
            {"description",
                "The default buffer size for file and network operations at Replication worker's file service."},
            {"default", 4194304}
        }},
        {"num_loader_processing_threads", {
            {"description",
                "The number of request processing threads in each Replication worker's ingest service."},
            {"default", 2}
        }},
        {"num_exporter_processing_threads", {
            {"description",
                "The number of request processing threads in each Replication worker's data exporting service."},
            {"default", 2}
        }},
        {"num_http_loader_processing_threads", {
            {"description",
                "The number of request processing threads in each Replication worker's HTTP-based ingest service."},
            {"default", 2}
        }}
    }},
    {"worker_defaults", {
        {"svc_port", {
            {"description",
                "The default port for the worker's replication service."},
            {"default", 25000}
        }},
        {"fs_port", {
            {"description",
                "The default port for the worker's file service."},
            {"default", 25001}
        }},
        {"data_dir", {
            {"description",
                "The default data directory from which the worker file service serves files"
                " to other workers. This folder is required to be the location where the MySQL"
                " service of Qserv worker stores its data."},
            {"default", "/qserv/data/mysql"}
        }},
        {"db_port", {
            {"description",
                "The default port of the MySQL service of the adjustent Qserv worker. Certain"
                " operations conducted by the Replication/Ingest require this."},
            {"default", 3306}
        }},
        {"db_user", {
            {"description",
                "The default MySQL user account of the adjacent Qserv worker. Certain"
                " operations conducted by the Replication/Ingest require this."},
            {"default", "root"}
        }},
        {"loader_port", {
            {"description",
                "The default port for the worker's binary file ingest service."},
            {"default", 25002}
        }},
        {"loader_tmp_dir", {
            {"description",
                "The default location for temporary files stored by the worker's binary"
                " file ingest service before ingesting them into the adjacent Qserv worker's"
                " MySQL database."},
            {"default", "/qserv/data/ingest"}
        }},
        {"exporter_port", {
            {"description",
                "The default port for the worker's table export service."},
            {"default", 25003}
        }},
        {"exporter_tmp_dir", {
            {"description",
                "The default location for temporary files stored by the worker's table"
                " export service before returning them a client."},
            {"default", "/qserv/data/export"}
        }},
        {"http_loader_port", {
            {"description",
                "The default port for the worker's HTTP-based REST service for ingesting table"
                " contributions into the adjacent Qserv worker's MySQL database."},
            {"default", 25004}
        }},
        {"http_loader_tmp_dir", {
            {"description",
                "The default location for temporary files stored by the worker's"
                " HTTP-based REST service ingesting table before ingesting them into"
                " the adjacent Qserv worker's MySQL database."},
            {"default", "/qserv/data/ingest"}
        }}
    }},
    {"workers", {
    }},
    {"databases", {
    }},
    {"database_families", {
    }}
});


string ConfigurationSchema::description(string const& category, string const& param) {
    return _attributeValue<string>(_schemaJson, category, param, "description", "");
}


bool ConfigurationSchema::readOnly(std::string const& category, std::string const& param) {
    return _attributeValue<unsigned int>(_schemaJson, category, param, "read-only", 0) != 0;
}


bool ConfigurationSchema::securityContext(std::string const& category, std::string const& param) {
    return _attributeValue<unsigned int>(_schemaJson, category, param, "security-context", 0) != 0;
}


bool ConfigurationSchema::emptyAllowed(string const& category, string const& param) {
    return _attributeValue<unsigned int>(_schemaJson, category, param, "empty-allowed", 0) != 0;
}


json ConfigurationSchema::defaultConfigData() {
    json result = json::object();
    vector<string> const generalCategories =
            {"meta", "common", "controller", "database", "xrootd", "worker", "worker_defaults"};
    for (string const& category: generalCategories) {
        json const& inCategoryJson = _schemaJson.at(category);
        json& outCategoryJson = result[category];
        for (auto&& itr: inCategoryJson.items()) {
            string const& param = itr.key();
            outCategoryJson[param] = itr.value().at("default");
        }
    }
    vector<string> const groupCategories = {"workers", "databases", "database_families"};
    for (string const& category: groupCategories) {
        result[category] = json::object();
    }
    return result;
}


json ConfigurationSchema::testConfigData() {
    json configJson;
    configJson["meta"]["version"] = ConfigurationSchema::version;
    configJson["common"] = json::object({
        {"request_buf_size_bytes", 8192},
        {"request_retry_interval_sec", 1}
    });
    configJson["controller"] = json::object({
        {"num_threads", 2},
        {"http_server_port", 8080},
        {"http_server_threads", 3},
        {"request_timeout_sec", 100},
        {"job_timeout_sec", 200},
        {"job_heartbeat_sec", 300},
        {"empty_chunks_dir", "/qserv/data/qserv"}
    });
    configJson["database"] = json::object({
        {"host", "localhost"},
        {"port", 13306},
        {"user", "qsreplica"},
        {"password", "changeme"},
        {"name", "qservReplica"},
        {"qserv_master_host", "localhost"},
        {"qserv_master_port", 3306},
        {"qserv_master_user", "qsmaster"},
        {"qserv_master_name", "qservMeta"},
        {"services_pool_size", 2},
        {"qserv_master_tmp_dir", "/qserv/data/ingest"}
    });
    configJson["xrootd"] = json::object({
        {"auto_notify", 0},
        {"host", "localhost"},
        {"port", 1104},
        {"request_timeout_sec", 400}
    });
    configJson["worker"] = json::object({
        {"technology", "POSIX"},
        {"num_svc_processing_threads", 4},
        {"num_fs_processing_threads", 5},
        {"fs_buf_size_bytes", 1024},
        {"num_loader_processing_threads", 6},
        {"num_exporter_processing_threads", 7},
        {"num_http_loader_processing_threads", 8}
    });
    configJson["worker_defaults"] = json::object({
        {"svc_port", 51000},
        {"fs_port", 52000},
        {"data_dir", "/data"},
        {"db_port", 3306},
        {"db_user", "root"},
        {"loader_port", 53000},
        {"loader_tmp_dir", "/tmp"},
        {"exporter_port", 54000},
        {"exporter_tmp_dir", "/tmp"},
        {"http_loader_port", 55000},
        {"http_loader_tmp_dir", "/tmp"}
    });

    // A configuration of tis worker is complete as it has all required
    // parameters.
    configJson["workers"]["worker-A"] = json::object({
        {"name", "worker-A"},
        {"is_enabled", 1},
        {"is_read_only", 0},
        {"svc_host", "host-A"},
        {"svc_port", 51001},
        {"fs_host", "host-A"},
        {"fs_port", 52001},
        {"data_dir", "/data/A"},
        {"db_host", "host-A"},
        {"db_port", 53306},
        {"db_user", "qsmaster"},
        {"loader_host", "host-A"},
        {"loader_port", 53002},
        {"loader_tmp_dir", "/tmp/A"},
        {"exporter_host", "host-A"},
        {"exporter_port", 53003},
        {"exporter_tmp_dir", "/tmp/export/A"},
        {"http_loader_host", "host-A"},
        {"http_loader_port", 53004},
        {"http_loader_tmp_dir", "/tmp/http/A"}
    });

    // This configuration is incomplete. An assumption is that the corresponding
    // defaults will be loaded when the Configuration class will be processing
    // this definition.
    configJson["workers"]["worker-B"] = json::object({
        {"name", "worker-B"},
        {"is_enabled", 1},
        {"is_read_only", 1},
        {"svc_host", "host-B"},
        {"fs_host", "host-B"},
        {"data_dir", "/data/B"},
        {"db_host", "host-B"},
        {"loader_host", "host-B"},
        {"exporter_host", "host-B"},
        {"http_loader_host", "host-B"}
    });

    // This configuration is incomplete. An assumption is that the corresponding
    // defaults will be loaded when the Configuration class will be processing
    // this definition.
    configJson["workers"]["worker-C"] = json::object({
        {"name", "worker-C"},
        {"is_enabled", 0},
        {"is_read_only", 0},
        {"svc_host", "host-C"},
        {"fs_host", "host-C"},
        {"db_host", "host-C"},
        {"loader_host", "host-C"},
        {"exporter_host", "host-C"},
        {"http_loader_host", "host-C"}
    });
    configJson["database_families"]["production"] = json::object({
        {"name", "production"},
        {"min_replication_level", 10},
        {"num_stripes", 11},
        {"num_sub_stripes", 12},
        {"overlap", 0.01667}
    });
    configJson["database_families"]["test"] = json::object({
        {"name", "test"},
        {"min_replication_level", 13},
        {"num_stripes", 14},
        {"num_sub_stripes", 15},
        {"overlap", 0.001}
    });
    configJson["databases"]["db1"] = json::object({
        {"database", "db1"},
        {"family_name", "production"},
        {"is_published", 1},
        {"director_table", "Table11"},
        {"director_key", "id1"},
        {"chunk_id_key", "chunkId1"},
        {"sub_chunk_id_key", "subChunkId1"},
        {"tables", {
            {"Table11", {
                {"name", "Table11"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl11"},
                {"longitude_key", "ra11"},
                {"columns", {}}
            }},
            {"MetaTable11", {
                {"name", "MetaTable11"},
                {"is_partitioned", 0}
            }}
        }}
    });
    configJson["databases"]["db2"] = json::object({
        {"database", "db2"},
        {"family_name", "production"},
        {"is_published", 1},
        {"director_table", "Table21"},
        {"director_key", "id2"},
        {"chunk_id_key", "chunkId2"},
        {"sub_chunk_id_key", "subChunkId2"},
        {"tables", {
            {"Table21", {
                {"name", "Table21"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl21"},
                {"longitude_key", "ra21"},
                {"columns", {}}
            }},
            {"Table22", {
                {"name", "Table22"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl22"},
                {"longitude_key", "ra22"},
                {"columns", {}}
            }},
            {"MetaTable21", {
                {"name", "MetaTable21"},
                {"is_partitioned", 0}
            }},
            {"MetaTable22", {
                {"name", "MetaTable22"},
                {"is_partitioned", 0}
            }}
        }}
    });
    configJson["databases"]["db3"] = json::object({
        {"database", "db3"},
        {"family_name", "production"},
        {"is_published", 1},
        {"director_table", "Table31"},
        {"director_key", "id3"},
        {"chunk_id_key", "chunkId3"},
        {"sub_chunk_id_key", "subChunkId3"},
        {"tables", {
            {"Table31", {
                {"name", "Table31"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl31"},
                {"longitude_key", "ra31"},
                {"columns", {}}
            }},
            {"Table32", {
                {"name", "Table32"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl32"},
                {"longitude_key", "ra32"},
                {"columns", {}}
            }},
            {"Table33", {
                {"name", "Table33"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl33"},
                {"longitude_key", "ra33"},
                {"columns", {}}
            }},
            {"MetaTable31", {
                {"name", "MetaTable31"},
                {"is_partitioned", 0}
            }},
            {"MetaTable32", {
                {"name", "MetaTable32"},
                {"is_partitioned", 0}
            }},
            {"MetaTable33", {
                {"name", "MetaTable33"},
                {"is_partitioned", 0}
            }}
        }}
    });
    configJson["databases"]["db4"] = json::object({
        {"database", "db4"},
        {"family_name", "test"},
        {"is_published", 1},
        {"director_table", "Table41"},
        {"director_key", "id4"},
        {"chunk_id_key", "chunkId4"},
        {"sub_chunk_id_key", "subChunkId4"},
        {"tables", {
            {"Table41", {
                {"name", "Table41"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl41"},
                {"longitude_key", "ra41"},
                {"columns", {}}
            }},
            {"Table42", {
                {"name", "Table42"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl42"},
                {"longitude_key", "ra42"},
                {"columns", {}}
            }}
        }}
    });
    configJson["databases"]["db5"] = json::object({
        {"database", "db5"},
        {"family_name", "test"},
        {"is_published", 1},
        {"director_table", "Table51"},
        {"director_key", "id5"},
        {"chunk_id_key", "chunkId5"},
        {"sub_chunk_id_key", "subChunkId5"},
        {"tables", {
            {"Table51", {
                {"name", "Table51"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl51"},
                {"longitude_key", "decl51"},
                {"columns", {}}
            }}
        }}
    });
    configJson["databases"]["db6"] = json::object({
        {"database", "db6"},
        {"family_name", "test"},
        {"is_published", 0},
        {"director_table", "Table61"},
        {"director_key", "id6"},
        {"chunk_id_key", "chunkId6"},
        {"sub_chunk_id_key", "subChunkId6"},
        {"tables", {
            {"Table61", {
                {"name", "Table61"},
                {"is_partitioned", 1},
                {"latitude_key",  "decl61"},
                {"longitude_key", "ra61"},
                {"columns", {}}
            }},
            {"MetaTable61", {
                {"name", "MetaTable61"},
                {"is_partitioned", 0}
            }}
        }}
    });
    return configJson;
}


json const ConfigurationSchema::_schemaMySQL = R"""([
{   "table": "config",
    "columns":[
        {"COLUMN_NAME": "category", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "param",    "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "value",    "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["category", "param"]]
    ],
    "constraints":[
    ]
},
{   "table": "config_worker",
    "columns":[
        {"COLUMN_NAME": "name",                 "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_enabled",           "COLUMN_TYPE": "tinyint(1)",           "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_read_only",         "COLUMN_TYPE": "tinyint(1)",           "IS_NULLABLE": 0},
        {"COLUMN_NAME": "svc_host",             "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "svc_port",             "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "fs_host",              "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "fs_port",              "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "data_dir",             "COLUMN_TYPE": "varchar(255)",         "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "db_host",              "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "db_port",              "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "db_user",              "COLUMN_TYPE": "varchar(255)",         "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "loader_host",          "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "loader_port",          "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "loader_tmp_dir",       "COLUMN_TYPE": "varchar(255)",         "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "exporter_host",        "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "exporter_port",        "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "exporter_tmp_dir",     "COLUMN_TYPE": "varchar(255)",         "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "http_loader_host",     "COLUMN_TYPE": "varchar(255)",         "IS_NULLABLE": 0},
        {"COLUMN_NAME": "http_loader_port",     "COLUMN_TYPE": "smallint(5) unsigned", "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "http_loader_tmp_dir",  "COLUMN_TYPE": "varchar(255)",         "COLUMN_DEFAULT": "NULL"}
    ],
    "keys":[
        ["PRIMARY KEY", ["name"]],
        ["UNIQUE KEY", ["svc_host",         "svc_port"]],
        ["UNIQUE KEY", ["fs_host",          "fs_port"]],
        ["UNIQUE KEY", ["db_host",          "db_port"]],
        ["UNIQUE KEY", ["loader_host",      "loader_port"]],
        ["UNIQUE KEY", ["exporter_host",    "exporter_port"]],
        ["UNIQUE KEY", ["http_loader_host", "http_loader_port"]]
    ],
    "constraints":[
    ]
},
{   "table": "config_worker_ext",
    "columns":[
        {"COLUMN_NAME": "worker_name", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "param",       "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "value",       "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["worker_name", "param"]]
    ],
    "constraints":[
        {"FK": ["worker_name"], "REF": ["config_worker", ["name"]]}
    ]
},
{   "table": "config_database_family",
    "columns":[
        {"COLUMN_NAME": "name",                  "COLUMN_TYPE": "varchar(255)",     "IS_NULLABLE": 0},
        {"COLUMN_NAME": "min_replication_level", "COLUMN_TYPE": "int(10) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "num_stripes",           "COLUMN_TYPE": "int(10) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "num_sub_stripes",       "COLUMN_TYPE": "int(10) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "overlap",               "COLUMN_TYPE": "double",           "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["name"]]
    ],
    "constraints":[]
},
{   "table": "config_database",
    "columns":[
        {"COLUMN_NAME": "database",         "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "family_name",      "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_published",     "COLUMN_TYPE": "tinyint(1)",   "COLUMN_DEFAULT": "1"},
        {"COLUMN_NAME": "chunk_id_key",     "COLUMN_TYPE": "varchar(255)", "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "sub_chunk_id_key", "COLUMN_TYPE": "varchar(255)", "COLUMN_DEFAULT": "''"}
    ],
    "keys":[
        ["PRIMARY KEY", ["database"]],
        ["UNIQUE KEY",  ["database", "family_name"]]
    ],
    "constraints":[
        {"FK": ["family_name"], "REF": ["config_database_family", ["name"]]}
    ]
},
{   "table": "config_database_table",
    "columns":[
        {"COLUMN_NAME": "database",       "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "table",          "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_partitioned", "COLUMN_TYPE": "tinyint(1)",   "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_director",    "COLUMN_TYPE": "tinyint(1)",   "IS_NULLABLE": 0},
        {"COLUMN_NAME": "director_key",   "COLUMN_TYPE": "varchar(255)", "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "latitude_key",   "COLUMN_TYPE": "varchar(255)", "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "longitude_key",  "COLUMN_TYPE": "varchar(255)", "COLUMN_DEFAULT": "''"}
    ],
    "keys":[
        ["PRIMARY KEY", ["database", "table"]]
    ],
    "constraints":[
        {"FK": ["database"], "REF": ["config_database", ["database"]]}
    ]
},
{   "table": "config_database_table_schema",
    "columns":[
        {"COLUMN_NAME": "database",     "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "table",        "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "col_position", "COLUMN_TYPE": "int(11)",      "IS_NULLABLE": 0},
        {"COLUMN_NAME": "col_name",     "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "col_type",     "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["UNIQUE KEY", ["database", "table", "col_position"]],
        ["UNIQUE KEY", ["database", "table", "col_name"]]
    ],
    "constraints":[
        {"FK": ["database", "table"], "REF": ["config_database_table", ["database", "table"]]}
    ]
},
{   "table": "controller",
    "columns":[
        {"COLUMN_NAME": "id",         "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "hostname",   "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "pid",        "COLUMN_TYPE": "int(11)",             "IS_NULLABLE": 0},
        {"COLUMN_NAME": "start_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]]
    ],
    "constraints":[
    ]
},
{   "table": "job",
    "columns":[
        {"COLUMN_NAME": "id",             "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "controller_id",  "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "parent_job_id",  "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "type",           "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "state",          "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "ext_state",      "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "begin_time",     "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "end_time",       "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "heartbeat_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "priority",       "COLUMN_TYPE": "int(11)",             "IS_NULLABLE": 0},
        {"COLUMN_NAME": "exclusive",      "COLUMN_TYPE": "tinyint(1)",          "IS_NULLABLE": 0},
        {"COLUMN_NAME": "preemptable",    "COLUMN_TYPE": "tinyint(1)",          "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]]
    ],
    "constraints":[
        {"FK": ["controller_id"], "REF": ["controller", ["id"]]},
        {"FK": ["parent_job_id"], "REF": ["job",        ["id"]]}
    ]
},
{   "table": "job_ext",
    "columns":[
        {"COLUMN_NAME": "job_id", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "param",  "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "value",  "COLUMN_TYPE": "longblob",     "IS_NULLABLE": 0}
    ],
    "keys":[
        ["KEY", ["job_id"]],
        ["KEY", ["job_id", "param"]]
    ],
    "constraints":[
        {"FK": ["job_id"], "REF": ["job", ["id"]]}
    ]
},
{   "table": "request",
    "columns":[
        {"COLUMN_NAME": "id",             "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "job_id",         "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "name",           "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "worker",         "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "priority",       "COLUMN_TYPE": "int(11)",             "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "state",          "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "ext_state",      "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "server_status",  "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "''"},
        {"COLUMN_NAME": "c_create_time",  "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "c_start_time",   "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "w_receive_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "w_start_time",   "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "w_finish_time",  "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "c_finish_time",  "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]]
    ],
    "constraints":[
        {"FK": ["job_id"], "REF": ["job", ["id"]]}
    ]
},
{   "table": "request_ext",
    "columns":[
        {"COLUMN_NAME": "request_id", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "param",      "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "value",      "COLUMN_TYPE": "longblob",     "IS_NULLABLE": 0}
    ],
    "keys":[
        ["KEY", ["request_id"]],
        ["KEY", ["request_id", "param"]]
    ],
    "constraints":[
        {"FK": ["request_id"], "REF": ["request", ["id"]]}
    ]
},
{   "table": "replica",
    "columns":[
        {"COLUMN_NAME": "id",          "COLUMN_TYPE": "int(11)",             "IS_NULLABLE": 0, "AUTO_INCREMENT": 1},
        {"COLUMN_NAME": "worker",      "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "database",    "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "chunk",       "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0},
        {"COLUMN_NAME": "verify_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]],
        ["KEY",         ["worker", "database"]],
        ["UNIQUE KEY",  ["worker", "database", "chunk"]]
    ],
    "constraints":[
        {"FK": ["worker"],   "REF": ["config_worker",   ["name"]]},
        {"FK": ["database"], "REF": ["config_database", ["database"]]}
    ]
},
{   "table": "replica_file",
    "columns":[
        {"COLUMN_NAME": "replica_id",        "COLUMN_TYPE": "int(11)",             "IS_NULLABLE": 0},
        {"COLUMN_NAME": "name",              "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "size",              "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "mtime",             "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0},
        {"COLUMN_NAME": "cs",                "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "begin_create_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "end_create_time",   "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["replica_id", "name"]]
    ],
    "constraints":[
        {"FK": ["replica_id"], "REF": ["replica", ["id"]]}
    ]
},
{   "table": "controller_log",
    "columns":[
        {"COLUMN_NAME": "id",            "COLUMN_TYPE": "int(11)",             "IS_NULLABLE": 0, "AUTO_INCREMENT": 1},
        {"COLUMN_NAME": "controller_id", "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "time",          "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "task",          "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "operation",     "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "status",        "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "request_id",    "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "NULL"},
        {"COLUMN_NAME": "job_id",        "COLUMN_TYPE": "varchar(255)",        "COLUMN_DEFAULT": "NULL"}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]]
    ],
    "constraints":[
        {"FK": ["controller_id"], "REF": ["controller", ["id"]]},
        {"FK": ["request_id"],    "REF": ["request",    ["id"]]},
        {"FK": ["job_id"],        "REF": ["job",        ["id"]]}
    ]
},
{   "table": "controller_log_ext",
    "columns":[
        {"COLUMN_NAME": "controller_log_id", "COLUMN_TYPE": "int(11)",      "IS_NULLABLE": 0},
        {"COLUMN_NAME": "key",               "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "val",               "COLUMN_TYPE": "text",         "IS_NULLABLE": 0}
    ],
    "keys":[
    ],
    "constraints":[
        {"FK": ["controller_log_id"], "REF": ["controller_log", ["id"]]}
    ]
},
{   "table": "transaction",
    "columns":[
        {"COLUMN_NAME": "id",         "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0, "AUTO_INCREMENT": 1},
        {"COLUMN_NAME": "database",   "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "state",      "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "begin_time", "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "end_time",   "COLUMN_TYPE": "bigint(20) unsigned", "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "context",    "COLUMN_TYPE": "mediumblob",          "COLUMN_DEFAULT": "''"}
    ],
    "keys":[
        ["UNIQUE KEY",  ["id"]],
        ["PRIMARY KEY", ["id", "database"]]
    ],
    "constraints":[
        {"FK": ["database"], "REF": ["config_database", ["database"]]}
    ]
},
{   "table": "transaction_contrib",
    "columns":[
        {"COLUMN_NAME": "id",             "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0, "AUTO_INCREMENT": 1},
        {"COLUMN_NAME": "transaction_id", "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0},
        {"COLUMN_NAME": "worker",         "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "database",       "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "table",          "COLUMN_TYPE": "varchar(255)",        "IS_NULLABLE": 0},
        {"COLUMN_NAME": "chunk",          "COLUMN_TYPE": "int(10) unsigned",    "IS_NULLABLE": 0},
        {"COLUMN_NAME": "is_overlap",     "COLUMN_TYPE": "tinyint(1)",          "IS_NULLABLE": 0},
        {"COLUMN_NAME": "url",            "COLUMN_TYPE": "text",                "IS_NULLABLE": 0},
        {"COLUMN_NAME": "begin_time",     "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0, "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "end_time",       "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0, "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "num_bytes",      "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0, "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "num_rows",       "COLUMN_TYPE": "bigint(20) unsigned", "IS_NULLABLE": 0, "COLUMN_DEFAULT": "0"},
        {"COLUMN_NAME": "success",        "COLUMN_TYPE": "tinyint(1)",          "IS_NULLABLE": 0, "COLUMN_DEFAULT": "0"}
    ],
    "keys":[
        ["PRIMARY KEY", ["id"]]
    ],
    "constraints":[
        {"FK": ["transaction_id"],    "REF": ["transaction",           ["id"]]},
        {"FK": ["worker"],            "REF": ["config_worker",         ["name"]]},
        {"FK": ["database", "table"], "REF": ["config_database_table", ["database", "table"]]}
    ]
},
{   "table": "database_ingest",
    "columns":[
        {"COLUMN_NAME": "database", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "category", "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "param",    "COLUMN_TYPE": "varchar(255)", "IS_NULLABLE": 0},
        {"COLUMN_NAME": "value",    "COLUMN_TYPE": "text",         "IS_NULLABLE": 0}
    ],
    "keys":[
        ["PRIMARY KEY", ["database", "category", "param"]]
    ],
    "constraints":[
        {"FK": ["database"], "REF": ["config_database", ["database"]]}
    ]
}
])"""_json;


map<string, set<string>> ConfigurationSchema::parameters() {
    set<string> const groupCategories = {"workers", "database_families", "databases"};
    map<string, set<string>> result;
    json const data = defaultConfigData();
    for (auto&& categoryItr: data.items()) {
        string const& category = categoryItr.key();
        if (groupCategories.count(category) != 0) continue;
        for (auto&& parameterItr: data.at(category).items()) {
            string const& parameter = parameterItr.key();
            result[category].insert(parameter);
        }
    }
    return result;
}


Configuration::Ptr ConfigurationSchema::create(std::string const& configUrl, bool reset) {
    string const context = "ConfigurationSchema::" + string(__func__) + "  ";

    // Parse URL using standard defaults for the current version of the configuration.
    // After that update the database info in the configuration to match values of
    // the parameters that were parsed in the connection string.
    json data = defaultConfigData();
    database::mysql::ConnectionParams connectionParams;
    try {
        connectionParams = database::mysql::ConnectionParams::parse(
            configUrl,
            data.at("database").at("host").get<string>(),
            data.at("database").at("port").get<uint16_t>(),
            data.at("database").at("user").get<string>(),
            data.at("database").at("password").get<string>()
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_DEBUG, context << "failed to parse configUrl='" << configUrl
                << "', ex: " << ex.what());
        throw;
    }
    data["database"]["host"] = connectionParams.host;
    data["database"]["port"] = connectionParams.port;
    data["database"]["user"] = connectionParams.user;
    data["database"]["password"] = connectionParams.password;
    data["database"]["name"] = connectionParams.database;
 
    // Now try connecting to a database. If it won't throw an exception then check if
    // the database is empty, and if not then address the issue depeniding on a value
    // of flag 'reset' passed into the method.
    {
        // NOTE: using the RAII style connection management handler that will take care
        //       of aborting transactions in case of any errors.
        database::mysql::ConnectionHandler h;
        try {
            h.conn = database::mysql::Connection::open(connectionParams);
            // Check if the database is empty, so that it could be populated with
            // the current schema.
            vector<string> tables;
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                tables = conn->executeAllValuesSelect<string>(
                    "SELECT " + conn->sqlId("TABLE_NAME") +
                    " FROM " + conn->sqlId("information_schema", "TABLES") +
                    " WHERE " + conn->sqlEqual("TABLE_SCHEMA", connectionParams.database),
                    "TABLE_NAME");
            });
            if (!tables.empty()) {
                if (reset) {
                    LOGS(_log, LOG_LVL_WARN, context << "the database open via configUrl="
                            << connectionParams.toString() << " is not empty. Deleting the database "
                            << "and populating it with the current schema.");
                    // IMPORTANT: using 'DROP DATABASE' instead of 'DROP TABLE' for each table to
                    // avoid problems that may arise during the operation due to possible table
                    // dependencies (FK->PK relationships, triggers, etc.).
                    h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                        conn->execute("DROP DATABASE " + conn->sqlId(connectionParams.database));
                    });
                } else {
                    throw runtime_error(
                            context + "the database open via configUrl=" + connectionParams.toString()
                            + " is not empty.");
                }
            }
        } catch (database::mysql::ER_BAD_DB_ERROR_ const& ex) {
            // That's what we need - the database doesn't exists yet.
            ;
        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "database operation failed, ex: " << ex.what());
            throw;
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "a failure occurred during opening or resetting a database,"
                    << " configUrl=" << connectionParams.toString() << ", ex: " << ex.what());
            throw;
        }
    }

    // Create the database.
    {
        database::mysql::ConnectionHandler h;
        try {
            // Make a connection w/o assuming any default database. This will prevent
            // the MySQL API from throwing an exception on non-existing database. 
            database::mysql::ConnectionParams connectionParamsNoDatabase = connectionParams;
            connectionParamsNoDatabase.database = string();
            h.conn = database::mysql::Connection::open(connectionParamsNoDatabase);
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                conn->execute("CREATE DATABASE " + conn->sqlId(connectionParams.database));
            });
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "a failure occurred during creating a database,"
                    << " configUrl=" << connectionParams.toString() << ", ex: " << ex.what());
            throw;
        }
    }

    // At this point the database is guaranteed to exist and be empty of any schema.
    // Make a fresh connection to the database and proceed with populating the database
    // with the current schema.
    {
        database::mysql::ConnectionHandler h;
        try {
            h.conn = database::mysql::Connection::open(connectionParams);
            // NOTE: creating tables in the same order they're defined in the current schema.
            // This is needed to satisfy dependency requirements between teh tables.
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                for (auto&& tableJson: _schemaMySQL) {
                    string const sql = _tableCreateStatement(tableJson, conn);
                    LOGS(_log, LOG_LVL_DEBUG, context << "table creation statement: " << sql);
                    conn->execute(sql);
                }
                for (auto&& sql: _defaultConfigStatements(conn)) {
                    LOGS(_log, LOG_LVL_DEBUG, context << "default config statement: " << sql);
                    conn->execute(sql);
                }            
            });
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "failed to connect to the database via configUrl="
                    << connectionParams.toString() << " or poulate it with the current schema"
                    << " and the default configuration, ex: " << ex.what());
            throw;
        }
    }
    return Configuration::load(configUrl);
}


void ConfigurationSchema::upgrade(string const& configUrl) {
    string const context = "ConfigurationSchema::" + string(__func__) + "  ";

    // Parse URL using standard defaults for the current version of the configuration.
    json const data = defaultConfigData();
    database::mysql::ConnectionParams connectionParams;
    try {
        connectionParams = database::mysql::ConnectionParams::parse(
            configUrl,
            data.at("database").at("host").get<string>(),
            data.at("database").at("port").get<uint16_t>(),
            data.at("database").at("user").get<string>(),
            data.at("database").at("password").get<string>()
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_DEBUG, context << "failed to parse configUrl='" << configUrl
                << "', ex: " << ex.what());
        throw;
    }

    // Establish a new database connection (RAII-style connection handler)
    database::mysql::ConnectionHandler h;
    try {
        h.conn = database::mysql::Connection::open(connectionParams);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed to connect to the database server,"
                << " configUrl='" << connectionParams.toString() << "', ex: " << ex.what());
        throw;
    }

    // Check schema version if any exists.
    int detectedVersion = 0;
    try {
        h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
            string const colname = "value";
            bool const found = conn->executeSingleValueSelect(
                    "SELECT   " + conn->sqlId(colname) + " FROM " + conn->sqlId("config")
                    + " WHERE " + conn->sqlEqual("category", "meta")
                    + " AND   " + conn->sqlEqual("param", "version"),
                    colname, detectedVersion);
            if (!found) {
                detectedVersion = 0;
            }
        });
    } catch (database::mysql::EmptyResultSetError const&) {
        detectedVersion = 0;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed to read the schema version from the database, configUrl='"
                << connectionParams.toString() << "', ex: " << ex.what());
        throw;
    }
    if (detectedVersion >= version) {
        throw runtime_error(
                "schema version " + to_string(detectedVersion) + " found in the database is not strictly"
                + " lesser than " + to_string(version) + " required by the current implementation, configUrl='"
                + connectionParams.toString() + "'");
    }

    // Go over a list of required tables and create the missing ones (if any).
    // For existing tables check for the missing columns in the tables that were found.
    // Create those as needed.
    // NOTE: that the tables are traversed in the reversed dependency order that takes into
    //       account the FK/PK relationships between the tables. 
    for (auto&& tableJson: _schemaMySQL) {
        string const table = tableJson.at("table").get<string>();
        bool found = false;
        try {
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                string const columnName = "TABLE_NAME";
                string value;
                found = conn->executeSingleValueSelect(
                        "SELECT " + conn->sqlId(columnName) +
                        " FROM  " + conn->sqlId("information_schema", "TABLES") +
                        " WHERE " + conn->sqlEqual("TABLE_SCHEMA", connectionParams.database) +
                        "   AND " + conn->sqlEqual(columnName, table),
                        columnName, value);
            });
        } catch (database::mysql::EmptyResultSetError const&) {
            ;
        } catch (exception const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "failed to locate table info in the information schema,"
                    << " configUrl='" << connectionParams.toString() << "', ex: " << ex.what());
            throw;
        }
        if (!found) {
            // Create the missing table.
            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                conn->execute(_tableCreateStatement(tableJson, conn));
            });
            // Populate general configuration parameters if the table hosting those parameters
            // was missing.
            if (table == "config") {
                for (auto&& sql: _defaultConfigStatements(h.conn)) {
                    h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                        conn->execute(sql);
                    });
                }
            }
        } else {
            // Table-specific corrections for upgrading from no-version 0 to the current
            // version 1.
            if (table == "config") {
 
                // Add missing parameters of the general configuration groups if needed.
                set<string> const groupCategories = {"workers", "databases", "database_families"};
                for (auto&& categoryItr: data.items()) {
                    string const& category = categoryItr.key();

                    // Skip the group categories.
                    if (groupCategories.count(category) != 0) continue;

                    json const& categoryJson = categoryItr.value();
                    for (auto&& paramItr: categoryJson.items()) {
                        string const& param = paramItr.key();

                        // The read-only parameters aren't suppoosed to be stored in the database.
                        if (readOnly(category, param)) continue;

                        // Check if the parameter is in the database.
                        bool found = false;
                        try {
                            h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                                string const columnName = "value";
                                string value;
                                found = conn->executeSingleValueSelect(
                                        "SELECT " + conn->sqlId(columnName) + " FROM  " + conn->sqlId(table) +
                                        " WHERE " + conn->sqlEqual("category", category) +
                                        "   AND " + conn->sqlEqual("param", param),
                                        columnName, value);
                            });
                        } catch (database::mysql::EmptyResultSetError const&) {
                            ;
                        } catch (exception const& ex) {
                            LOGS(_log, LOG_LVL_ERROR, context << "failed to locate a parameter info in the table '"
                                    << table << "', configUrl='" << connectionParams.toString()
                                    << "', ex: " << ex.what());
                            throw;
                        }
                        if (!found) {
                            // Add the missing parameter.
                            string const valueStr = json2string(context, paramItr.value());
                            try {
                                h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                                    conn->executeInsertQuery(table, category, param, valueStr);
                                });
                            } catch (exception const& ex) {
                                LOGS(_log, LOG_LVL_ERROR, context << "failed to insert the default configuration"
                                        << " parameter (" << category << "," << param << ") into the table '" << table
                                        << "', configUrl='" << connectionParams.toString() << "', ex: " << ex.what());
                                throw;
                            }
                        }
                    }
                }
            } else if (table == "transaction") {
                // Adding the column that may or may not be present in the pre-version 1 schemas
                // of the table. Hence, the first step is to check if the colum is indeed not present
                // in the schema.
                string const column = "context";
                found = false;
                try {
                    found = tableColumnExists(h.conn, connectionParams.database, table, column);
                } catch (exception const& ex) {
                    LOGS(_log, LOG_LVL_ERROR, context << "failed to locate the column '" << column
                            << "' of the table '" << table << "' in the information schema, configUrl='"
                            << connectionParams.toString() << "', ex: " << ex.what());
                    throw;
                }
                if (!found) {
                    try {
                        json const& columnJson = columnDef(context, _schemaMySQL, table, column);
                        string const columnType = columnJson.at("COLUMN_TYPE").get<string>();
                        bool const isNotNullable =
                                (columnJson.count("IS_NULLABLE") != 0) &&
                                (columnJson.at("IS_NULLABLE").get<unsigned int>() != 0);
                        string const defaultValueSpec = columnJson.count("COLUMN_DEFAULT") != 0 ?
                                "DEFAULT " + columnJson.at("COLUMN_DEFAULT").get<string>() : string();
                        h.conn->executeInOwnTransaction([&](decltype(h.conn) conn) {
                            conn->execute("ALTER TABLE " + conn->sqlId(table) +
                                          " ADD COLUMN " + conn->sqlId(column) + " " + columnType +
                                          (isNotNullable ? " NOT NULL" : "") +
                                          " " + defaultValueSpec);
                        });
                    } catch (exception const& ex) {
                        LOGS(_log, LOG_LVL_ERROR, context << "failed to add the column '" << column
                                << "' to the table '" << table << "', configUrl='" << connectionParams.toString()
                                << "', ex: " << ex.what());
                        throw;
                    }
                }
            }
        }
    }

    // And finally, make sure the schema version is properly updated.
    // Note that the algorithm still works for cases when the metadata entry already
    // exists in the table.
    {
        string const table = "config";
        string const category = "meta";
        string const param = "version";
        h.conn->executeInsertOrUpdate(
            [&](decltype(h.conn) conn) {
                conn->executeInsertQuery(
                        table,
                        category,
                        param,
                        version
                );
            },
            [&](decltype(h.conn) conn) {
                conn->executeSimpleUpdateQuery(
                        table,
                        conn->sqlEqual("category", category) + " AND " + conn->sqlEqual("param", param),
                        make_pair("value", version)
                );
            }
        );
    }
}


vector<string> ConfigurationSchema::schema(bool includeInitStatements,
                                           string const& idQuote,
                                           string const& valueQuote) {
    vector<string> result;
    for (auto&& tableJson: _schemaMySQL) {
        result.push_back(_tableCreateStatement(tableJson, nullptr, idQuote));
    }
    if (includeInitStatements) {
        for (auto&& sql: _defaultConfigStatements(nullptr, idQuote, valueQuote)) {
            result.push_back(sql);
        }
    }
    return result;
}


string ConfigurationSchema::json2string(string const& context, json const& obj) {
    if (obj.is_string()) return obj.get<string>();
    if (obj.is_boolean()) return obj.get<bool>() ? "1" : "0";
    if (obj.is_number_unsigned()) return to_string(obj.get<uint64_t>());
    if (obj.is_number_integer()) return to_string(obj.get<int64_t>());
    if (obj.is_number_float()) return to_string(obj.get<double>());
    throw invalid_argument(context + "unsupported data type of the value: " + obj.dump());
}


string ConfigurationSchema::_tableCreateStatement(json const& tableJson,
                                                  std::shared_ptr<database::mysql::Connection> const& conn,
                                                  string const& idQuote)
{
    string sql;
    for (auto&& columnJson: tableJson.at("columns")) {
        string const columnName = columnJson.at("COLUMN_NAME").get<string>();
        string const columnType = columnJson.at("COLUMN_TYPE").get<string>();
        bool const autoIncrement =
            (columnJson.count("AUTO_INCREMENT") != 0) &&
            (columnJson.at("AUTO_INCREMENT").get<unsigned int>() != 0);
        bool const isNotNullable =
                (columnJson.count("IS_NULLABLE") != 0) &&
                (columnJson.at("IS_NULLABLE").get<unsigned int>() != 0);
        string const columnDefault = columnJson.count("COLUMN_DEFAULT") != 0 ?
                columnJson.at("COLUMN_DEFAULT").get<string>() : string();

        if (!sql.empty()) sql += ", ";
        sql += sqlId(columnName, conn, idQuote) + " " + columnType;
        if (isNotNullable) sql += " NOT NULL";
        if (autoIncrement) sql += " AUTO_INCREMENT";
        if (!columnDefault.empty()) sql += " DEFAULT " + columnDefault;
    }
    for (auto&& keyJson: tableJson.at("keys")) {
        string const keyType = keyJson.at(0).get<string>();
        json const& keyColumnsJson = keyJson.at(1);
        string keyColumns;
        for (auto&& colJson: keyColumnsJson) {
            if (!keyColumns.empty()) keyColumns += ", ";
            keyColumns += sqlId(colJson.get<string>(), conn, idQuote);
        }
        sql += ", " + keyType + " (" + keyColumns + ")";
    }
    string constraints;
    for (auto&& constraintsJson: tableJson.at("constraints")) {
        string fk;
        for (auto&& colJson: constraintsJson.at("FK")) {
            if (!fk.empty()) fk += ",";
            fk += sqlId(colJson.get<string>(), conn, idQuote);
        }
        string ref;
        for (auto&& colJson: constraintsJson.at("REF").at(1)) {
            if (!ref.empty()) ref += ", ";
            ref += sqlId(colJson.get<string>(), conn, idQuote);
        }
        sql += ", FOREIGN KEY (" + fk + ") REFERENCES "
                + sqlId(constraintsJson.at("REF").at(0).get<string>(), conn, idQuote)
                + " (" + ref + ") ON DELETE CASCADE ON UPDATE CASCADE";
    }
    return "CREATE TABLE " + sqlId(tableJson.at("table").get<string>(), conn, idQuote)
            + " (" + sql + ") ENGINE=InnoDB";
}


vector<string> ConfigurationSchema::_defaultConfigStatements(
        std::shared_ptr<database::mysql::Connection> const& conn,
        string const& idQuote,
        string const& valueQuote)
{
    vector<string> result;
    json const data = defaultConfigData();
    vector<string> const categories = {"meta", "common", "controller", "database", "xrootd", "worker", "worker_defaults"};
    for (auto&& category: categories) {
        for (auto&& itr: data.at(category).items()) {
            string const& param = itr.key();
            json const& valueJson = itr.value();
            string const context = "ConfigurationSchema::" + string(__func__) + "['" + category + "','" + param + "']  ";
            string const sql = "INSERT INTO " + sqlId("config", conn, idQuote) +  " VALUES ("
                    + sqlValue(category, conn, valueQuote) + ","
                    + sqlValue(param, conn, valueQuote) + ","
                    + sqlValue(json2string(context, valueJson), conn, valueQuote) + ")";
            result.push_back(sql);
        }
    }
    return result;
}

}}} // namespace lsst::qserv::replica
