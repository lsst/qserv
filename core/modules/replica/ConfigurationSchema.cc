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

// Qserv headers
#include "global/constants.h"
#include "replica/Common.h"

using namespace std;
using json = nlohmann::json;

namespace {
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
}

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


string ConfigurationSchema::json2string(string const& context, json const& obj) {
    if (obj.is_string()) return obj.get<string>();
    if (obj.is_boolean()) return obj.get<bool>() ? "1" : "0";
    if (obj.is_number_unsigned()) return to_string(obj.get<uint64_t>());
    if (obj.is_number_integer()) return to_string(obj.get<int64_t>());
    if (obj.is_number_float()) return to_string(obj.get<double>());
    throw invalid_argument(context + "unsupported data type of the value: " + obj.dump());
}

}}} // namespace lsst::qserv::replica
