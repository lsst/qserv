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

// Third-party headers
#include "boost/asio.hpp"

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

/**
 * The schema definition is nested dictionary in which the top-level key reprsents
 * the so called "categories" of parameters. Each entry under a category defines
 * a single parameter. Values of these parameters are obtained and modified
 * using the Configuration API methods 'get<T>` and 'set<T>`.
 *
 * All parameters have two mandatory attributes:
 *  - The attribute "description" contains the documentation string explaining the attribute
 *  - The attribute "default" holds the default value of the attribute. The value's type depends
 *    on the attribute's role, and once it's defined here it's enforced through the rest of
 *    the implementation. For instance, the type can't be changed via the method 'Configuration::set<T>'.
 *
 * Some parameters are also allowed to have the optional attributes:
 *   - The attribute "read-only" set to 1 would indicate that the parameter's state
 *     can't be changed via method 'Configuration::set<T>'.
 *   - The attribute "empty-allowed" set to 1 would relax parameter value's validation
 *     by method 'Configuration::set<T>' to allow 0 for numeric types and the empty string
 *     fr strings.
 *   - The attribute "security-context" if set to 1 would indicate to the API user that
 *     the parameter has some the security-sensitive context (passwords, authorization keys,
 *     etc.). Parameters possesing this attribute are supposed to be used with care by
 *     the dependent automation tools to avoid exposing sensitive information in log files,
 *     reports, etc.
 */
json const ConfigurationSchema::_schemaJson = json::object({
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
        {"http_max_listen_conn", {
            {"description",
                "The maximum length of the queue of pending connections sent to the controller's HTTP server."
                " Must be greater than 0."},
            {"default", boost::asio::socket_base::max_listen_connections}
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
        {"qserv_master_user", {
            {"description",
                "The MySQL user account of a service where Qserv 'czar' maintains its persistent state."},
            {"default", "qsmaster"}
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
            {"restricted", {
                {"type", "set"},
                {"values", json::array({"FS", "POSIX", "TEST"})}
            }},
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
        }},
        {"http_max_listen_conn", {
            {"description",
                "The maximum length of the queue of pending connections sent to the Replication worker's"
                " HTTP-based ingest service. Must be greater than 0."},
            {"default", boost::asio::socket_base::max_listen_connections}
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


json ConfigurationSchema::defaultConfigData() {
    json result = json::object();
    vector<string> const generalCategories =
            {"common", "controller", "database", "xrootd", "worker", "worker_defaults"};
    for (string const& category: generalCategories) {
        json const& inCategoryJson = _schemaJson.at(category);
        json& outCategoryJson = result[category];
        for (auto&& itr: inCategoryJson.items()) {
            string const& param = itr.key();
            outCategoryJson[param] = itr.value().at("default");
        }
    }
    return result;
}


map<string, set<string>> ConfigurationSchema::parameters() {
    map<string, set<string>> result;
    json const data = defaultConfigData();
    for (auto&& categoryItr: data.items()) {
        string const& category = categoryItr.key();
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


bool ConfigurationSchema::_emptyAllowed(string const& category, string const& param) {
    return _attributeValue<unsigned int>(_schemaJson, category, param, "empty-allowed", 0) != 0;
}


json ConfigurationSchema::_restrictor(string const& category, string const& param) {
    return _attributeValue<json>(_schemaJson, category, param, "restricted", json());
}

}}} // namespace lsst::qserv::replica
