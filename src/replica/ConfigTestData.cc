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
#include "replica/ConfigTestData.h"

using namespace std;
using json = nlohmann::json;

namespace lsst {
namespace qserv {
namespace replica {

map<string, set<string>> ConfigTestData::parameters() {
    return map<string, set<string>>({
        {   "common", 
            {   "request_buf_size_bytes",
                "request_retry_interval_sec"
            }
        },
        {   "controller", 
            {   "num_threads",
                "http_server_threads",
                "http_server_port",
                "http_max_listen_conn",
                "request_timeout_sec",
                "job_timeout_sec",
                "job_heartbeat_sec",
                "empty_chunks_dir",
                "worker_evict_priority_level",
                "health_monitor_priority_level",
                "ingest_priority_level",
                "catalog_management_priority_level"
            }
        },
        {   "database",
            {   "services_pool_size",
                "host",
                "port",
                "user",
                "password",
                "name",
                "qserv_master_user",
                "qserv_master_services_pool_size",
                "qserv_master_tmp_dir"
            }
        },
        {   "xrootd", 
            {   "auto_notify",
                "request_timeout_sec",
                "host",
                "port"
            }
        },
        {   "worker", 
            {   "technology",
                "num_svc_processing_threads",
                "num_fs_processing_threads",
                "fs_buf_size_bytes",
                "num_loader_processing_threads",
                "num_exporter_processing_threads",
                "num_http_loader_processing_threads",
                "num_async_loader_processing_threads",
                "async_loader_auto_resume",
                "async_loader_cleanup_on_resume",
                "http_max_listen_conn"
            }
        },
        {   "worker_defaults", 
            {   "svc_port",
                "fs_port",
                "data_dir",
                "loader_port",
                "loader_tmp_dir",
                "exporter_port",
                "exporter_tmp_dir",
                "http_loader_port",
                "http_loader_tmp_dir"
            }
        }
    });
}


json ConfigTestData::data() {
    json obj;
    json& generalObj = obj["general"];
    generalObj["common"] = json::object({
        {"request_buf_size_bytes", 8192},
        {"request_retry_interval_sec", 1}
    });
    generalObj["controller"] = json::object({
        {"num_threads", 2},
        {"http_server_port", 8080},
        {"http_max_listen_conn", 256},
        {"http_server_threads", 3},
        {"request_timeout_sec", 100},
        {"job_timeout_sec", 200},
        {"job_heartbeat_sec", 300},
        {"empty_chunks_dir", "/qserv/data/qserv"},
        {"worker_evict_priority_level", 1},
        {"health_monitor_priority_level", 2},
        {"ingest_priority_level", 3},
        {"catalog_management_priority_level", 4}
    });
    generalObj["database"] = json::object({
        {"host", "localhost"},
        {"port", 13306},
        {"user", "qsreplica"},
        {"password", "changeme"},
        {"name", "qservReplica"},
        {"qserv_master_user", "qsmaster"},
        {"services_pool_size", 2},
        {"qserv_master_tmp_dir", "/qserv/data/ingest"}
    });
    generalObj["xrootd"] = json::object({
        {"auto_notify", 0},
        {"host", "localhost"},
        {"port", 1104},
        {"request_timeout_sec", 400}
    });
    generalObj["worker"] = json::object({
        {"technology", "POSIX"},
        {"num_svc_processing_threads", 4},
        {"num_fs_processing_threads", 5},
        {"fs_buf_size_bytes", 1024},
        {"num_loader_processing_threads", 6},
        {"num_exporter_processing_threads", 7},
        {"num_http_loader_processing_threads", 8},
        {"num_async_loader_processing_threads", 9},
        {"async_loader_auto_resume", 0},
        {"async_loader_cleanup_on_resume", 0},
        {"http_max_listen_conn", 512}
    });
    generalObj["worker_defaults"] = json::object({
        {"svc_port", 51000},
        {"fs_port", 52000},
        {"data_dir", "/data"},
        {"loader_port", 53000},
        {"loader_tmp_dir", "/tmp"},
        {"exporter_port", 54000},
        {"exporter_tmp_dir", "/tmp"},
        {"http_loader_port", 55000},
        {"http_loader_tmp_dir", "/tmp"}
    });

    // A configuration of tis worker is complete as it has all required
    // parameters.
    obj["workers"]["worker-A"] = json::object({
        {"name", "worker-A"},
        {"is_enabled", 1},
        {"is_read_only", 0},
        {"svc_host", "host-A"},
        {"svc_port", 51001},
        {"fs_host", "host-A"},
        {"fs_port", 52001},
        {"data_dir", "/data/A"},
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
    obj["workers"]["worker-B"] = json::object({
        {"name", "worker-B"},
        {"is_enabled", 1},
        {"is_read_only", 1},
        {"svc_host", "host-B"},
        {"fs_host", "host-B"},
        {"data_dir", "/data/B"},
        {"loader_host", "host-B"},
        {"exporter_host", "host-B"},
        {"http_loader_host", "host-B"}
    });

    // This configuration is incomplete. An assumption is that the corresponding
    // defaults will be loaded when the Configuration class will be processing
    // this definition.
    obj["workers"]["worker-C"] = json::object({
        {"name", "worker-C"},
        {"is_enabled", 0},
        {"is_read_only", 0},
        {"svc_host", "host-C"},
        {"fs_host", "host-C"},
        {"loader_host", "host-C"},
        {"exporter_host", "host-C"},
        {"http_loader_host", "host-C"}
    });
    obj["database_families"]["production"] = json::object({
        {"name", "production"},
        {"min_replication_level", 10},
        {"num_stripes", 11},
        {"num_sub_stripes", 12},
        {"overlap", 0.01667}
    });
    obj["database_families"]["test"] = json::object({
        {"name", "test"},
        {"min_replication_level", 13},
        {"num_stripes", 14},
        {"num_sub_stripes", 15},
        {"overlap", 0.001}
    });
    obj["databases"]["db1"] = json::object({
        {"database", "db1"},
        {"family_name", "production"},
        {"is_published", 1},
        {"tables", {
            {"Table11", {
                {"name", "Table11"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key", "id11"},
                {"latitude_key", "decl11"},
                {"longitude_key", "ra11"},
                {"columns", {}}
            }},
            {"MetaTable11", {
                {"name", "MetaTable11"},
                {"is_partitioned", 0}
            }}
        }}
    });
    obj["databases"]["db2"] = json::object({
        {"database", "db2"},
        {"family_name", "production"},
        {"is_published", 1},
        {"tables", {
            {"Table21", {
                {"name", "Table21"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key", "id21"},
                {"latitude_key", "decl21"},
                {"longitude_key", "ra21"},
                {"columns", {}}
            }},
            {"Table22", {
                {"name", "Table22"},
                {"is_partitioned", 1},
                {"director", "Table21"},
                {"director_key", "id22"},
                {"latitude_key", "decl22"},
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
    obj["databases"]["db3"] = json::object({
        {"database", "db3"},
        {"family_name", "production"},
        {"is_published", 1},
        {"tables", {
            {"Table31", {
                {"name", "Table31"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key", "id31"},
                {"latitude_key", "decl31"},
                {"longitude_key", "ra31"},
                {"columns", {}}
            }},
            {"Table32", {
                {"name", "Table32"},
                {"is_partitioned", 1},
                {"director", "Table31"},
                {"director_key", "id32"},
                {"latitude_key", "decl32"},
                {"longitude_key", "ra32"},
                {"columns", {}}
            }},
            {"Table33", {
                {"name", "Table33"},
                {"is_partitioned", 1},
                {"director", "Table31"},
                {"director_key", "id33"},
                {"latitude_key", ""},
                {"longitude_key", ""},
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
    obj["databases"]["db4"] = json::object({
        {"database", "db4"},
        {"family_name", "test"},
        {"is_published", 1},
        {"tables", {
            {"Table41", {
                {"name", "Table41"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key",  "id41"},
                {"latitude_key",  "decl41"},
                {"longitude_key", "ra41"},
                {"columns", {}}
            }},
            {"Table42", {
                {"name", "Table42"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key",  "id42"},
                {"latitude_key",  "decl42"},
                {"longitude_key", "ra42"},
                {"columns", {}}
            }}
        }}
    });
    obj["databases"]["db5"] = json::object({
        {"database", "db5"},
        {"family_name", "test"},
        {"is_published", 1},
        {"tables", {
            {"Table51", {
                {"name", "Table51"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key",  "id51"},
                {"latitude_key",  "decl51"},
                {"longitude_key", "ra51"},
                {"columns", {}}
            }}
        }}
    });
    obj["databases"]["db6"] = json::object({
        {"database", "db6"},
        {"family_name", "test"},
        {"is_published", 0},
        {"tables", {
            {"Table61", {
                {"name", "Table61"},
                {"is_partitioned", 1},
                {"director", ""},
                {"director_key",  "id61"},
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
    return obj;
}

}}} // namespace lsst::qserv::replica
