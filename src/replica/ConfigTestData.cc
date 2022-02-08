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
            {   "request-buf-size-bytes",
                "request-retry-interval-sec"
            }
        },
        {   "redirector",
            {   "host",
                "port",
                "max-listen-conn",
                "threads",
                "heartbeat-ival-sec"
            }
        },
        {   "controller", 
            {   "num-threads",
                "http-server-port",
                "http-max-listen-conn",
                "http-server-threads",
                "request-timeout-sec",
                "job-timeout-sec",
                "job-heartbeat-sec",
                "empty-chunks-dir",
                "worker-evict-priority-level",
                "health-monitor-priority-level",
                "ingest-priority-level",
                "catalog-management-priority-level",
                "auto-register-workers"
            }
        },
        {   "database",
            {   "services-pool-size",
                "host",
                "port",
                "user",
                "password",
                "name",
                "qserv-master-user",
                "qserv-master-services-pool-size",
                "qserv-master-tmp-dir"
            }
        },
        {   "xrootd", 
            {   "auto-notify",
                "request-timeout-sec",
                "host",
                "port",
                "allow-reconnect",
                "reconnect-timeout"
            }
        },
        {   "worker", 
            {   "technology",
                "num-svc-processing-threads",
                "num-fs-processing-threads",
                "fs-buf-size-bytes",
                "num-loader-processing-threads",
                "num-exporter-processing-threads",
                "num-http-loader-processing-threads",
                "num-async-loader-processing-threads",
                "async-loader-auto-resume",
                "async-loader-cleanup-on-resume",
                "http-max-listen-conn",
                "svc-port",
                "fs-port",
                "data-dir",
                "loader-port",
                "loader-tmp-dir",
                "exporter-port",
                "exporter-tmp-dir",
                "http-loader-port",
                "http-loader-tmp-dir"
            }
        }
    });
}


json ConfigTestData::data() {
    json obj;
    json& generalObj = obj["general"];
    generalObj["common"] = json::object({
        {"request-buf-size-bytes", 8192},
        {"request-retry-interval-sec", 1}
    });
    generalObj["redirector"] = json::object({
        {"host", "127.0.0.1"},
        {"port", 8081},
        {"max-listen-conn", 512},
        {"threads", 4},
        {"heartbeat-ival-sec", 10}
    });
    generalObj["controller"] = json::object({
        {"num-threads", 2},
        {"http-server-port", 8080},
        {"http-max-listen-conn", 256},
        {"http-server-threads", 3},
        {"request-timeout-sec", 100},
        {"job-timeout-sec", 200},
        {"job-heartbeat-sec", 300},
        {"empty-chunks-dir", "/qserv/data/qserv"},
        {"worker-evict-priority-level", 1},
        {"health-monitor-priority-level", 2},
        {"ingest-priority-level", 3},
        {"catalog-management-priority-level", 4},
        {"auto-register-workers", 1}
    });
    generalObj["database"] = json::object({
        {"host", "localhost"},
        {"port", 13306},
        {"user", "qsreplica"},
        {"password", "changeme"},
        {"name", "qservReplica"},
        {"qserv-master-user", "qsmaster"},
        {"services-pool-size", 2},
        {"qserv-master-tmp-dir", "/qserv/data/ingest"}
    });
    generalObj["xrootd"] = json::object({
        {"auto-notify", 0},
        {"host", "localhost"},
        {"port", 1104},
        {"request-timeout-sec", 400},
        {"allow-reconnect", 0},
        {"reconnect-timeout", 500}
    });
    generalObj["worker"] = json::object({
        {"technology", "POSIX"},
        {"num-svc-processing-threads", 4},
        {"num-fs-processing-threads", 5},
        {"fs-buf-size-bytes", 1024},
        {"num-loader-processing-threads", 6},
        {"num-exporter-processing-threads", 7},
        {"num-http-loader-processing-threads", 8},
        {"num-async-loader-processing-threads", 9},
        {"async-loader-auto-resume", 0},
        {"async-loader-cleanup-on-resume", 0},
        {"http-max-listen-conn", 512},
        {"svc-port", 51000},
        {"fs-port", 52000},
        {"data-dir", "/data"},
        {"loader-port", 53000},
        {"loader-tmp-dir", "/tmp"},
        {"exporter-port", 54000},
        {"exporter-tmp-dir", "/tmp"},
        {"http-loader-port", 55000},
        {"http-loader-tmp-dir", "/tmp"}
    });

    // A configuration of this worker is complete as it has all required
    // parameters.
    obj["workers"]["worker-A"] = json::object({
        {"name", "worker-A"},
        {"is-enabled", 1},
        {"is-read-only", 0},
        {"svc-host", "host-A"},
        {"svc-port", 51001},
        {"fs-host", "host-A"},
        {"fs-port", 52001},
        {"data-dir", "/data/A"},
        {"loader-host", "host-A"},
        {"loader-port", 53002},
        {"loader-tmp-dir", "/tmp/A"},
        {"exporter_host", "host-A"},
        {"exporter-port", 53003},
        {"exporter-tmp-dir", "/tmp/export/A"},
        {"http-loader-host", "host-A"},
        {"http-loader-port", 53004},
        {"http-loader-tmp-dir", "/tmp/http/A"}
    });

    // This configuration is incomplete. An assumption is that the corresponding
    // defaults will be loaded when the Configuration class will be processing
    // this definition.
    obj["workers"]["worker-B"] = json::object({
        {"name", "worker-B"},
        {"is-enabled", 1},
        {"is-read-only", 1},
        {"svc-host", "host-B"},
        {"fs-host", "host-B"},
        {"data-dir", "/data/B"},
        {"loader-host", "host-B"},
        {"exporter_host", "host-B"},
        {"http-loader-host", "host-B"}
    });

    // This configuration is incomplete. An assumption is that the corresponding
    // defaults will be loaded when the Configuration class will be processing
    // this definition.
    obj["workers"]["worker-C"] = json::object({
        {"name", "worker-C"},
        {"is-enabled", 0},
        {"is-read-only", 0},
        {"svc-host", "host-C"},
        {"fs-host", "host-C"},
        {"loader-host", "host-C"},
        {"exporter_host", "host-C"},
        {"http-loader-host", "host-C"}
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
