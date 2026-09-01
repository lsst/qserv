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
#include "replica/config/ConfigTestDataRegistry.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

map<string, set<string>> ConfigTestDataRegistry::parameters() {
    return map<string, set<string>>(
            {{"common", {"asio-num-threads", "request-buf-size-bytes"}},
             {"registry", {"host", "port", "max-listen-conn", "threads", "heartbeat-ival-sec"}},
             {"database",
              {"services-pool-size", "host", "port", "user", "password", "name", "qserv-master-user",
               "qserv-master-services-pool-size", "qserv-master-tmp-dir"}}});
}

json ConfigTestDataRegistry::data() {
    json obj;
    json& generalObj = obj["general"];
    generalObj["common"] = json::object({{"asio-num-threads", 2}, {"request-buf-size-bytes", 8192}});
    generalObj["registry"] = json::object({{"host", "127.0.0.1"},
                                           {"port", 8081},
                                           {"max-listen-conn", 512},
                                           {"threads", 4},
                                           {"heartbeat-ival-sec", 10}});
    generalObj["database"] = json::object({{"host", "localhost"},
                                           {"port", 13306},
                                           {"user", "qsreplica"},
                                           {"password", "changeme"},
                                           {"name", "qservReplica"},
                                           {"qserv-master-user", "qsmaster"},
                                           {"services-pool-size", 2},
                                           {"qserv-master-tmp-dir", "/qserv/data/ingest"}});
    obj["workers"] = json::array();
    obj["database_families"] = json::array();
    obj["databases"] = json::array();
    obj["czars"] = json::array();
    return obj;
}

}  // namespace lsst::qserv::replica
