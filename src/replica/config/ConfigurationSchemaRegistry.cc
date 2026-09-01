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
#include "replica/config/ConfigurationSchemaRegistry.h"

// System headers
#include <thread>

// Qserv headers
#include "replica/util/Common.h"
#include "replica/util/ProtocolBuffer.h"

// Third party headers
#include "boost/asio.hpp"
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

namespace {
int const max_listen_connections = boost::asio::socket_base::max_listen_connections;
int const num_threads = thread::hardware_concurrency();
}  // namespace

namespace lsst::qserv::replica {

json const registrySchemaJson = json::object(
        {{"common",
          {{"asio-num-threads",
            {{"description", "The number of shared threads managed by BOOST ASIO. Must be greater than 0."},
             {"default", min(8, num_threads)}}},
           {"request-buf-size-bytes",
            {{"description", "The default buffer size for network communications. Must be greater than 0."},
             {"default", 4096}}}}},
         {"registry",
          {{"host",
            {{"description", "The IP address or the DNS host name for the registry's HTTP server."},
             {"default", "localhost"}}},
           {"port",
            {{"description", "The port number for the registry's HTTP server. Must be greater than 0."},
             {"default", 25082}}},
           {"max-listen-conn",
            {{"description",
              "The maximum length of the queue of pending connections sent to the registry's HTTP server."
              " Must be greater than 0."},
             {"default", max_listen_connections}}},
           {"threads",
            {{"description",
              "The number of threads managed by BOOST ASIO for the HTTP server. Must be greater than 0."},
             {"default", min(8, num_threads)}}},
           {"heartbeat-ival-sec",
            {{"description",
              "The heartbeat interval for interactions with the workers Registry service. Must be greater "
              "than 0."},
             {"default", 5}}}}},
         {"database",
          {{"services-pool-size",
            {{"description", "The pool size at the client database services connector."},
             {"default", max(8, num_threads)}}},
           {"host",
            {{"description",
              "The host name of the MySQL server where the Replication system maintains its persistent "
              "state."
              " Note that this parameter can't be updated through the Configuration service as it's"
              " set up at the startup time of the Replication/Ingest system."},
             {"read-only", 1},
             {"default", "localhost"}}},
           {"port",
            {{"description",
              "The port number of the MySQL server where the Replication maintains its persistent state."
              " Note that this parameter can't be updated through the Configuration service as it's"
              " set up at the startup time of the Replication/Ingest system."},
             {"read-only", 1},
             {"default", 3306}}},
           {"user",
            {{"description",
              "The MySQL user account of a service where the Replication system maintains its persistent "
              "state."
              " Note that this parameter can't be updated through the Configuration service as it's"
              " set up at the startup time of the Replication/Ingest system."},
             {"read-only", 1},
             {"default", "qsreplica"}}},
           {"password",
            {{"description",
              "A password for the MySQL account where the Replication system maintains its persistent "
              "state"},
             {"read-only", 1},
             {"security-context", 1},
             {"empty-allowed", 1},
             {"default", ""}}},
           {"name",
            {{"description",
              "The name of a MySQL database for a service where the Replication system maintains its"
              " persistent state. Note that this parameter can't be updated through the Configuration"
              "  service as it's set up at the startup time of the Replication/Ingest system."},
             {"read-only", 1},
             {"default", "qservReplica"}}},
           {"qserv-master-services-pool-size",
            {{"description",
              "The pool size at the client database services connector for the Qserv Master database."},
             {"default", 2}}},
           {"qserv-master-user",
            {{"description",
              "The MySQL user account of a service where Qserv 'czar' maintains its persistent state."},
             {"default", "qsmaster"}}},
           {"qserv-master-tmp-dir",
            {{"description",
              "The temporary folder for exchanging data with the Qserv 'czar' database service."},
             {"default", "/qserv/data/ingest"}}}}}});

ConfigurationSchemaRegistry::ConfigurationSchemaRegistry() : ConfigurationSchema(registrySchemaJson) {}

}  // namespace lsst::qserv::replica
