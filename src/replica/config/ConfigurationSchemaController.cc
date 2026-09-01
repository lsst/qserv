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
#include "replica/config/ConfigurationSchemaController.h"

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

json const controllerSchemaJson = json::object(
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
           {"heartbeat-ival-sec",
            {{"description",
              "The heartbeat interval for interactions with the workers Registry service. Must be greater "
              "than 0."},
             {"default", 5}}}}},
         {"controller",
          {{"request-timeout-sec",
            {{"description",
              "The default timeout for completing worker requests. A value depends on"
              " a scale of catalogs served by Qserv and ingested by the Replication/Ingest system."
              " It's recommended to set this parameter to 3600 seconds or higher. The value must be"
              " greater than 0."},
             {"default", 28800}}},
           {"request-retry-interval-sec",
            {{"description", "The default retry timeout for network communications. Must be greater than 0."},
             {"default", 1}}},
           {"job-timeout-sec",
            {{"description",
              "The default timeout for completing jobs. A value depends on"
              " a scale of catalogs served by Qserv and ingested by the Replication/Ingest system."
              " Some complex jobs run on the large catalogs may take many hours."
              " It's recommended to set this parameter to 3600 seconds or higher. The value must be"
              " greater than 0."},
             {"default", 86400}}},
           {"job-heartbeat-sec",
            {{"description", "The heartbeat interval for jobs. A value of 0 disables heartbeats."},
             {"empty-allowed", 1},
             {"default", 0}}},
           {"num-requests-per-worker",
            {{"description",
              "The number of simultaneous requests to be sent to each Replication worker service."
              " For the best performance of the system, the number should match the number of"
              " the service threads at the worker. This number must be greater than 0."},
             {"default", 2}}},
           {"http-server-threads",
            {{"description",
              "The number of threads managed by BOOST ASIO for the HTTP server. Must be greater than 0."},
             {"default", min(8, num_threads)}}},
           {"http-server-port",
            {{"description", "The port number for the controller's HTTP server. Must be greater than 0."},
             {"default", 25081}}},
           {"http-max-listen-conn",
            {{"description",
              "The maximum length of the queue of pending connections sent to the controller's HTTP "
              "server."
              " Must be greater than 0."},
             {"default", max_listen_connections}}},
           {"max-repl-level",
            {{"description",
              "The maximum replicaton level that applies to any database family. This"
              " hard limit is meant to prevent the Replication system's algorithms from utilizing too"
              " much storage. The limit is enforced at run time. The number must be greater than 0."},
             {"default", 4}}},
           {"worker-evict-priority-level",
            {{"description",
              "The priority level of the worker eviction task that is run to compensate for"
              " the missing chunk replicas should be a worker became offline for an extended"
              " period of time."},
             {"empty-allowed", 1},
             {"default", PRIORITY_VERY_HIGH}}},
           {"health-monitor-priority-level",
            {{"description", "The priority level of the Cluster Health Monitoring task."},
             {"empty-allowed", 1},
             {"default", PRIORITY_VERY_HIGH}}},
           {"ingest-priority-level",
            {{"description", "The priority level of the time-critical catalog ingest activities."},
             {"empty-allowed", 1},
             {"default", PRIORITY_HIGH}}},
           {"catalog-management-priority-level",
            {{"description",
              "The priority level of the routine catalog management activities, such as scanning"
              " and recording replica dispositions, fixing up missing replicas, etc."},
             {"empty-allowed", 1},
             {"default", PRIORITY_LOW}}},
           {"auto-register-workers",
            {{"description",
              "Automatically scale a collection of workers by registering new workers reported by the "
              "Registry"
              " service. If the flag is set to 0 then new workers will be ignored."},
             {"empty-allowed", 1},
             {"default", 0}}},
           {"auto-register-czars",
            {{"description",
              "Automatically scale a collection of Czars by registering new Czars reported by the "
              "Registry"
              " service. If the flag is set to 0 then new Czars will be ignored."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"ingest-job-monitor-ival-sec",
            {{"description",
              "An interval (seconds) for monitoring progress of jobs submitted by the Controller during"
              " asynchronous ingest operations."},
             {"default", 60}}},
           {"num-director-index-connections",
            {{"description",
              "The number of the MySQL connection to the Qserv 'czar's database in the connection pool "
              "that"
              " is used by the 'director' index builder job. If using the InnoDB storage engine for"
              " the 'director' index table, a value of this parameter should be set to 2,"
              " which would allow the second MySQL thread to prepare data while the first thread"
              " is loading data into the table. Setting the parameter to some large number won't"
              " yield any benefits in terms of the overall performance of the index ingest. This"
              " will just result in the useless increase in the CPU time consumed by MySQL."},
             {"default", 2}}},
           {"director-index-charset-name",
            {{"description",
              "The name of the default character set for ingesting contributions into the"
              " director index tables. The name can be any valid character set recognized by"
              " the MySQL server."},
             {"default", "latin1"}}},
           {"director-index-engine",
            {{"description", "The default MySQL engine of the 'director' index tables."},
             {"default", "InnoDB"}}}}},
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
             {"default", "/qserv/data/ingest"}}}}},
         {"xrootd",
          {{"auto-notify",
            {{"description", "Automatically notify Qserv on changes in replica disposition."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"request-timeout-sec",
            {{"description", "The default timeout for communications with Qserv over XRootD/SSI."},
             {"default", 1800}}},
           {"host",
            {{"description",
              "The service location (the host name or an IP address) of XRootD/SSI for"
              " communications with Qserv."},
             {"default", "localhost"}}},
           {"port",
            {{"description",
              "A port number for the XRootD/SSI service needed for communications with Qserv."},
             {"default", 1094}}},
           {"allow-reconnect",
            {{"description",
              "XRootD/SSI connection handling mode. Set 0 to disable automatic reconnects."
              " Any other number would allow reconnects."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"reconnect-timeout",
            {{"description",
              "The default value limiting a duration of time for making automatic"
              " reconnects to the XRootD/SSI services before failing and reporting error"
              " (if the server is not up, or if it's not reachable for some reason)"},
             {"default", 3600}}}}}});

ConfigurationSchemaController::ConfigurationSchemaController() : ConfigurationSchema(controllerSchemaJson) {}

}  // namespace lsst::qserv::replica
