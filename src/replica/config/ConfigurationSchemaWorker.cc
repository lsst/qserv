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
#include "replica/config/ConfigurationSchemaWorker.h"

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

json const workerSchemaJson = json::object(
        {{"common",
          {{"asio-num-threads",
            {{"description", "The number of shared threads managed by BOOST ASIO. Must be greater than 0."},
             {"default", min(8, num_threads)}}},
           {"request-buf-size-bytes",
            {{"description", "The default buffer size for network communications. Must be greater than 0."},
             {"default", 4096}}}}},
         {"security",
          {{"auth-key",
            {{"description",
              "An authorization key for operations affecting the state of Qserv or"
              " the Replication/Ingest system."},
             {"empty-allowed", 1},
             {"security-context", 1},
             {"default", ""}}},
           {"admin-auth-key",
            {{"description",
              "An administrator-level authorization key for critical operations affecting"
              " the state of Qserv of the Replication/Ingest system."},
             {"empty-allowed", 1},
             {"security-context", 1},
             {"default", ""}}},
           {"http-user",
            {{"description", "The login name of a user for connecting to the Replication service."},
             {"empty-allowed", 1},
             {"default", ""}}},
           {"http-password",
            {{"description",
              "The login password of a user for connecting to the Replication service. The value "
              "of the password is ignored if the user is not specified. The password will be used for"
              " authenticating the user. The password can't be empty if the user is specified."},
             {"empty-allowed", 1},
             {"security-context", 1},
             {"default", ""}}},
           {"instance-id",
            {{"description",
              "A unique identifier of a Qserv instance served by the Replication System."
              " Its value will be passed along various internal communication lines of"
              " the system to ensure that all services are related to the same instance."
              " This mechanism also prevents 'cross-talks' between two (or many) Replication"
              " System's setups in case of an accidental mis-configuration."},
             {"default", "qserv"}}}}},
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
         {"worker",
          {{"request-timeout-sec",
            {{"description",
              "The default timeout for completing worker requests. A value depends on"
              " a scale of catalogs served by Qserv and ingested by the Replication/Ingest system."
              " It's recommended to set this parameter to 3600 seconds or higher. The value must be"
              " greater than 0. Note that the timeout may be explicitly set in the Controller"
              " in the requests sent to the worker. In this cases, the value specified in the request"
              " will take precedence over the default timeout."},
             {"default", 28800}}},
           {"num-threads",
            {{"description", "The number of threads managed by BOOST ASIO. Must be greater than 0."},
             {"default", min(8, num_threads)}}},
           {"num-svc-processing-threads",
            {{"description", "The number of request processing threads in each Replication worker service."},
             {"default", min(8, num_threads)}}},
           {"num-http-svc-threads",
            {{"description",
              "The number of threads in each HTTP server frontend of Replication worker service."},
             {"default", min(8, num_threads)}}},
           {"num-fs-processing-threads",
            {{"description",
              "The number of request processing threads in each Replication worker's file service."},
             {"default", min(8, num_threads)}}},
           {"fs-buf-size-bytes",
            {{"description",
              "The default buffer size for file and network operations at Replication worker's file "
              "service."},
             {"default", 4194304}}},
           {"exporter-threads",
            {{"description",
              "The number of threads in each HTTP server frontend of Replication worker's data export "
              "service."},
             {"default", min(8, num_threads)}}},
           {"num-http-loader-processing-threads",
            {{"description",
              "The number of request processing threads in each Replication worker's HTTP-based ingest "
              "service."},
             {"default", min(8, num_threads)}}},
           {"num-async-loader-processing-threads",
            {{"description",
              "The number of request processing threads in each Replication worker's ASYNC ingest service."},
             {"default", min(8, num_threads)}}},
           {"async-loader-auto-resume",
            {{"description",
              "The flag controlling the behavior of Replication worker's ASYNC ingest service after"
              " its (deliberate or accidental) restarts. If the value of the parameter is not 0 then"
              " the service will resume processing incomplete (queued or on-going) requests."
              " Note that requests that were in the final state of loading data into MySQL before"
              " the restart won't be resumed. These will be marked as failed."
              " Setting a value of the parameter to 0 will result in failing all incomplete contribution"
              " requests existed before the restart. Note that requests failed in the last (loading) stage"
              " can't be resumed, and they will require aborting the corresponding super-transaction."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"async-loader-cleanup-on-resume",
            {{"description",
              "The flag controlling the behavior of Replication worker's ASYNC ingest service after"
              " a restart of the service. If the value of the parameter is not 0 the service will"
              " try cleaning up temporary files that might be left on disk by incomplete (queued or on-going)"
              " requests. This option may be disabled to allow debugging the service."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"http-max-listen-conn",
            {{"description",
              "The maximum length of the queue of pending connections sent to the Replication worker's"
              " HTTP-based ingest service. Must be greater than 0."},
             {"default", max_listen_connections}}},
           {"http-max-queued-requests",
            {{"description",
              "The maximum number of pending requests, i.e. requests accept()ed by"
              " the listener but still waiting to be routed by the HTTP server."
              " If set to 0 then no specific limit will be enforced. It's recommented to keep"
              " the default value unless there are specific reasons to change it."},
             {"empty-allowed", 1},
             {"default", 0}}},
           {"exporter-max-queued-requests",
            {{"description",
              "The maximum number of pending requests, i.e. requests accept()ed by"
              " the listener but still waiting to be routed by the HTTP-based Worker data export server."
              " If set to 0 then no specific limit will be enforced. It's recommented to keep"
              " the default value unless there are specific reasons to change it."},
             {"empty-allowed", 1},
             {"default", 0}}},
           {"http-svc-max-queued-requests",
            {{"description",
              "The maximum number of pending requests, i.e. requests accept()ed by"
              " the listener but still waiting to be routed by the HTTP-based Worker Replication server."
              " If set to 0 then no specific limit will be enforced. It's recommented to keep"
              " the default value unless there are specific reasons to change it."},
             {"empty-allowed", 1},
             {"default", 0}}},
           {"svc-port",
            {{"description", "The port number for the worker replication service."}, {"default", 25000}}},
           {"http-svc-port",
            {{"description", "The port number for the HTTP-based worker replication service."},
             {"default", 25005}}},
           {"fs-port",
            {{"description", "The port number for the worker's file service."}, {"default", 25001}}},
           {"data-dir",
            {{"description",
              "The data directory from which the worker file service serves files"
              " to other workers. This folder is required to be the location where the MySQL"
              " service of Qserv worker stores its data."},
             {"default", "/qserv/data/mysql"}}},
           {"loader-max-warnings",
            {{"description",
              "The maximum number of warnings to retain after executing LOAD DATA [LOCAL] INFILE"
              " when ingesting contributions into the adjacent Qserv worker's MySQL database."
              " The warnings (if any) will be recorded in the persisent state of the"
              " Replication/Inhgest system and returned to the ingest workflow upon request."},
             {"default", 64}}},
           {"exporter-port",
            {{"description", "The port number for the worker's HTTP-based table export service."},
             {"default", 25003}}},
           {"exporter-tmp-dir",
            {{"description",
              "A location for temporary files stored by the worker's HTTP-based table"
              " export service before returning them a client."},
             {"default", "/qserv/data/export"}}},
           {"http-loader-port",
            {{"description",
              "The port number for the worker's HTTP-based REST service for ingesting table"
              " contributions into the adjacent Qserv worker's MySQL database."},
             {"default", 25004}}},
           {"http-loader-tmp-dir",
            {{"description",
              "A location for temporary files stored by the worker's"
              " HTTP-based REST service ingesting table before ingesting them into"
              " the adjacent Qserv worker's MySQL database."},
             {"default", "/qserv/data/ingest"}}},
           {"ingest-charset-name",
            {{"description",
              "The name of the default character set for parsing the payload of"
              " the contributions. The name can be any valid character set recognized by"
              " the MySQL server."},
             {"default", "latin1"}}},
           {"ingest-num-retries",
            {{"description",
              "The default number of the automated retries of failed contribution attempts"
              " in cases when such retries are still possible. The limit can be changed for"
              " individual contributions. Note that the effective number of retries specified"
              " by this parameter or the one set in the contribution requests can not"
              " exceed the 'hard' limit set in the related parameter 'worker','ingest-max-retries'."
              " Setting a value of the parameter to 0 will disable automatic retries (unless they are"
              " explicitly enabled or requested by the ingest workflows for individual contributions)."},
             {"empty-allowed", 1},
             {"default", 1}}},
           {"ingest-max-retries",
            {{"description",
              "The maximum number of the automated retries of failed contribution attempts"
              " in cases when such retries are still possible. The parameter represents the 'hard'"
              " limit for the number of retries regardless of what's specified in the related"
              " parameter 'worker','ingest-num-retries' or in the contributions requests."
              " The primary purpose of the parameter is to prevent accidental overloading"
              " of the ingest system should a very large number of retries accidentally specified"
              " by the ingest workflows for individual contributions. Setting a value of the parameter"
              " to 0 will unconditionally disable any retries."},
             {"empty-allowed", 1},
             {"default", 10}}},
           {"director-index-record-size",
            {{"description",
              "The recommended record size (in bytes) for reading from the 'director' index file."
              " Note that the size should not exceed the 'hard' limit of the Google Protobuf message"
              " size of " +
                      to_string(ProtocolBuffer::HARD_LIMIT) +
                      " bytes. Any number set higher than this limit will"
                      " get truncated down to match the limit at run time."},
             {"default", 16 * 1024 * 1024}}},
           {"create-databases-on-scan",
            {{"description",
              "The flag controlling the behavior of the worker's replica lookup algorithms during"
              " scanning the data directory for existing files. If the flag is set to 1"
              " then any missing databases will be created automatically. Database access privileges"
              " will be granted to the Qserv user 'qsmaster' for the newly created databases."
              " The database will be also be also registered as 'enabled' in the worker's"
              " persistent state. If the flag is set to 0 then missing databases will be ignored."},
             {"default", 1},
             {"empty-allowed", 1}}}}}});

ConfigurationSchemaWorker::ConfigurationSchemaWorker() : ConfigurationSchema(workerSchemaJson) {}

}  // namespace lsst::qserv::replica
