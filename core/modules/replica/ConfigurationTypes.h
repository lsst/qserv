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
#ifndef LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H
#define LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H

/**
 * This header defines ancillary helper types which are meant to reduce code
 * duplications in applications dealing with class Configuration.
 */

// System headers
#include <string>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Configuration.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Structure ConfigurationGeneralParams defines helpers for the general parameters
 * of the Configuration system's API. The helpers are needed to eliminate code
 * duplication and streamline implementations in some command line tools and services
 * dealing with the Replication System's Configuration.
 * 
 * Each helper is represented by a dedicated
 * structure (w/o any explicitly given type name) which has the following members:
 * 
 * key:
 *   the short name of a key to be used in various context when a text-based
 *   reference to the corresponding Configuration parameter is needed (within
 *   protocols and application's implementations).
 * 
 * description:
 *   an expanded description of the parameter, its role, etc. A value of this
 *   member is used in the command-line and Web UI applications where parameters
 *   are presented to users.
 * 
 * value:
 *   is a member storing a transient value of the parameter before forwarding
 *   it to the Configuration by method 'save'.
 * 
 * updatable:
 *   flag indicating if the transient value of the parameter can be saved back
 *   into the Configuration. This flag can be used by the command-line tools
 *   and Web UI applications.
 * 
 * save:
 *   a method which stores the transient state of the parameter stored in
 *   member 'value' into the Configuration.
 *
 * get - is a type-aware method returning a value of the corresponding parameter
 * retrieved from the Configuration.
 * 
 * str - is a method which returns a value of the parameter pulled from
 * the Configuration and serializied into a string.
 */
struct ConfigurationGeneralParams {

    struct {

        std::string const key         = "NET_BUF_SIZE_BYTES";
        std::string const description = "The default buffer size for network communications.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setRequestBufferSizeBytes(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->requestBufferSizeBytes(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } requestBufferSizeBytes;

    struct {

        std::string const key         = "NET_RETRY_TIMEOUT_SEC";
        std::string const description = "The default retry timeout for network communications.";
        unsigned int      value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setRetryTimeoutSec(value);
        }
        unsigned int get(Configuration::Ptr const& config) const { return config->retryTimeoutSec(); }
        std::string  str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } retryTimeoutSec;

    struct {

        std::string const key         = "CONTR_NUM_THREADS";
        std::string const description = "The number of threads managed by BOOST ASIO.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerThreads(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->controllerThreads(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } controllerThreads;

    struct {

        std::string const key         = "CONTR_HTTP_PORT";
        std::string const description = "The port number for the controller's HTTP server.";
        uint16_t          value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerHttpPort(value);
        }
        uint16_t    get(Configuration::Ptr const& config) const { return config->controllerHttpPort(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } controllerHttpPort;

    struct {

        std::string const key         = "CONTR_NUM_HTTP_THREADS";
        std::string const description = "The number of threads managed by BOOST ASIO for the HTTP server.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerHttpThreads(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->controllerHttpThreads(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } controllerHttpThreads;

    struct {

        std::string const key         = "CONTR_REQUEST_TIMEOUT_SEC";
        std::string const description = "The default timeout for completing worker requests.";
        unsigned int      value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setControllerRequestTimeoutSec(value);
        }
        unsigned int get(Configuration::Ptr const& config) const { return config->controllerRequestTimeoutSec(); }
        std::string  str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } controllerRequestTimeoutSec;

    struct {

        std::string const key         = "CONTR_JOB_TIMEOUT_SEC";
        std::string const description = "default timeout for completing jobs";
        unsigned int      value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setJobTimeoutSec(value);
        }

        unsigned int get(Configuration::Ptr const& config) const { return config->jobTimeoutSec(); }
        std::string  str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } jobTimeoutSec;

    struct {

        std::string const key         = "CONTR_JOB_HEARTBEAT_SEC";
        std::string const description = "The heartbeat interval for jobs. A value of 0 disables heartbeats.";
        unsigned int      value       = std::numeric_limits<unsigned int>::max();

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != std::numeric_limits<unsigned int>::max()) {
                config->setJobHeartbeatTimeoutSec(value);
            }
        }
        unsigned int get(Configuration::Ptr const& config) const { return config->jobHeartbeatTimeoutSec(); }
        std::string  str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } jobHeartbeatTimeoutSec;

    struct {

        std::string const key         = "QSERV_AUTO_NOTIFY";
        std::string const description = "Automatically notify Qserv on changes in replica disposition"
                                        " (0 disables this feature).";
        int               value       = -1;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value >= 0) config->setXrootdAutoNotify(value != 0);
        }
        int         get(Configuration::Ptr const& config) const { return config->xrootdAutoNotify() ? 1 : 0; }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } xrootdAutoNotify;

    struct {

        std::string const key         = "XROOTD_HOST";
        std::string const description = "The service location (the host name or an IP address) of XRootD/SSI for"
                                        " communications with Qserv.";
        std::string       value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (not value.empty()) config->setXrootdHost(value);
        }
        std::string get(Configuration::Ptr const& config) const { return config->xrootdHost(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } xrootdHost;

    struct {

        std::string const key         = "XROOTD_PORT";
        std::string const description = "A port number for the XRootD/SSI service needed for communications"
                                        " with Qserv.";
        uint16_t          value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setXrootdPort(value);
        }
        uint16_t    get(Configuration::Ptr const& config) const { return config->xrootdPort(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } xrootdPort;

    struct {

        std::string const key         = "XROOT_COMM_TIMEOUT_SEC";
        std::string const description = "The default timeout for communications with Qserv over XRootD/SSI.";
        unsigned int      value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setXrootdTimeoutSec(value);
        }
        unsigned int get(Configuration::Ptr const& config) const { return config->xrootdTimeoutSec(); }
        std::string  str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } xrootdTimeoutSec;

    struct {

        std::string const key         = "DB_TECHNOLOGY";
        std::string const description = "The name of a database technology for the persistent state.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->databaseTechnology(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } databaseTechnology;

    struct {

        std::string const key         = "DB_HOST";
        std::string const description = "database service location";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->databaseHost(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } databaseHost;

    struct {

        std::string const key         = "DB_PORT";
        std::string const description = "The database service port.";

        bool const updatable = false;

        uint16_t    get(Configuration::Ptr const& config) const { return config->databasePort(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } databasePort;

    struct {

        std::string const key         = "DB_USER";
        std::string const description = "A user account for connecting to the database service.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->databaseUser(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } databaseUser;

    struct {

        std::string const key         = "DB_PASSWORD";
        std::string const description = "A password for connecting to the database service.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config, bool scramble=true) const {
            return scramble ? "xxxxxx" : config->databasePassword();
        }
        std::string str(Configuration::Ptr const& config, bool scramble) const { return get(config, scramble); }

    } databasePassword;

    struct {

        std::string const key         = "DB_NAME";
        std::string const description = "The name of the default database schema.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->databaseName(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } databaseName;

    struct {

        std::string const key         = "DB_SVC_POOL_SIZE";
        std::string const description = "The pool size at the client database services connector.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setDatabaseServicesPoolSize(value);
        }        
        size_t      get(Configuration::Ptr const& config) const { return config->databaseServicesPoolSize(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } databaseServicesPoolSize;

    struct {

        std::string const key         = "QSERV_MASTER_DB_HOST";
        std::string const description = "database service location for the Qserv Master database.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->qservMasterDatabaseHost(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } qservMasterDatabaseHost;

    struct {

        std::string const key         = "QSERV_MASTER_DB_PORT";
        std::string const description = "The database service port for the Qserv Master database.";

        bool const updatable = false;

        uint16_t    get(Configuration::Ptr const& config) const { return config->qservMasterDatabasePort(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } qservMasterDatabasePort;

    struct {

        std::string const key         = "QSERV_MASTER_DB_USER";
        std::string const description = "A user account for connecting to the Qserv Master database.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->qservMasterDatabaseUser(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } qservMasterDatabaseUser;

    struct {

        std::string const key         = "QSERV_MASTER_DB_PASSWORD";
        std::string const description = "A password for connecting to the Qserv Master database.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config, bool scramble=true) const {
            return scramble ? "xxxxxx" : config->qservMasterDatabasePassword();
        }
        std::string str(Configuration::Ptr const& config, bool scramble) const { return get(config, scramble); }

    } qservMasterDatabasePassword;

    struct {

        std::string const key         = "QSERV_MASTER_DB_NAME";
        std::string const description = "The name of the default database schema for the Qserv Master database.";

        bool const updatable = false;

        std::string get(Configuration::Ptr const& config) const { return config->qservMasterDatabaseName(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } qservMasterDatabaseName;

    struct {

        std::string const key         = "QSERV_MASTER_DB_SVC_POOL_SIZE";
        std::string const description = "The pool size at the client database services connector for the Qserv Master database.";
        size_t            value;

        bool const updatable = false;
    
        size_t      get(Configuration::Ptr const& config) const { return config->qservMasterDatabaseServicesPoolSize(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } qservMasterDatabaseServicesPoolSize;

    struct {

        std::string const key         = "WORKER_TECHNOLOGY";
        std::string const description = "The name of a technology for implementing requests.";
        std::string       value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (not value.empty()) config->setWorkerTechnology(value);
        }
        std::string get(Configuration::Ptr const& config) const { return config->workerTechnology(); }
        std::string str(Configuration::Ptr const& config) const { return get(config); }

    } workerTechnology;

    struct {

        std::string const key         = "WORKER_NUM_PROC_THREADS";
        std::string const description = "The number of request processing threads in each worker service.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setWorkerNumProcessingThreads(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->workerNumProcessingThreads(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } workerNumProcessingThreads;

    struct {

        std::string const key         = "WORKER_FS_NUM_PROC_THREADS";
        std::string const description = "The number of request processing threads in each worker's"
                                        " file server.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setFsNumProcessingThreads(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->fsNumProcessingThreads(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } fsNumProcessingThreads;

    struct {

        std::string const key         = "WORKER_FS_BUF_SIZE_BYTES";
        std::string const description = "Buffer size for file and network operations at worker's"
                                        " file server.";
        size_t            value;

        bool const updatable = true;

        void save(Configuration::Ptr const& config) {
            if (value != 0) config->setWorkerFsBufferSizeBytes(value);
        }
        size_t      get(Configuration::Ptr const& config) const { return config->workerFsBufferSizeBytes(); }
        std::string str(Configuration::Ptr const& config) const { return std::to_string(get(config)); }

    } workerFsBufferSizeBytes;

    /**
     * Pull general parameters from the Configuration and put them into
     * a JSON array.
     *
     * @param config
     *   pointer to the Configuration object
     *
     * @return
     *   JSON array
     */
    nlohmann::json toJson(Configuration::Ptr const& config) const;
};



}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_CONFIGURATIONTYPES_H
