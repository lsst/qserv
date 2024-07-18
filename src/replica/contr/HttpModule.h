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
#ifndef LSST_QSERV_HTTPMODULE_H
#define LSST_QSERV_HTTPMODULE_H

// System headers
#include <memory>
#include <string>

// Qserv headers
#include "http/QhttpModule.h"
#include "qhttp/Request.h"
#include "qhttp/Response.h"
#include "replica/config/Configuration.h"
#include "replica/contr/EventLogger.h"
#include "replica/contr/HttpProcessorConfig.h"

// Forward declarations
namespace lsst::qserv {
namespace css {
class CssAccess;
}  // namespace css
namespace replica {
class DatabaseInfo;
namespace database::mysql {
class Connection;
}  // namespace database::mysql
}  // namespace replica
}  // namespace lsst::qserv

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class HttpModule is a base class for requests processing modules
 * of an HTTP server built into the Master Replication Controller.
 */
class HttpModule : public EventLogger, public http::QhttpModule {
public:
    HttpModule() = delete;
    HttpModule(HttpModule const&) = delete;
    HttpModule& operator=(HttpModule const&) = delete;

    virtual ~HttpModule() = default;

protected:
    /**
     *
     * @param controller       The Controller provides the network I/O services (BOOST ASIO)
     * @param taskName         The name of a task in a context of the Master Replication Controller
     * @param processorConfig  Shared parameters of the HTTP services
     * @param req              The HTTP request
     * @param resp             The HTTP response channel
     */
    HttpModule(Controller::Ptr const& controller, std::string const& taskName,
               HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
               qhttp::Response::Ptr const& resp);

    unsigned int czarResponseTimeoutSec() const { return _processorConfig.czarResponseTimeoutSec; }
    unsigned int workerResponseTimeoutSec() const { return _processorConfig.workerResponseTimeoutSec; }
    unsigned int qservSyncTimeoutSec() const { return _processorConfig.qservSyncTimeoutSec; }
    unsigned int workerReconfigTimeoutSec() const { return _processorConfig.workerReconfigTimeoutSec; }

    /// @see http::Module::context()
    virtual std::string context() const final;

    /// @param database The name of a database to connect to.
    /// @return A connection object for the Qserv Master Database server.
    std::shared_ptr<database::mysql::Connection> qservMasterDbConnection(std::string const& database) const;

    /// @param readOnly The open mode for the connection.
    /// @return A connection object for operations with Qserv CSS.
    std::shared_ptr<css::CssAccess> qservCssAccess(bool readOnly = false) const;

    /**
     * This method will tell all (or a subset of) workers to reload cache Configuration
     * parameters. The operation is needed after significant changes in the Replication
     * system's configuration occur, such as creating new databases or tables.
     * This is to implement an explicit model of making workers aware about changes
     * in the mostly static state of the system.
     * @param databaseInfo  defines a scope of the operation (used for status and error reporting)
     * @param allWorkers  'true' if all workers are involved into the operation
     * @param workerResponseTimeoutSec  do not wait longer than the specified number of seconds
     * @return non-empty string to indicate a error
     */
    std::string reconfigureWorkers(DatabaseInfo const& databaseInfo, bool allWorkers,
                                   unsigned int workerResponseTimeoutSec) const;

    /**
     * Fetch a mode of building the "director" index as requested by a catalog
     * ingest workflow and recorded at the database creation time. A value of
     * the parameter is recorded in a database.
     *
     * @param database The name of a database for which a value of the parameter
     *   is requested.
     * @return 'true' if the index was requested to be built automatically w/o any
     *   explicit requests from a catalog ingest workflow.
     */
    bool autoBuildDirectorIndex(std::string const& database) const;

    /**
     * Get database info for a database that was specified in a request, either explicitly
     * in attribute "database" or implicitly in attribute "transation_id". The method may
     * do an optional check on the database state as directed by the optional parameter
     * 'throwIfPublished'.
     *
     * @param func The name of a method called the operation.
     * @param throwIfPublished The optional flag that if 'true' will tell the method to check
     *   the state of the database and throw an exception if it's already published.
     * @return The database info object.
     * @throw std::invalid_argument If neither obe the above-mentioned attributes were
     *    provided in a request.
     * @throw http::Error If the database is already "published" and a value of
     *   the parameter 'throwIfPublished' is set to 'true'.
     */
    DatabaseInfo getDatabaseInfo(std::string const& func, bool throwIfPublished = true) const;

private:
    HttpProcessorConfig const _processorConfig;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_HTTPMODULE_H
