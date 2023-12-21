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
#ifndef LSST_QSERV_REPLICA_WORKERDIRECTORINDEXREQUEST_H
#define LSST_QSERV_REPLICA_WORKERDIRECTORINDEXREQUEST_H

// System headers
#include <string>

// Qserv headers
#include "replica/proto/protocol.pb.h"
#include "replica/worker/WorkerRequest.h"

// Forward declarations
namespace lsst::qserv::replica::database::mysql {
class Connection;
class ConnectionPool;
}  // namespace lsst::qserv::replica::database::mysql

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerDirectorIndexRequest queries a director table (the whole or just one MySQL
 * partition, depending on parameters of the request) of a database
 * to extracts data to be loaded into the "director" index.
 */
class WorkerDirectorIndexRequest : public WorkerRequest {
public:
    typedef std::shared_ptr<WorkerDirectorIndexRequest> Ptr;

    /// Forward declaration for the connection pool
    typedef std::shared_ptr<database::mysql::ConnectionPool> ConnectionPoolPtr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider provider is needed to access the Configuration of
     *   a setup and for validating the input parameters
     * @param connectionPool a pool of persistent database connections
     * @param worker the name of a worker. The name must match the worker which
     *   is going to execute the request.
     * @param id an identifier of a client request
     * @param priority indicates the importance of the request
     * @param (optional) onExpired request expiration callback function.
     *   If nullptr is passed as a parameter then the request will never expire.
     * @param (optional) requestExpirationIvalSec request expiration interval.
     *   If 0 is passed into the method then a value of the corresponding
     *   parameter for the Controller-side requests will be pulled from
     *   the Configuration.
     * @param request ProtoBuf body of the request
     * @return pointer to the created object
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, ConnectionPoolPtr const& connectionPool,
                      std::string const& worker, std::string const& id, int priority,
                      ExpirationCallbackType const& onExpired, unsigned int requestExpirationIvalSec,
                      ProtocolRequestDirectorIndex const& request);

    WorkerDirectorIndexRequest() = delete;
    WorkerDirectorIndexRequest(WorkerDirectorIndexRequest const&) = delete;
    WorkerDirectorIndexRequest& operator=(WorkerDirectorIndexRequest const&) = delete;

    ~WorkerDirectorIndexRequest() override = default;

    /// @return the original request
    ProtocolRequestDirectorIndex const& request() const { return _request; }

    /**
     * Extract request status into the Protobuf response object.
     *
     * @param response Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseDirectorIndex& response) const;

    bool execute() override;

private:
    WorkerDirectorIndexRequest(ServiceProvider::Ptr const& serviceProvider,
                               ConnectionPoolPtr const& connectionPool, std::string const& worker,
                               std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                               unsigned int requestExpirationIvalSec,
                               ProtocolRequestDirectorIndex const& request);

    /**
     * The query generator uses parameters of a request to compose
     * a desired query.
     *
     * @param conn a reference to the database connector is needed to process
     *   arguments to meet requirements of the database query processing engine.
     * @return a query as per the input request
     * @throws std::invalid_argument if the input parameters are not supported
     */
    std::string _query(std::shared_ptr<database::mysql::Connection> const& conn) const;

    /**
     * Read the content of the file into memory starting from the given offset.
     * @note The maximum number of bytes to read is set in the Configuration
     *   parameter (worker, director-index-record-size). And it should not
     *   exceed a limit set in ProtocolBuffer::HARD_LIMIT.
     * @param offset A position of the first byte in the file to read.
     * @return The completion status to be returned to the Controller.
     */
    ProtocolStatusExt _readFile(size_t offset);

    /// Get rid of the temporary file if it's still tehre.
    void _removeFile() const;

    ConnectionPoolPtr const _connectionPool;
    ProtocolRequestDirectorIndex const _request;

    /// The path name of a temporary folder where the file will be stored.
    /// The folder gets created before extracting data from the MySQL table
    /// into the file.
    std::string const _tmpDirName;

    /// The full path name of a temporary file into which the TSV/CSV dump will be made.
    /// This file will get deleted when its whole content is sent to the Controller.
    std::string const _fileName;

    /// The size of the file is determined each time before reading it.
    size_t _fileSizeBytes = 0;

    /// Cached error to be sent to a client
    std::string _error;

    /// In-memory storage for the content of the file upon a successful completion
    /// of the data extraction query.
    std::string _data;
};

/// Class WorkerDirectorIndexRequest provides an actual implementation
typedef WorkerDirectorIndexRequest WorkerDirectorIndexRequestFS;

/// Class WorkerDirectorIndexRequest provides an actual implementation
typedef WorkerDirectorIndexRequest WorkerDirectorIndexRequestPOSIX;

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERDIRECTORINDEXREQUEST_H
