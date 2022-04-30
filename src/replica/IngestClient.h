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
#ifndef LSST_QSERV_REPLICA_INGESTCLIENT_H
#define LSST_QSERV_REPLICA_INGESTCLIENT_H

// System headers
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Csv.h"

// Forward declarations
namespace lsst::qserv::replica {
class ProtocolBuffer;
class ProtocolIngestResponse;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class IngestClientError represents exceptions thrown by IngestClient
 * on errors.
 */
class IngestClientError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Class IngestClient is a client-side API for the point-to-point catalog
 * data ingest service.
 */
class IngestClient : public std::enable_shared_from_this<IngestClient> {
public:
    typedef std::shared_ptr<IngestClient> Ptr;

    /// The default record size when reading from an input file.
    constexpr static size_t defaultRecordSizeBytes = 1048576;

    /**
     * Establish a connection to the remote service. If the operation is successful
     * then a valid pointer will be returned and the data could be could be send via
     * method IngestClient::send(). Otherwise return the null pointer.
     *
     * @param workerHost the name or an IP address of a worker node where
     *   the ingest service is run
     * @param workerPort the port number of the ingest service
     * @param transactionId an identifier of a super-transaction which is required
     *   to be started before attempting the ingest
     * @param tableName the base name of a table to be loaded. The table should not
     *   include partition numbers, 'overlap', etc. Note that for the regular tables
     *   the base name of the table is the same name as the actual name of the table.
     * @param chunk the number of a chunk. The parameter is ignored for
     *   non-partitioned tables.
     * @param isOverlap a flag indicating if this is the chunk 'overlap' table.
     *   The parameter is ignored for non-partitioned tables.
     * @param inputFilePath the path (relative or absolute) name for a file
     *   whose content will be transferred to to the remote service.
     * @param authKey  an authorization key which should also be known to the server.
     * @param dialectInput optional parameteres specifying a dialect of the input file
     * @param recordSizeBytes  an optional parameter specifying the record size for
     *   reading from the input file and for sending data to a server.
     * @throws IngestClientError for any problem occurred when establishing
     *   a connection or during the initial handshake with the server
     */
    static Ptr connect(std::string const& workerHost, uint16_t workerPort, TransactionId transactionId,
                       std::string const& tableName, unsigned int chunk, bool isOverlap,
                       std::string const& inputFilePath, std::string const& authKey = std::string(),
                       csv::DialectInput const& dialectInput = csv::DialectInput(),
                       size_t recordSizeBytes = defaultRecordSizeBytes);

    // Default construction and copy semantics are prohibited

    IngestClient() = delete;
    IngestClient(IngestClient const&) = delete;
    IngestClient& operator=(IngestClient const&) = delete;

    /// Non-trivial destructor is needed to close a connection to the server
    ~IngestClient();

    /**
     * Send the whole file. Note, this is a blocking operation
     * for a thread which calls the method.
     *
     * @throws IngestClientError for any problem occurred when sending the file
     *   content to the server
     */
    void send();

    /// @return the number of bytes read from an input file
    size_t sizeBytes() const { return _sizeBytes; }

    /// @return the flag indicating if the failed attempt is allowed to be retried
    /// @throws std::logic_error if the request hasn't been completed
    bool retryAllowed() const;

private:
    IngestClient(std::string const& workerHost, uint16_t workerPort, TransactionId transactionId,
                 std::string const& tableName, unsigned int chunk, bool isOverlap,
                 std::string const& inputFilePath, std::string const& authKey,
                 csv::DialectInput const& dialectInput, size_t recordSizeBytes);

    /// @return a context string for the logger and exceptions
    std::string _context(std::string const& func) const {
        return "IngestClient::" + func + "[" + _workerHost + ":" + std::to_string(_workerPort) + "]  ";
    }

    /**
     * Establish a connection with the service and perform the initial
     * handshake.
     *
     * @throws IngestClientError for any problem occurred when establishing
     *   a connection or during the initial handshake with the server.
     */
    void _connectImpl();

    /**
     * Read a response message from the server
     *
     * @param response an object to be initialized upon successful completion
     *   of the operation
     * @throws IngestClientError for any problem occurred when communicating
     *   with the server
     */
    void _readResponse(ProtocolIngestResponse& response);

    /**
     * Analyze a error condition and if there is a problem then report it
     * into the logging stream and throw an exception. The method will also
     * attempt to shutdown and close a connection with the server.
     * If no problem is found in the error code the method will do nothing.
     *
     * @param ec error code to be checked
     * @param func the name of a method which requested the test
     * @param msg a message to be reported in case of a problem. The input message
     *   will be extended with an explanation of the problem extracted from
     *   the error code.
     * @throws IngestClientError if a problem was found
     */
    void _assertErrorCode(boost::system::error_code const& ec, std::string const& func,
                          std::string const& msg);

    /**
     * Unconditionally abort the operation by shutting down and closing
     * the server connection, logging a error message and throwing an exception.
     *
     * @param func the name of a method which requested the abort
     * @param error an error message to be reported
     * @throws IngestClientError is always thrown by the method
     */
    void _abort(std::string const& func, std::string const& error);

    /// Make an attempt to shutdown and close a connection with the server
    void _closeConnection();

    // Input parameters

    std::string const _workerHost;
    uint16_t const _workerPort;
    TransactionId const _transactionId;
    std::string const _tableName;
    unsigned int const _chunk;
    bool const _isOverlap;
    std::string const _inputFilePath;
    std::string const _authKey;
    csv::DialectInput const _dialectInput;
    size_t const _recordSizeBytes;

    /// Buffer for data moved over the network.
    std::unique_ptr<ProtocolBuffer> _bufferPtr;

    boost::asio::io_service _io_service;
    boost::asio::ip::tcp::socket _socket;

    bool _sent = false;          /// Set to 'true' after a successful completion of the ingest.
    bool _retryAllowed = false;  /// Set to 'true' to indicate that failed request may be retried.
    size_t _sizeBytes = 0;       /// The number of bytes read from an input file
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_INGESTCLIENT_H
