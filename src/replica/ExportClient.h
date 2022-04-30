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
#ifndef LSST_QSERV_REPLICA_EXPORTCLIENT_H
#define LSST_QSERV_REPLICA_EXPORTCLIENT_H

// System headers
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/ProtocolBuffer.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ExportClientError represents exceptions thrown by ExportClient
 * on errors.
 */
class ExportClientError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

/**
 * Class ExportClient is a client-side API for the point-to-point
 * table exporting service.
 */
class ExportClient : public std::enable_shared_from_this<ExportClient> {
public:
    typedef std::shared_ptr<ExportClient> Ptr;

    enum ColumnSeparator { COMMA, TAB };

    /**
     * Establish a connection to the remote service. If the operation is successful
     * then a valid pointer will be returned and the data could be could be received via
     * method ExportClient::receive(). Otherwise return the null pointer.
     *
     * @param workerHost the name or an IP address of a worker node where
     *   the ingest service is run
     * @param workerPort the port number of the ingest service
     * @param databaseName the name of database where the table resides
     * @param tableName the base name of a table to be exported. The table should not
     *   include partition numbers, 'overlap', etc. Note that for the regular tables
     *   the base name of the table is the same name as the actual name of the table.
     * @param chunk the number of a chunk. The parameter is ignored for
     *   non-partitioned tables.
     * @param isOverlap a flag indicating if this is the chunk 'overlap' table.
     *   The parameter is ignored for non-partitioned tables.
     * @param outputFilePath the path (relative or absolute) name for a local file
     *   where the content will be saved. Note, the previous content of the file
     *   will get overwritten.
     * @param columnSeparator a character which separates columns within each row
     * @param authKey  an authorization key which should also be known to the server.
     * @throws ExportClientError for any problem occurred when establishing
     *   a connection or during the initial handshake with the server
     */
    static Ptr connect(std::string const& workerHost, uint16_t workerPort, std::string const& databaseName,
                       std::string const& tableName, unsigned int chunk, bool isOverlap,
                       std::string const& outputFilePath, ColumnSeparator columnSeparator = COMMA,
                       std::string const& authKey = std::string());

    ExportClient() = delete;
    ExportClient(ExportClient const&) = delete;
    ExportClient& operator=(ExportClient const&) = delete;

    /// Non-trivial destructor is needed to close a connection to the server
    ~ExportClient();

    /**
     * Receive the whole file. Note, this is a blocking operation
     * for a thread which calls the method.
     *
     * @throws ExportClientError for any problem occurred when receiving the file
     *   content from a server
     */
    void receive();

    /// @return the number of rows received from a a server
    size_t totalNumRows() const { return _totalNumRows; }

    /// @return the number of bytes written into an input file
    size_t sizeBytes() const { return _sizeBytes; }

private:
    ExportClient(std::string const& workerHost, uint16_t workerPort, std::string const& databaseName,
                 std::string const& tableName, unsigned int chunk, bool isOverlap,
                 std::string const& outputFilePath, ColumnSeparator columnSeparator,
                 std::string const& authKey);

    /// @return a context string for the logger and exceptions
    std::string _context(std::string const& func) const {
        return "ExportClient::" + func + "[" + _workerHost + ":" + std::to_string(_workerPort) + "]  ";
    }

    /**
     * Establish a connection with the service.
     *
     * @throws ExportClientError for any problem occurred when establishing a connection.
     */
    void _connectImpl();

    /**
     * Send a message to a server.
     *
     * @param message The message to be serialized and sent.
     * @param context The context of the on-going operation for error reporting.
     *
     * @throws ExportClientError for any problem occurred during communication
     *   with a server.
     */
    template <class MESSAGE>
    void _send(MESSAGE const& message, std::string const& context) {
        _bufferPtr->resize();
        _bufferPtr->serialize(message);
        boost::system::error_code ec;
        boost::asio::write(_socket, boost::asio::buffer(_bufferPtr->data(), _bufferPtr->size()), ec);
        _assertErrorCode(ec, __func__, context);
    }

    /**
     * Receive a message of the specified type from the server.
     *
     * @param message An object to be initialized upon successful completion of the operation.
     * @param context The context of the on-going operation for error reporting.
     *
     * @throws ExportClientError for any problem occurred when communicating
     *   with a server or message parsing.
     */
    template <class MESSAGE>
    void _receive(MESSAGE& message, std::string const& context) {
        uint32_t const messageLengthBytes = _receiveFrameHeaderAndBody(context);
        try {
            _bufferPtr->parse(message, messageLengthBytes);
        } catch (std::exception const& ex) {
            _abort(__func__, "message parsing failed: " + std::string(ex.what()));
        }
    }

    /**
     * Read and parse the protocol frame header carrying the length of the subsequent
     * message body. Also, read the body of the message into the buffer. Upon
     * the successful completion of the method тhe buffer will contain the message
     * to be parsed.
     *
     * @param context The context of the on-going operation for error reporting.
     * @return The number of bytes in the message body.
     */
    uint32_t _receiveFrameHeaderAndBody(std::string const& context);

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
     * @throws ExportClientError if a problem was found
     */
    void _assertErrorCode(boost::system::error_code const& ec, std::string const& func,
                          std::string const& msg);

    /**
     * Unconditionally abort the operation by shutting down and closing
     * the server connection, logging a error message and throwing an exception.
     *
     * @param func the name of a method which requested the abort
     * @param error an error message to be reported
     * @throws ExportClientError is always thrown by the method
     */
    void _abort(std::string const& func, std::string const& error);

    /// Make an attempt to shutdown and close a connection with the server
    void _closeConnection();

    // Input parameters

    std::string const _workerHost;
    uint16_t const _workerPort;
    std::string const _databaseName;
    std::string const _tableName;
    unsigned int const _chunk;
    bool const _isOverlap;
    std::string const _outputFilePath;
    ColumnSeparator const _columnSeparator;
    std::string const _authKey;

    // Buffer for data moved over the network. The initial buffer capacity
    // would be adjusted during the initial handshake with the server.

    size_t _bufferCapacity;
    std::unique_ptr<ProtocolBuffer> _bufferPtr;

    /// The maximum number of rows to be requested from the server. A value of
    /// this parameter is adjusted upon receiving the first batch of rows from
    /// the server.
    long _numRowsPerReceive = 1;

    boost::asio::io_service _io_service;
    boost::asio::ip::tcp::socket _socket;

    bool _received = false;  /// Set to 'true' after a successful completion of
                             /// the table export.

    size_t _totalSizeBytes = 0;  /// The number of bytes expected from a server (set during the handshake)
    size_t _sizeBytes = 0;       /// The number of bytes written into an output file so far
    size_t _totalNumRows = 0;    /// The number of received received from a server so far
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_EXPORTCLIENT_H
