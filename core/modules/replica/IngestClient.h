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

/**
 * This header represents the client-side API for the point-to-point
 * catalog data ingest service of the Replication system.
 */

// System headers
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>

// Third party headers
#include <boost/asio.hpp>

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class ProtocolBuffer;
    class ProtocolIngestResponse;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class IngestClientError represents exceptions thrown by IngestClient on errors
 */
class IngestClientError : public std::runtime_error {
public:
    /// @param what reason for the exception
    IngestClientError(std::string const& msg)
        :   std::runtime_error(msg) {
    }
};

/**
 * Class IngestClient is a client-side API for the point-to-point catalog
 * data ingest service.
 */
class IngestClient : public std::enable_shared_from_this<IngestClient>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<IngestClient> Ptr;

    enum ColumnSeparator {
        COMMA,
        TAB
    };

    /**
     * Establish a connection to the remote service. If the operation is successful
     * then a valid pointer will be returned and the data could be could be send via
     * method IngestClient::sendData(). Otherwise return the null pointer.
     *
     * @param workerHost
     *   the name or an IP address of a worker node where the ingest service
     *   is run
     *
     * @param workerPort
     *   the port number of the ingest service
     *
     * @param transactionId
     *   an identifier of a super-transaction which is required to be started
     *   before attempting the ingest
     *
     * @param tableName
     *   the base name of a table to be loaded. The table should not include
     *   partition numbers, 'overlap', etc.
     *
     * @param chunk
     *   the number of a chunk
     *
     * @param isOverlap
     *   a flag indicating if this is the chunk 'overlap' table
     *
     * @param inputFilePath
     *   the path (relative or absolute) name for a file whose content will be
     *   transferred to to the remote service
     * 
     * @param columnSeparator
     *   a character which separates columns within each row
     * 
     * @throws IngestClientError
     *   if any problem occurred when establishing a connection or during
     *   the initial handshake with the server
     */
    static Ptr connect(std::string const& workerHost,
                       uint16_t workerPort,
                       unsigned int transactionId,
                       std::string const& tableName,
                       unsigned int chunk,
                       bool isOverlap,
                       std::string const& inputFilePath,
                       ColumnSeparator columnSeparator=COMMA);

    // Default construction and copy semantics are prohibited

    IngestClient() = delete;
    IngestClient(IngestClient const&) = delete;
    IngestClient &operator=(IngestClient const&) = delete;

    /// Non-trivial destructor is needed to close a connection to the server
    ~IngestClient();

    /**
     * Send the whole file. Note, this is the blocking operation
     * for a thread which calls the method.
     * 
     * @throws IngestClientError
     *   if any problem occurred when sending the file content to the server
     */
    void send();

    /// @return the number of rows sent to a server
    size_t totalNumRows() const { return _totalNumRows; }

    /// @return the number of bytes read from an input file
    size_t sizeBytes() const { return _sizeBytes; }

private:


    /// @see IngestClient::connect()
    IngestClient(std::string const& workerHost,
                 uint16_t workerPort,
                 unsigned int transactionId,
                 std::string const& tableName,
                 unsigned int chunk,
                 bool isOverlap,
                 std::string const& inputFilePath,
                 ColumnSeparator columnSeparator);

    /// @return a context string for the logger and exceptions
    std::string _context(std::string const& func) const {
        return "IngestClient::" + func +
               "[" + _workerHost + ":" + std::to_string(_workerPort) + "]  ";

    }

    /**
     * Establish a connection with the service and perform the initial
     * handshake.
     * 
     * @throws IngestClientError
     *   if any problem occurred when establishing a connection or during
     *   the initial handshake with the server
     */
    void _connectImpl();

    /**
     * Read a response message from the server
     *
     * @param response
     *   an object to be initialized upon successful completion
     *   of the operation
     * 
     * @throws IngestClientError
     *   if any problem occurred when communicating with the server
     */
    void _readResponse(ProtocolIngestResponse& response);

    /**
     * Analyze a error condition and if there is a problem then report it
     * into the logging stream and throw an exception. The method will also
     * attempt to shutdown and close a connection with the server.
     * If no problem will be found in the error code the method will do nothing.
     *
     * @param ec
     *   the error code to be checked
     *
     * @param func
     *   the name of a method which requested the test
     *
     * @param msg
     *   a message to be reported in case of a problem. The will be extended
     *   with an explanation of the problem extracted from the error code.
     *
     * @throws IngestClientError
     *   if a problem was found
     */
    void _assertErrorCode(boost::system::error_code const& ec,
                          std::string const& func,
                          std::string const& msg);

    /**
     * Unconditionally abort the operation by shutting down and closing
     * the server connection, logging a error message and throwing an exception.
     *
     * @param func
     *   the name of a method which requested the abort
     *
     * @param error
     *   an error message to be reported
     *
     * @throws IngestClientError
     *   is always thrown by the method
     */
    void _abort(std::string const& func,
                std::string const& error);

    /// Make an attempt to shutdown and close a connection with the server
    void _closeConnection();

    // Input parameters

    std::string     const _workerHost;
    uint16_t        const _workerPort;
    unsigned int    const _transactionId;
    std::string     const _tableName;
    unsigned int    const _chunk;
    bool            const _isOverlap;
    std::string     const _inputFilePath;
    ColumnSeparator const _columnSeparator;

    // Buffer for data moved over the network. The initial buffer capacity
    // would be adjusted during the initial handshake with the server.

    size_t _bufferCapacity = 1024;
    std::unique_ptr<ProtocolBuffer> _bufferPtr;

    /// The maximum number of rows to be sent to the server. A value of
    /// this parameter is adjusted during the initial handshake with the server.
    size_t _numRowsPerSend = 1;

    boost::asio::io_service      _io_service;
    boost::asio::ip::tcp::socket _socket;

    bool _sent = false;     /// Set to 'true' after a successful completion of
                            /// the ingest.

    size_t _totalNumRows = 0;   /// The number of rows sent to a server
    size_t _sizeBytes = 0;      /// The number of bytes read from an input file
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_INGESTCLIENT_H
