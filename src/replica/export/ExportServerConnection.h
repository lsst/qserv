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
#ifndef LSST_QSERV_REPLICA_EXPORTSERVERCONNECTION_H
#define LSST_QSERV_REPLICA_EXPORTSERVERCONNECTION_H

// System headers
#include <fstream>
#include <memory>

// Third party headers
#include "boost/asio.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/protocol.pb.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/Common.h"
#include "replica/util/Csv.h"
#include "replica/util/ProtocolBuffer.h"

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class ExportServerConnection is used in the server-side implementation of
 * the point-to-point table exporting service of the Replication system.
 * Each object of the class handles a dedicated client.
 *
 * Objects of this class are instantiated by ExportServer. After that
 * the server calls this class's method beginProtocol() which starts
 * a series of asynchronous operations to communicate with remote client.
 * When all details of an incoming request are obtained from the client
 * the connection object begins actual processing of the request and
 * communicates with a client as required by the file transfer protocol.
 * All communications are asynchronous and they're using Google Protobuf.
 *
 * The lifespan of this object is exactly one request until it's fully
 * satisfied or any failure during request execution (when fetching data from
 * a database, or communicating with a client) occurs. When this happens the object
 * stops doing anything.
 */
class ExportServerConnection : public std::enable_shared_from_this<ExportServerConnection> {
public:
    typedef std::shared_ptr<ExportServerConnection> Ptr;

    /// This parameter determines a suggested size of the messages sent to clients
    static size_t networkBufSizeBytes;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider is needed to access Configuration
     * @param workerName  the name of a worker this service is acting
     * upon (used to pull worker-specific configuration options for
     * the service)
     * @param io_service service object for the network I/O operations
     */
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                      boost::asio::io_service& io_service);

    ExportServerConnection() = delete;
    ExportServerConnection(ExportServerConnection const&) = delete;
    ExportServerConnection& operator=(ExportServerConnection const&) = delete;

    /// Destructor (non-trivial because some resources need to be properly released)
    ~ExportServerConnection();

    /// @return network socket associated with the connection.
    boost::asio::ip::tcp::socket& socket() { return _socket; }

    /**
     * Begin communicating asynchronously with a client. See further details
     * in the implementation file of this class.
     */
    void beginProtocol();

private:
    ExportServerConnection(ServiceProvider::Ptr const& serviceProvider, std::string const& workerName,
                           boost::asio::io_service& io_service);

    /// Initiate (ASYNC) read of the handshake request from a client
    void _receiveHandshake();

    /**
     * The callback on finishing (either successfully or not) of the asynchronous
     * read of the handshake request from a client. The request will be parsed,
     * analyzed and if everything is right then the table dump will be taken and
     * and stored at the corresponding location. After that an invitation
     * to proceed with the data transfer protocol will be sent asynchronously
     * to the client.
     *
     * @param ec A error code to be evaluated.
     * @param bytes_transferred  The number of bytes received from a client.
     */
    void _handshakeReceived(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Initiate a reply (ASYNC) to a client to the "handshake" request.
     *
     * @param error  An optional message to be delivered to a client.
     */
    void _sendHandshakeResponse(std::string const& error = std::string());

    /**
     * The callback on finishing (either successfully or not) of the asynchronous response
     * to the handshake confirmation.
     *
     * @param ec A error code to be evaluated.
     * @param bytes_transferred  The number of bytes received from a client.
     */
    void _handshakeResponseSent(boost::system::error_code const& ec, size_t bytes_transferred);

    /// Initiate (ASYNC) read of the next data request from a client
    void _receiveDataRequest();

    /**
     * The callback on finishing (either successfully or not) of asynchronous request
     * for the next batch of data from a client. This method will read (SYNC) a body
     * of the message. Then the next batch of data will be read from the table dump
     * file and sent (ASYNC) to the client.
     *
     * @param ec A error code to be evaluated.
     * @param bytes_transferred  The number of bytes received from a client.
     */
    void _dataRequestReceived(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * The callback on finishing (either successfully or not) of the asynchronous response
     * to the client's data request. In case of the successful completion of the operation
     * the server will ...
     *
     * @param ec A error code to be evaluated.
     * @param bytes_transferred  The number of bytes received from a client.
     */
    void _dataResponseSent(boost::system::error_code const& ec, size_t bytes_transferred);

    /**
     * Send back a message with status FAILED and the error message.
     *
     * @param error  A message to be delivered to a client.
     * @param handshakeError  If the flag is set to 'true' then a client needs to be informed
     *   on a problem occured during the initial handshake.
     */
    void _failed(std::string const& error, bool handshakeError = false) {
        _closeFile();
        if (handshakeError) _sendHandshakeResponse(error);
    }

    /// Dump the content of the table into the output file
    void _dumpTableIntoFile() const;

    /// Make sure the currently open/created file gets closed and deleted
    void _closeFile();

    // Input parameters

    ServiceProvider::Ptr const _serviceProvider;
    std::string const _workerName;

    /// A socket for communication with clients
    boost::asio::ip::tcp::socket _socket;

    /// Buffer management class facilitating serialization/de-serialization
    /// of data sent over the network
    std::shared_ptr<ProtocolBuffer> const _bufferPtr;

    // Parameters defining a scope of the operation are set from the handshake
    // request received from a client.

    std::string _database;
    std::string _table;
    unsigned int _chunk = 0;
    bool _isOverlap = false;
    csv::Dialect _dialect;

    /// The database descriptor and the state of the table are set after receiving
    /// and validating a handshake message from a client.
    DatabaseInfo _databaseInfo;
    bool _isPartitioned = false;

    // A file for temporary storing a TSV/CSV dump of a table before (while)
    // sending its content to a client.

    std::string _fileName;  ///< An absolute path name for the file
    std::ifstream _file;    ///< The input file stream

    size_t _fileSizeBytes = 0;     ///< The total number of bytes in the file
    size_t _totalNumRowsSent = 0;  ///< The number of rows sent so far
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_EXPORTSERVERCONNECTION_H
