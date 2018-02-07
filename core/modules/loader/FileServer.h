// -*- LSST-C++ -*-
/*
 * LSST Data Management System
 * Copyright 2017 LSST Corporation.
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
#ifndef LSST_QSERV_LOADER_FILESERVER_H
#define LSST_QSERV_LOADER_FILESERVER_H

// System headers
#include <boost/asio.hpp>
#include <memory>

// Qserv headers
// #include "replica/FileServerConnection.h" &&& old?


namespace lsst {
namespace qserv {

namespace proto {
class LoaderFileRequest;
}

/* &&& old?
namespace loader {

class FileServerConnection;

class FileServerConfig {
public:
    using Ptr = std::shared_ptr<FileServerConfig>;

    FileServerConfig() {}

    FileServerConfig(FileServerConfig const&) = delete;
    FileServerConfig& operator=(FileServerConfig const&) = delete;

    int getPort() { return _port; }
    int getTargetPoolSize() { return _targetPoolSize; }
    int getFileBufferSize() { return _fileBufferSize; }

private:
    int _port{13333};
    int _targetPoolSize{5};
    int _fileBufferSize{4000000}; // &&& needs a better guess.
};
*/
namespace loader {

class FileServerConnection;

class FileServerConfig {
public:
    using Ptr = std::shared_ptr<FileServerConfig>;

    FileServerConfig() {}

    FileServerConfig(FileServerConfig const&) = delete;
    FileServerConfig& operator=(FileServerConfig const&) = delete;

    int getPort() { return _port; }
    int getTargetPoolSize() { return _targetPoolSize; }
    int getFileBufferSize() { return _fileBufferSize; }

private:
    int _port{13333};
    int _targetPoolSize{5};
    int _fileBufferSize{4000000}; // &&& needs a better guess.
};


/// This class is meant to simply provide the requested file.
/// Based on FileServer in Igor Gaponenko's replication system.
class FileServer : public std::enable_shared_from_this<FileServer>  {
public:
<<<<<<< 5b7a8886eb7965917f9ed0b73048fee54786e72e
/* &&& old?
=======
>>>>>>> Modified FileServerConnection.
    using Ptr = std::shared_ptr<FileServer>;

    using DataType = char;
    using DataBuffer = std::vector<DataType>;
    using DataSizeType = uint32_t; // If this changes, fix associated ntohl calls.
    using DataBufferPtr = std::shared_ptr<DataBuffer>;
<<<<<<< 5b7a8886eb7965917f9ed0b73048fee54786e72e
*/
    typedef std::shared_ptr<FileServer> Ptr;

    // Factory function to ensure proper creation for enable_shared_from_this.
    static Ptr create(FileServerConfig::Ptr const& fileServerConfig);

    FileServer () = delete;
    FileServer (FileServer const&) = delete;
    FileServer & operator= (FileServer const&) = delete;

    /// Where the work happens, run in a separate thread.
    void run ();

    FileServerConfig::Ptr getConfig() { return _fileServerConfig; }
    boost::asio::io_service& getIoService() { return _io_service; }

private:
    explicit FileServer (FileServerConfig::Ptr const& fileServerConfig);

    /// Following function accept and handle the file requests.
    void beginAccept ();  // &&& add _
    void handleAccept (std::shared_ptr<FileServerConnection> const& connection,
                       boost::system::error_code const&             ec);

    /// Return the context string
    std::string context () const { return "FILE-SERVER  "; } // &&& is this needed?

    FileServerConfig::Ptr _fileServerConfig;

    boost::asio::io_service        _io_service; // &&& change to _ioService
    boost::asio::ip::tcp::acceptor _acceptor;
};



/**
  * Class FileServerConnection is used for handling file read requests from
  * remote clients. One instance of the class serves one file from one client
  * at a time.
  *
  * Objects of this class are inistantiated by FileServer. After that
  * the server calls this class's method beginProtocol() which starts
  * a series of asynchronous operations to communicate with remote client.
  * When all details of an incoming request are obtained from the client
  * the connection object begins actual processing of the request and
  * communicates with a client as required by the file transfer protocol.
  * All communications are asynchronous using Google protobuf.
  *
  * The lifespan of this object is exactly one request until it's fully
  * satisfied or any failure during request execution (when reading a file,
  * or communicating with a client) occures. When this happens the object
  * stops doing anything.
  *
  * This is essentially an RPC protocol which runs in a loop this sequence of steps
  * starting with a call to beginProtocol().
  *   - ASYNC: read a frame header of a request
  *   - SYNC:  read the request header (a specification of a file, etc.)
  *   - ASYNC: write a frame header of a reply to the request
  *            followed by a status (to tell a client if the specified file
  *            is available or not, and if so then what would be its size, etc.)
  *   - ASYNC: if the request is accepted then begin streaming the content of
  *            a file in a series of records until it's done.
  *
  * NOTES: A reason why the read phase is split into three steps is
  *        that a client is expected to send all components of the request
  *        (frame header and request header) at once. This means
  *        the whole incomming message will be already available on the server's
  *        host memory when an asyncronous handler for the frame header will fire.
  *        However, due to a variable length of the request we should know its length
  *        before attempting to read the rest of the incomming message as this (the later)
  *        will require two things: 1) to ensure we have enough buffer space
  *        allocated, and 2) to tell the asynchrnous reader function
  *        how many bytes exactly are we going to read.
  *
  * The chain ends when a client disconnects or when an error condition
  * is met.
  *
  * Based on FileServerConnection in Igor Gaponenko's replication system.
  */
class FileServerConnection :   public std::enable_shared_from_this<FileServerConnection> {
public:
    using Ptr = std::shared_ptr<FileServerConnection>;

    /// Factory to ensure correct construction for enable_shared_from_this.
    static Ptr create(FileServer::Ptr const& fileServer);

    FileServerConnection() = delete;
    FileServerConnection(FileServerConnection const&) = delete;
    FileServerConnection& operator=(FileServerConnection const&) = delete;

    virtual ~FileServerConnection();

    /// Return a network socket associated with the connection.
    boost::asio::ip::tcp::socket& socket() { return _socket; }

    ///Begin communicating asynchroniously with a client.
    void beginProtocol();

private:
    FileServerConnection (FileServer::Ptr const& fileServer,
            std::shared_ptr<FileServerConfig> const& fileServerConfig);

    ///Begin reading the frame header of a new request
    void receiveRequest (); // &&& add _

    /// Parse the request and begin file transfer.
    void requestReceived (boost::system::error_code const& ec, size_t bytes_transferred); // &&& add _

    /// Begin sending a result back to a client
    void sendResponse (); // &&& add _

    /// Send the next record for the file
    void sendData (); // &&& add _

    /// The callback on finishing (either successfully or not) of aynchronious writes.
    void responseSent (boost::system::error_code const& ec, size_t bytes_transferred); // &&& add _

    /// The callback on finishing (either successfully or not) of aynchronious writes.
    void dataSent (boost::system::error_code const& ec, size_t bytes_transferred);

    bool _isErrorCode (boost::system::error_code ec, std::string const& scope);
/* &&& old
    bool _readIntoBuffer (FileServer::DataBuffer &buff);
    bool _readMessage(FileServer::DataBuffer &lengthBuff, proto::LoaderFileRequest &message);
    FileServer::DataSizeType _parseMsgLength(FileServer::DataBuffer &buff);
*/
    std::weak_ptr<FileServer> _fileServer;
    boost::asio::ip::tcp::socket _socket;

/* &&& old?
    bool _readIntoBuffer (FileServer::DataBuffer &buff);
    bool _readMessage(FileServer::DataBuffer &lengthBuff, proto::LoaderFileRequest &message);
    FileServer::DataSizeType _parseMsgLength(FileServer::DataBuffer &buff);

    std::weak_ptr<FileServer> _fileServer;
    boost::asio::ip::tcp::socket _socket;

    // std::shared_ptr<ProtocolBuffer> _bufferPtr; ///< Buffer serialization. &&& delete

    /// Buffer for sending size of data. There can only one request at a time!
    FileServer::DataBuffer     _sizeBufVect;
    /// Buffer for the actual data. Destroy when done so we're not stuck with a giant buffer.
    FileServer::DataBufferPtr _buffer;

    std::shared_ptr<proto::LoaderFileRequest> _protoLoaderFileRequest;
*/
    std::shared_ptr<ProtocolBuffer> _bufferPtr; ///< Buffer serialization.

    std::string _fileName; ///< The name of the file being transferred.
    std::FILE* _filePtr;   ///< The file.
    size_t _fileBufSize{0};   ///< The file record buffer size (bytes)
    std::vector<uint8_t> _fileVect; ///< container for the fileBuf
    uint8_t *_fileBuf;     ///< Pointer to the start of _fileVect's internal array.

};


}}} // namespace lsst::qserv::loader

#endif // LSST_QSERV_LOADER_FILESERVER_H
