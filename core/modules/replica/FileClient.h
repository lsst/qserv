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
#ifndef LSST_QSERV_REPLICA_FILECLIENT_H
#define LSST_QSERV_REPLICA_FILECLIENT_H

/**
 * This header represents the client-side API for the point-to-point
 * file migration service of the Replication system.
 */

// System headers
#include <ctime>
#include <memory>
#include <stdexcept>
#include <string>

// Qserv headers
#include "replica/Configuration.h"

// Third party headers
#include <boost/asio.hpp>

// Qserv headers
#include "replica/ServiceProvider.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class ProtocolBuffer;

/**
 * Class FileClientError represents exceptions thrown by FileClient on errors
 */
class FileClientError : public std::runtime_error {
public:
    
    /**
     * Construct an object
     *
     * @param what - reason for the exception
     */
    FileClientError(std::string const& msg)
        :   std::runtime_error(msg) {
    }
};

/**
  * Class FileClient is a client-side API for the point-to-point file migration
  * service.
  */
class FileClient : public std::enable_shared_from_this<FileClient>  {

public:

    /// The pointer type for instances of the class
    typedef std::shared_ptr<FileClient> Ptr;

    /**
     * Open a file and return a smart pointer to an object of this class.
     * If the operation is successful then a valid pointer will be returned
     * and the file content could be read via method FileClient::read().
     * Otherwise return the null pointer.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param workerName      - the name of a worker where the file resides
     * @param databaseName    - the name of a database the file belongs to
     * @param fileName        - the file to read or examine
     */
    static Ptr open(ServiceProvider::Ptr const& serviceProvider,
                    std::string const& workerName,
                    std::string const& databaseName,
                    std::string const& fileName) {

        return instance(serviceProvider,
                        workerName,
                        databaseName,
                        fileName,
                        true /* readContent */);
    }

    /**
     * Open a file and return a smart pointer to an object of this class.
     * If the operation is successful then a valid pointer will be returned.
     * If the operation fails the method will return the null pointer.
     *
     * ATTENTION:
     *   Unlike the previous method FileClient::open() the returned file object
     *   can't be used to read the file content (via FileClient::read()).
     *   The method of opening files is meant to be used for checking the availability
     *   of files and getting various metadata (size, etc.) about the files.
     *   Any attempts to call method FileClient::read() will result in
     *   throwing FileClientError.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param workerName      - the name of a worker where the file resides
     * @param databaseName    - the name of a database the file belongs to
     * @param fileName        - the file to read or examine
     */
    static Ptr stat(ServiceProvider::Ptr const& serviceProvider,
                    std::string const& workerName,
                    std::string const& databaseName,
                    std::string const& fileName) {

        return instance(serviceProvider,
                        workerName,
                        databaseName,
                        fileName,
                        false /* readContent */);
    }

    // Default construction and copy semantics are prohibited

    FileClient() = delete;
    FileClient(FileClient const&) = delete;
    FileClient &operator=(FileClient const&) = delete;

    ~FileClient() = default;

    // Trivial get methods

    std::string const& worker() const;
    std::string const& database() const;
    std::string const& file() const;

    /// @return the size of a file (as reported by a server)
    size_t size() const { return _size; }

    /// @return the last modification time (mtime) of the file
    std::time_t mtime() const { return _mtime; }

    /**
     * Read (up to, but not exceeding) the specified number of bytes into a buffer.
     *
     * The method will throw the FileClientError exception should any error
     * occurs during the operation. Illegal parameters (zero buffer pointer
     * or the buffer size of 0) will be reported by std::invalid_argument exception.
     *
     * @param buf     - a pointer to a valid buffer where the data will be placed
     * @param bufSize - a size of the buffer (would determine the maximum number of bytes
     *                  which can be read at a single call to the method)
     *
     * @return the actual number of bytes read or 0 if the end of file reached
     */
    size_t read(uint8_t* buf, size_t bufSize);

private:

    /**
     * Open a file in the requested mode and return a smart pointer to an object
     * of this class. If the operation is successful then a valid pointer will
     * be returned.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param workerName      - the name of a worker where the file resides
     * @param databaseName    - the name of a database the file belongs to
     * @param fileName        - the file to read or examine
     * @param readContent     - the mode in which the file will be used
     */
    static Ptr instance(ServiceProvider::Ptr const& serviceProvider,
                        std::string const& workerName,
                        std::string const& databaseName,
                        std::string const& fileName,
                        bool readContent);

    /**
     * Construct an object with the specified configuration.
     *
     * The constructor may throw the std::invalid_argument exception after
     * validating its arguments.
     *
     * @param serviceProvider - for configuration, etc. services
     * @param workerName      - the name of a worker where the file resides
     * @param databaseName    - the name of a database the file belongs to
     * @param fileName        - the file to read or examine
     * @param readContent     - indicates if a file is open for reading its content
     */
    FileClient(ServiceProvider::Ptr const& serviceProvider,
               std::string const& workerName,
               std::string const& databaseName,
               std::string const& fileName,
               bool readContent);

    /**
     * Try opening the file
     *
     * @return 'true' if successful
     */
    bool _openImpl();

private:

    /// Cached worker descriptors obtained from the Configuration
    WorkerInfo const _workerInfo;

    /// Cached database descriptors obtained from the Configuration
    DatabaseInfo const _databaseInfo;

    /// The name of a file to be read
    std::string const _fileName;

    /// The flag indicating of the file was open with an intend of reading
    // its content
    bool const _readContent;

    /// Buffer for data moved over the network
    std::unique_ptr<ProtocolBuffer> _bufferPtr;

    // The mutable state of the object

    boost::asio::io_service      _io_service;
    boost::asio::ip::tcp::socket _socket;

    /// The size of the file in bytes (to be determined after contacting a server)
    size_t _size;

    /// The last modification time (mtime) of the file
    std::time_t _mtime;

    /// The flag which will be set after hitting the end of the input stream
    bool _eof;
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_FILECLIENT_H
