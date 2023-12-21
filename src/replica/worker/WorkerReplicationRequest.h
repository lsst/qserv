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
#ifndef LSST_QSERV_REPLICA_WORKERREPLICATIONREQUEST_H
#define LSST_QSERV_REPLICA_WORKERREPLICATIONREQUEST_H

// System headers
#include <cstdio>
#include <cstdint>
#include <ctime>
#include <map>
#include <string>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/util/ReplicaInfo.h"
#include "replica/worker/WorkerRequest.h"

// Forward declarations
namespace lsst::qserv::replica {
class FileClient;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class WorkerReplicationRequest represents a context and a state of replication
 * requests within the worker servers. It can also be used for testing the framework
 * operation as its implementation won't make any changes to any files or databases.
 *
 * Real implementations of the request processing must derive from this class.
 */
class WorkerReplicationRequest : public WorkerRequest {
public:
    typedef std::shared_ptr<WorkerReplicationRequest> Ptr;

    /**
     * Static factory method is needed to prevent issue with the lifespan
     * and memory management of instances created otherwise (as values or via
     * low-level pointers).
     *
     * @param serviceProvider provider is needed to access the Configuration of
     *   a setup and for validating the input parameters
     * @param worker the name of a worker. It must be the same worker as the one
     *   where the request is going to be processed.
     * @param type the type name of a request
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
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestReplicate const& request);

    WorkerReplicationRequest() = delete;
    WorkerReplicationRequest(WorkerReplicationRequest const&) = delete;
    WorkerReplicationRequest& operator=(WorkerReplicationRequest const&) = delete;

    ~WorkerReplicationRequest() override = default;

    // Trivial get methods

    std::string const& database() const { return _request.database(); }
    unsigned int chunk() const { return _request.chunk(); }
    std::string const& sourceWorker() const { return _request.worker(); }
    std::string const& sourceWorkerHost() const { return _request.worker_host(); }
    uint16_t sourceWorkerPort() const { return _request.worker_port(); }
    std::string const& sourceWorkerHostPort() const { return _sourceWorkerHostPort; }
    std::string const& sourceWorkerDataDir() const { return _request.worker_data_dir(); }

    /**
     * Extract request status into the Protobuf response object.
     * @param response Protobuf response to be initialized
     */
    void setInfo(ProtocolResponseReplicate& response) const;

    bool execute() override;

protected:
    WorkerReplicationRequest(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                             std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                             unsigned int requestExpirationIvalSec, ProtocolRequestReplicate const& request);

    /// Result of the operation
    ReplicaInfo replicaInfo;

private:
    // Input parameters
    ProtocolRequestReplicate const _request;

    /// The cached connection parameters for the source worker (for error reporting and debugging).
    std::string const _sourceWorkerHostPort;
};

/**
 * Class WorkerReplicationRequestPOSIX provides an actual implementation for
 * the replication requests based on the direct manipulation of files on
 * a POSIX file system.
 */
class WorkerReplicationRequestPOSIX : public WorkerReplicationRequest {
public:
    typedef std::shared_ptr<WorkerReplicationRequestPOSIX> Ptr;

    /// @see WorkerReplicationRequest::created()
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestReplicate const& request);

    WorkerReplicationRequestPOSIX() = delete;
    WorkerReplicationRequestPOSIX(WorkerReplicationRequestPOSIX const&) = delete;
    WorkerReplicationRequestPOSIX& operator=(WorkerReplicationRequestPOSIX const&) = delete;

    ~WorkerReplicationRequestPOSIX() final = default;

    bool execute() final;

protected:
    WorkerReplicationRequestPOSIX(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                                  std::string const& id, int priority,
                                  ExpirationCallbackType const& onExpired,
                                  unsigned int requestExpirationIvalSec,
                                  ProtocolRequestReplicate const& request);
};

/**
 * Class WorkerReplicationRequestFS provides an actual implementation for
 * the replication requests based on the direct manipulation of local files
 * on a POSIX file system and for reading remote files using the built-into-worker
 * simple file server.
 */
class WorkerReplicationRequestFS : public WorkerReplicationRequest {
public:
    typedef std::shared_ptr<WorkerReplicationRequestFS> Ptr;

    /// @see WorkerReplicationRequest::created()
    static Ptr create(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                      std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                      unsigned int requestExpirationIvalSec, ProtocolRequestReplicate const& request);

    WorkerReplicationRequestFS() = delete;
    WorkerReplicationRequestFS(WorkerReplicationRequestFS const&) = delete;
    WorkerReplicationRequestFS& operator=(WorkerReplicationRequestFS const&) = delete;

    /// Destructor (non trivial one is needed to release resources)
    ~WorkerReplicationRequestFS() final;

    bool execute() final;

protected:
    /// @see WorkerReplicationRequestFS::create()
    WorkerReplicationRequestFS(ServiceProvider::Ptr const& serviceProvider, std::string const& worker,
                               std::string const& id, int priority, ExpirationCallbackType const& onExpired,
                               unsigned int requestExpirationIvalSec,
                               ProtocolRequestReplicate const& request);

private:
    /**
     * Open files associated with the current state of iterator _fileItr.
     *
     * @param lock lock which must be acquired before calling this method
     * @return 'false' in case of any error
     */
    bool _openFiles(replica::Lock const& lock);

    /**
     * The final stage to be executed just once after copying the content
     * of the remote files into the local temporary ones. It will rename
     * the temporary files into the standard ones.
     *
     * Resources will also be released.
     *
     * @param lock A lock to be acquired before calling this method
     * @return always 'true'
     */
    bool _finalize(replica::Lock const& lock);

    /**
     * Close connections, de-allocate resources, etc.
     *
     * Any connections and open files will be closed, the buffers will be
     * released to prevent unnecessary resource utilization. Note that
     * request objects can stay in the server's memory for an extended
     * period of time.
     *
     * @param lock A lock to be acquired before calling this method
     */
    void _releaseResources(replica::Lock const& lock);

    /**
     * Update file migration statistics
     *
     * @param lock A lock to be acquired before calling this method
     */
    void _updateInfo(replica::Lock const& lock);

    /// Cached descriptor of the database obtained from the Configuration
    DatabaseInfo const _databaseInfo;

    /// The flag indicating if the initialization phase of the operation
    /// has already completed
    bool _initialized;

    /// Short names of files to be copied
    std::vector<std::string> const _files;

    /// The iterator pointing to the currently processed file.
    /// If it's set to _files.end() then it means the operation
    /// has finished.
    std::vector<std::string>::const_iterator _fileItr;

    /// This object represents the currently open (if any) input file
    /// on the source worker node
    std::shared_ptr<FileClient> _inFilePtr;

    /// The file pointer for the temporary output file
    std::FILE* _tmpFilePtr;

    /// The FileDescr structure encapsulates various parameters of a file
    struct FileDescr {
        /// The input file size as reported by a remote server
        size_t inSizeBytes = 0;

        /// The actual number of bytes read so far (changes as the operation
        /// is progressing)
        size_t outSizeBytes = 0;

        /// The last modification time of the file (seconds since UNISX Epoch)
        std::time_t mtime = 0;

        /// Control sum computed locally while copying the file
        uint64_t cs = 0;

        /// The absolute path of a temporary file at a local directory.
        boost::filesystem::path tmpFile;

        /// The final (canonic) file name the temporary file will be renamed as
        /// upon a successful completion of the operation.
        boost::filesystem::path outFile;

        /// When the file transfer started
        uint64_t beginTransferTime = 0;

        /// When the file transfer ended
        uint64_t endTransferTime = 0;
    };

    /// Cached file descriptions mapping from short file names into
    /// the corresponding parameters
    std::map<std::string, FileDescr> _file2descr;

    /// The buffer for storing file payload read from a remote file service
    uint8_t* _buf;

    /// The size of the buffer
    size_t _bufSize;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_WORKERREPLICATIONREQUEST_H
