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

// Class header
#include "replica/worker/WorkerReplicationRequest.h"

// System headers
#include <cerrno>
#include <cstring>
#include <stdexcept>

// Qserv headers
#include "replica/config/ConfigDatabase.h"
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/FileUtils.h"
#include "replica/worker/FileClient.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerReplicationRequest");

}  // namespace

namespace lsst::qserv::replica {

WorkerReplicationRequest::Ptr WorkerReplicationRequest::create(ServiceProvider::Ptr const& serviceProvider,
                                                               string const& worker, string const& id,
                                                               int priority,
                                                               ExpirationCallbackType const& onExpired,
                                                               unsigned int requestExpirationIvalSec,
                                                               ProtocolRequestReplicate const& request) {
    auto ptr = WorkerReplicationRequest::Ptr(new WorkerReplicationRequest(
            serviceProvider, worker, id, priority, onExpired, requestExpirationIvalSec, request));
    ptr->init();
    return ptr;
}

WorkerReplicationRequest::WorkerReplicationRequest(ServiceProvider::Ptr const& serviceProvider,
                                                   string const& worker, string const& id, int priority,
                                                   ExpirationCallbackType const& onExpired,
                                                   unsigned int requestExpirationIvalSec,
                                                   ProtocolRequestReplicate const& request)
        : WorkerRequest(serviceProvider, worker, "REPLICATE", id, priority, onExpired,
                        requestExpirationIvalSec),
          _request(request),
          _sourceWorkerHostPort(request.worker_host() + ":" + to_string(request.worker_port())),
          _initialized(false),
          _tmpFilePtr(nullptr),
          _buf(0),
          _bufSize(serviceProvider->config()->get<size_t>("worker", "fs-buf-size-bytes")) {}

WorkerReplicationRequest::~WorkerReplicationRequest() {
    replica::Lock lock(_mtx, context(__func__));
    _releaseResources(lock);
}

void WorkerReplicationRequest::setInfo(ProtocolResponseReplicate& response) const {
    LOGS(_log, LOG_LVL_DEBUG, context(__func__));
    replica::Lock lock(_mtx, context(__func__));
    response.set_allocated_target_performance(performance().info().release());
    response.set_allocated_replica_info(_replicaInfo.info().release());
    *(response.mutable_request()) = _request;
}

bool WorkerReplicationRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG,
         context(__func__) << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
                           << "  database: " << database() << "  chunk: " << chunk());

    // Validate parameters of the request
    if (worker() == sourceWorker()) {
        throw invalid_argument(context(__func__) + "workers are the same in the request.");
    }
    if (sourceWorkerHost().empty()) {
        throw invalid_argument(context(__func__) +
                               "the DNS name or an IP address of the worker not provided.");
    }
    if (_request.worker_port() > std::numeric_limits<uint16_t>::max()) {
        throw overflow_error(context(__func__) + "the port number " + to_string(_request.worker_port()) +
                             " is not in the valid range of 0.." +
                             to_string(std::numeric_limits<uint16_t>::max()));
    }
    if (sourceWorkerDataDir().empty()) {
        throw invalid_argument(context(__func__) + "the data path name at the remote worker not provided.");
    }

    replica::Lock lock(_mtx, context(__func__));
    checkIfCancelling(lock, __func__);

    // Obtain the list of files to be migrated
    //
    // IMPLEMENTATION NOTES:
    //
    // - Note using the overloaded operator '/' which is used to form
    //   folders and files path names below. The operator will concatenate
    //   names and also insert a file separator for an operating system
    //   on which this code will get compiled.
    //
    // - Temporary file names at a destination folders are prepended with
    //   prefix '_' to prevent colliding with the canonical names. They will
    //   be renamed in the last step.
    //
    // - All operations with the file system namespace (creating new non-temporary
    //   files, checking for folders and files, renaming files, creating folders, etc.)
    //   are guarded by acquiring replica::Lock lock(_mtxDataFolderOperations) where it's needed.

    WorkerRequest::ErrorContext errorContext;

    ///////////////////////////////////////////////////////
    //       Initialization phase (runs only once)       //
    ///////////////////////////////////////////////////////

    if (!_initialized) {
        _initialized = true;

        // The method will throw ConfigUnknownDatabase if the database is invalid.
        DatabaseInfo const databaseInfo = _serviceProvider->config()->databaseInfo(_request.database());

        // Cache the collection of short names of files to be copied.
        _files = FileUtils::partitionedFiles(databaseInfo, _request.chunk());

        fs::path const outDir = fs::path(serviceProvider()->config()->get<string>("worker", "data-dir")) /
                                database::mysql::obj2fs(_request.database());

        vector<fs::path> tmpFiles;
        vector<fs::path> outFiles;
        for (auto&& file : _files) {
            fs::path const tmpFile = outDir / ("_" + file);
            tmpFiles.push_back(tmpFile);

            fs::path const outFile = outDir / file;
            outFiles.push_back(outFile);

            _file2descr[file].inSizeBytes = 0;
            _file2descr[file].outSizeBytes = 0;
            _file2descr[file].mtime = 0;
            _file2descr[file].cs = 0;
            _file2descr[file].tmpFile = tmpFile;
            _file2descr[file].outFile = outFile;
            _file2descr[file].beginTransferTime = 0;
            _file2descr[file].endTransferTime = 0;
        }

        // Check input files, check and sanitize the destination folder

        boost::system::error_code ec;
        {
            replica::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));

            // Check for a presence of input files and calculate space requirement

            uintmax_t totalBytes = 0;          // the total number of bytes in all input files to be moved
            map<string, uintmax_t> file2size;  // the number of bytes in each file

            for (auto&& file : _files) {
                // Open the file on the remote server in the no-content-read mode
                FileClient::Ptr inFilePtr = FileClient::stat(_serviceProvider, sourceWorkerHost(),
                                                             sourceWorkerPort(), _request.database(), file);
                errorContext =
                        errorContext or
                        reportErrorIf(not inFilePtr, ProtocolStatusExt::FILE_ROPEN,
                                      "failed to open input file on remote worker: " + sourceWorker() + " (" +
                                              sourceWorkerHostPort() + "), database: " + _request.database() +
                                              ", file: " + file);
                if (errorContext.failed) {
                    setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
                    return true;
                }
                file2size[file] = inFilePtr->size();
                totalBytes += inFilePtr->size();
                _file2descr[file].inSizeBytes = inFilePtr->size();
                _file2descr[file].mtime = inFilePtr->mtime();
            }

            // Check and sanitize the output directory

            bool const outDirExists = fs::exists(outDir, ec);
            errorContext =
                    errorContext or
                    reportErrorIf(ec.value() != 0, ProtocolStatusExt::FOLDER_STAT,
                                  "failed to check the status of output directory: " + outDir.string()) or
                    reportErrorIf(not outDirExists, ProtocolStatusExt::NO_FOLDER,
                                  "the output directory doesn't exist: " + outDir.string());

            // The files with canonical(!) names should NOT exist at the destination
            // folder.
            for (auto&& file : outFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext = errorContext or
                               reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FILE_STAT,
                                             "failed to check the status of output file: " + file.string()) or
                               reportErrorIf(fs::exists(stat), ProtocolStatusExt::FILE_EXISTS,
                                             "the output file already exists: " + file.string());
            }

            // Check if there are any files with the temporary names at the destination
            // folder and if so then get rid of them.
            for (auto&& file : tmpFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext =
                        errorContext or
                        reportErrorIf(stat.type() == fs::status_error, ProtocolStatusExt::FILE_STAT,
                                      "failed to check the status of temporary file: " + file.string());
                if (fs::exists(stat)) {
                    fs::remove(file, ec);
                    errorContext = errorContext or
                                   reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_DELETE,
                                                 "failed to remove temporary file: " + file.string());
                }
            }

            // Make sure a file system at the destination has enough space
            // to accommodate new files
            //
            // NOTE: this operation runs after cleaning up temporary files
            fs::space_info const space = fs::space(outDir, ec);
            errorContext =
                    errorContext or
                    reportErrorIf(
                            ec.value() != 0, ProtocolStatusExt::SPACE_REQ,
                            "failed to obtaine space information at output folder: " + outDir.string()) or
                    reportErrorIf(space.available < totalBytes, ProtocolStatusExt::NO_SPACE,
                                  "not enough free space availble at output folder: " + outDir.string());

            // Pre-create temporary files with the final size to assert disk space
            // availability before filling these files with the actual payload.
            for (auto&& file : _files) {
                fs::path const tmpFile = _file2descr[file].tmpFile;

                // Create a file of size 0
                FILE* tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
                errorContext = errorContext or
                               reportErrorIf(not tmpFilePtr, ProtocolStatusExt::FILE_CREATE,
                                             "failed to open/create temporary file: " + tmpFile.string() +
                                                     ", error: " + strerror(errno));
                if (tmpFilePtr) {
                    fflush(tmpFilePtr);
                    fclose(tmpFilePtr);
                }

                // Resize the file (will be filled with \0)
                fs::resize_file(tmpFile, file2size[file], ec);
                errorContext = errorContext or
                               reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_RESIZE,
                                             "failed to resize the temporary file: " + tmpFile.string());
            }
        }
        if (errorContext.failed) {
            setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
            return true;
        }

        // Allocate the record buffer
        _buf = new uint8_t[_bufSize];
        if (not _buf) {
            throw runtime_error("WorkerReplicationRequest::" + string(__func__) +
                                "  buffer allocation failed");
        }

        // Setup the iterator for the name of the very first file to be copied
        _fileItr = _files.begin();
        if (not _openFiles(lock)) return true;
    }

    // Copy the next record from the currently open remote file
    // into the corresponding temporary files at the destination folder
    // w/o acquiring the directory lock.
    //
    // NOTE: the while loop below is meant to skip files which are empty
    while (_files.end() != _fileItr) {
        // Copy the next record if any is available
        size_t num = 0;
        try {
            num = _inFilePtr->read(_buf, _bufSize);
            if (num) {
                if (num == fwrite(_buf, sizeof(uint8_t), num, _tmpFilePtr)) {
                    // Update the descriptor (the number of bytes copied so far
                    // and the control sum)
                    _file2descr[*_fileItr].outSizeBytes += num;
                    uint64_t& cs = _file2descr[*_fileItr].cs;
                    for (uint8_t *ptr = _buf, *end = _buf + num; ptr != end; ++ptr) {
                        cs += *ptr;
                    }

                    // Keep updating this stats while copying the files
                    _file2descr[*_fileItr].endTransferTime = util::TimeUtils::now();
                    _updateInfo(lock);

                    // Keep copying the same file
                    return false;
                }
                errorContext = errorContext or reportErrorIf(true, ProtocolStatusExt::FILE_WRITE,
                                                             "failed to write into temporary file: " +
                                                                     _file2descr[*_fileItr].tmpFile.string() +
                                                                     ", error: " + strerror(errno));
            }
        } catch (FileClientError const& ex) {
            errorContext =
                    errorContext or
                    reportErrorIf(true, ProtocolStatusExt::FILE_READ,
                                  "failed to read input file from remote worker: " + sourceWorker() + " (" +
                                          sourceWorkerHostPort() + "), database: " + _request.database() +
                                          ", file: " + *_fileItr);
        }

        // Make sure the number of bytes copied from the remote server
        // matches expectations.
        errorContext =
                errorContext or
                reportErrorIf(_file2descr[*_fileItr].inSizeBytes != _file2descr[*_fileItr].outSizeBytes,
                              ProtocolStatusExt::FILE_READ,
                              "short read of the input file from remote worker: " + sourceWorker() + " (" +
                                      sourceWorkerHostPort() + "), database: " + _request.database() +
                                      ", file: " + *_fileItr);
        if (errorContext.failed) {
            setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
            _releaseResources(lock);
            return true;
        }

        // Flush and close the current file
        fflush(_tmpFilePtr);
        fclose(_tmpFilePtr);
        _tmpFilePtr = 0;

        // Keep updating this stats after finishing to copy each file
        _file2descr[*_fileItr].endTransferTime = util::TimeUtils::now();
        _updateInfo(lock);

        // Move the iterator to the name of the next file to be copied
        ++_fileItr;
        if (_files.end() != _fileItr) {
            if (not _openFiles(lock)) {
                _releaseResources(lock);
                return true;
            }
        }
    }

    // Finalize the operation, de-allocate resources, etc.
    return _finalize(lock);
}

bool WorkerReplicationRequest::_openFiles(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context(__func__) << "  sourceWorkerHostPort: " << sourceWorkerHostPort() << "  database: "
                           << _request.database() << "  chunk: " << chunk() << "  file: " << *_fileItr);

    WorkerRequest::ErrorContext errorContext;

    // Open the input file on the remote server
    _inFilePtr = FileClient::open(_serviceProvider, sourceWorkerHost(), sourceWorkerPort(),
                                  _request.database(), *_fileItr);
    errorContext = errorContext or
                   reportErrorIf(not _inFilePtr, ProtocolStatusExt::FILE_ROPEN,
                                 "failed to open input file on remote worker: " + sourceWorker() + " (" +
                                         sourceWorkerHostPort() + "), database: " + _request.database() +
                                         ", file: " + *_fileItr);
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return false;
    }

    // Reopen a temporary output file locally in the 'append binary mode'
    // then 'rewind' to the beginning of the file before writing into it.
    fs::path const tmpFile = _file2descr[*_fileItr].tmpFile;

    _tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
    errorContext = errorContext or reportErrorIf(not _tmpFilePtr, ProtocolStatusExt::FILE_OPEN,
                                                 "failed to open temporary file: " + tmpFile.string() +
                                                         ", error: " + strerror(errno));
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return false;
    }
    rewind(_tmpFilePtr);
    _file2descr[*_fileItr].beginTransferTime = util::TimeUtils::now();
    return true;
}

bool WorkerReplicationRequest::_finalize(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         context(__func__) << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
                           << "  database: " << database() << "  chunk: " << chunk());

    // Unconditionally regardless of the completion of the file renaming attempt
    _releaseResources(lock);

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquiring the directory lock to guarantee a consistent view onto the folder.
    replica::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));

    // ATTENTION: as per ISO/IEC 9945 the file rename operation will
    //            remove empty files. Not sure if this should be treated
    //            in a special way?
    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code ec;
    for (auto&& file : _files) {
        fs::path const tmpFile = _file2descr[file].tmpFile;
        fs::path const outFile = _file2descr[file].outFile;

        fs::rename(tmpFile, outFile, ec);
        errorContext = errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_RENAME,
                                                     "failed to rename file: " + tmpFile.string());
        fs::last_write_time(outFile, _file2descr[file].mtime, ec);
        errorContext = errorContext or reportErrorIf(ec.value() != 0, ProtocolStatusExt::FILE_MTIME,
                                                     "failed to change 'mtime' of file: " + tmpFile.string());
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }
    setStatus(lock, ProtocolStatus::SUCCESS);
    return true;
}

void WorkerReplicationRequest::_updateInfo(replica::Lock const& lock) {
    size_t totalInSizeBytes = 0;
    size_t totalOutSizeBytes = 0;
    ReplicaInfo::FileInfoCollection fileInfoCollection;
    for (auto&& file : _files) {
        fileInfoCollection.emplace_back(
                ReplicaInfo::FileInfo({file, _file2descr[file].outSizeBytes, _file2descr[file].mtime,
                                       to_string(_file2descr[file].cs), _file2descr[file].beginTransferTime,
                                       _file2descr[file].endTransferTime, _file2descr[file].inSizeBytes}));
        totalInSizeBytes += _file2descr[file].inSizeBytes;
        totalOutSizeBytes += _file2descr[file].outSizeBytes;
    }
    ReplicaInfo::Status const status =
            (_files.size() == fileInfoCollection.size()) and (totalInSizeBytes == totalOutSizeBytes)
                    ? ReplicaInfo::Status::COMPLETE
                    : ReplicaInfo::Status::INCOMPLETE;

    // Fill in the info on the chunk before finishing the operation
    WorkerReplicationRequest::_replicaInfo =
            ReplicaInfo(status, worker(), database(), chunk(), util::TimeUtils::now(), fileInfoCollection);
}

void WorkerReplicationRequest::_releaseResources(replica::Lock const& lock) {
    // Drop a connection to the remote server
    _inFilePtr.reset();

    // Close the output file
    if (_tmpFilePtr) {
        fflush(_tmpFilePtr);
        fclose(_tmpFilePtr);
        _tmpFilePtr = nullptr;
    }

    // Release the record buffer
    if (_buf) {
        delete[] _buf;
        _buf = nullptr;
    }
}

}  // namespace lsst::qserv::replica
