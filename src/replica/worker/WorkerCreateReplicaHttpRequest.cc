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
#include "replica/worker/WorkerCreateReplicaHttpRequest.h"

// System headers
#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <system_error>
#include <vector>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/config/ConfigDatabase.h"
#include "replica/mysql/DatabaseMySQLUtils.h"
#include "replica/proto/Protocol.h"
#include "replica/services/ServiceProvider.h"
#include "replica/util/FileUtils.h"
#include "replica/worker/FileClient.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = std::filesystem;
using json = nlohmann::json;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerCreateReplicaHttpRequest");

}  // namespace

#define _CONTEXT context("WorkerCreateReplicaHttpRequest::", __func__)
#define _SET_STATUS_FAILED(extendedStatus_, error_)       \
    LOGS(_log, LOG_LVL_ERROR, _CONTEXT << " " << error_); \
    setStatus(lock, protocol::Status::FAILED, extendedStatus_, error_);

namespace lsst::qserv::replica {

shared_ptr<WorkerCreateReplicaHttpRequest> WorkerCreateReplicaHttpRequest::create(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired) {
    auto ptr = shared_ptr<WorkerCreateReplicaHttpRequest>(
            new WorkerCreateReplicaHttpRequest(serviceProvider, worker, hdr, params, onExpired));
    ptr->init();
    return ptr;
}

WorkerCreateReplicaHttpRequest::WorkerCreateReplicaHttpRequest(
        shared_ptr<ServiceProvider> const& serviceProvider, string const& worker,
        protocol::QueuedRequestHdr const& hdr, protocol::RequestParams const& params,
        ExpirationCallbackType const& onExpired)
        : WorkerHttpRequest(serviceProvider, worker, "REPLICATE", hdr, params, onExpired),
          _sourceWorker(params.requiredString("worker")),
          _sourceWorkerHost(params.requiredString("worker_host")),
          _sourceWorkerPort(params.requiredUInt16("worker_port")),
          _sourceWorkerHostPort(_sourceWorkerHost + ":" + to_string(_sourceWorkerPort)),
          _databaseName(params.requiredString("database")),
          _chunkNumber(params.requiredUInt32("chunk")),
          _bufSize(serviceProvider->config()->get<size_t>("worker", "fs-buf-size-bytes")) {
    if (worker == _sourceWorker) {
        throw invalid_argument(_CONTEXT + " workers are the same in the request.");
    }
    if (_sourceWorkerHost.empty()) {
        throw invalid_argument(_CONTEXT + " the DNS name or an IP address of the worker not provided.");
    }
}

WorkerCreateReplicaHttpRequest::~WorkerCreateReplicaHttpRequest() {
    _releaseResources(replica::Lock(mtx, _CONTEXT));
}

json WorkerCreateReplicaHttpRequest::getResult() const {
    return json::object({{"replica_info", _replicaInfo.toJson()}});
}

bool WorkerCreateReplicaHttpRequest::execute() {
    LOGS(_log, LOG_LVL_DEBUG,
         _CONTEXT << " sourceWorkerHostPort: " << _sourceWorkerHostPort << " database: " << _databaseName
                  << " chunk: " << _chunkNumber);

    replica::Lock lock(mtx, _CONTEXT);
    checkIfCancelling(lock, _CONTEXT);

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
    //   are guarded by acquiring replica::Lock lock(mtxDataFolderOperations) where it's needed.

    ///////////////////////////////////////////////////////
    //       Initialization phase (runs only once)       //
    ///////////////////////////////////////////////////////

    if (!_initialized) {
        _initialized = true;

        // The method will throw ConfigUnknownDatabase if the database is invalid.
        DatabaseInfo const databaseInfo = serviceProvider()->config()->databaseInfo(_databaseName);

        // Cache the collection of short names of files to be copied.
        _files = FileUtils::partitionedFiles(databaseInfo, _chunkNumber);

        fs::path const outDir = fs::path(serviceProvider()->config()->get<string>("worker", "data-dir")) /
                                database::mysql::obj2fs(_databaseName);

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
        std::error_code ec;
        {
            replica::Lock dataFolderLock(mtxDataFolderOperations, _CONTEXT);

            // Check for a presence of input files and calculate space requirement
            uintmax_t totalBytes = 0;          // the total number of bytes in all input files to be moved
            map<string, uintmax_t> file2size;  // the number of bytes in each file

            for (auto&& file : _files) {
                // Open the file on the remote server in the no-content-read mode
                auto const inFilePtr = FileClient::stat(serviceProvider(), _sourceWorkerHost,
                                                        _sourceWorkerPort, _databaseName, file);
                if (inFilePtr == nullptr) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_STAT,
                                       "failed to stat input file on remote worker: " + _sourceWorker + " (" +
                                               _sourceWorkerHostPort + "), database: " + _databaseName +
                                               ", file: " + file);
                    return true;
                }
                file2size[file] = inFilePtr->size();
                totalBytes += inFilePtr->size();
                _file2descr[file].inSizeBytes = inFilePtr->size();
                _file2descr[file].mtime = inFilePtr->mtime();
            }

            // Check and sanitize the output directory
            bool const outDirExists = fs::exists(outDir, ec);
            if (ec.value() != 0) {
                _SET_STATUS_FAILED(protocol::StatusExt::FOLDER_STAT,
                                   "failed to check the status of output directory: " + outDir.string() +
                                           ", code: " + to_string(ec.value()) + ", error: " + ec.message());
                return true;
            }
            if (!outDirExists) {
                _SET_STATUS_FAILED(protocol::StatusExt::NO_FOLDER,
                                   "the output directory doesn't exist: " + outDir.string());
                return true;
            }

            // The files with canonical(!) names should NOT exist at the destination
            // folder.
            for (auto&& file : outFiles) {
                fs::file_status const stat = fs::status(file, ec);
                if (stat.type() == fs::file_type::none) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_STAT,
                                       "failed to check the status of output file: " + file.string() +
                                               ", code: " + to_string(ec.value()) +
                                               ", error: " + ec.message());
                    return true;
                }
                if (fs::exists(stat)) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_EXISTS,
                                       "the output file already exists: " + file.string());
                    return true;
                }
            }

            // Check if there are any files with the temporary names at the destination
            // folder and if so then get rid of them.
            for (auto&& file : tmpFiles) {
                fs::file_status const stat = fs::status(file, ec);
                if (stat.type() == fs::file_type::none) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_STAT,
                                       "failed to check the status of temporary file: " + file.string() +
                                               ", code: " + to_string(ec.value()) +
                                               ", error: " + ec.message());
                    return true;
                }

                if (fs::exists(stat)) {
                    fs::remove(file, ec);
                    if (ec.value() != 0) {
                        _SET_STATUS_FAILED(protocol::StatusExt::FILE_DELETE,
                                           "failed to remove temporary file: " + file.string() + ", code: " +
                                                   to_string(ec.value()) + ", error: " + ec.message());
                        return true;
                    }
                }
            }

            // Make sure a file system at the destination has enough space
            // to accommodate new files
            //
            // NOTE: this operation runs after cleaning up temporary files
            fs::space_info const space = fs::space(outDir, ec);
            if (ec.value() != 0) {
                _SET_STATUS_FAILED(
                        protocol::StatusExt::SPACE_REQ,
                        "failed to obtaine space information at output folder: " + outDir.string() +
                                ", code: " + to_string(ec.value()) + ", error: " + ec.message());
                return true;
            }
            if (space.available < totalBytes) {
                _SET_STATUS_FAILED(protocol::StatusExt::NO_SPACE,
                                   "not enough free space availble at output folder: " + outDir.string() +
                                           ", required: " + to_string(totalBytes) +
                                           " bytes, available: " + to_string(space.available) + " bytes");
                return true;
            }

            // Pre-create temporary files with the final size to assert disk space
            // availability before filling these files with the actual payload.
            for (auto&& file : _files) {
                fs::path const tmpFile = _file2descr[file].tmpFile;

                // Create a file of size 0
                FILE* tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
                if (tmpFilePtr == nullptr) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_CREATE,
                                       "failed to open/create temporary file: " + tmpFile.string() +
                                               ", errno: " + to_string(errno) +
                                               ", error: " + strerror(errno));
                    return true;
                }
                fflush(tmpFilePtr);
                fclose(tmpFilePtr);

                // Resize the file (will be filled with \0)
                fs::resize_file(tmpFile, file2size[file], ec);
                if (ec.value() != 0) {
                    _SET_STATUS_FAILED(protocol::StatusExt::FILE_RESIZE,
                                       "failed to resize the temporary file: " + tmpFile.string() +
                                               " to the size: " + to_string(file2size[file]) + ", code: " +
                                               to_string(ec.value()) + ", error: " + ec.message());
                    return true;
                }
            }
        }

        // Allocate the record buffer
        _buf = new uint8_t[_bufSize];
        if (_buf == nullptr) throw runtime_error(_CONTEXT + " buffer allocation failed");

        // Setup the iterator for the name of the very first file to be copied
        _fileItr = _files.begin();
        if (!_openFiles(lock)) return true;
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
                _SET_STATUS_FAILED(
                        protocol::StatusExt::FILE_WRITE,
                        "failed to write into temporary file: " + _file2descr[*_fileItr].tmpFile.string() +
                                ", errno: " + to_string(errno) + ", error: " + strerror(errno));
                _releaseResources(lock);
                return true;
            }
        } catch (FileClientError const& ex) {
            _SET_STATUS_FAILED(protocol::StatusExt::FILE_READ,
                               "failed to read input file from remote worker: " + _sourceWorker + " (" +
                                       _sourceWorkerHostPort + "), database: " + _databaseName +
                                       ", file: " + *_fileItr);
            _releaseResources(lock);
            return true;
        }

        // Make sure the number of bytes copied from the remote server
        // matches expectations.
        if (_file2descr[*_fileItr].inSizeBytes != _file2descr[*_fileItr].outSizeBytes) {
            _SET_STATUS_FAILED(protocol::StatusExt::FILE_READ,
                               "short read of the input file from remote worker: " + _sourceWorker + " (" +
                                       _sourceWorkerHostPort + "), database: " + _databaseName +
                                       ", file: " + *_fileItr);
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
            if (!_openFiles(lock)) {
                _releaseResources(lock);
                return true;
            }
        }
    }

    // Finalize the operation, de-allocate resources, etc.
    return _finalize(lock);
}

bool WorkerCreateReplicaHttpRequest::_openFiles(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         _CONTEXT << " sourceWorkerHostPort: " << _sourceWorkerHostPort << " database: " << _databaseName
                  << " chunk: " << _chunkNumber << " file: " << *_fileItr);

    // Open the input file on the remote server
    _inFilePtr = FileClient::open(serviceProvider(), _sourceWorkerHost, _sourceWorkerPort, _databaseName,
                                  *_fileItr);
    if (_inFilePtr == nullptr) {
        _SET_STATUS_FAILED(protocol::StatusExt::FILE_ROPEN,
                           "failed to open input file on remote worker: " + _sourceWorker + " (" +
                                   _sourceWorkerHostPort + "), database: " + _databaseName +
                                   ", file: " + *_fileItr);
        return false;
    }

    // Reopen a temporary output file locally in the 'append binary mode'
    // then 'rewind' to the beginning of the file before writing into it.
    fs::path const tmpFile = _file2descr[*_fileItr].tmpFile;
    _tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
    if (_tmpFilePtr == nullptr) {
        _SET_STATUS_FAILED(protocol::StatusExt::FILE_OPEN,
                           "failed to open temporary file: " + tmpFile.string() +
                                   ", errno: " + to_string(errno) + ", error: " + strerror(errno));
        return false;
    }
    rewind(_tmpFilePtr);
    _file2descr[*_fileItr].beginTransferTime = util::TimeUtils::now();
    return true;
}

bool WorkerCreateReplicaHttpRequest::_finalize(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG,
         _CONTEXT << " sourceWorkerHostPort: " << _sourceWorkerHostPort << " database: " << _databaseName
                  << " chunk: " << _chunkNumber);

    // Unconditionally regardless of the completion of the file renaming attempt
    _releaseResources(lock);

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquiring the directory lock to guarantee a consistent view onto the folder.
    replica::Lock dataFolderLock(mtxDataFolderOperations, _CONTEXT);

    // ATTENTION: as per ISO/IEC 9945 the file rename operation will
    //            remove empty files. Not sure if this should be treated
    //            in a special way?
    std::error_code ec;
    for (auto&& file : _files) {
        fs::path const tmpFile = _file2descr[file].tmpFile;
        fs::path const outFile = _file2descr[file].outFile;

        fs::rename(tmpFile, outFile, ec);
        if (ec.value() != 0) {
            _SET_STATUS_FAILED(protocol::StatusExt::FILE_RENAME,
                               "failed to rename file: " + tmpFile.string() +
                                       " into file: " + outFile.string() +
                                       ", code: " + to_string(ec.value()) + ", error: " + ec.message());
            return true;
        }
        try {
            replica::setMTime(outFile.string(), _file2descr[file].mtime);
        } catch (exception const& ex) {
            _SET_STATUS_FAILED(
                    protocol::StatusExt::FILE_MTIME,
                    "failed to change 'mtime' of file: " + outFile.string() + ", ex: " + string(ex.what()));
            return true;
        }
    }
    setStatus(lock, protocol::Status::SUCCESS);
    return true;
}

void WorkerCreateReplicaHttpRequest::_updateInfo(replica::Lock const& lock) {
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
    _replicaInfo = ReplicaInfo(status, worker(), _databaseName, _chunkNumber, util::TimeUtils::now(),
                               fileInfoCollection);
}

void WorkerCreateReplicaHttpRequest::_releaseResources(replica::Lock const& lock) {
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
