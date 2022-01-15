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
#include "replica/WorkerReplicationRequest.h"

// System headers
#include <cerrno>
#include <cstring>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/FileClient.h"
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerReplicationRequest");

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

///////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequest ////////////////////
///////////////////////////////////////////////////////////////////

WorkerReplicationRequest::Ptr WorkerReplicationRequest::create(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request) {
    return WorkerReplicationRequest::Ptr(new WorkerReplicationRequest(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerReplicationRequest::WorkerReplicationRequest(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request)
    :   WorkerRequest (
            serviceProvider,
            worker,
            "REPLICATE",
            id,
            priority,
            onExpired,
            requestExpirationIvalSec),
        _request(request),
        _sourceWorkerHostPort(request.worker_host() + ":" + to_string(request.worker_port())) {

    serviceProvider->config()->assertWorkerIsValid(request.worker());
    serviceProvider->config()->assertWorkersAreDifferent(worker, request.worker());
    if (request.worker_host().empty()) {
        throw invalid_argument(
                "WorkerReplicationRequest::" + string(__func__)
                + " the DNS name or an IP address of the worker not provided.");
    }
    if (request.worker_port() > std::numeric_limits<uint16_t>::max()) {
        throw overflow_error(
                "WorkerReplicationRequest::" + string(__func__) + " the port number "
                + to_string(request.worker_port()) + " is not in the valid range of 0.."
                + to_string(std::numeric_limits<uint16_t>::max()));
    }
    if (request.worker_data_dir().empty()) {
        throw invalid_argument(
                "WorkerReplicationRequest::" + string(__func__)
                + " the data path name at the remote worker not provided.");
    }
}


void WorkerReplicationRequest::setInfo(ProtocolResponseReplicate& response) const {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__));

    util::Lock lock(_mtx, context(__func__));

    response.set_allocated_target_performance(performance().info().release());
    response.set_allocated_replica_info(replicaInfo.info().release());

    *(response.mutable_request()) = _request;
}


bool WorkerReplicationRequest::execute() {

   LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
         << "  db: " << database()
         << "  chunk: " << chunk());

    bool const complete = WorkerRequest::execute();
    if (complete) {
        replicaInfo = ReplicaInfo(
                ReplicaInfo::COMPLETE, worker(), database(), chunk(),
                PerformanceUtils::now(), ReplicaInfo::FileInfoCollection());
    }
    return complete;
}


////////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestPOSIX ////////////////////
////////////////////////////////////////////////////////////////////////

WorkerReplicationRequestPOSIX::Ptr WorkerReplicationRequestPOSIX::create (
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request) {
    return WorkerReplicationRequestPOSIX::Ptr(new WorkerReplicationRequestPOSIX(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerReplicationRequestPOSIX::WorkerReplicationRequestPOSIX(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request)
    :   WorkerReplicationRequest(
            serviceProvider,
            worker,
            id,
            priority,
            onExpired,
            requestExpirationIvalSec,
            request) {
}


bool WorkerReplicationRequestPOSIX::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
         << "  sourceWorkerDataDir: " << sourceWorkerDataDir()
         << "  database: " << database()
         << "  chunk: " << chunk());

    util::Lock lock(_mtx, context(__func__));

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
    //   are guarded by acquiring util::Lock lock(_mtxDataFolderOperations) where it's needed.

    WorkerInfo   const outWorkerInfo = _serviceProvider->config()->workerInfo(worker());
    DatabaseInfo const databaseInfo  = _serviceProvider->config()->databaseInfo(database());

    fs::path const inDir  = fs::path(sourceWorkerDataDir()) / database();
    fs::path const outDir = fs::path(outWorkerInfo.dataDir) / database();

    vector<string> const files = FileUtils::partitionedFiles(databaseInfo, chunk());

    vector<fs::path> inFiles;
    vector<fs::path> tmpFiles;
    vector<fs::path> outFiles;

    map<string, fs::path> file2inFile;
    map<string, fs::path> file2tmpFile;
    map<string, fs::path> file2outFile;

    map<fs::path,time_t> inFile2mtime;

    for (auto&& file: files) {

        fs::path const inFile = inDir / file;
        inFiles.push_back(inFile);
        file2inFile[file] = inFile;

        fs::path const tmpFile = outDir / ("_" + file);
        tmpFiles.push_back(tmpFile);
        file2tmpFile[file] = tmpFile;

        fs::path const outFile = outDir / file;
        outFiles.push_back(outFile);
        file2outFile[file] = outFile;
    }

    // Check input files, check and sanitize the destination folder

    uintmax_t totalBytes = 0;   // the total number of bytes in all input files to be moved

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;
    {
        util::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__) + ":1");

        // Check for a presence of input files and calculate space requirement

        for (auto&& file: inFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ProtocolStatusExt::FILE_STAT,
                        "failed to check the status of input file: " + file.string())
                or reportErrorIf(
                        not fs::exists(stat),
                        ProtocolStatusExt::NO_FILE,
                        "the input file does not exist: " + file.string());

            totalBytes += fs::file_size(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FILE_SIZE,
                        "failed to get the size of input file: " + file.string());

            inFile2mtime[file] = fs::last_write_time(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FILE_MTIME,
                        "failed to get the mtime of input file: " + file.string());
        }

        // Check and sanitize the output directory

        bool const outDirExists = fs::exists(outDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec.value() != 0,
                    ProtocolStatusExt::FOLDER_STAT,
                    "failed to check the status of output directory: " + outDir.string())
            or reportErrorIf(
                    not outDirExists,
                    ProtocolStatusExt::NO_FOLDER,
                    "the output directory doesn't exist: " + outDir.string());

        // The files with canonical(!) names should NOT exist at the destination
        // folder.

        for (auto&& file: outFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ProtocolStatusExt::FILE_STAT,
                        "failed to check the status of output file: " + file.string())
                or reportErrorIf(
                        fs::exists(stat),
                        ProtocolStatusExt::FILE_EXISTS,
                        "the output file already exists: " + file.string());
        }

        // Check if there are any files with the temporary names at the destination
        // folder and if so then get rid of them.

        for (auto&& file: tmpFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ProtocolStatusExt::FILE_STAT,
                        "failed to check the status of temporary file: " + file.string());

            if (fs::exists(stat)) {
                fs::remove(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            ec.value() != 0,
                            ProtocolStatusExt::FILE_DELETE,
                            "failed to remove temporary file: " + file.string());
            }
        }

        // Make sure a file system at the destination has enough space
        // to accommodate new files
        //
        // NOTE: this operation runs after cleaning up temporary files

        fs::space_info const space = fs::space(outDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec.value() != 0,
                    ProtocolStatusExt::SPACE_REQ,
                    "failed to obtain space information at output folder: " + outDir.string())
            or reportErrorIf(
                    space.available < totalBytes,
                    ProtocolStatusExt::NO_SPACE,
                    "not enough free space available at output folder: " + outDir.string());
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }

    // Begin copying files into the destination folder under their
    // temporary names w/o acquiring the directory lock.

    for (auto&& file: files) {

        fs::path const inFile  = file2inFile [file];
        fs::path const tmpFile = file2tmpFile[file];

        fs::copy_file(inFile, tmpFile, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec.value() != 0,
                    ProtocolStatusExt::FILE_COPY,
                    "failed to copy file: " + inFile.string() + " into: " + tmpFile.string());
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquiring the directory lock to guarantee a consistent view onto the folder.

    {
        util::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__) + ":2");

        // ATTENTION: as per ISO/IEC 9945 the file rename operation will
        //            remove empty files. Not sure if this should be treated
        //            in a special way?

        for (auto&& file: files) {

            fs::path const inFile  = file2inFile [file];
            fs::path const tmpFile = file2tmpFile[file];
            fs::path const outFile = file2outFile[file];

            fs::rename(tmpFile, outFile, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FILE_RENAME,
                        "failed to rename file: " + tmpFile.string());

            fs::last_write_time(outFile, inFile2mtime[inFile], ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FILE_MTIME,
                        "failed to set the mtime of output file: " + outFile.string());
        }
    }
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }

    // For now (before finalizing the progress reporting protocol) just return
    // the percentage of the total amount of data moved

    setStatus(lock, ProtocolStatus::SUCCESS);
    return true;
}


/////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestFS ////////////////////
/////////////////////////////////////////////////////////////////////

WorkerReplicationRequestFS::Ptr WorkerReplicationRequestFS::create (
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request) {
    return WorkerReplicationRequestFS::Ptr(new WorkerReplicationRequestFS(
        serviceProvider,
        worker,
        id,
        priority,
        onExpired,
        requestExpirationIvalSec,
        request
    ));
}


WorkerReplicationRequestFS::WorkerReplicationRequestFS(
        ServiceProvider::Ptr const& serviceProvider,
        string const& worker,
        string const& id,
        int priority,
        ExpirationCallbackType const& onExpired,
        unsigned int requestExpirationIvalSec,
        ProtocolRequestReplicate const& request)
    :   WorkerReplicationRequest(
            serviceProvider,
            worker,
            id,
            priority,
            onExpired,
            requestExpirationIvalSec,
            request),
        _outWorkerInfo(_serviceProvider->config()->workerInfo(worker)),
        _databaseInfo(_serviceProvider->config()->databaseInfo(request.database())),
        _initialized(false),
        _files(FileUtils::partitionedFiles(_databaseInfo, request.chunk())),
        _tmpFilePtr(nullptr),
        _buf(0),
        _bufSize(serviceProvider->config()->get<size_t>("worker", "fs-buf-size-bytes")) {
}


WorkerReplicationRequestFS::~WorkerReplicationRequestFS() {
    util::Lock lock(_mtx, context(__func__));
    _releaseResources(lock);
}


bool WorkerReplicationRequestFS::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
         << "  database: " << database()
         << "  chunk: " << chunk());

    util::Lock lock(_mtx, context(__func__));

    // Abort the operation right away if that's the case

    if (_status == ProtocolStatus::IS_CANCELLING) {
        setStatus(lock, ProtocolStatus::CANCELLED);
        throw WorkerRequestCancelled();
    }

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
    //   are guarded by acquiring util::Lock lock(_mtxDataFolderOperations) where it's needed.

    WorkerRequest::ErrorContext errorContext;

    ///////////////////////////////////////////////////////
    //       Initialization phase (runs only once)       //
    ///////////////////////////////////////////////////////

    if (not _initialized) {
        _initialized = true;

        fs::path const outDir = fs::path(_outWorkerInfo.dataDir) / database();

        vector<fs::path> tmpFiles;
        vector<fs::path> outFiles;

        for (auto&& file: _files) {

            fs::path const tmpFile = outDir / ("_" + file);
            tmpFiles.push_back(tmpFile);

            fs::path const outFile = outDir / file;
            outFiles.push_back(outFile);

            _file2descr[file].inSizeBytes       = 0;
            _file2descr[file].outSizeBytes      = 0;
            _file2descr[file].mtime             = 0;
            _file2descr[file].cs                = 0;
            _file2descr[file].tmpFile           = tmpFile;
            _file2descr[file].outFile           = outFile;
            _file2descr[file].beginTransferTime = 0;
            _file2descr[file].endTransferTime   = 0;
        }

        // Check input files, check and sanitize the destination folder

        boost::system::error_code ec;
        {
            util::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));

            // Check for a presence of input files and calculate space requirement

            uintmax_t totalBytes = 0;           // the total number of bytes in all input files to be moved
            map<string, uintmax_t> file2size;   // the number of bytes in each file

            for (auto&& file: _files) {

                // Open the file on the remote server in the no-content-read mode
                FileClient::Ptr inFilePtr = FileClient::stat(_serviceProvider,
                                                             sourceWorkerHost(),
                                                             sourceWorkerPort(),
                                                             _databaseInfo.name,
                                                             file);
                errorContext = errorContext
                    or reportErrorIf(
                        not inFilePtr,
                        ProtocolStatusExt::FILE_ROPEN,
                        "failed to open input file on remote worker: " + sourceWorker() +
                        " (" + sourceWorkerHostPort() +
                        "), database: " + _databaseInfo.name +
                        ", file: " + file);

                if (errorContext.failed) {
                    setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
                    return true;
                }
                file2size[file] = inFilePtr->size();
                totalBytes     += inFilePtr->size();

                _file2descr[file].inSizeBytes = inFilePtr->size();
                _file2descr[file].mtime       = inFilePtr->mtime();
            }

            // Check and sanitize the output directory

            bool const outDirExists = fs::exists(outDir, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::FOLDER_STAT,
                        "failed to check the status of output directory: " + outDir.string())
                or reportErrorIf(
                        not outDirExists,
                        ProtocolStatusExt::NO_FOLDER,
                        "the output directory doesn't exist: " + outDir.string());

            // The files with canonical(!) names should NOT exist at the destination
            // folder.

            for (auto&& file: outFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            stat.type() == fs::status_error,
                            ProtocolStatusExt::FILE_STAT,
                            "failed to check the status of output file: " + file.string())
                    or reportErrorIf(
                            fs::exists(stat),
                            ProtocolStatusExt::FILE_EXISTS,
                            "the output file already exists: " + file.string());
            }

            // Check if there are any files with the temporary names at the destination
            // folder and if so then get rid of them.

            for (auto&& file: tmpFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            stat.type() == fs::status_error,
                            ProtocolStatusExt::FILE_STAT,
                            "failed to check the status of temporary file: " + file.string());

                if (fs::exists(stat)) {
                    fs::remove(file, ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec.value() != 0,
                                ProtocolStatusExt::FILE_DELETE,
                                "failed to remove temporary file: " + file.string());
                }
            }

            // Make sure a file system at the destination has enough space
            // to accommodate new files
            //
            // NOTE: this operation runs after cleaning up temporary files

            fs::space_info const space = fs::space(outDir, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec.value() != 0,
                        ProtocolStatusExt::SPACE_REQ,
                        "failed to obtaine space information at output folder: " + outDir.string())
                or reportErrorIf(
                        space.available < totalBytes,
                        ProtocolStatusExt::NO_SPACE,
                        "not enough free space availble at output folder: " + outDir.string());

            // Pre-create temporary files with the final size to assert disk space
            // availability before filling these files with the actual payload.

            for (auto&& file: _files) {

                fs::path const tmpFile = _file2descr[file].tmpFile;

                // Create a file of size 0

                FILE* tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
                errorContext = errorContext
                    or reportErrorIf(
                            not tmpFilePtr,
                            ProtocolStatusExt::FILE_CREATE,
                            "failed to open/create temporary file: " + tmpFile.string() +
                            ", error: " + strerror(errno));
                if (tmpFilePtr) {
                    fflush(tmpFilePtr);
                    fclose(tmpFilePtr);
                }

                // Resize the file (will be filled with \0)

                fs::resize_file(tmpFile, file2size[file], ec);
                errorContext = errorContext
                    or reportErrorIf(
                            ec.value() != 0,
                            ProtocolStatusExt::FILE_RESIZE,
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
            throw runtime_error(
                    "WorkerReplicationRequestFS::" + string(__func__) +
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
                    for (uint8_t *ptr = _buf, *end = _buf + num;
                         ptr != end; ++ptr) { cs += *ptr; }

                    // Keep updating this stats while copying the files
                    _file2descr[*_fileItr].endTransferTime = PerformanceUtils::now();
                    _updateInfo(lock);

                    // Keep copying the same file
                    return false;
                }
                errorContext = errorContext
                    or reportErrorIf(
                        true,
                        ProtocolStatusExt::FILE_WRITE,
                        "failed to write into temporary file: " + _file2descr[*_fileItr].tmpFile.string() +
                        ", error: " + strerror(errno));
            }

        } catch (FileClientError const& ex) {
            errorContext = errorContext
                or reportErrorIf(
                    true,
                    ProtocolStatusExt::FILE_READ,
                    "failed to read input file from remote worker: " + sourceWorker() + " (" + sourceWorkerHostPort() +
                    "), database: " + _databaseInfo.name +
                    ", file: " + *_fileItr);
        }

        // Make sure the number of bytes copied from the remote server
        // matches expectations.
        errorContext = errorContext
            or reportErrorIf(
                _file2descr[*_fileItr].inSizeBytes != _file2descr[*_fileItr].outSizeBytes,
                ProtocolStatusExt::FILE_READ,
                "short read of the input file from remote worker: " + sourceWorker() + " (" + sourceWorkerHostPort() +
                "), database: " + _databaseInfo.name +
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
        _file2descr[*_fileItr].endTransferTime = PerformanceUtils::now();
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


bool WorkerReplicationRequestFS::_openFiles(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
         << "  database: " << database()
         << "  chunk: " << chunk()
         << "  file: " << *_fileItr);

    WorkerRequest::ErrorContext errorContext;

    // Open the input file on the remote server
    _inFilePtr = FileClient::open(_serviceProvider,
                                  sourceWorkerHost(),
                                  sourceWorkerPort(),
                                  _databaseInfo.name,
                                  *_fileItr);
    errorContext = errorContext
        or reportErrorIf(
            not _inFilePtr,
            ProtocolStatusExt::FILE_ROPEN,
            "failed to open input file on remote worker: " + sourceWorker() + " (" + sourceWorkerHostPort() +
            "), database: " + _databaseInfo.name +
            ", file: " + *_fileItr);

    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return false;
    }

    // Reopen a temporary output file locally in the 'append binary mode'
    // then 'rewind' to the beginning of the file before writing into it.

    fs::path const tmpFile = _file2descr[*_fileItr].tmpFile;

    _tmpFilePtr = fopen(tmpFile.string().c_str(), "wb");
    errorContext = errorContext
        or reportErrorIf(
            not _tmpFilePtr,
            ProtocolStatusExt::FILE_OPEN,
            "failed to open temporary file: " + tmpFile.string() +
            ", error: " + strerror(errno));
    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return false;
    }
    rewind(_tmpFilePtr);

    _file2descr[*_fileItr].beginTransferTime = PerformanceUtils::now();

    return true;
}


bool WorkerReplicationRequestFS::_finalize(util::Lock const& lock) {

    LOGS(_log, LOG_LVL_DEBUG, context(__func__)
         << "  sourceWorkerHostPort: " << sourceWorkerHostPort()
         << "  database: " << database()
         << "  chunk: " << chunk());

    // Unconditionally regardless of the completion of the file renaming attempt
    _releaseResources(lock);

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquiring the directory lock to guarantee a consistent view onto the folder.

    util::Lock dataFolderLock(_mtxDataFolderOperations, context(__func__));

    // ATTENTION: as per ISO/IEC 9945 the file rename operation will
    //            remove empty files. Not sure if this should be treated
    //            in a special way?

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    for (auto&& file: _files) {

        fs::path const tmpFile = _file2descr[file].tmpFile;
        fs::path const outFile = _file2descr[file].outFile;

        fs::rename(tmpFile, outFile, ec);
        errorContext = errorContext
            or reportErrorIf (
                    ec.value() != 0,
                    ProtocolStatusExt::FILE_RENAME,
                    "failed to rename file: " + tmpFile.string());

        fs::last_write_time(outFile, _file2descr[file].mtime, ec);
        errorContext = errorContext
            or reportErrorIf (
                    ec.value() != 0,
                    ProtocolStatusExt::FILE_MTIME,
                    "failed to change 'mtime' of file: " + tmpFile.string());
    }

    if (errorContext.failed) {
        setStatus(lock, ProtocolStatus::FAILED, errorContext.extendedStatus);
        return true;
    }
    setStatus(lock, ProtocolStatus::SUCCESS);
    return true;
}


void WorkerReplicationRequestFS::_updateInfo(util::Lock const& lock) {

    size_t totalInSizeBytes  = 0;
    size_t totalOutSizeBytes = 0;

    ReplicaInfo::FileInfoCollection fileInfoCollection;
    for (auto&& file: _files) {
        fileInfoCollection.emplace_back(
            ReplicaInfo::FileInfo({
                file,
                _file2descr[file].outSizeBytes,
                _file2descr[file].mtime,
                to_string(_file2descr[file].cs),
                _file2descr[file].beginTransferTime,
                _file2descr[file].endTransferTime,
                _file2descr[file].inSizeBytes
            })
        );
        totalInSizeBytes  += _file2descr[file].inSizeBytes;
        totalOutSizeBytes += _file2descr[file].outSizeBytes;
    }
    ReplicaInfo::Status const status =
        (_files.size()    == fileInfoCollection.size()) and
        (totalInSizeBytes == totalOutSizeBytes) ?
            ReplicaInfo::Status::COMPLETE :
            ReplicaInfo::Status::INCOMPLETE;

    // Fill in the info on the chunk before finishing the operation

    WorkerReplicationRequest::replicaInfo = ReplicaInfo(
            status, worker(), database(), chunk(), PerformanceUtils::now(), fileInfoCollection);
}


void WorkerReplicationRequestFS::_releaseResources(util::Lock const& lock) {

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
        delete [] _buf;
        _buf = nullptr;
    }
}

}}} // namespace lsst::qserv::replica
