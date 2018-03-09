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

// Class header
#include "replica/WorkerReplicationRequest.h"

// System headers
#include <cerrno>
#include <cstring>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/FileClient.h"
#include "replica/FileUtils.h"
#include "replica/Performance.h"
#include "replica/ServiceProvider.h"

// These macros to appear witin each block which requires thread safety
// at the corresponding level
#define LOCK_DATA_FOLDER std::lock_guard<std::mutex> lock(_mtxDataFolderOperations)
#define LOCK_GUARD       std::lock_guard<std::mutex> lock(_mtx)

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

WorkerReplicationRequest::pointer WorkerReplicationRequest::create(
                                        ServiceProvider::pointer const& serviceProvider,
                                        std::string const& worker,
                                        std::string const& id,
                                        int                priority,
                                        std::string const& database,
                                        unsigned int       chunk,
                                        std::string const& sourceWorker) {
    return WorkerReplicationRequest::pointer(
        new WorkerReplicationRequest(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequest::WorkerReplicationRequest(
                                ServiceProvider::pointer const& serviceProvider,
                                std::string const& worker,
                                std::string const& id,
                                int                priority,
                                std::string const& database,
                                unsigned int       chunk,
                                std::string const& sourceWorker)
    :   WorkerRequest (
            serviceProvider,
            worker,
            "REPLICATE",
            id,
            priority),
        _database(database),
        _chunk(chunk),
        _sourceWorker(sourceWorker),
        _replicaInfo() {

    _serviceProvider->assertWorkerIsValid(sourceWorker);
    _serviceProvider->assertWorkersAreDifferent(worker, sourceWorker);
}

ReplicaInfo WorkerReplicationRequest::replicaInfo() const {

    // This implementation guarantees that a consistent snapshot of
    // the object will be returned to a calling thread while
    // a processing thread may be attempting to update the object.

    LOCK_GUARD;

    ReplicaInfo const info = _replicaInfo;

    return info;
}
    
bool WorkerReplicationRequest::execute() {

   LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  sourceWorker: " << sourceWorker()
         << "  db: "           << database()
         << "  chunk: "        << chunk());

    // TODO: provide the actual implementation instead of the dummy one.

    bool const complete = WorkerRequest::execute();
    if (complete) {
        _replicaInfo = ReplicaInfo(ReplicaInfo::COMPLETE,
                                   worker(),
                                   database(),
                                   chunk(),
                                   PerformanceUtils::now(),
                                   ReplicaInfo::FileInfoCollection());
    }
    return complete;
}

////////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestPOSIX ////////////////////
////////////////////////////////////////////////////////////////////////

WorkerReplicationRequestPOSIX::pointer WorkerReplicationRequestPOSIX::create (
                                    ServiceProvider::pointer const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database,
                                    unsigned int       chunk,
                                    std::string const& sourceWorker) {
    return WorkerReplicationRequestPOSIX::pointer(
        new WorkerReplicationRequestPOSIX(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequestPOSIX::WorkerReplicationRequestPOSIX(
                                    ServiceProvider::pointer const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database,
                                    unsigned int       chunk,
                                    std::string const& sourceWorker)
    :   WorkerReplicationRequest(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker) {
}

bool WorkerReplicationRequestPOSIX::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  sourceWorker: " << sourceWorker()
         << "  database: "     << database()
         << "  chunk: "        << chunk());

    // Obtain the list of files to be migrated
    //
    // IMPLEMENTATION NOTES:
    //
    // - Note using the overloaded operator '/' which is used to form
    //   folders and files path names below. The operator will concatename
    //   names and also insert a file separator for an operationg system
    //   on which this code will get compiled.
    //
    // - Temporary file names at a destination folders are prepended with
    //   prefix '_' to prevent colliding with the canonical names. They will
    //   be renamed in the last step.
    //
    // - All operations with the file system namespace (creating new non-temporary
    //   files, checking for folders and files, renaming files, creating folders, etc.)
    //   are guarded by acquering LOCK_DATA_FOLDER where it's needed.

    WorkerInfo   const& inWorkerInfo  = _serviceProvider->config()->workerInfo(sourceWorker());
    WorkerInfo   const& outWorkerInfo = _serviceProvider->config()->workerInfo(worker());
    DatabaseInfo const& databaseInfo  = _serviceProvider->config()->databaseInfo(database());

    fs::path const inDir  = fs::path(inWorkerInfo.dataDir)  / database();
    fs::path const outDir = fs::path(outWorkerInfo.dataDir) / database();

    std::vector<std::string> const files =
        FileUtils::partitionedFiles(databaseInfo, chunk());

    std::vector<fs::path> inFiles;
    std::vector<fs::path> tmpFiles;
    std::vector<fs::path> outFiles;

    std::map<std::string,fs::path> file2inFile;
    std::map<std::string,fs::path> file2tmpFile;
    std::map<std::string,fs::path> file2outFile;

    std::map<fs::path,std::time_t> inFile2mtime;

    for (auto const& file: files) {

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
        LOCK_DATA_FOLDER;

        // Check for a presence of input files and calculate space requirement

        for (auto const& file: inFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of input file: " + file.string())
                or reportErrorIf(
                        not fs::exists(stat),
                        ExtendedCompletionStatus::EXT_STATUS_NO_FILE,
                        "the input file does not exist: " + file.string());

            totalBytes += fs::file_size(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_SIZE,
                        "failed to get the size of input file: " + file.string());

            inFile2mtime[file] = fs::last_write_time(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                        "failed to get the mtime of input file: " + file.string());
        }

        // Check and sanitize the output directory

        bool const outDirExists = fs::exists(outDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                    "failed to check the status of output directory: " + outDir.string())
            or reportErrorIf(
                    not outDirExists,
                    ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                    "the output directory doesn't exist: " + outDir.string());

        // The files with canonical(!) names should NOT exist at the destination
        // folder.

        for (auto const& file: outFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of output file: " + file.string())
                or reportErrorIf(
                        fs::exists(stat),
                        ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS,
                        "the output file already exists: " + file.string());
        }

        // Check if there are any files with the temporary names at the destination
        // folder and if so then get rid of them.

        for (auto const& file: tmpFiles) {
            fs::file_status const stat = fs::status(file, ec);
            errorContext = errorContext
                or reportErrorIf(
                        stat.type() == fs::status_error,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                        "failed to check the status of temporary file: " + file.string());

            if (fs::exists(stat)) {
                fs::remove(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            ec,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE,
                            "failed to remove temporary file: " + file.string());
            }
        }

        // Make sure a file system at the destination has enough space
        // to accomodate new files
        //
        // NOTE: this operation runs after cleaning up temporary files

        fs::space_info const space = fs::space(outDir, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ,
                    "failed to obtaine space information at output folder: " + outDir.string())
            or reportErrorIf(
                    space.available < totalBytes,
                    ExtendedCompletionStatus::EXT_STATUS_NO_SPACE,
                    "not enough free space availble at output folder: " + outDir.string());
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Begin copying files into the destination folder under their
    // temporary names w/o acquring the directory lock.

    for (auto const& file: files) {

        fs::path const inFile  = file2inFile [file];
        fs::path const tmpFile = file2tmpFile[file];

        fs::copy_file(inFile, tmpFile, ec);
        errorContext = errorContext
            or reportErrorIf(
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_COPY,
                    "failed to copy file: " + inFile.string() + " into: " + tmpFile.string());
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquering the directory lock to guarantee a consistent view onto the folder.

    {
        LOCK_DATA_FOLDER;

        // ATTENTION: as per ISO/IEC 9945 thie file rename operation will
        //            remove empty files. Not sure if this should be treated
        //            in a special way?

        for (auto const& file: files) {

            fs::path const inFile  = file2inFile [file];
            fs::path const tmpFile = file2tmpFile[file];
            fs::path const outFile = file2outFile[file];

            fs::rename(tmpFile, outFile, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME,
                        "failed to rename file: " + tmpFile.string());

            fs::last_write_time(outFile, inFile2mtime[inFile], ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                        "failed to set the mtime of output file: " + outFile.string());
        }
    }
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // For now (before finalizing the progress reporting protocol) just return
    // the perentage of the total amount of data moved

    setStatus(STATUS_SUCCEEDED);
    return true;
}

/////////////////////////////////////////////////////////////////////
///////////////////// WorkerReplicationRequestFS ////////////////////
/////////////////////////////////////////////////////////////////////

WorkerReplicationRequestFS::pointer WorkerReplicationRequestFS::create (
                                            ServiceProvider::pointer const& serviceProvider,
                                            std::string const& worker,
                                            std::string const& id,
                                            int                priority,
                                            std::string const& database,
                                            unsigned int       chunk,
                                            std::string const& sourceWorker) {
    return WorkerReplicationRequestFS::pointer(
        new WorkerReplicationRequestFS(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker));
}

WorkerReplicationRequestFS::WorkerReplicationRequestFS(
                                    ServiceProvider::pointer const& serviceProvider,
                                    std::string const& worker,
                                    std::string const& id,
                                    int                priority,
                                    std::string const& database,
                                    unsigned int       chunk,
                                    std::string const& sourceWorker)
    :   WorkerReplicationRequest(
                serviceProvider,
                worker,
                id,
                priority,
                database,
                chunk,
                sourceWorker),
        _inWorkerInfo(_serviceProvider->config()->workerInfo(sourceWorker)),
        _outWorkerInfo(_serviceProvider->config()->workerInfo(worker)),
        _databaseInfo(_serviceProvider->config()->databaseInfo(database)),
        _initialized(false),
        _files(FileUtils::partitionedFiles(_databaseInfo, chunk)),
        _tmpFilePtr(nullptr),
        _buf(0),
        _bufSize(serviceProvider->config()->workerFsBufferSizeBytes()) {
}

WorkerReplicationRequestFS::~WorkerReplicationRequestFS() {
    releaseResources();
}

bool WorkerReplicationRequestFS::execute () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "execute"
         << "  sourceWorker: " << sourceWorker()
         << "  database: "     << database()
         << "  chunk: "        << chunk());

    // Abort the operation right away if that's the case

    if (_status == STATUS_IS_CANCELLING) {
        setStatus(STATUS_CANCELLED);
        throw WorkerRequestCancelled();
    }

    // Obtain the list of files to be migrated
    //
    // IMPLEMENTATION NOTES:
    //
    // - Note using the overloaded operator '/' which is used to form
    //   folders and files path names below. The operator will concatename
    //   names and also insert a file separator for an operationg system
    //   on which this code will get compiled.
    //
    // - Temporary file names at a destination folders are prepended with
    //   prefix '_' to prevent colliding with the canonical names. They will
    //   be renamed in the last step.
    //
    // - All operations with the file system namespace (creating new non-temporary
    //   files, checking for folders and files, renaming files, creating folders, etc.)
    //   are guarded by acquering LOCK_DATA_FOLDER where it's needed.

    WorkerRequest::ErrorContext errorContext;

    ///////////////////////////////////////////////////////
    //       Initialization phase (runs only once)       //
    ///////////////////////////////////////////////////////
 
    if (not _initialized) {
        _initialized = true;

        fs::path const outDir = fs::path(_outWorkerInfo.dataDir) / database();
    
        std::vector<fs::path> tmpFiles;
        std::vector<fs::path> outFiles;
    
        for (auto const& file: _files) {

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
            LOCK_DATA_FOLDER;
    
            // Check for a presence of input files and calculate space requirement
    
            uintmax_t totalBytes = 0;                   // the total number of bytes in all input files to be moved
            std::map<std::string,uintmax_t> file2size;  // the number of bytes in each file
    
            for (auto const& file: _files) {
    
                // Open the file on the remote server in the no-content-read mode
                FileClient::pointer inFilePtr = FileClient::stat(_serviceProvider,
                                                                 _inWorkerInfo.name,
                                                                 _databaseInfo.name,
                                                                 file);
                errorContext = errorContext
                    or reportErrorIf(
                        not inFilePtr,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN,
                        "failed to open input file on remote worker: " + _inWorkerInfo.name +
                        ", database: " + _databaseInfo.name +
                        ", file: " + file);
        
                if (errorContext.failed) {
                    setStatus(STATUS_FAILED, errorContext.extendedStatus);
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
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_FOLDER_STAT,
                        "failed to check the status of output directory: " + outDir.string())
                or reportErrorIf(
                        not outDirExists,
                        ExtendedCompletionStatus::EXT_STATUS_NO_FOLDER,
                        "the output directory doesn't exist: " + outDir.string());
    
            // The files with canonical(!) names should NOT exist at the destination
            // folder.
    
            for (auto const& file: outFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            stat.type() == fs::status_error,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                            "failed to check the status of output file: " + file.string())
                    or reportErrorIf(
                            fs::exists(stat),
                            ExtendedCompletionStatus::EXT_STATUS_FILE_EXISTS,
                            "the output file already exists: " + file.string());
            }
    
            // Check if there are any files with the temporary names at the destination
            // folder and if so then get rid of them.
    
            for (auto const& file: tmpFiles) {
                fs::file_status const stat = fs::status(file, ec);
                errorContext = errorContext
                    or reportErrorIf(
                            stat.type() == fs::status_error,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_STAT,
                            "failed to check the status of temporary file: " + file.string());
    
                if (fs::exists(stat)) {
                    fs::remove(file, ec);
                    errorContext = errorContext
                        or reportErrorIf(
                                ec,
                                ExtendedCompletionStatus::EXT_STATUS_FILE_DELETE,
                                "failed to remove temporary file: " + file.string());
                }
            }
    
            // Make sure a file system at the destination has enough space
            // to accomodate new files
            //
            // NOTE: this operation runs after cleaning up temporary files
    
            fs::space_info const space = fs::space(outDir, ec);
            errorContext = errorContext
                or reportErrorIf(
                        ec,
                        ExtendedCompletionStatus::EXT_STATUS_SPACE_REQ,
                        "failed to obtaine space information at output folder: " + outDir.string())
                or reportErrorIf(
                        space.available < totalBytes,
                        ExtendedCompletionStatus::EXT_STATUS_NO_SPACE,
                        "not enough free space availble at output folder: " + outDir.string());
    
            // Precreate temporary files with the final size to assert disk space
            // availability before filling these files with the actual payload.
    
            for (auto const& file: _files) {
                
                fs::path const tmpFile = _file2descr[file].tmpFile;
    
                // Create a file of size 0
    
                std::FILE* tmpFilePtr = std::fopen(tmpFile.string().c_str(), "wb");
                errorContext = errorContext
                    or reportErrorIf(
                            not tmpFilePtr,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_CREATE,
                            "failed to open/create temporary file: " + tmpFile.string() +
                            ", error: " + std::strerror(errno));
                if (tmpFilePtr) {
                    std::fflush(tmpFilePtr);
                    std::fclose(tmpFilePtr);
                }
     
                // Resize the file (will be filled with \0)
    
                fs::resize_file(tmpFile, file2size[file], ec);
                errorContext = errorContext
                    or reportErrorIf(
                            ec,
                            ExtendedCompletionStatus::EXT_STATUS_FILE_RESIZE,
                            "failed to resize the temporary file: " + tmpFile.string());
            }
        }
        if (errorContext.failed) {
            setStatus(STATUS_FAILED, errorContext.extendedStatus);
            return true;
        }
        
        // Allocate the record buffer
        _buf = new uint8_t[_bufSize];
        if (not _buf) {
            throw std::runtime_error(
                            "WorkerReplicationRequestFS::execute()  buffer allocation failed");
        }

        // Setup the iterator for the name of the very first file to be copied
        _fileItr = _files.begin();

        if (openFiles()) { return true; }
    }

    // Copy the next record from the currently open remote file
    // into the corresponding temporary files at the destination folder
    // w/o acquring the directory lock.
    //
    // NOTE: the while loop below is meant to skip files which are empty

    while (_files.end() != _fileItr) {

        // Copy the next record if any is available
 
        size_t num = 0;
        try {
            num = _inFilePtr->read(_buf, _bufSize);
            if (num) {
                if (num == std::fwrite(_buf, sizeof(uint8_t), num, _tmpFilePtr)) {

                    // Update the descriptor (the number of bytes copied so far
                    // and the control sum)
                    _file2descr[*_fileItr].outSizeBytes += num;
                    uint64_t& cs = _file2descr[*_fileItr].cs;
                    for (uint8_t *ptr = _buf, *end = _buf + num;
                         ptr != end; ++ptr) { cs += *ptr; }

                    // Keep updating this stats while copying the files
                    _file2descr[*_fileItr].endTransferTime = PerformanceUtils::now();
                    updateInfo();

                    // Keep copying the same file
                    return false;
                }
                errorContext = errorContext
                    or reportErrorIf(
                        true,
                        ExtendedCompletionStatus::EXT_STATUS_FILE_WRITE,
                        "failed to write into temporary file: " + _file2descr[*_fileItr].tmpFile.string() +
                        ", error: " + std::strerror(errno));
            }
                
        } catch (FileClientError const& ex) {
            errorContext = errorContext
                or reportErrorIf(
                    true,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_READ,
                    "failed to read input file from remote worker: " + _inWorkerInfo.name +
                    ", database: " + _databaseInfo.name +
                    ", file: " + *_fileItr);
        }

        // Make sure the number of bytes copied from the remote server
        // matches expectations.
        errorContext = errorContext
            or reportErrorIf(
                _file2descr[*_fileItr].inSizeBytes != _file2descr[*_fileItr].outSizeBytes,
                ExtendedCompletionStatus::EXT_STATUS_FILE_READ,
                "short read of the input file from remote worker: " + _inWorkerInfo.name +
                ", database: " + _databaseInfo.name +
                ", file: " + *_fileItr);

        if (errorContext.failed) {
            setStatus(STATUS_FAILED, errorContext.extendedStatus);
            releaseResources();
            return true;
        }
        // Flush and close the current file

        std::fflush(_tmpFilePtr);
        std::fclose(_tmpFilePtr);
        _tmpFilePtr = 0;

        // Keep updating this stats after finishing to copy each file
        _file2descr[*_fileItr].endTransferTime = PerformanceUtils::now();
        updateInfo ();

        // Move the iterator to the name of the next file to be copied
        ++_fileItr;
        if (_files.end() != _fileItr) {
            if (openFiles()) {
                releaseResources();
                return true;
            }
        }
    }

    // Finalize the operation, deallocate resources, etc.
    return finalize();
}

bool WorkerReplicationRequestFS::openFiles () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "openFiles"
         << "  sourceWorker: " << sourceWorker()
         << "  database: "     << database()
         << "  chunk: "        << chunk()
         << "  file: "         << *_fileItr);

    WorkerRequest::ErrorContext errorContext;

    // Open the input file on the remote server
    _inFilePtr = FileClient::open(_serviceProvider,
                                  _inWorkerInfo.name,
                                  _databaseInfo.name,
                                  *_fileItr);
    errorContext = errorContext
        or reportErrorIf(
            not _inFilePtr,
            ExtendedCompletionStatus::EXT_STATUS_FILE_ROPEN,
            "failed to open input file on remote worker: " + _inWorkerInfo.name +
            ", database: " + _databaseInfo.name +
            ", file: " + *_fileItr);

    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }

    // Reopen a temporary output file locally in the 'append binary mode'
    // then 'rewind' to the begining of the file before writing into it.

    fs::path const tmpFile = _file2descr[*_fileItr].tmpFile;

    _tmpFilePtr = std::fopen(tmpFile.string().c_str(), "wb");
    errorContext = errorContext
        or reportErrorIf(
            not _tmpFilePtr,
            ExtendedCompletionStatus::EXT_STATUS_FILE_OPEN,
            "failed to open temporary file: " + tmpFile.string() +
            ", error: " + std::strerror(errno));
    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }
    std::rewind(_tmpFilePtr);

    _file2descr[*_fileItr].beginTransferTime = PerformanceUtils::now();

    return false;
}

bool WorkerReplicationRequestFS::finalize () {

    LOGS(_log, LOG_LVL_DEBUG, context() << "finalize"
         << "  sourceWorker: " << sourceWorker()
         << "  database: "     << database()
         << "  chunk: "        << chunk());

    // Unconditionally regardless of the completion of the file ranaming attemp
    releaseResources();

    // Rename temporary files into the canonical ones
    // Note that this operation changes the directory namespace in a way
    // which may affect other users (like replica lookup operations, etc.). Hence we're
    // acquering the directory lock to guarantee a consistent view onto the folder.

    LOCK_DATA_FOLDER;

    // ATTENTION: as per ISO/IEC 9945 thie file rename operation will
    //            remove empty files. Not sure if this should be treated
    //            in a special way?

    WorkerRequest::ErrorContext errorContext;
    boost::system::error_code   ec;

    for (auto const& file: _files) {

        fs::path const tmpFile = _file2descr[file].tmpFile;
        fs::path const outFile = _file2descr[file].outFile;

        fs::rename(tmpFile, outFile, ec);
        errorContext = errorContext
            or reportErrorIf (
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_RENAME,
                    "failed to rename file: " + tmpFile.string());

        fs::last_write_time(outFile, _file2descr[file].mtime, ec);
        errorContext = errorContext
            or reportErrorIf (
                    ec,
                    ExtendedCompletionStatus::EXT_STATUS_FILE_MTIME,
                    "failed to change 'mtime' of file: " + tmpFile.string());
    }

    if (errorContext.failed) {
        setStatus(STATUS_FAILED, errorContext.extendedStatus);
        return true;
    }
    setStatus(STATUS_SUCCEEDED);
    return true;
}

void WorkerReplicationRequestFS::updateInfo() {

    size_t totalInSizeBytes  = 0;
    size_t totalOutSizeBytes = 0;

    ReplicaInfo::FileInfoCollection fileInfoCollection;
    for (auto const& file: _files) {
        fileInfoCollection.emplace_back(
            ReplicaInfo::FileInfo({
                file,
                _file2descr[file].outSizeBytes,
                _file2descr[file].mtime,
                std::to_string(_file2descr[file].cs),
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

    LOCK_GUARD;     // to guarantee a consistent snapshot of that data structure
                    // if other threads will be requesting its copy while it'll be being
                    // updated below.

    _replicaInfo = ReplicaInfo(
        status,
        worker(),
        database(),
        chunk(),
        PerformanceUtils::now(),
        fileInfoCollection);
}

void
WorkerReplicationRequestFS::releaseResources() {

    // Drop a connection to the remore server
    _inFilePtr.reset();

    // Close the output file
    if (_tmpFilePtr) {
        std::fflush(_tmpFilePtr);
        std::fclose(_tmpFilePtr);
        _tmpFilePtr = nullptr;
    }

    // Release the record buffer
    if (_buf) {
        delete [] _buf;
        _buf = nullptr;
    }
}

}}} // namespace lsst::qserv::replica