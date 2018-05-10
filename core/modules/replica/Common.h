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
#ifndef LSST_QSERV_REPLICA_COMMON_H
#define LSST_QSERV_REPLICA_COMMON_H

/// Common.h declares:
///
/// enum ExtendedCompletionStatus
/// (see individual class documentation for more information)

// System headers
#include <string>

// Qserv headers
#include "proto/replication.pb.h"
#include "util/Mutex.h"

// Forward declarations

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

/// Extended completion status of the worker side file operations
enum ExtendedCompletionStatus {
    EXT_STATUS_NONE,            // unspecified problem
    EXT_STATUS_INVALID_PARAM,   // invalid parameter(s) of a request
    EXT_STATUS_INVALID_ID,      // an invalid request identifier
    EXT_STATUS_DUPLICATE,       // a duplicate request
    EXT_STATUS_FOLDER_STAT,     // failed to obtain fstat() for a folder
    EXT_STATUS_FILE_STAT,       // failed to obtain fstat() for a file
    EXT_STATUS_FILE_SIZE,       // failed to obtain a size of a file
    EXT_STATUS_FOLDER_READ,     // failed to read the contents of a folder
    EXT_STATUS_FILE_READ,       // failed to read the contents of a file
    EXT_STATUS_FILE_ROPEN,      // failed to open a remote file
    EXT_STATUS_FILE_CREATE,     // failed to create a file
    EXT_STATUS_FILE_OPEN,       // failed to open a file
    EXT_STATUS_FILE_RESIZE,     // failed to resize a file
    EXT_STATUS_FILE_WRITE,      // failed to write into a file
    EXT_STATUS_FILE_COPY,       // failed to copy a file
    EXT_STATUS_FILE_DELETE,     // failed to delete a file
    EXT_STATUS_FILE_RENAME,     // failed to rename a file
    EXT_STATUS_FILE_EXISTS,     // file already exists
    EXT_STATUS_SPACE_REQ,       // space inquery requst failed
    EXT_STATUS_NO_FOLDER,       // folder doesn't exist
    EXT_STATUS_NO_FILE,         // file doesn't exist
    EXT_STATUS_NO_ACCESS,       // no access to a file or a folder
    EXT_STATUS_NO_SPACE,        // no space left on a device as required by an operation
    EXT_STATUS_FILE_MTIME       // get/set 'mtime' operation failed
};

/// Return the string representation of the extended status
std::string status2string(ExtendedCompletionStatus status);

/// Translate Protobuf status into the transient one
ExtendedCompletionStatus translate(proto::ReplicationStatusExt status);

/// Translate transient extended status into the Protobuf one
proto::ReplicationStatusExt translate(ExtendedCompletionStatus status);

/**
 * The utility class for generating unique identifiers, etc.
 */
class Generators {

public:

    /// Generate a unique identifier
    static std::string uniqueId();

private:

    /// For thread safety where it's required
    static util::Mutex _mtx;
};

/**
 * Parameters of the replica creation requests
 */
struct ReplicationRequestParams {

    int          priority;
    std::string  database;
    unsigned int chunk;
    std::string  sourceWorker;

    /// The default constructor
    ReplicationRequestParams();

    /// The normal constructor
    explicit ReplicationRequestParams(proto::ReplicationRequestReplicate const& message);
};

/**
 * Parameters of the replica deletion requests
 */
struct DeleteRequestParams {

    int          priority;
    std::string  database;
    unsigned int chunk;
    std::string  sourceWorker;

    /// The default constructor
    DeleteRequestParams();

    /// The normal constructor
    explicit DeleteRequestParams(proto::ReplicationRequestDelete const& message);
};

/**
 * Parameters of the replica lookup requests
 */
struct FindRequestParams {

    int          priority;
    std::string  database;
    unsigned int chunk;

    /// The default constructor
    FindRequestParams();

    /// The normal constructor
    explicit FindRequestParams(proto::ReplicationRequestFind const& message);
};

/**
 * Parameters of the many replica lookup requests
 */
struct FindAllRequestParams {

    int          priority;
    std::string  database;

    /// The default constructor
    FindAllRequestParams();

    /// The normal constructor
    explicit FindAllRequestParams(proto::ReplicationRequestFindAll const& message);
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_COMMON_H
