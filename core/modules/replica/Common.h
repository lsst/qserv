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
#ifndef LSST_QSERV_REPLICA_COMMON_H
#define LSST_QSERV_REPLICA_COMMON_H

/**
 * This declares various small utilities, such data types, functions and
 * classes which are shared by the code in the rest of this package.
 * It would be not practical to put each of these utilities in a separate
 * header.
 */

// System headers
#include <cstdint>
#include <list>
#include <ostream>
#include <string>
#include <tuple>

// Qserv headers
#include "replica/protocol.pb.h"
#include "util/Mutex.h"

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

// Constants

/// The number of the 'overflow' chunks
unsigned int const overflowChunkNumber = 1234567890;

/// Extended completion status of the worker side file operations
enum ExtendedCompletionStatus {
    EXT_STATUS_NONE,            // unspecified problem
    EXT_STATUS_INVALID_PARAM,   // invalid parameter(s) of a request
    EXT_STATUS_INVALID_ID,      // an invalid request identifier
    EXT_STATUS_DUPLICATE,       // a duplicate request
    EXT_STATUS_FOLDER_STAT,     // failed to obtain fstat() for a folder
    EXT_STATUS_FOLDER_CREATE,   // failed to create a folder
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
    EXT_STATUS_SPACE_REQ,       // space availability request failed
    EXT_STATUS_NO_FOLDER,       // folder doesn't exist
    EXT_STATUS_NO_FILE,         // file doesn't exist
    EXT_STATUS_NO_ACCESS,       // no access to a file or a folder
    EXT_STATUS_NO_SPACE,        // no space left on a device as required by an operation
    EXT_STATUS_FILE_MTIME,      // get/set 'mtime' operation failed
    EXT_STATUS_MYSQL_ERROR,     // MySQL operation failed
    EXT_STATUS_LARGE_RESULT,    // result exceeds a limit set in a request
    EXT_STATUS_NO_SUCH_TABLE,   // a reason why a MySQL operation failed 
    EXT_STATUS_NOT_PARTITIONED_TABLE,   // why a MySQL operation for removing partitions failed
    EXT_STATUS_NO_SUCH_PARTITION        // why a MySQL operation for for selecting data from a table failed
};

/// Return the string representation of the extended status
std::string status2string(ExtendedCompletionStatus status);

/// Translate Protobuf status into the transient one
ExtendedCompletionStatus translate(ProtocolStatusExt status);

/// Translate transient extended status into the Protobuf one
ProtocolStatusExt translate(ExtendedCompletionStatus status);

/**
 * Class Generators is the utility class for generating a set of unique
 * identifiers, etc. Each call to the class's method 'next()' will produce
 * a new identifier.
 */
class Generators {
public:
    /// @return next unique identifier
    static std::string uniqueId();

private:
    /// For thread safety where it's required
    static util::Mutex _mtx;
};


/**
 * This class is an abstraction for column definitions. A column has
 * a name and a type.
 */
class SqlColDef {
public:

    SqlColDef() = default;
    SqlColDef(std::string const name_,
              std::string const type_)
        :    name(name_),
             type(type_) {
    }

    SqlColDef(SqlColDef const&) = default;
    SqlColDef& operator=(SqlColDef const&) = default;

    ~SqlColDef() = default;


    std::string name;
    std::string type;
};

/**
 * Class ReplicationRequestParams encapsulates parameters of the replica
 * creation requests.
 */
class ReplicationRequestParams {
public:
    int          priority = 0;
    std::string  database;
    unsigned int chunk = 0;
    std::string  sourceWorker;

    ReplicationRequestParams() = default;

    explicit ReplicationRequestParams(ProtocolRequestReplicate const& request);
};

/**
 * Class DeleteRequestParams represents parameters of the replica
 * deletion requests.
 */
class DeleteRequestParams {
public:
    int          priority = 0;
    std::string  database;
    unsigned int chunk = 0;
    std::string  sourceWorker;

    DeleteRequestParams() = default;

    explicit DeleteRequestParams(ProtocolRequestDelete const& request);
};

/**
 * Class FindRequestParams represents parameters of a single replica
 * lookup (finding) requests.
 */
class FindRequestParams {
public:
    int          priority = 0;
    std::string  database;
    unsigned int chunk = 0;

    FindRequestParams() = default;

    explicit FindRequestParams(ProtocolRequestFind const& request);
};

/**
 * Class FindAllRequestParams represents parameters of the replica
 * group (depends on a scope of the corresponding request) lookup (finding)
 * requests.
 */
class FindAllRequestParams {
public:
    int          priority = 0;
    std::string  database;

    FindAllRequestParams() = default;

    explicit FindAllRequestParams(ProtocolRequestFindAll const& request);
};

/**
 * Class EchoRequestParams represents parameters of the echo requests.
 */
class EchoRequestParams {
public:
    int          priority = 0;
    std::string  data;
    uint64_t     delay = 0;

    EchoRequestParams() = default;

    explicit EchoRequestParams(ProtocolRequestEcho const& request);
};

/// The type for the super-transaction identifiers
typedef uint32_t TransactionId;

/**
 * Class SqlRequestParams represents parameters of the SQL requests.
 */
class SqlRequestParams {
public:
    int priority = 0;

    enum Type {
        QUERY,
        CREATE_DATABASE,
        DROP_DATABASE,
        ENABLE_DATABASE,
        DISABLE_DATABASE,
        GRANT_ACCESS,
        CREATE_TABLE,
        DROP_TABLE,
        REMOVE_TABLE_PARTITIONING,
        DROP_TABLE_PARTITION
    };
    Type type = QUERY;

    uint64_t maxRows = 0;

    std::string query;
    std::string user;
    std::string password;
    std::string database;
    std::string table;
    std::string engine;
    std::string partitionByColumn;

    TransactionId transactionId = 0;

    std::list<SqlColDef> columns;

    SqlRequestParams() = default;

    explicit SqlRequestParams(ProtocolRequestSql const& request);

    std::string type2str() const;
};

std::ostream& operator<<(std::ostream& os, SqlRequestParams const& params);

/**
 * Class IndexRequestParams represents parameters of requests extracting data
 * to be loaded into the "secondary index".
 */
class IndexRequestParams {
public:
    int           priority = 0;
    std::string   database;
    unsigned int  chunk = 0;
    bool          hasTransactions = false;
    TransactionId transactionId = 0;

    IndexRequestParams() = default;

    explicit IndexRequestParams(ProtocolRequestIndex const& request);
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_COMMON_H
