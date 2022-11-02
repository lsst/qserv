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
#ifndef LSST_QSERV_REPLICA_TRANSACTIONCONTRIB_H
#define LSST_QSERV_REPLICA_TRANSACTIONCONTRIB_H

// System headers
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Csv.h"

// This header declarations
namespace lsst::qserv::replica {

// Forward declarations
namespace database::mysql {
class Warning;
}  // namespace database::mysql

/**
 * Class TransactionContribInfo encapsulates a contribution into a table made
 * at a worker in a scope of the "super-transaction".
 */
class TransactionContribInfo {
public:
    // -----------------------------------------------------------------------------
    // These data members are initialized by the meaningful values after the initial
    // recording of the info in the database. After that they would never change.

    /// The unique identifier of a contribution is used mostly for the state
    /// tracking purposes. The identifier is set after the initial record on
    /// ingesting the contribution is recorded in the persistent state.
    unsigned int id = std::numeric_limits<unsigned int>::max();

    /// The unique identifier of a parent transaction.
    TransactionId transactionId = std::numeric_limits<TransactionId>::max();

    std::string worker;  ///< The name name of a worker

    std::string database;  ///< The name of a database
    std::string table;     ///< The base name of a table where the contribution was made

    unsigned int chunk = 0;  ///< (optional) The chunk number (partitioned tables only)
    bool isOverlap = false;  ///< (optional) A flavor of the chunked table (partitioned tables only)

    std::string url;          ///< The data source specification
    std::string charsetName;  ///< The name of the MySQL character set.

    /// The type selector is used in the where the tri-state is required.
    enum class TypeSelector : int { SYNC, ASYNC, SYNC_OR_ASYNC };

    /// @return The string representation of teh selector.
    static std::string typeSelector2str(TypeSelector typeSelector);

    bool async = false;  ///< The type of the request

    // Parameters needed for parsing the contribution.

    csv::DialectInput dialectInput;

    // Optional extended parameters needed for pulling contributions over
    // the HTTP/HTTPS protocol.

    std::string httpMethod;
    std::string httpData;
    std::vector<std::string> httpHeaders;

    // These counters are set only in case of the successful completion of the request
    // indicated by the status code 'FINISHED'.

    uint64_t numBytes = 0;  ///< The total number of bytes read from the source
    uint64_t numRows = 0;   ///< The total number of rows read from the source

    // -------------------------------------------------------------------------------
    // These data members are meant to be used for tracking the on-going or completion
    // status of an operation as it's being processed by the Ingest system. These are
    // meant to be used for error or the performance analysis. These are the notes on
    // how to interpret timestamps.
    //
    //   'createTime'
    //     The timestamp is never 0 as it's set after receiving a request. Note that
    //     the request may fail at this stage due to incorrect parameters, etc.
    //     In this case the status 'CREATE_FAILED' will be set. Should this be the case
    //     values of all other timestamps will be set to 0.
    //
    //   'startTime'
    //     A time when the request processing started (normally by pulling a file
    //     from the input data source specified by 'url'). Note that the request
    //     may not start due to changing conditions, such an incorrect state of
    //     the corresponding transaction, a lack of resources, etc. Should this be
    //     the case the status code 'START_FAILED' will be set. Values of the timestamps
    //     'readTime' and 'loadTime' will be also set to 0.
    //
    //   'readTime'
    //     A time when the input file was completely read and preprocessed, or in case
    //     of any failure of the operation. In the latter case the status code 'READ_FAILED'
    //     will be set. In this case a value of the timestamp 'loadTime' will be set to 0.
    //
    //   'loadTime'
    //    A time when loading of the (preprocessed) input file into MySQL finished or
    //    failed. Should the latter be the case the status code 'LOAD_FAILED' will be set.
    //

    uint64_t createTime = 0;  ///< The timestamp (milliseconds) when the request was received
    uint64_t startTime = 0;   ///< The timestamp (milliseconds) when the request processing started
    uint64_t readTime =
            0;  ///< The timestamp (milliseconds) when finished reading/preprocessing the input file
    uint64_t loadTime = 0;  ///< The timestamp (milliseconds) when finished loading the file into MySQL

    /// The current (or completion) status of the ingest operation.
    /// @note The completion status value 'CANCELLED' is meant to be used
    //    for processing requests in the asynchronous mode.
    enum class Status : int {
        IN_PROGRESS = 0,  // The transient state of a request before it's FINISHED or failed
        CREATE_FAILED,    // The request was received and rejected right away (incorrect parameters, etc.)
        START_FAILED,  // The request couldn't start after being pulled from a queue due to changed conditions
        READ_FAILED,   // Reading/preprocessing of the input file failed
        LOAD_FAILED,   // Loading into MySQL failed
        CANCELLED,     // The request was explicitly cancelled by the ingest workflow (ASYNC)
        FINISHED       // The request succeeded
    } status;

    /// The temporary file that was created to store pre-processed content of the input
    /// file before ingesting it into MySQL. The file is supposed to be deleted after finishing
    /// ingesting the contribution or in case of any failures. Though, in some failure modes
    /// the file may stay on disk and it may need to be cleaned up by the ingest service.
    std::string tmpFile;

    // The error context (if any).

    int httpError = 0;    ///< An HTTP response code, if applies to the request
    int systemError = 0;  ///< The UNIX errno captured at a point where a problem occurred
    std::string error;    ///< The human-readable explanation of the error

    /// @return The string representation of the status code.
    /// @throws std::invalid_argument If the status code isn't supported by the implementation.
    static std::string const& status2str(Status status);

    /// @return The status code corresponding to the input string.
    /// @throws std::invalid_argument If the string didn't match any known code.
    static Status str2status(std::string const& str);

    /// @return An ordered collection of all known status codes
    static std::vector<Status> const& statusCodes();

    /// Set to 'true' if the request could be retried w/o restarting the corresponding
    /// super-transaction.
    bool retryAllowed = false;

    /// The maximum number of warnings to be captured when ingesting
    /// the contribution. Leaving this number to 0 will result in assuming
    /// the corresponding default configured at the system.
    unsigned int maxNumWarnings = 0;

    /// The total number of warnings. Note that this number could be higher than
    /// the number of elements in the collection of warnings defined above.
    unsigned int numWarnings = 0;

    /// Optional warnings reported by MySQL after loading data.
    std::list<database::mysql::Warning> warnings;

    /// The total number of rows affected by the loading operation.
    uint64_t numRowsLoaded = 0;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

private:
    static std::map<TransactionContribInfo::Status, std::string> const _transactionContribStatus2str;
    static std::map<std::string, TransactionContribInfo::Status> const _transactionContribStr2status;
    static std::vector<TransactionContribInfo::Status> const _transactionContribStatusCodes;
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_TRANSACTIONCONTRIB_H
