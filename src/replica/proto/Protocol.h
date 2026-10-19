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
#ifndef LSST_QSERV_REPLICA_PROTOCOL_H
#define LSST_QSERV_REPLICA_PROTOCOL_H

// System headers
#include <cstdint>
#include <string>
#include <vector>

// Third party headers
#include "nlohmann/json.hpp"

// This header declarations
namespace lsst::qserv::replica::protocol {

/**
 * @brief Types of the queued requests.
 */
enum class QueuedRequestType : unsigned int {
    _FIRST = 0,
    REPLICA_CREATE = _FIRST,  ///< replicate a chunk
    REPLICA_DELETE,           ///< delete an existing replica of a chunk
    REPLICA_FIND,             ///< find out if a replica is present, and report its state
    REPLICA_FIND_ALL,         ///< find all replicas and report their states
    TEST_ECHO,                ///< test the worker-side framework
    SQL,                      ///< execute an SQL statement against the worker's database
    INDEX,                    ///< extract and return a collection of the "director" index data
    _LAST = INDEX
};

/**
 * @brief Translate a string representation of the queued request type into the corresponding enum value.
 * @param str The string representation of the queued request type.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the string doesn't match any of the known queued request types.
 */
QueuedRequestType toQueuedRequestType(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the queued request type into the corresponding enum
 * value.
 * @param value The unsigned integer representation of the queued request type.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known queued request types.
 */
QueuedRequestType toQueuedRequestType(unsigned int value);

/**
 * @brief Translate the queued request type into its string representation.
 * @param status The queued request type.
 * @return the string representation of the queued request type.
 * @throws std::invalid_argument if the status doesn't match any of the known queued request types.
 */
std::string toString(QueuedRequestType status);

/**
 * @brief Translate the queued request type into its unsigned integer representation.
 * @param status The queued request type.
 * @return the underlying unsigned integer representation.
 * @throws std::invalid_argument if the status doesn't match any of the known queued request types.
 */
unsigned int toUnsignedInt(QueuedRequestType status);

/**
 * @brief Subtypes of the SQL requests.
 */
enum class SqlRequestType : unsigned int {
    _FIRST = 0,
    QUERY = _FIRST,
    CREATE_DATABASE,
    DROP_DATABASE,
    ENABLE_DATABASE,   ///< in Qserv
    DISABLE_DATABASE,  ///< in Qserv
    GRANT_ACCESS,
    CREATE_TABLE,
    DROP_TABLE,
    REMOVE_TABLE_PARTITIONING,
    DROP_TABLE_PARTITION,
    GET_TABLE_INDEX,
    CREATE_TABLE_INDEX,
    DROP_TABLE_INDEX,
    ALTER_TABLE,
    TABLE_ROW_STATS,
    _LAST = TABLE_ROW_STATS

};

/**
 * @brief Translate a string representation of the SQL request type into the corresponding enum value.
 * @param str The string representation of the SQL request type.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the string doesn't match any of the known SQL request types.
 */
SqlRequestType toSqlRequestType(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the SQL request type into
 * the corresponding enum value.
 * @param value The unsigned integer representation of the SQL request type.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known SQL request types.
 */
SqlRequestType toSqlRequestType(unsigned int);

/**
 * @brief Translate the SQL request type into its string representation.
 * @param status The SQL request type.
 * @return the string representation of the SQL request type.
 * @throws std::invalid_argument if the status doesn't match any of the known SQL request types.
 */
std::string toString(SqlRequestType status);

/**
 * @brief Translate the SQL request type into its unsigned integer representation.
 * @param status The SQL request type.
 * @return the underlying unsigned integer representation.
 * @throws std::invalid_argument if the status doesn't match any of the known SQL request types.
 */
unsigned int toUnsignedInt(SqlRequestType status);

/**
 * @brief Types of the table indexes specified in the index management requests.
 */
enum class SqlIndexSpec : unsigned int {
    _FIRST = 1,
    DEFAULT = _FIRST,
    UNIQUE,
    FULLTEXT,
    SPATIAL,
    _LAST = SPATIAL
};

/**
 * @brief Translate a string representation of the SQL index specification into the corresponding enum value.
 * @param str The string representation of the SQL index specification.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the string doesn't match any of the known SQL index specifications.
 */
SqlIndexSpec toSqlIndexSpec(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the SQL index specification into the corresponding
 * enum value.
 * @param value The unsigned integer representation of the SQL index specification.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known SQL index specifications.
 */
SqlIndexSpec toSqlIndexSpec(unsigned int value);

/**
 * @brief Translate the SQL index specification into its string representation.
 * @param spec The SQL index specification.
 * @return the string representation of the SQL index specification.
 * @throws std::invalid_argument if the spec doesn't match any of the known SQL index specifications.
 */
std::string toString(SqlIndexSpec spec);

/**
 * @brief Translate the SQL index specification into its unsigned integer representation.
 * @param spec The SQL index specification.
 * @return the underlying unsigned integer representation.
 * @throws std::invalid_argument if the spec doesn't match any of the known SQL index specifications.
 */
unsigned int toUnsignedInt(SqlIndexSpec spec);

/**
 * @brief Status values returned by all request related to operations at workers.
 * Request management operations always return messages whose types
 * match the return types of the corresponding (original) replica-related requests.
 * Service management requests have their own set of status values.
 */
enum class Status : unsigned int {
    _FIRST = 0,
    CREATED = _FIRST,
    SUCCESS,
    QUEUED,
    IN_PROGRESS,
    IS_CANCELLING,
    BAD,
    FAILED,
    CANCELLED,
    _LAST = CANCELLED
};

/**
 * @brief Translate a string representation of the status into the corresponding enum value.
 * @param str The string representation of the status.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the string doesn't match any of the known status values.
 */
Status toStatus(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the status into the corresponding enum value.
 * @param value The unsigned integer representation of the status.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known status values.
 */
Status toStatus(unsigned int value);

/**
 * @brief Translate a status enum value into its string representation.
 * @param status The status enum value.
 * @return the string representation of the status.
 * @throws std::invalid_argument if the status doesn't match any of the known status values.
 */
std::string toString(Status status);

/**
 * @brief Translate a status enum value into its unsigned integer representation.
 * @param status The status enum value.
 * @return the underlying unsigned integer representation.
 * @throws std::invalid_argument if the status doesn't match any of the known status values.
 */
unsigned int toUnsignedInt(Status status);

/**
 * @brief Extended status values apply to requests in the final states BAD or FAILED only.
 * In all other cases, the extended status is expected to be StatusExt::NONE.
 */
enum class StatusExt : unsigned int {
    _FIRST = 0,
    NONE = _FIRST,          ///< Unspecified problem.
    INVALID_PARAM,          ///< Invalid parameter(s) of a request.
    INVALID_ID,             ///< An invalid request identifier.
    FOLDER_STAT,            ///< Failed to obtain fstat() for a folder.
    FOLDER_CREATE,          ///< Failed to create a folder.
    FILE_STAT,              ///< Failed to obtain fstat() for a file.
    FILE_SIZE,              ///< Failed to obtain a size of a file.
    FOLDER_READ,            ///< Failed to read the contents of a folder.
    FILE_READ,              ///< Failed to read the contents of a file.
    FILE_ROPEN,             ///< Failed to open a remote file.
    FILE_CREATE,            ///< Failed to create a file.
    FILE_OPEN,              ///< Failed to open a file.
    FILE_RESIZE,            ///< Failed to resize a file.
    FILE_WRITE,             ///< Failed to write into a file.
    FILE_COPY,              ///< Failed to copy a file.
    FILE_DELETE,            ///< Failed to delete a file.
    FILE_RENAME,            ///< Failed to rename a file.
    FILE_EXISTS,            ///< File already exists.
    SPACE_REQ,              ///< Space availability check failed.
    NO_FOLDER,              ///< Folder doesn't exist.
    NO_FILE,                ///< File doesn't exist.
    NO_ACCESS,              ///< No access to a file or a folder.
    NO_SPACE,               ///< No space left on a device as required by an operation.
    FILE_MTIME,             ///< Get/set 'mtime' operation failed.
    MYSQL_ERROR,            ///< General MySQL error (other than any specific ones listed here).
    LARGE_RESULT,           ///< Result exceeds a limit set in a request.
    NO_SUCH_TABLE,          ///< No table found while performing a MySQL operation.
    NOT_PARTITIONED_TABLE,  ///< The table is not MySQL partitioned as it was expected.
    NO_SUCH_PARTITION,      ///< No MySQL partition found in a table as it was expected.
    MULTIPLE,               ///< Multiple unspecified errors encountered when processing a request.
    OTHER_EXCEPTION,        ///< Other exception not listed here.
    FOREIGN_INSTANCE,       ///< Detected a request from a Controller serving an unrelated Qserv.
    DUPLICATE_KEY,          ///< Duplicate key found when creating an index or altering a table schema.
    CANT_DROP_KEY,          ///< Can't drop a field or a key which doesn't exist.
    CONFIG_NO_SUCH_DB,      ///< A database mentioned in the worker request was not found at the worker's
                            ///< configuration.
    CONFIG_NO_SUCH_TABLE,   ///< A table mentioned in the worker request was not found at the worker's
                            ///< configuration.
    _LAST = CONFIG_NO_SUCH_TABLE
};

/**
 * @brief Convert a string representation of an extended status to the corresponding StatusExt enum value.
 * @param str The string representation of the extended status.
 * @return The corresponding StatusExt enum value.
 * @throw std::invalid_argument if the string does not correspond to any known StatusExt value.
 */
StatusExt toStatusExt(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the extended status into the corresponding enum
 * value.
 * @param value The unsigned integer representation of the extended status.
 * @return the corresponding enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known extended status values.
 */
StatusExt toStatusExt(unsigned int value);

/**
 * @brief Translate an extended status enum value into its string representation.
 * @param extendedStatus The extended status enum value.
 * @return the string representation of the extended status.
 * @throws std::invalid_argument if the extended status doesn't match any of the known extended status values.
 */
std::string toString(StatusExt extendedStatus);

/**
 * @brief Convert an extended status to its unsigned integer representation.
 * @param extendedStatus The extended status to convert.
 * @return The unsigned integer representation of the extended status.
 */
unsigned int toUnsignedInt(StatusExt extendedStatus);

/**
 * @brief Translate a combination of status and extended status into its string representation.
 * @param status The status enum value.
 * @param extendedStatus The extended status enum value.
 * @return the string representation of the combined status.
 */
std::string toString(Status status, StatusExt extendedStatus);

/**
 * @brief Types of service requests affecting the overall state of the worker replication service.
 */
enum ServiceRequestType {
    _FIRST = 0,
    SERVICE_SUSPEND = _FIRST,
    SERVICE_RESUME,
    SERVICE_STATUS,
    SERVICE_REQUESTS,
    SERVICE_DRAIN,
    SERVICE_RECONFIG,
    _LAST = SERVICE_RECONFIG
};

/**
 * @brief Convert a string representation of a service request type to the corresponding ServiceRequestType
 * enum value.
 * @param str The string representation of the service request type.
 * @return The corresponding ServiceRequestType enum value.
 * @throw std::invalid_argument if the string does not correspond to any known ServiceRequestType value.
 */
ServiceRequestType toServiceRequestType(std::string const& str);

/**
 * @brief Translate an unsigned integer representation of the service request type into the corresponding
 * ServiceRequestType enum value.
 * @param value The unsigned integer representation of the service request type.
 * @return the corresponding ServiceRequestType enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known service request type values.
 */
ServiceRequestType toServiceRequestType(unsigned int value);

/**
 * @brief Translate a service request type enum value into its string representation.
 * @param requestType The service request type enum value.
 * @return the string representation of the service request type.
 * @throws std::invalid_argument if the request type doesn't match any of the known service request types.
 */
std::string toString(ServiceRequestType requestType);

/**
 * @brief Translate a service request type enum value into its unsigned integer representation.
 * @param requestType The service request type enum value.
 * @return the unsigned integer representation of the service request type.
 * @throws std::invalid_argument if the request type doesn't match any of the known service request types.
 */
unsigned int toUnsignedInt(ServiceRequestType requestType);

/// Status of a service.
enum class ServiceState : unsigned int {
    _FIRST = 0,
    SUSPEND_IN_PROGRESS = _FIRST,
    SUSPENDED,
    RUNNING,
    _LAST = RUNNING
};

/**
 * @brief Convert a string representation of a service state to the corresponding ServiceState enum value.
 * @param stateStr The string representation of the service state.
 * @return The corresponding ServiceState enum value.
 * @throw std::invalid_argument if the string does not correspond to any known ServiceState value.
 */
ServiceState toServiceState(std::string const& stateStr);

/**
 * @brief Translate an unsigned integer representation of the service state into the corresponding
 * ServiceState enum value.
 * @param value The unsigned integer representation of the service state.
 * @return the corresponding ServiceState enum value.
 * @throws std::invalid_argument if the value doesn't match any of the known service state values.
 */
ServiceState toServiceState(unsigned int value);

/**
 * @brief Translate a service state enum value into its string representation.
 * @param state The service state enum value.
 * @return the string representation of the service state.
 * @throws std::invalid_argument if the state doesn't match any of the known service states.
 */
std::string toString(ServiceState state);

/**
 * @brief Translate a service state enum value into its unsigned integer representation.
 * @param state The service state enum value.
 * @return the unsigned integer representation of the service state.
 * @throws std::invalid_argument if the state doesn't match any of the known service states.
 */
unsigned int toUnsignedInt(ServiceState state);

/// The header to be sent with the requests processed through the worker's queueing system.
struct QueuedRequestHdr {
    std::string id;
    int priority;
    unsigned int timeout;
    QueuedRequestHdr(std::string const& id_, int priority_, unsigned int timeout_)
            : id(id_), priority(priority_), timeout(timeout_) {}
    nlohmann::json toJson() const { return {{"id", id}, {"priority", priority}, {"timeout", timeout}}; };
};

/**
 * The class RequestParams provides methods for parsing the input parameters of the request.
 *
 * The methods will throw std::invalid_argument if the parameters are not found or have invalid values.
 * @note When parsing the boolean parameters the corresponding methods will try to convert
 * the value of a parameter to boolean if it's the JSON boolean value (false or true) or
 * an integer (0 or != 0).
 */
class RequestParams {
public:
    explicit RequestParams(nlohmann::json const& req = nlohmann::json());
    RequestParams(RequestParams const&) = default;
    RequestParams& operator=(RequestParams const&) = default;
    ~RequestParams() = default;

    nlohmann::json toJson() const { return _req; }

    bool has(std::string const& name) const;
    std::string requiredString(std::string const& name) const;
    std::string optionalString(std::string const& name,
                               std::string const& defaultValue = std::string()) const;
    bool requiredBool(std::string const& name) const;
    bool optionalBool(std::string const& name, bool defaultValue = false) const;
    std::uint16_t requiredUInt16(std::string const& name) const;
    std::uint16_t optionalUInt16(std::string const& name, std::uint16_t defaultValue = 0) const;
    std::uint32_t requiredUInt32(std::string const& name) const;
    std::uint32_t optionalUInt32(std::string const& name, std::uint32_t defaultValue = 0) const;
    std::int32_t requiredInt32(std::string const& name) const;
    std::int32_t optionalInt32(std::string const& name, std::int32_t defaultValue = 0) const;
    std::uint64_t requiredUInt64(std::string const& name) const;
    std::uint64_t optionalUInt64(std::string const& name, std::uint64_t defaultValue = 0) const;
    double requiredDouble(std::string const& name) const;
    double optionalDouble(std::string const& name, double defaultValue = 0.0) const;
    std::vector<std::string> requiredStringVec(std::string const& name) const;
    std::vector<std::string> optionalStringVec(
            std::string const& name,
            std::vector<std::string> const& defaultValue = std::vector<std::string>()) const;
    std::vector<std::uint64_t> requiredUInt64Vec(std::string const& name) const;
    std::vector<std::uint64_t> optionalUInt64Vec(
            std::string const& name,
            std::vector<std::uint64_t> const& defaultValue = std::vector<std::uint64_t>()) const;
    nlohmann::json const& requiredVec(std::string const& name) const;
    nlohmann::json const& requiredObj(std::string const& name) const;

private:
    /**
     * Extract the parameter from the request and return its value.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter
     * @throw std::invalid_argument if the parameter is not found
     */
    nlohmann::json const& _required(std::string const& name) const;

    /**
     * Extract the parameter from the request and validate its value as an array.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter validated as an array
     * @throw std::invalid_argument if the parameter is not found or is not an array
     */
    nlohmann::json const& _requiredVec(std::string const& name) const;

    /**
     * Extract the parameter from the request and validate its value as an object.
     * @param name the name of the parameter to be extracted
     * @return the value of the parameter validated as an object
     * @throw std::invalid_argument if the parameter is not found or is not an object
     */
    nlohmann::json const& _requiredObj(std::string const& name) const;

    nlohmann::json _req;
};

}  // namespace lsst::qserv::replica::protocol

#endif  // LSST_QSERV_REPLICA_PROTOCOL_H
