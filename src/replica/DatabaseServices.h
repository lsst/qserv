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
#ifndef LSST_QSERV_REPLICA_DATABASESERVICES_H
#define LSST_QSERV_REPLICA_DATABASESERVICES_H

// System headers
#include <cstdint>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>
#include <unordered_map>

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Csv.h"
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"
#include "util/Mutex.h"

// Forward declarations
namespace lsst::qserv::replica {
class Configuration;
class ControllerIdentity;
class NamedMutexRegistry;
class QservMgtRequest;
class Performance;
class Request;
}  // namespace lsst::qserv::replica

// This header declarations
namespace lsst::qserv::replica {

/**
 * Class DatabaseServicesError is an exception class for reporting errors
 * in database operations.
 */
class DatabaseServicesError : public std::runtime_error {
public:
    DatabaseServicesError(std::string const& msg) : std::runtime_error(std::string(__func__) + ": " + msg) {}
};

/**
 * Class DatabaseServicesNotFound is an exception class for reporting content
 * which can't be found in the database.
 */
class DatabaseServicesNotFound : public std::runtime_error {
public:
    DatabaseServicesNotFound(std::string const& msg)
            : std::runtime_error(std::string(__func__) + ": " + msg) {}
};

/**
 * Class ControllerEvent encapsulating various info on events logged
 * by Controllers. These objects are retrieved from the persistent logs.
 *
 * @see DatabaseServices::logControllerEvent
 * @see DatabaseServices::readControllerEvents
 */
class ControllerEvent {
public:
    uint32_t id = 0;  /// A unique identifier of the event in the persistent log.
                      /// Note, that a value of this field is retrieved from the database.

    std::string controllerId;  /// Unique identifier of the Controller instance
    uint64_t timeStamp = 0;    /// 64-bit timestamp (ms) of the event

    std::string task;       /// The name of a Controller task defining a scope of the operation
    std::string operation;  /// The name of an operation (request, job, other action)
    std::string status;     /// The optional status of the operation
    std::string requestId;  /// The optional identifier of a request
    std::string jobId;      /// The optional identifier of a job

    std::list<std::pair<std::string, std::string>> kvInfo;  /// The optional collection (key-value pairs)
                                                            /// of the event-specific data

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

/**
 * Class ControllerInfo encapsulates a persistent state of the Controller
 * object fetched from the database.
 */
class ControllerInfo {
public:
    std::string id;  /// Unique identifier of the Controller instance

    uint64_t started = 0;  /// 64-bit timestamp (ms) for its start time

    std::string hostname;  /// The name of a host where the Controller was run

    int pid = 0;  /// the PID of the Controller's process

    /**
     * Translate an instance into a JSON object
     *
     * @param isCurrent (optional) flag which will set an extra property 'current'
     *   to the desired state (as defined by the value of the flag)
     *
     * @return JSON representation of the object
     */
    nlohmann::json toJson(bool isCurrent = false) const;
};

/**
 * Class RequestInfo encapsulates a persistent state of the Request (its
 * subclasses) objects fetched from the database.
 */
class RequestInfo {
public:
    std::string id;      /// Unique identifier of the Request instance
    std::string jobId;   /// Unique identifier of the parent Job instance
    std::string name;    /// The name (actually - its specific type) of the request
    std::string worker;  /// The name of a worker where the request was sent

    int priority = 0;  /// The priority level

    std::string state;          /// The primary state
    std::string extendedState;  /// The secondary state
    std::string serverStatus;   /// The optional status of the request obtained from the corresponding
                                /// worker service after the request was (or was attempted to be) executed.

    // Timestamps recorded during the lifetime of the request

    uint64_t controllerCreateTime = 0;
    uint64_t controllerStartTime = 0;
    uint64_t controllerFinishTime = 0;
    uint64_t workerReceiveTime = 0;
    uint64_t workerStartTime = 0;
    uint64_t workerFinishTime = 0;

    /// The optional collection (key-value pairs) of extended attributes
    std::list<std::pair<std::string, std::string>> kvInfo;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

/**
 * Class JobInfo encapsulates a persistent state of the Job (its
 * subclasses) objects fetched from the database.
 */
class JobInfo {
public:
    std::string id;             /// Unique identifier of the Job instance
    std::string controllerId;   /// Unique identifier of the parent Controller instance)
    std::string parentJobId;    /// Unique identifier of the parent Job instance
    std::string type;           /// The type name of the job
    std::string state;          /// The primary state
    std::string extendedState;  /// The secondary state

    uint64_t beginTime = 0;  /// The timestamp (ms) when the job started
    uint64_t endTime = 0;    /// The timestamp (ms) when the job finished
    uint64_t heartbeatTime =
            0;  /// The optional timestamp (ms) when the job refreshed its state as "still alive"

    int priority = PRIORITY_NORMAL;  /// The priority level

    std::list<std::pair<std::string, std::string>> kvInfo;  /// The optional collection (key-value pairs)
                                                            /// of extended attributes

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

/**
 * Class TransactionInfo encapsulates a persistent state of the "super-transaction"
 * objects fetched from the database.
 * @note The default constructor will put objects of the class into the non-valid
 *   state. Objects retrieved from the database are all valid. The validity
 *   of the transaction objects can be verified by calling class method 'isValid()'.
 */
class TransactionInfo {
public:
    /// Allowed states for the transaction.
    /// See the implementation of method TransactionInfo::stateTransitionIsAllowed
    /// for possible state transitions in this FSA.
    enum class State : int {
        IS_STARTING = 0,  ///< the initial (and transitional) state, next states: (START, START_FAILED)
        STARTED,        ///< the active state allowing data ingests, next states: (IS_FINISHING, IS_ABORTING)
        IS_FINISHING,   ///< the transitional state, next states: (FINISHED, FINISH_FAILED, IS_ABORTING)
        IS_ABORTING,    ///< the transitional state, next states: (ABORTED, ABORT_FAILED)
        FINISHED,       ///< the final successful) state
        ABORTED,        ///< the final unsuccessful state
        START_FAILED,   ///< the failed (inactive) state, next states: (IS_ABORTING)
        FINISH_FAILED,  ///< the failed (inactive) state, next states: (IS_ABORTING)
        ABORT_FAILED    ///< the failed (inactive) state, next states: (IS_ABORTING)
    };

    static State string2state(std::string const& str);
    static std::string state2string(State state);

    /// @brief Verify if the proposed state transition is possible.
    /// @return 'true' if the transition is allowed.
    static bool stateTransitionIsAllowed(State currentState, State newState);

    /// Its unique identifier (the default value represents a non valid transaction)
    TransactionId id = std::numeric_limits<TransactionId>::max();

    /// The name of a database associated with the transaction
    std::string database;

    /// The current state of the transaction
    State state = State::IS_STARTING;

    uint64_t beginTime = 0;       ///< The timestamp (milliseconds) when it was created (IS_STARTING).
    uint64_t startTime = 0;       ///< The timestamp (milliseconds) when it started (STARTED).
    uint64_t transitionTime = 0;  ///< The timestamp (milliseconds) when a transition
                                  ///  commit (IS_FINISHING) / abort (IS_ABORTING) was initiated.
    uint64_t endTime = 0;         ///< The timestamp (milliseconds) is set after transitioning
                                  ///  into the final states (FINISHED, ABORTED) or the failed states
                                  ///  (START_FAILED, FINISH_FAILED, ABORT_FAILED). The timestamp is
                                  ///  reset to 0 when the transaction is moved from either
                                  ///  of the failed states into the transitional state IS_ABORTING.

    /// @return 'true' if the object is in the valid state (was retrieved from the database).
    bool isValid() const;

    /// (Optional) An arbitrary JSON object explaining the transaction.
    /// Normally this could be used later (during transaction abort/commit time, or for general
    /// bookkeeping/ data provenance purposes).
    /// The content of this attribute gets populated if a non-empty string was stored in
    /// the database when staring/ending transaction (method 'createTransaction'), ending
    /// it (method 'updateTransaction') updating its context (method 'updateTransaction') and
    /// if the corresponding transaction lookup method was invoked with the optional flag set
    /// as 'includeContext=true'.
    /// @note methods 'createTransaction' and 'updateTransaction' always
    ///   populates this attribute. Methods 'transaction' and 'transactions' do it only on demand,
    ///   of the corresponding flag passed into those methods is set. Keep in mind that a value of
    ///   the context attribute could be as large as 16 MB as defined by MySQL type 'MEDIUMBLOB'.
    ///   Therefore do not pull the context unless it's strictly necessary.
    ///
    /// @see method DatabaseServices::createTransaction
    /// @see method DatabaseServices::updateTransaction
    /// @see DatabaseServices::transaction
    /// @see DatabaseServices::transactions
    nlohmann::json context = nlohmann::json::object();

    /**
     * Class represents events logged in the lifetime of a transaction.
     */
    class Event {
    public:
        /// The unique identifier of the event
        TransactionEventId id = std::numeric_limits<TransactionEventId>::max();

        /// A state of a transaction when the event was recorded
        State transactionState = State::IS_STARTING;

        /// The name of the event
        std::string name;

        /// The timestamp (milliseconds) when the event was recorded.
        uint64_t time = 0;

        /// The optional data (parameters, context) of the event. The content
        /// of the field depends on the event.
        nlohmann::json data = nlohmann::json::object();

        /// @return JSON representation of the object
        nlohmann::json toJson() const;
    };

    /// (Optional) A collection of events recorded in the lifetime of the transaction.
    /// The log gets populated with event recording actions taken over data at various stages,
    /// Events are recorded and state transitions and while the transaction is within some
    /// state.
    /// The collection is pulled from the database when the corresponding methods are called
    /// with flag 'includeLog=true'.
    std::list<Event> log;

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

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

    std::string url;  ///< The data source specification

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

    /// @return JSON representation of the object
    nlohmann::json toJson() const;

private:
    static std::map<TransactionContribInfo::Status, std::string> const _transactionContribStatus2str;
    static std::map<std::string, TransactionContribInfo::Status> const _transactionContribStr2status;
    static std::vector<TransactionContribInfo::Status> const _transactionContribStatusCodes;
};

/**
 * Class DatabaseIngestParam encapsulate a persistent state of the database ingest
 * parameters which are required to be carried over through the catalog ingests.
 */
class DatabaseIngestParam {
public:
    std::string database;  /// The name of a database for which a parameter is defined
    std::string category;  /// The name of the parameter's category.
    std::string param;     /// The name of the parameter.
    std::string value;     /// A value of the parameter.

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};

/**
 * Row counter for the table entry.
 */
class TableRowStatsEntry {
public:
    TransactionId transactionId = 0;
    unsigned int chunk = 0;  ///< The optional parameter for the "regular" tables.
    bool isOverlap = false;  ///< The optional parameter for the "regular" tables.
    size_t numRows = 0;
    uint64_t updateTime = 0;

    TableRowStatsEntry(TransactionId transactionId_, unsigned int chunk_, bool isOverlap_, size_t numRows_,
                       uint64_t updateTime_);

    TableRowStatsEntry() = default;
    TableRowStatsEntry(TableRowStatsEntry const&) = default;
    TableRowStatsEntry& operator=(TableRowStatsEntry const&) = default;
    ~TableRowStatsEntry() = default;

    /// @return JSON representation of the object.
    nlohmann::json toJson() const;
};

/**
 * Class TableRowStats represents a containers for the statistics captured
 * in a scope of a table.
 */
class TableRowStats {
public:
    std::string database;  /// The name of a database.
    std::string table;     /// The base name of a table.
    std::list<TableRowStatsEntry> entries;

    TableRowStats(std::string const& database_, std::string const& table_);

    TableRowStats() = default;
    TableRowStats(TableRowStats const&) = default;
    TableRowStats& operator=(TableRowStats const&) = default;
    ~TableRowStats() = default;

    /// @return JSON representation of the object.
    nlohmann::json toJson() const;
};

/**
 * Class DatabaseServices is a high-level interface to the database services
 * for replication entities: Controller, Job and Request.
 *
 * This is also a base class for database technology-specific implementations
 * of the service.
 *
 * Methods of this class may through database-specific exceptions, as well
 * as general purpose exceptions explained in their documentation
 * below.
 */
class DatabaseServices : public std::enable_shared_from_this<DatabaseServices> {
public:
    /// The pointer type for instances of the class
    typedef std::shared_ptr<DatabaseServices> Ptr;

    /// Forward declaration for the smart reference to Job objects
    typedef std::shared_ptr<Configuration> ConfigurationPtr;

    /**
     * The factory method for instantiating a proper service object based
     * on an application configuration.
     *
     * @param configuration - the configuration service
     * @return pointer to the created object
     */
    static Ptr create(ConfigurationPtr const& configuration);

    // Copy semantics is prohibited

    DatabaseServices(DatabaseServices const&) = delete;
    DatabaseServices& operator=(DatabaseServices const&) = delete;

    virtual ~DatabaseServices() = default;

    /**
     * Save the state of the Controller. Note this operation can be called
     * just once for a particular instance of the Controller.
     *
     * @param identity an object encapsulating a unique identity of
     *   the Controller instance.
     *
     * @param startTime a time (milliseconds since UNIX Epoch) when an instance
     *   of the Controller was created.
     *
     * @throws std::logic_error if this Controller's state is already found in a database
     */
    virtual void saveState(ControllerIdentity const& identity, uint64_t startTime) = 0;

    /**
     * Save the state of the Job. This operation can be called many times for
     * a particular instance of the Job.
     * @param job a reference to a Job object
     */
    virtual void saveState(Job const& job) = 0;

    /**
     * Update the heartbeat timestamp for the job's entry
     *
     * @param job
     *   reference to a Job object
     */
    virtual void updateHeartbeatTime(Job const& job) = 0;

    /**
     * Save the state of the QservMgtRequest. This operation can be called many times for
     * a particular instance of the QservMgtRequest.
     *
     * The Performance object is explicitly passed as a parameter to avoid
     * making a blocked call back to the request which may create a deadlock.
     *
     * @param request a reference to a QservMgtRequest object
     * @param performance a reference to a Performance object
     * @param serverError a server error message (if any)
     */
    virtual void saveState(QservMgtRequest const& request, Performance const& performance,
                           std::string const& serverError) = 0;

    /**
     * Save the state of the Request. This operation can be called many times for
     * a particular instance of the Request.
     *
     * The Performance object is explicitly passed as a parameter to avoid
     * making a blocked call back to the request which may create a deadlock.
     *
     * @param request a reference to a Request object
     * @param performance a reference to a Performance object
     */
    virtual void saveState(Request const& request, Performance const& performance) = 0;

    /**
     * Update a state of a target request.
     *
     * This method is supposed to be called by monitoring requests (State* and Stop*)
     * to update state of the corresponding target requests.
     *
     * @param request a reference to the monitoring Request object
     * @param targetRequestId an identifier of a target request
     * @param targetRequestPerformance performance counters of a target request
     *   obtained from a worker
     */
    virtual void updateRequestState(Request const& request, std::string const& targetRequestId,
                                    Performance const& targetRequestPerformance) = 0;

    /**
     * Update the status of replica in the corresponding tables.
     *
     * @param info a replica to be added/updated or deleted
     */
    virtual void saveReplicaInfo(ReplicaInfo const& info) = 0;

    /**
     * Update the status of multiple replicas using a collection reported
     * by a request. The method will cross-check replicas reported by the
     * request in a context of the specific worker and a database and resync
     * the database state in this context. Specifically, this means
     * the following:
     *
     * - replicas not present in the collection will be deleted from the database
     * - new replicas not present in the database will be registered in there
     * - existing replicas will be updated in the database
     *
     * @param worker the name of a worker (as per the request)
     * @param database the name of a database (as per the request)
     * @param infoCollection a collection of replicas
     * @throw std::invalid_argument if the database is unknown or empty
     */
    virtual void saveReplicaInfoCollection(std::string const& worker, std::string const& database,
                                           ReplicaInfoCollection const& infoCollection) = 0;

    /**
     * Locate replicas which have the oldest verification timestamps.
     * Return 'true' and populate a collection with up to the 'maxReplicas'
     * if any found.
     *
     * @note no assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replica a reference to an object to be initialized
     * @param maxReplicas (optional) the maximum number of replicas to be returned
     * @param enabledWorkersOnly (optional) if set to 'true' then only consider known
     *   workers which are enabled in the Configuration
     * @param allDatabases (optional) a flag which if set to 'true' will include into the search all
     *   known database entries regardless of their PUBLISHED status. Otherwise
     *   a subset of databases as determined by the second flag 'isPublished'
     *   will get assumed.
     * @param isPublished (optional) a flag which is used if flag 'all' is set to 'false'
     *   to narrow a collection of databases included into the search.
     */
    virtual void findOldestReplicas(std::vector<ReplicaInfo>& replicas, size_t maxReplicas = 1,
                                    bool enabledWorkersOnly = true, bool allDatabases = false,
                                    bool isPublished = true) = 0;

    /**
     * Find all replicas for the specified chunk and the database.
     *
     * @note no assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replicas a collection of replicas (if any found)
     * @param chunk a chunk whose replicas will be looked for
     * @param database the name of a database limiting a scope of the lookup operation
     * @param enabledWorkersOnly (optional) if set to 'true' then only consider known
     *   workers which are enabled in the Configuration
     * @param includeFileInfo a flag will instructs the method wether to provide
     *   the detailed file info for each replica as well.
     *
     * @throw std::invalid_argument if the database is unknown or empty
     */
    virtual void findReplicas(std::vector<ReplicaInfo>& replicas, unsigned int chunk,
                              std::string const& database, bool enabledWorkersOnly = true,
                              bool includeFileInfo = true) = 0;

    /**
     * Find all replicas for the specified collection of chunks and the database.
     * This is an optimized version of the single chunk lookup method defined above,
     *
     * @note no assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replicas a collection of replicas (if any found)
     * @param chunks a collection chunk numbers whose replicas will be looked for
     * @param database the name of a database limiting a scope of the lookup operation
     * @param enabledWorkersOnly (optional) if set to 'true' then only consider known
     *   workers which are enabled in the Configuration
     * @param includeFileInfo a flag will instructs the method wether to provide
     *   the detailed file info for each replica as well.
     *
     * @throw std::invalid_argument if the database is unknown or empty
     */
    virtual void findReplicas(std::vector<ReplicaInfo>& replicas, std::vector<unsigned int> const& chunks,
                              std::string const& database, bool enabledWorkersOnly = true,
                              bool includeFileInfo = true) = 0;

    /**
     * Find all replicas for the specified worker and a database (or all
     * databases if no specific one is requested).
     *
     * @note No assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replicas a collection of replicas (if any found)
     * @param worker the name of a worker
     * @param database (optional)the name of a database
     * @param allDatabases (optional) a flag which if set to 'true' will include
     *   into the search all known database entries regardless of their PUBLISHED
     *   status. Otherwise a subset of databases as determined by the second flag
     *   'isPublished' will get assumed. Note that this flag is used only if no
     *   specific database name is provided as a value of the previous parameter
     *   'database'.
     * @param isPublished (optional) a flag which is used if flag 'all' is set to
     *   'false' to narrow a collection of databases included into the search.
     * @param includeFileInfo a flag will instructs the method wether to provide
     *   the detailed file info for each replica as well.
     *
     * @throw std::invalid_argument if the worker name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas, std::string const& worker,
                                    std::string const& database = std::string(), bool allDatabases = false,
                                    bool isPublished = true, bool includeFileInfo = true) = 0;

    /**
     * Find the number of replicas for the specified worker and a database (or all
     * databases if no specific one is requested).
     *
     * @note No assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param worker the name of a worker
     * @param database (optional) the name of a database
     * @param allDatabases (optional) a flag which if set to 'true' will include
     *   into the search all known database entries regardless of their PUBLISHED
     *   status. Otherwise a subset of databases as determined by the second flag
     *   'isPublished' will get assumed. Note that this flag is used only if
     *   no specific database name is provided as a value of the previous parameter
     *   'database'.
     * @param isPublished (optional) a flag which is used if flag 'all' is set to 'false'
     *   to narrow a collection of databases included into the search.
     *
     * @return the number of replicas
     *
     * @throw std::invalid_argument if the worker name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual uint64_t numWorkerReplicas(std::string const& worker, std::string const& database = std::string(),
                                       bool allDatabases = false, bool isPublished = true) = 0;

    /**
     * Find all replicas for the specified chunk on a worker.
     *
     * @note no assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replicas a collection of replicas (if any found)
     * @param chunk a chunk whose replicas will be looked for at the worker
     * @param worker the name of a worker
     * @param databaseFamily (optional) the name of a database family
     * @param allDatabases (optional) a flag which if set to 'true' will include
     *   into the search all known database entries regardless of their PUBLISHED
     *   status. Otherwise a subset of databases as determined by the second flag
     *   'isPublished' will get assumed.
     * @param isPublished (optional) a flag which is used if flag 'all' is set
     *   to 'false' to further narrow a collection of databases included into
     *   the search.
     *
     * @throw std::invalid_argument if the worker name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas, unsigned int chunk,
                                    std::string const& worker,
                                    std::string const& databaseFamily = std::string(),
                                    bool allDatabases = false, bool isPublished = true) = 0;

    /**
     * Find all replicas for the specified the database.
     *
     * @note no assumption on a new status of the replica collection
     *   passed into the method should be made if the operation fails.
     *
     * @param replicas a collection of replicas (if any found)
     * @param database the name of a database limiting a scope of the lookup operation
     * @param enabledWorkersOnly (optional) if set to 'true' then only consider known
     *   workers which are enabled in the Configuration
     *
     * @throw std::invalid_argument if the database is unknown or empty
     */
    virtual void findDatabaseReplicas(std::vector<ReplicaInfo>& replicas, std::string const& database,
                                      bool enabledWorkersOnly = true) = 0;

    /**
     * Find all unique chunk numbers for the specified the database.
     *
     * @note no assumption on a new status of the chunks collection
     *   passed into the method should be made if the operation fails.
     *
     * @param chunks a collection of chunk numbers (if any found)
     * @param database the name of a database limiting a scope of the lookup operation
     * @param enabledWorkersOnly (optional) if set to 'true' then only consider known
     *   workers which are enabled in the Configuration
     *
     * @throw std::invalid_argument if the database is unknown or empty
     */
    virtual void findDatabaseChunks(std::vector<unsigned int>& chunks, std::string const& database,
                                    bool enabledWorkersOnly = true) = 0;

    /**
     * @note the so called 'overflow' chunks will be implicitly excluded
     *   from the report.
     *
     * @param database the name of a database
     * @param workersToExclude a collection of workers to be excluded from
     *   the consideration. If the empty collection is passed as a value of
     *   the parameter then ALL known (regardless of their 'read-only or
     *   'disabled' status) workers will be considered.
     *
     * @return a map (a histogram) representing the actual replication level
     *   for a database. The key of the map is the replication level (the number of
     *   replicas found for chunks in the group), and the value is the number of
     *   chunks at this replication level.
     *
     * @throw std::invalid_argument if the specified database or any of the workers
     *   in the optional collection was not found in the configuration.
     */
    virtual std::map<unsigned int, size_t> actualReplicationLevel(
            std::string const& database,
            std::vector<std::string> const& workersToExclude = std::vector<std::string>()) = 0;

    /**
     * Locate so called 'orphan' chunks which only exist on a specific set of
     * workers which are supposed to be offline (or in some other unusable state).
     *
     * @param database the name of a database
     * @param uniqueOnWorkers a collection of workers where to look for the chunks
     *   in question
     *
     * @return the total number of chunks which only exist on any worker of
     *   the specified collection of unique workers, and not any other worker
     *   which is not in this collection. The method will always return 0 if
     *   the collection of workers passed into the method is empty.
     *
     * @throw std::invalid_argument if the specified database or any of the workers
     *   in the collection was not found in the configuration.
     */
    virtual size_t numOrphanChunks(std::string const& database,
                                   std::vector<std::string> const& uniqueOnWorkers) = 0;

    /**
     * Log a Controller event
     *
     * @param event an event to be logged
     */
    virtual void logControllerEvent(ControllerEvent const& event) = 0;

    /**
     * Search the log of controller events for events in the specified time range.
     *
     * @param controllerId (optional) a unique identifier of a Controller whose events will
     *   be searched for. The default value (the empty string) doesn't impose any filtering
     *   in this context and results in events reported by all controllers launched in the
     *   past.
     * @param fromTimeStamp (optional) the oldest (inclusive) timestamp for the search.
     * @param toTimeStamp (optional) the most recent (inclusive) timestamp for the search.
     * @param maxEntries (optional) the maximum number of events to be reported. The default
     *   values of 0 doesn't impose any limits.
     * @param task (optional) the filter for desired tasks. The default value (the empty
     *   string) doesn't impose any filtering in this context.
     * @param operation (optional) the filter for desired operations. The default
     *   value (the empty string) doesn't impose any filtering in this context.
     * @param operationStatus (optional) the filter for desired operation statuses.
     *   The default value (the empty string) doesn't impose any filtering in this context.
     *
     * @return a collection of events found within the specified time interval
     */
    virtual std::list<ControllerEvent> readControllerEvents(
            std::string const& controllerId = std::string(), uint64_t fromTimeStamp = 0,
            uint64_t toTimeStamp = std::numeric_limits<uint64_t>::max(), size_t maxEntries = 0,
            std::string const& task = std::string(), std::string const& operation = std::string(),
            std::string const& operationStatus = std::string()) = 0;

    /**
     * @param controllerId (optional) a unique identifier of a Controller whose events will
     *   be analyzed. The default value (the empty string) doesn't impose any filtering
     *   in this context and results in events scanned for all controllers launched in the
     *   past.
     * @return a dictionary of distinct values of the controller's event attributes
     *   obtained from the persistent log.
     */
    virtual nlohmann::json readControllerEventDict(std::string const& controllerId = std::string()) = 0;

    /**
     * Find an information on a controller.
     *
     * @param id a unique identifier of the Controller
     *
     * @return the description of the Controller
     *
     * @throws DatabaseServicesNotFound if no Controller was found for
     *   the specified identifier
     */
    virtual ControllerInfo controller(std::string const& id) = 0;

    /**
     * Find an information on controllers in the specified scope.
     *
     * @param fromTimeStamp (optional) the oldest (inclusive) timestamp for the search
     * @param toTimeStamp (optional) the most recent (inclusive) timestamp for the search
     * @param maxEntries (optional) the maximum number of controllers to be reported.
     *   The default values of 0 doesn't impose any limits.
     *
     * @return a collection of controllers descriptors sorted by the start time in
     *   in the descent order
     */
    virtual std::list<ControllerInfo> controllers(uint64_t fromTimeStamp = 0,
                                                  uint64_t toTimeStamp = std::numeric_limits<uint64_t>::max(),
                                                  size_t maxEntries = 0) = 0;

    /**
     * Find an information on a request.
     *
     * @param id a unique identifier of a request
     *
     * @return the description of the request
     *
     * @throws DatabaseServicesNotFound if no request was found for the specified identifier
     */
    virtual RequestInfo request(std::string const& id) = 0;

    /**
     * Find an information on requests in the specified scope
     *
     * @param jobId a unique identifier of a parent job
     * @param fromTimeStamp (optional) the oldest (inclusive) timestamp for the search
     * @param toTimeStamp (optional) the most recent (inclusive) timestamp for the search
     * @param maxEntries (optional) the maximum number of requests to be reported.
     *   The default values of 0 doesn't impose any limits.
     *
     * @return a collection of request descriptors sorted by the creation time in
     *   in the descent order
     */
    virtual std::list<RequestInfo> requests(std::string const& jobId = "", uint64_t fromTimeStamp = 0,
                                            uint64_t toTimeStamp = std::numeric_limits<uint64_t>::max(),
                                            size_t maxEntries = 0) = 0;

    /**
     * Find an information on a job
     *
     * @param id a unique identifier of a job
     *
     * @return the description of the job
     *
     * @throws DatabaseServicesNotFound if no job was found for the specified identifier
     */
    virtual JobInfo job(std::string const& id) = 0;

    /**
     * Find an information on jobs in the specified scope
     *
     * @param controllerId a unique identifier of a Controller
     * @param parentJobId a unique identifier of a parent job
     * @param fromTimeStamp (optional) the oldest (inclusive) timestamp for the search
     * @param toTimeStamp (optional) the most recent (inclusive) timestamp for the search
     * @param maxEntries (optional) the maximum number of jobs to be reported.
     *   The default values of 0 doesn't impose any limits.
     *
     * @return a collection of jobs descriptors sorted by the start time in
     *   in the descent order
     */
    virtual std::list<JobInfo> jobs(std::string const& controllerId = "", std::string const& parentJobId = "",
                                    uint64_t fromTimeStamp = 0,
                                    uint64_t toTimeStamp = std::numeric_limits<uint64_t>::max(),
                                    size_t maxEntries = 0) = 0;

    /// @param id the unique identifier of a transaction
    /// @param includeContext (optional) flag that (if 'true') would pull the transaction context
    /// @param includeLog (optional) flag that (if 'true') would pull the transaction log (events)
    /// @return a description of a super-transaction
    /// @throws DatabaseServicesNotFound if no such transaction found
    virtual TransactionInfo transaction(TransactionId id, bool includeContext = false,
                                        bool includeLog = false) = 0;

    /// @param databaseName (optional) the name of a database
    /// @param includeContext (optional) flag that (if 'true') would pull the transaction context
    /// @param includeLog (optional) flag that (if 'true') would pull the transaction log (events)
    /// @return a collection of super-transactions (all of them or for the specified database only)
    /// @throws std::invalid_argument if database name is not valid
    virtual std::vector<TransactionInfo> transactions(std::string const& databaseName = std::string(),
                                                      bool includeContext = false,
                                                      bool includeLog = false) = 0;

    /// @param state the desired state of the transactions
    /// @param includeContext (optional) flag that (if 'true') would pull the transacion context
    /// @param includeLog (optional) flag that (if 'true') would pull the transacion log (events)
    /// @return a collection of super-transactions (all of them or for the specified database only)
    virtual std::vector<TransactionInfo> transactions(TransactionInfo::State state,
                                                      bool includeContext = false,
                                                      bool includeLog = false) = 0;

    /// @param databaseName the name of a database
    /// @param namedMutexRegistry the registry for acquiring named mutex to be locked by the method
    /// @param namedMutexLock the lock on the named mutex initialed upon creation of the transaction
    ///   object in the database and (which is important) before committing the transactions.
    ///   The atomicity of transaction creation and locking builds a foundation for implementing
    ///   race-free transaction management in the Controller. The name of the locked mutex is
    ///   based on a unique identifier of the created transaction "transaction:<id>", where "<id>"
    ///   is a placeholder for the identifier. Other transaction management operations are expected
    ///   to acquire a lock on this mutex before attempting to modify a state of the transaction.
    /// @param transactionContext (optional) a user-define context explaining the transaction.
    ///   Note that a serialized value of this attribute could be as large as 16 MB as defined by
    ///   MySQL type 'MEDIUMBLOB', Longer strings will be automatically truncated.
    /// @return a descriptor of the new super-transaction
    /// @throws std::invalid_argument if database name is not valid, or if a value
    ///   of parameter 'transactionContext' is not a valid JSON object.
    /// @throws std::logic_error if super-transactions are not allowed for the database
    /// @see ServiceProvider::getNamedMutex
    virtual TransactionInfo createTransaction(
            std::string const& databaseName, NamedMutexRegistry& namedMutexRegistry,
            std::unique_ptr<util::Lock>& namedMutexLock,
            nlohmann::json const& transactionContext = nlohmann::json::object()) = 0;

    /// @brief Update the state of a transaction
    /// @param id the unique identifier of a transaction
    /// @param newState a new state to turn the transaction into. Note that not all possible
    ///   states are allowed for a transaction in the given state. See details on the transaction's
    ///   FSA in class TransactionInfo.
    /// @return an updated descriptor of the transactions that includes the requested modification
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::logic_error for values of the newState parameters that aren't allowed
    ///   in the current state of the transaction
    virtual TransactionInfo updateTransaction(TransactionId id, TransactionInfo::State newState) = 0;

    /// @brief Update or reset the context attribute of a transaction
    /// @param id the unique identifier of a transaction
    /// @param transactionContext a user-defined context explaining the transaction
    /// @return an updated descriptor of the transactions that includes the requested modification
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::invalid_argument if a value of parameter 'transactionContext'
    ///   is not a valid JSON object.
    /// @note this operation is allowed on transactions in any state
    /// @note the empty input object will reset the context in the database
    virtual TransactionInfo updateTransaction(
            TransactionId id, nlohmann::json const& transactionContext = nlohmann::json::object()) = 0;

    /// @brief Log life-time events for a transaction
    /// @param id the unique identifier of a transaction
    /// @param events life-time events to be recorded for the transaction, where the key
    ///   of the dictionary represents the name of an event, and its value is the data
    ///   representing optional values of the event. The data object can be empty.
    /// @return an updated descriptor of the transactions that includes the requested modification
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::invalid_argument for incorrect values of the input parameters
    virtual TransactionInfo updateTransaction(
            TransactionId id, std::unordered_map<std::string, nlohmann::json> const& events) = 0;

    /// @brief The convenience method for logging single events
    /// @param id the unique identifier of a transaction
    /// @param eventName the name of a life-time events to be recorded
    /// @param eventData the optional data describing the event
    /// @return an updated descriptor of the transactions that includes the requested modification
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::invalid_argument for incorrect values of the input parameters
    TransactionInfo updateTransaction(TransactionId id, std::string const& eventName,
                                      nlohmann::json const& eventData = nlohmann::json::object()) {
        return this->updateTransaction(
                id, std::unordered_map<std::string, nlohmann::json>({{eventName, eventData}}));
    }

    /// @return the desired contribution into a super-transaction (if found)
    /// @param id a unique identifier of the contribution
    /// @throws DatabaseServicesNotFound if no contribution was found for the specified identifier
    virtual TransactionContribInfo transactionContrib(unsigned int id) = 0;

    /// @return contributions into a super-transaction for the given selectors
    /// @param transactionId a unique identifier of the transaction
    /// @param table (optional) the base name of a table (all tables if not provided)
    /// @param worker (optional) the name of a worker (all workers if not provided)
    /// @param typeSelector (optional) type of the contributions
    virtual std::vector<TransactionContribInfo> transactionContribs(
            TransactionId transactionId, std::string const& table = std::string(),
            std::string const& worker = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC) = 0;

    /// @return contributions into a super-transaction for the given selectors
    /// @param transactionId a unique identifier of the transaction
    /// @param status the desired status of the contributions
    /// @param table (optional) the base name of a table (all tables if not provided)
    /// @param worker (optional) the name of a worker (all workers if not provided)
    /// @param typeSelector (optional) type of the contributions
    virtual std::vector<TransactionContribInfo> transactionContribs(
            TransactionId transactionId, TransactionContribInfo::Status status,
            std::string const& table = std::string(), std::string const& worker = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC) = 0;

    /// @return contributions into super-transactions for the given selectors
    /// @param database the name of a database
    /// @param table (optional) the base name of a table (all tables if not provided)
    /// @param worker (optional) the name of a worker (all workers if not provided)
    /// @param typeSelector (optional) type of the contributions
    virtual std::vector<TransactionContribInfo> transactionContribs(
            std::string const& database, std::string const& table = std::string(),
            std::string const& worker = std::string(),
            TransactionContribInfo::TypeSelector typeSelector =
                    TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC) = 0;

    /**
     * Insert the initial record on the contribution.
     *
     * @note The initial state of the contribution will be set to 'IN_PROGRESS' unless
     * flag \param failed will set to 'true'. In this case the status code
     * 'CREATE_FAILED' will be stored.
     *
     * @note The next method to be called to indicate further progress (unless failed)
     * on processing the contribution should be 'startedTransactionContrib'.
     *
     * @param info The transient state of the contribution to be synched.
     * @param failed (optional) The flag to indicate a failure.
     *
     * @return The initial record on the contribution.
     */
    virtual TransactionContribInfo createdTransactionContrib(
            TransactionContribInfo const& info, bool failed = false,
            TransactionContribInfo::Status statusOnFailed =
                    TransactionContribInfo::Status::CREATE_FAILED) = 0;

    /**
     * Update the persistent status of the contribution to indicate that it started
     * to be processed (or failed to be started).
     *
     * @note If a value of \param failed is set to 'true' the status of the contribution
     * will be switched to the final state 'START_FAILED'. In case of a failure
     * the following attributes from the input object will be also synced: 'httpError',
     * 'systemError', 'error', 'retryAllowed'.
     *
     * @note The next method to be called to indicate further progress (unless failed)
     * on processing the contribution should be 'readTransactionContrib'.
     *
     * @param info The transient state of the contribution to be synched.
     * @param failed (optional) The flag to indicate a failure.
     *
     * @return The updated record on the contribution.
     */
    TransactionContribInfo startedTransactionContrib(
            TransactionContribInfo info, bool failed = false,
            TransactionContribInfo::Status statusOnFailed = TransactionContribInfo::Status::START_FAILED);

    /**
     * Update the persistent status of the contribution to indicate that it the input
     * data file has been read/preprocessed (or failed to be read).
     *
     * @note If a value of \param failed is set to 'true' the status of the contribution
     * will be switched to the final state 'READ_FAILED'. In case of a failure
     * the following attributes from the input object will be also synced: 'httpError',
     * 'systemError', 'error', 'retryAllowed'.
     *
     * @note The next method to be called to indicate further progress (unless failed)
     * on processing the contribution should be 'loadedTransactionContrib'.
     *
     * @param info The transient state of the contribution to be synched.
     * @param failed (optional) The flag which if set to 'true' would indicate a error
     * @return The updated record on the contribution.
     */
    TransactionContribInfo readTransactionContrib(
            TransactionContribInfo info, bool failed = false,
            TransactionContribInfo::Status statusOnFailed = TransactionContribInfo::Status::READ_FAILED);

    /**
     * Update the persistent status of the contribution to indicate that it the input
     * data file has been loaded into MySQL (or failed to be read).
     *
     * @note If a value of \param failed is set to 'true' the status of the contribution
     * will be switched to the final state 'LOAD_FAILED'. In case of a failure
     * the following attributes from the input object will be also synced: 'httpError',
     * 'systemError', 'error', 'retryAllowed'.
     *
     * @note This is the final method to be called to indicate a progress
     * on processing the contribution.
     *
     * @param info The transient state of the contribution to be synched.
     * @param failed (optional) The flag which if set to 'true' would indicate a error
     * @return The updated record on the contribution.
     */
    TransactionContribInfo loadedTransactionContrib(
            TransactionContribInfo info, bool failed = false,
            TransactionContribInfo::Status statusOnFailed = TransactionContribInfo::Status::LOAD_FAILED);

    /**
     * Update mutable parameters of the contribution request in the database.
     * @param info The transient state of the contribution to be synched.
     * @return The updated record on the contribution.
     */
    virtual TransactionContribInfo updateTransactionContrib(TransactionContribInfo const& info) = 0;

    /// @return A descriptor of the parameter
    /// @throws DatabaseServicesNotFound If no such parameter found.
    virtual DatabaseIngestParam ingestParam(std::string const& database, std::string const& category,
                                            std::string const& param) = 0;

    /**
     * Find parameters in scope of database and (optionally) a category.
     *
     * @param database The name of a database.
     * @param category (optional) The name of a parameter's category. If the name is empty
     *   then parameters from all categories will be returned.
     *
     * @return A collection the parameter descriptors.
     */
    virtual std::vector<DatabaseIngestParam> ingestParams(std::string const& database,
                                                          std::string const& category = std::string()) = 0;

    /**
     * Save or update a value of a parameter.
     *
     * @param database The name of a database.
     * @param category The name of a parameter's category.
     * @param param The name of the parameter to be saved.
     * @param value A value of the parameter.
     */
    virtual void saveIngestParam(std::string const& database, std::string const& category,
                                 std::string const& param, std::string const& value) = 0;

    /**
     * Save or update a value of a parameter.
     *
     * @param info A descriptor (including its value) of the parameter.
     */
    void saveIngestParam(DatabaseIngestParam const& info) {
        saveIngestParam(info.database, info.category, info.param, info.value);
    }

    /**
     * Retrieve statistics for a table.
     * @param database The name of a database.
     * @param table The name of a table.
     * @param transactionId The optional identifier of a transaction.
     *   If the default value is used then entries across all transactions
     *   will be reported.
     */
    virtual TableRowStats tableRowStats(std::string const& database, std::string const& table,
                                        TransactionId transactionId = 0) = 0;

    /**
     * Save/update statistics of a table.
     * @param stats A collection to be saved.
     */
    virtual void saveTableRowStats(TableRowStats const& stats) = 0;

    /**
     * Delete statistics of a table.
     * @param database The name of a database.
     * @param table The name of a table.
     * @param overlapSelector The optional flavor of a table (partitioned tables only).
     */
    virtual void deleteTableRowStats(
            std::string const& database, std::string const& table,
            ChunkOverlapSelector overlapSelector = ChunkOverlapSelector::CHUNK_AND_OVERLAP) = 0;

protected:
    DatabaseServices() = default;

    /// @return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }
};

}  // namespace lsst::qserv::replica

#endif  // LSST_QSERV_REPLICA_DATABASESERVICES_H
