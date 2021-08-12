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

// Third party headers
#include "nlohmann/json.hpp"

// Qserv headers
#include "replica/Common.h"
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"

// Forward declarations
namespace lsst {
namespace qserv {
namespace replica {
    class Configuration;
    class ControllerIdentity;
    class QservMgtRequest;
    class Performance;
    class Request;
}}}  // Forward declarations

// This header declarations
namespace lsst {
namespace qserv {
namespace replica {

/**
 * Class DatabaseServicesError is an exception class for reporting errors
 * in database operations.
 */
class DatabaseServicesError : public std::runtime_error {
public:
    DatabaseServicesError(std::string const& msg)
        :   std::runtime_error(std::string(__func__) + ": " + msg) {
    }
};


/**
 * Class DatabaseServicesNotFound is an exception class for reporting content
 * which can't be found in the database.
 */
class DatabaseServicesNotFound: public std::runtime_error {
public :
    DatabaseServicesNotFound(std::string const& msg)
        :   std::runtime_error(std::string(__func__) + ": " + msg) {
    }
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
    uint32_t id = 0;    /// A unique identifier of the event in the persistent log.
                        /// Note, that a value of this field is retrieved from the database. 

    std::string controllerId;   /// Unique identifier of the Controller instance
    uint64_t    timeStamp = 0;  /// 64-bit timestamp (ms) of the event

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
    std::string id; /// Unique identifier of the Controller instance

    uint64_t started = 0;   /// 64-bit timestamp (ms) for its start time

    std::string hostname;   /// The name of a host where the Controller was run

    int pid = 0;    /// the PID of the Controller's process

    /**
     * Translate an instance into a JSON object
     *
     * @param isCurrent (optional) flag which will set an extra property 'current'
     *   to the desired state (as defined by the value of the flag)
     *
     * @return JSON representation of the object
     */
    nlohmann::json toJson(bool isCurrent=false) const;
};


/**
 * Class RequestInfo encapsulates a persistent state of the Request (its
 * subclasses) objects fetched from the database.
 */
class RequestInfo {
public:
    std::string id;     /// Unique identifier of the Request instance
    std::string jobId;  /// Unique identifier of the parent Job instance
    std::string name;   /// The name (actually - its specific type) of the request
    std::string worker; /// The name of a worker where the request was sent

    int priority = 0;   /// The priority level

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

    uint64_t beginTime = 0;     /// The timestamp (ms) when the job started
    uint64_t endTime = 0;       /// The timestamp (ms) when the job finished
    uint64_t heartbeatTime = 0; /// The optional timestamp (ms) when the job refreshed its state as "still alive"

    int priority = 0;   /// The priority level

    bool exclusive = false; /// The scheduling parameter of the job allowing it to run w/o
                            /// interfering with other jobs in relevant execution contexts

    bool preemptable = true;    /// The scheduling parameter allowing the job to be cancelled
                                /// by job schedulers if needed

    std::list<std::pair<std::string, std::string>> kvInfo;  /// The optional collection (key-value pairs)
                                                            /// of extended attributes

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};


/**
 * Class TransactionInfo encapsulates a persistent state of
 * the "super-transaction" objects fetched from the database.
 */
class TransactionInfo {
public:
    /// Allowed states for the transaction
    enum State {
        STARTED,
        FINISHED,
        ABORTED
    };

    static State string2state(std::string const& str);
    static std::string state2string(State state);

    /// Its unique identifier (the default value represents a non valid transaction)
    TransactionId id = std::numeric_limits<TransactionId>::max();

    /// The name of a database associated with the transaction
    std::string database;

    State state = State::STARTED;

    uint64_t beginTime = 0; /// The timestamp (milliseconds) when it started
    uint64_t endTime = 0;   /// The timestamp (milliseconds) when it was committed/aborted

    /// (Optional) An arbitrary JSON object explaining the transaction.
    /// Normally this could be used later (during transaction abort/commit time, or for general
    /// bookkeeping/ data provenance purposes).
    /// The content of this attribute gets populated if a non-empty string was stored in
    /// the database when staring/ending transaction (methods 'beginTransaction', 'endTransaction')
    /// or updating the context (method 'updateTransaction') and if the corresponding
    /// transaction lookup method was invoked with the optional flag set as 'includeContext=true'.
    /// @note methods 'beginTranaction', 'endTransaction', and 'updateTransaction' always
    ///   populates this attribute. Methods 'transaction' and 'transactions' do it only on demand,
    ///   of the corresponding flag passed into those methods is set. Keep in mind that a value of
    ///   the context attribute could be as large as 16 MB as defined by MySQL type 'MEDIUMBLOB'.
    ///   Therefore do not pull the context unless it's strictly necessary.
    ///
    /// @see method DatabaseServices::beginTransaction
    /// @see method DatabaseServices::endTransaction
    /// @see method DatabaseServices::updateTransaction
    /// @see DatabaseServices::transaction
    /// @see DatabaseServices::transactions
   nlohmann::json context = nlohmann::json::object();

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
    // These data members are initialized by the meaninfull values after the initial
    // recording of the info in the database. After that they would never change.

    /// The unique identifier of a contribution is used mostly for the state
    /// tracking purposes. The identifier is set after the initial record on
    /// ingesting the contribution is recorded in the persistentstate.
    unsigned int id = std::numeric_limits<unsigned int>::max();

    /// The unique identifier of a parent transaction.
    TransactionId transactionId = std::numeric_limits<TransactionId>::max();

    std::string worker;         /// The name name of a worker

    std::string database;       /// The name of a database
    std::string table;          /// The base name of a table where the contribution was made

    unsigned int chunk = 0;     /// (optional) The chunk number (partitioned tables only)
    bool isOverlap = false;     /// (optional) A flavor of the chunked table (partitioned tables only)

    std::string url;            /// The data source specification

    // ---------------------------------------------------------------------------
    // These data members are meant to be used for tracking a status of an operation
    // as it's being progressing and for the performance analysis.
    //
    //   beginTime - is set after the initial recording of the info in the database
    //   endTime   - is set after finishing (regardless of an outcome) the operation
    //
    //      IMPORTANT:
    //         It's possible for the 'endTime' attribute to be never set in case of
    //         a catastrophic failure of the worker ingest service. This may
    //         create a potential ambiguity in interpreting a state of
    //         the contribution before its parent transaction finishes.
    //         Seeing the default value of 0 while the transaction is still
    //         open may indicate an on-going ingest, or a failed ingest.
    //         Once the transaction finishes zero value would always indicate
    //         a failure.
    //         The same rules apply to attrubutes 'numBytes', 'numRows' and 'success'
    //
    //   numBytes  - is set upon the successful completion of the operation
    //   numRows   - is set upon the successful completion of the operation
    //   success   - is set to 'true' if the operation has succeeded

    uint64_t beginTime = 0;     /// The timestamp (milliseconds) when the ingest started
    uint64_t endTime = 0;       /// The timestamp (milliseconds) when the ingest finished
    uint64_t numBytes = 0;      /// The total number of bytes read from the source
    uint64_t numRows = 0;       /// The total number of rows read from the source

    bool success = false;       /// The completion status of the ingest operation

    /// @return JSON representation of the object
    nlohmann::json toJson() const;
};


/**
 * Class DatabaseIngestParam encapsulate a persistent state of the database ingest
 * parameters which are required to be carried over through the catalog ingests.
 */
class DatabaseIngestParam {
public:
    std::string database;   /// The name of a database for which a parameter is defined
    std::string category;   /// The name of the parameter's category.
    std::string param;      /// The name of the parameter.
    std::string value;      /// A value of the parameter.

    /// @return JSON representation of the object
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
class DatabaseServices: public std::enable_shared_from_this<DatabaseServices> {
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
    virtual void saveState(ControllerIdentity const& identity,
                           uint64_t startTime) = 0;

    /**
     * Save the state of the Job. This operation can be called many times for
     * a particular instance of the Job.
     *
     * The Job::Option object is explicitly passed as a parameter to avoid
     * making a blocked call back to the job which may create a deadlock.
     *
     * @param job a reference to a Job object
     * @param options a reference to a Job options object
     */
    virtual void saveState(Job const& job,
                           Job::Options const& options) = 0;

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
    virtual void saveState(QservMgtRequest const& request,
                           Performance const& performance,
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
    virtual void saveState(Request const& request,
                           Performance const& performance) = 0;

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
    virtual void updateRequestState(Request const& request,
                                    std::string const& targetRequestId,
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
    virtual void saveReplicaInfoCollection(std::string const& worker,
                                           std::string const& database,
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
    virtual void findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                    size_t maxReplicas=1,
                                    bool enabledWorkersOnly=true,
                                    bool allDatabases=false,
                                    bool isPublished=true) = 0;

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
    virtual void findReplicas(std::vector<ReplicaInfo>& replicas,
                              unsigned int chunk,
                              std::string const& database,
                              bool enabledWorkersOnly=true,
                              bool includeFileInfo=true) = 0;

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
    virtual void findReplicas(std::vector<ReplicaInfo>& replicas,
                              std::vector<unsigned int> const& chunks,
                              std::string const& database,
                              bool enabledWorkersOnly=true,
                              bool includeFileInfo=true) = 0;

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
     * @throw std::invalid_argument if the worker is unknown or its name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    std::string const& worker,
                                    std::string const& database=std::string(),
                                    bool allDatabases=false,
                                    bool isPublished=true,
                                    bool includeFileInfo=true) = 0;

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
     * @throw std::invalid_argument if the worker is unknown or its name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual uint64_t numWorkerReplicas(std::string const& worker,
                                       std::string const& database=std::string(),
                                       bool allDatabases=false,
                                       bool isPublished=true) = 0;

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
     * @throw std::invalid_argument if the worker is unknown or its name is empty,
     *   or if the database family is unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    unsigned int chunk,
                                    std::string const& worker,
                                    std::string const& databaseFamily=std::string(),
                                    bool allDatabases=false,
                                    bool isPublished=true) = 0;

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
    virtual void findDatabaseReplicas(std::vector<ReplicaInfo>& replicas,
                                      std::string const& database,
                                      bool enabledWorkersOnly=true) = 0;

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
    virtual void findDatabaseChunks(std::vector<unsigned int>& chunks,
                                    std::string const& database,
                                    bool enabledWorkersOnly=true) = 0;

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
                                                std::vector<std::string> const& workersToExclude =
                                                    std::vector<std::string>()) = 0;

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
                                            std::string const& controllerId=std::string(),
                                            uint64_t fromTimeStamp=0,
                                            uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                            size_t maxEntries=0,
                                            std::string const& task=std::string(),
                                            std::string const& operation=std::string(),
                                            std::string const& operationStatus=std::string()) = 0;

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
    virtual std::list<ControllerInfo> controllers(
                                        uint64_t fromTimeStamp=0,
                                        uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                        size_t maxEntries=0) = 0;

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
    virtual std::list<RequestInfo> requests(std::string const& jobId="",
                                            uint64_t fromTimeStamp=0,
                                            uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                            size_t maxEntries=0) = 0;
    
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
    virtual std::list<JobInfo> jobs(std::string const& controllerId="",
                                    std::string const& parentJobId="",
                                    uint64_t fromTimeStamp=0,
                                    uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                    size_t maxEntries=0) = 0;

    /// @param id the unique identifier of a transaction
    /// @param includeContext (optional) flag that (if 'true') would pull the transacion context
    /// @return a description of a super-transaction
    /// @throws DatabaseServicesNotFound if no such transaction found
    virtual TransactionInfo transaction(TransactionId id,
                                        bool includeContext=false) = 0;

    /// @param databaseName (optional) the name of a database
    /// @param includeContext (optional) flag that (if 'true') would pull the transacion context
    /// @return a collection of super-transactions (all of them or for the specified database only)
    /// @throws std::invalid_argument if database name is not valid
    virtual std::vector<TransactionInfo> transactions(std::string const& databaseName=std::string(),
                                                      bool includeContext=false) = 0;

    /// @param databaseName the name of a database
    /// @param transactionContext (optional) a user-define context explaining the transaction.
    ///   Note that a serialized value of this attribute could be as large as 16 MB as defined by
    ///   MySQL type 'MEDIUMBLOB', Longer strings will be automatically truncated.
    /// @return a descriptor of the new super-transaction 
    /// @throws std::invalid_argument if database name is not valid, or if a value
    ///   of parameter 'transactionContext' is not a valid JSON object.
    /// @throws std::logic_error if super-transactions are not allowed for the database
    virtual TransactionInfo beginTransaction(std::string const& databaseName,
                                             nlohmann::json const& transactionContext=nlohmann::json::object()) = 0;

    /// @param id the unique identifier of a transaction
    /// @param abort (optional) flag indicating if the transaction is being committed or aborted
    /// @return an updated descriptor of the (committed or aborted) super-transaction 
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::logic_error if the transaction has already ended
    virtual TransactionInfo endTransaction(TransactionId id,
                                           bool abort=false) = 0;

    /// @param a user-defined context explaining the transaction
    /// @return an updated descriptor of the transactions that includes the requested modification
    /// @throws DatabaseServicesNotFound if no such transaction found
    /// @throws std::invalid_argument if a value of parameter 'transactionContext'
    ///   is not a valid JSON object.
    /// @note this operation is allowed on transactions in any state
    /// @note the empty input object will reset the context in the database
    virtual TransactionInfo updateTransaction(TransactionId id,
                                              nlohmann::json const& transactionContext=nlohmann::json::object()) = 0;

    /// @return contributions into a super-transaction for the given selectors
    /// @param transactionId a unique identifier of the transaction
    /// @param table (optional) the base name of a table (all tables if not provided)
    /// @param worker (optional) the name of a worker (all workers if not provided)
    virtual std::vector<TransactionContribInfo> transactionContribs(TransactionId transactionId,
                                                                    std::string const& table=std::string(),
                                                                    std::string const& worker=std::string()) = 0;

    /// @return contributions into super-transactions for the given selectors
    /// @param database the name of a database
    /// @param table (optional) the base name of a table (all tables if not provided)
    /// @param worker (optional) the name of a worker (all workers if not provided)
    virtual std::vector<TransactionContribInfo> transactionContribs(std::string const& database,
                                                                    std::string const& table=std::string(),
                                                                    std::string const& worker=std::string()) = 0;

    /// Insert the initial record on the contribution before its size and the operation's
    /// outcome will be known. For the later use method endTransactionContrib.
    /// @param transactionId a unique identifier of the transaction
    /// @param table the base name of a table
    /// @param chunk the chunk number (ignored for non-partitioned tables)
    /// @param isOverlap the kind of a table (ignored for non-partitioned tables)
    /// @param worker the name of a worker
    /// @param url the data source specification
    /// @return the initial record on the contribution
    virtual TransactionContribInfo beginTransactionContrib(TransactionId transactionId,
                                                           std::string const& table,
                                                           unsigned int chunk,
                                                           bool isOverlap,
                                                           std::string const& worker,
                                                           std::string const& url) = 0;

    /// Update or finalize the contribution status
    /// @param info the transient state of the contribution to be synched with
    ///   the persistent store
    /// @return the updated record on the contribution
    virtual TransactionContribInfo endTransactionContrib(TransactionContribInfo const& info) = 0;

    /// @return A descriptor of the parameter
    /// @throws DatabaseServicesNotFound If no such parameter found.
    virtual DatabaseIngestParam ingestParam(std::string const& database,
                                            std::string const& category,
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
                                                          std::string const& category=std::string()) = 0;

    /**
     * Save or update a value of a parameter.
     * 
     * @param database The name of a database.
     * @param category The name of a parameter's category.
     * @param param The name of the parameter to be saved.
     * @param value A value of the parameter.
     */
    virtual void saveIngestParam(std::string const& database,
                                 std::string const& category,
                                 std::string const& param,
                                 std::string const& value) = 0;

    /**
     * Save or update a value of a parameter.
     * 
     * @param info A descriptor (including its value) of the parameter.
     */
    void saveIngestParam(DatabaseIngestParam const& info) {
        saveIngestParam(info.database, info.category, info.param, info.value);
    }

protected:

    DatabaseServices() = default;

    /// @return shared pointer of the desired subclass (no dynamic type checking)
    template <class T>
    std::shared_ptr<T> shared_from_base() {
        return std::static_pointer_cast<T>(shared_from_this());
    }
};

}}} // namespace lsst::qserv::replica

#endif // LSST_QSERV_REPLICA_DATABASESERVICES_H
