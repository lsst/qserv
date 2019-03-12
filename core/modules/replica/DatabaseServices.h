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
#include "replica/Job.h"
#include "replica/ReplicaInfo.h"

// This header declarations

namespace lsst {
namespace qserv {
namespace replica {

// Forward declarations
class Configuration;
struct ControllerIdentity;
class QservMgtRequest;
class Performance;
class Request;


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
 * Data structure ControllerEvent encapsulating various info on events logged
 * by Controllers. These objects are retrieved from the persistent logs.
 * 
 * @see DatabaseServices::logControllerEvent
 * @see DatabaseServices::readControllerEvents
 */
struct ControllerEvent {

    /// A unique identifier of the event in the persistent log. Note, that
    /// a value of this field is retrieved from the database. 
    uint32_t id;

    /// Unique identifier of the Controller instance
    std::string controllerId;

    /// 64-bit timestamp (nanoseconds) of the event
    uint64_t timeStamp;

    /// The name of a Controller task defining a scope of the operation
    std::string task;

    /// The name of an operation (request, job, other action)
    std::string operation;

    /// The optional status of the operation
    std::string status;

    /// The optional identifier of a request
    std::string requestId;

    /// The optional identifier of a job
    std::string jobId;

    /// The optional collection (key-value pairs) of the event-specific data
    std::list<std::pair<std::string, std::string>> kvInfo;

    /**
     * @return JSON representation of the event
     */
    nlohmann::json toJson() const;
};


/**
 * Data structure ControllerInfo encapsulates a persistent state of the Controller
 * object fetched from the database.
 */
struct ControllerInfo {

    /// Unique identifier of the Controller instance
    std::string id;

    /// 64-bit timestamp (nanoseconds) for its start time
    uint64_t started;

    /// The name of a host where the Controller was run
    std::string hostname;

    /// the PID of the Controller's process
    int pid;

    /**
     * Translate the structure into a JSON object
     *
     * @param isCurrent
     *   (optional) flag which will set an extra property 'current' to the desired
     *    state (as defined by the value of the flag)
     *
     * @return
     *   JSON representation of the structure
     */
    nlohmann::json toJson(bool isCurrent=false) const;
};


/**
 * Data structure RequestInfo encapsulates a persistent state of the Request (its
 * subclasses) objects fetched from the database.
 */
struct RequestInfo {

    /// Unique identifier of the Request instance
    std::string id;

    /// Unique identifier of the parent Job instance
    std::string jobId;

    /// The name (actually - its specific type) of the request
    std::string name;

    /// The name of a worker where the request was sent
    std::string worker;

    /// The priority level
    int priority;

    /// The primary state
    std::string state;

    /// The secondary state
    std::string extendedState;

    /// The optional status of the request obtained from the corresponding worker
    /// service after the request was (or was attempted to be) executed.
    std::string serverStatus;

    /// The timestamp (nanoseconds) when the request was created by Controller
    uint64_t controllerCreateTime;
    
    /// The timestamp (nanoseconds) when the request was started by Controller
    uint64_t controllerStartTime;
    
    /// The timestamp (nanoseconds) when the request was declared as FINISHED by Controller
    uint64_t controllerFinishTime;

    /// The timestamp (nanoseconds) when the request was received by the corresponding worker
    uint64_t workerReceiveTime;
    
    /// The timestamp (nanoseconds) when the request was started by the corresponding worker
    uint64_t workerStartTime;
    
    /// The timestamp (nanoseconds) when the request was declared as FINISHED by
    /// the corresponding worker
    uint64_t workerFinishTime;
    
    /// The optional collection (key-value pairs) of extended attributes
    std::list<std::pair<std::string, std::string>> kvInfo;

    /**
     * Translate the structure into a JSON object
     *
     * @return
     *   JSON representation of the structure
     */
    nlohmann::json toJson() const;
};


/**
 * Data structure JobInfo encapsulates a persistent state of the Job (its
 * subclasses) objects fetched from the database.
 */
struct JobInfo {

    /// Unique identifier of the Job instance
    std::string id;

    /// Unique identifier of the parent Controller instance)
    std::string controllerId;

    /// Unique identifier of the parent Job instance
    std::string parentJobId;

    /// The type name of the job
    std::string type;

    /// The primary state
    std::string state;

    /// The secondary state
    std::string extendedState;

    /// The timestamp (nanoseconds) when the job started
    uint64_t beginTime;
    
    /// The timestamp (nanoseconds) when the job finished
    uint64_t endTime;

    /// The optional timestamp (nanoseconds) when the job refreshed its state as "still alive"
    uint64_t heartbeatTime;

    /// The priority level
    int priority;

    /// The scheduling parameter of the job allowing it to run w/o interfering
    /// with other jobs in relevant execution contexts
    bool exclusive;

    /// The scheduling parameter allowing the job to be cancelled by job
    /// schedulers if needed
    bool preemptable;

    /// The optional collection (key-value pairs) of extended attributes
    std::list<std::pair<std::string, std::string>> kvInfo;

    /**
     * Translate the structure into a JSON object
     *
     * @return
     *   JSON representation of the structure
     */
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
     * @param identity  - a data structure encapsulating a unique identity of
     *                    the Controller instance.
     * @param startTime - a time (milliseconds since UNIX Epoch) when an instance of
     *                    the Controller was created.
     *
     * @throws std::logic_error - if this Controller's state is already found in a database
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
     * @param job     - reference to a Job object
     * @param options - reference to a Job options object
     */
    virtual void saveState(Job const& job,
                           Job::Options const& options) = 0;

    /**
     * Update the heartbeat timestamp for the job's entry
     *
     * @param job - reference to a Job object
     */
     virtual void updateHeartbeatTime(Job const& job) = 0;

    /**
     * Save the state of the QservMgtRequest. This operation can be called many times for
     * a particular instance of the QservMgtRequest.
     *
     * The Performance object is explicitly passed as a parameter to avoid
     * making a blocked call back to the request which may create a deadlock.
     *
     * @param request     - reference to a QservMgtRequest object
     * @param performance - reference to a Performance object
     * @param serverError - server error message (if any)
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
     * @param request     - reference to a Request object
     * @param performance - reference to a Performance object
     */
    virtual void saveState(Request const& request,
                           Performance const& performance) = 0;

    /**
     * Update a state of a target request.
     *
     * This method is supposed to be called by monitoring requests (State* and Stop*)
     * to update state of the corresponding target requests.
     *
     * @param request                  - reference to the monitoring Request object
     * @param targetRequestId          - identifier of a target request
     * @param targetRequestPerformance - performance counters of a target request
     *                                   obtained from a worker
     */
    virtual void updateRequestState(Request const& request,
                                    std::string const& targetRequestId,
                                    Performance const& targetRequestPerformance) = 0;

    /**
     * Update the status of replica in the corresponding tables.
     *
     * @param info - a replica to be added/updated or deleted
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
     * @param worker         - worker name (as per the request)
     * @param database       - database name (as per the request)
     * @param infoCollection - collection of replicas
     */
    virtual void saveReplicaInfoCollection(std::string const& worker,
                                           std::string const& database,
                                           ReplicaInfoCollection const& infoCollection) = 0;

    /**
     * Locate replicas which have the oldest verification timestamps.
     * Return 'true' and populate a collection with up to the 'maxReplicas'
     * if any found.
     *
     * ATTENTION: no assumption on a new status of the replica object
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replica            - reference to an object to be initialized
     * @param maxReplicas        - maximum number of replicas to be returned
     * @param enabledWorkersOnly - (optional) if set to 'true' then only consider known
     *                             workers which are enabled in the Configuration
     */
    virtual void findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                    size_t maxReplicas=1,
                                    bool enabledWorkersOnly=true) = 0;

    /**
     * Find all replicas for the specified chunk and the database.
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas           - collection of replicas (if any found)
     * @param chunk              - chunk number
     * @param database           - database name
     * @param enabledWorkersOnly - (optional) if set to 'true' then only consider known
     *                             workers which are enabled in the Configuration
     *
     * @throw std::invalid_argument - if the database is unknown or empty
     */
    virtual void findReplicas(std::vector<ReplicaInfo>& replicas,
                              unsigned int chunk,
                              std::string const& database,
                              bool enabledWorkersOnly=true) = 0;

    /**
     * Find all replicas for the specified worker and a database (or all
     * databases if no specific one is requested).
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails.
     *
     * @param replicas - collection of replicas (if any found)
     * @param worker   - worker name
     * @param database - (optional) database name
     *
     * @throw std::invalid_argument - if the worker is unknown or its name
     *                                is empty, or if the database family is
     *                                unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    std::string const& worker,
                                    std::string const& database=std::string()) = 0;

    /**
     * Find the number of replicas for the specified worker and a database (or all
     * databases if no specific one is requested).
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails.
     *
     * @param worker   - worker name
     * @param database - (optional) database name
     *
     * @return the number of replicas
     *
     * @throw std::invalid_argument - if the worker is unknown or its name
     *                                is empty, or if the database family is
     *                                unknown (if provided)
     */
    virtual uint64_t numWorkerReplicas(std::string const& worker,
                                       std::string const& database=std::string()) = 0;

    /**
     * Find all replicas for the specified chunk on a worker.
     *
     * ATTENTION: no assumption on a new status of the replica collection
     * passed into the method should be made if the operation fails
     * (returns 'false').
     *
     * @param replicas       - collection of replicas (if any found)
     * @param chunk          - chunk number
     * @param worker         - worker name of a worker
     * @param databaseFamily - (optional) database family name
     *
     * @throw std::invalid_argument - if the worker is unknown or its name is empty,
     *                                or if the database family is unknown (if provided)
     */
    virtual void findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                    unsigned int chunk,
                                    std::string const& worker,
                                    std::string const& databaseFamily=std::string()) = 0;

    /**
     * @return a map (a histogram) of representing the actual replication level
     * for a database. The key of the map is the replication level (the number of
     * replicas found for chunks in the group), and the key is the number of
     * chunks at this replication level.
     * 
     * @note
     *   the so called 'overflow' chunks will be implicitly excluded
     *   from the report.
     *
     * @param database
     *   the name of a database
     *
     * @param workersToExclude
     *   a collection of workers to be excluded from the consideration. If the empty
     *   collection is passed as a value of the parameter then ALL known (regardless
     *   of their 'read-only or 'disabled' status) workers will be considered.
     *
     * @throw std::invalid_argument
     *   if the specified database or any of the workers in the optional collection
     *   was not found in the configuration.
     */
    virtual std::map<unsigned int, size_t> actualReplicationLevel(
                                                std::string const& database,
                                                std::vector<std::string> const& workersToExclude =
                                                    std::vector<std::string>()) = 0;

    /**
     * @return a total number of chunks which only exist on any worker of
     * the specified collection of unique workers, and not any other worker
     * which is not in this collection. The method will always return 0 if
     * the collection of workers passed into the method is empty.
     *
     * @note
     *   this operation is meant to locate so called 'orphan' chunks which only
     *   exist on a specific set of workers which are supposed to be offline
     *   (or in some other unusable state).
     *
     * @param database
     *   the name of a database
     *
     * @param uniqueOnWorkers
     *   a collection of workers where to look for the chunks in question
     *
     * @throw std::invalid_argument
     *   if the specified database or any of the workers in the collection
     *   was not found in the configuration.
     */
    virtual size_t numOrphanChunks(std::string const& database,
                                   std::vector<std::string> const& uniqueOnWorkers) = 0;

    /**
     * Log a Controller event
     *
     * @param event
     *   event to be logged
     */
    virtual void logControllerEvent(ControllerEvent const& event) = 0;

    /**
     * Search the log of controller events for events in the specified time range.
     *
     * @param controllerId
     *   unique identifier of a Controller whose events will be searched
     *
     * @param fromTimeStamp
     *   (optional) the oldest (inclusive) timestamp for the search.
     * 
     * @param toTimeStamp
     *   (optional) the most recent (inclusive) timestamp for the search.
     *
     * @param maxEntries
     *   (optional) the maximum number of events to be reported. The default
     *   values of 0 doesn't impose any limits.
     *
     * @return
     *   collection of events found within the specified time interval
     */
    virtual std::list<ControllerEvent> readControllerEvents(
                                            std::string const& controllerId,
                                            uint64_t fromTimeStamp=0,
                                            uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                            size_t maxEntries=0) = 0;

    /**
     * Find an information on a controller
     * 
     * @param id
     *   the unique identifier of the Controller
     * 
     * @return
     *   the description of the Controller
     * 
     * @throws DatabaseServicesNotFound
     *   if no Controller was found for the specified identifier
     */
    virtual ControllerInfo controller(std::string const& id) = 0;

    /**
     * Find an information on controllers in the specified scope.
     *
     * @param fromTimeStamp
     *   (optional) the oldest (inclusive) timestamp for the search
     * 
     * @param toTimeStamp
     *   (optional) the most recent (inclusive) timestamp for the search
     *
     * @param maxEntries
     *   (optional) the maximum number of controllers to be reported. The default
     *   values of 0 doesn't impose any limits.
     *
     * @return
     *   a collection of controllers descriptors sorted by the start time in
     *   in the descent order
     */
    virtual std::list<ControllerInfo> controllers(
                                        uint64_t fromTimeStamp=0,
                                        uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                        size_t maxEntries=0) = 0;

    /**
     * Find an information on a request
     * 
     * @param id
     *   the unique identifier of a request
     * 
     * @return
     *   the description of the request
     * 
     * @throws DatabaseServicesNotFound
     *   if no request was found for the specified identifier
     */
    virtual RequestInfo request(std::string const& id) = 0;
    
    /**
     * Find an information on requests in the specified scope
     * 
     * @param jobId
     *   the unique identifier of a parent job
     *
     * @param fromTimeStamp
     *   (optional) the oldest (inclusive) timestamp for the search
     * 
     * @param toTimeStamp
     *   (optional) the most recent (inclusive) timestamp for the search
     *
     * @param maxEntries
     *   (optional) the maximum number of requests to be reported. The default
     *   values of 0 doesn't impose any limits.
     *
     * @return
     *   a collection of request descriptors sorted by the creation time in
     *   in the descent order
     */
    virtual std::list<RequestInfo> requests(std::string const& jobId="",
                                            uint64_t fromTimeStamp=0,
                                            uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                            size_t maxEntries=0) = 0;
    
    /**
     * Find an information on a job
     * 
     * @param id
     *   the unique identifier of a job
     * 
     * @return
     *   the description of the job
     * 
     * @throws DatabaseServicesNotFound
     *   if no job was found for the specified identifier
     */
    virtual JobInfo job(std::string const& id) = 0;

    /**
     * Find an information on jobs in the specified scope
     *
     * @param controllerId
     *   the unique identifier of a Controller
     *
     * @param parentJobId
     *   the unique identifier of a parent job
     *
     * @param fromTimeStamp
     *   (optional) the oldest (inclusive) timestamp for the search
     * 
     * @param toTimeStamp
     *   (optional) the most recent (inclusive) timestamp for the search
     *
     * @param maxEntries
     *   (optional) the maximum number of jobs to be reported. The default
     *   values of 0 doesn't impose any limits.
     *
     * @return
     *   a collection of jobs descriptors sorted by the start time in
     *   in the descent order
     */
    virtual std::list<JobInfo> jobs(std::string const& controllerId="",
                                    std::string const& parentJobId="",
                                    uint64_t fromTimeStamp=0,
                                    uint64_t toTimeStamp=std::numeric_limits<uint64_t>::max(),
                                    size_t maxEntries=0) = 0;

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
