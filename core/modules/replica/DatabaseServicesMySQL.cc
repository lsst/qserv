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
#include "replica/DatabaseServicesMySQL.h"

// System headers
#include <algorithm>
#include <ctime>
#include <stdexcept>
#include <sstream>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DeleteRequest.h"
#include "replica/FindAllRequest.h"
#include "replica/FindRequest.h"
#include "replica/Performance.h"
#include "replica/ReplicaInfo.h"
#include "replica/ReplicationRequest.h"
#include "replica/StatusRequest.h"
#include "replica/StopRequest.h"

// This macro to appear witin each block which requires thread safety
#define LOCK(MUTEX) std::lock_guard<std::mutex> lock(MUTEX)

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesMySQL");

using namespace lsst::qserv::replica;

/**
 * Return 'true' if the specified string is found in a collection.
 *
 * Typical usage:
 * @code
 * bool yesFound = in("what to find", {
 *                    "candidate 1",
 *                    "or candidate 1",
 *                    "what to find",
 *                    "else"});
 * @code
 */
bool in(std::string const& val,
        std::vector<std::string> const& col) {
    return col.end() != std::find(col.begin(), col.end(), val);
}

/**
 * Return 'true' if the specified state is found in a collection.
 *
 * Typical use:
 * @code
 * bool yesFound = in(Request::ExtendedState::SUCCESS, {
 *                          Request::ExtendedState::SUCCESS,
 *                          Request::ExtendedState::SERVER_ERROR,
 *                          Request::ExtendedState::SERVER_CANCELLED});
 * @code
 */
bool in(Request::ExtendedState val,
        std::vector<Request::ExtendedState> const& col) {
    return col.end() != std::find(col.begin(), col.end(), val);
}

/**
 * Try converting to the specified type, then (if successful) extract
 * the target request identifier to be returned via the corresponding function's
 * parameter passed in by a reference. Return false otherwise.
 *
 * ATTENTION: this function will cause the complite time error if the target
 * type won't have the identifier extration method with th eexpected name
 * and a signature.
 */
template <class T>
bool targetRequestDataT(Request::Ptr const& request,
                        std::string& id,
                        Performance& performance) {
    typename T::Ptr ptr = std::dynamic_pointer_cast<T>(request);
    if (ptr) {
        id          = ptr->targetRequestId();
        performance = ptr->targetPerformance();
        return true;
    }
    return false;
}

/**
 * Extract the target request identifier from a request. Note, this is just
 * a wrapper over the above definied function. The metghod accepts requests
 * of the Status* or Stop* types.
 *
 * @param ptr - a request to be tested
 * @return an identifier of the target request
 * @throw std::logic_error for unsupported requsts
 */
void targetRequestData(Request::Ptr const& ptr,
                       std::string& id,
                       Performance& performance) {

    std::string const context = "DatabaseServicesMySQL::targetRequestData  ";
    std::string const name    = ptr->type();

    if (("REQUEST_STATUS:REPLICA_CREATE" == name) and targetRequestDataT<StatusReplicationRequest>(ptr, id, performance)) { return; }
    if (("REQUEST_STATUS:REPLICA_DELETE" == name) and targetRequestDataT<StatusDeleteRequest>(     ptr, id, performance)) { return; }
    if (("REQUEST_STOP:REPLICA_CREATE"   == name) and targetRequestDataT<StopReplicationRequest>(  ptr, id, performance)) { return; }
    if (("REQUEST_STOP:REPLICA_DELETE"   == name) and targetRequestDataT<StopDeleteRequest>(       ptr, id, performance)) { return; }

    throw std::logic_error(
                    context + "unsupported request type " + name +
                    ", or request's actual type and type name mismatch");
}

// Helper methods whose role is to reduce the amount of the boilerplate
// code in the implementations of the correspondig save* methods
// and to make those methos easier to use and maintain.

template <class T>
typename T::Ptr safeAssign(Request::Ptr const& request) {
    static std::string  const context = "DatabaseServicesMySQL::safeAssign[Request]  ";
    typename T::Ptr const ptr = std::dynamic_pointer_cast<T>(request);
    if (ptr) { return ptr; }
    throw std::logic_error(context + "incorrect upcast for request id: " +
                           request->id() + ", type: " + request->type());
}

/**
 * Return the replica info data from eligible requests
 *
 * @param request - a request to be analyzed
 * @return        - a reference to the replica info object
 *
 * @throw std::logic_error for unsupported requsts
 */
ReplicaInfo const& replicaInfo(Request::Ptr const& request) {

    std::string const& context = "DatabaseServicesMySQL::replicaInfo  ";

    if ("REPLICA_CREATE"                == request->type()) { return safeAssign<ReplicationRequest>(      request)->responseData(); }
    if ("REPLICA_DELETE"                == request->type()) { return safeAssign<DeleteRequest>(           request)->responseData(); }
    if ("REPLICA_FIND"                  == request->type()) { return safeAssign<FindRequest>(             request)->responseData(); }
    if ("REQUEST_STATUS:REPLICA_CREATE" == request->type()) { return safeAssign<StatusReplicationRequest>(request)->responseData(); }
    if ("REQUEST_STATUS:REPLICA_DELETE" == request->type()) { return safeAssign<StatusDeleteRequest>(     request)->responseData(); }
    if ("REQUEST_STATUS:REPLICA_FIND"   == request->type()) { return safeAssign<StatusFindRequest>(       request)->responseData(); }
    if ("REQUEST_STOP:REPLICA_CREATE"   == request->type()) { return safeAssign<StopReplicationRequest>(  request)->responseData(); }
    if ("REQUEST_STOP:REPLICA_DELETE"   == request->type()) { return safeAssign<StopDeleteRequest>(       request)->responseData(); }
    if ("REQUEST_STOP:REPLICA_FIND"     == request->type()) { return safeAssign<StopFindRequest>(         request)->responseData(); }

    throw std::logic_error(context + "operation is not supported for request id: " +
                           request->id() + ", type: " + request->type());
}

/**
 * Return the name of a database from the target parameters of eligible requests
 *
 * @param request - a request to be analyzed
 * @return        - the name of a database
 *
 * @throw std::logic_error for unsupported requsts
 */
std::string const& databaseNameOfRequest(Request::Ptr const& request) {

    static std::string const context = "DatabaseServicesMySQL::databaseNameOfRequest  ";

    if ("REPLICA_FIND_ALL"                == request->type()) { return safeAssign<FindAllRequest>(      request)->targetRequestParams().database; }
    if ("REQUEST_STATUS:REPLICA_FIND_ALL" == request->type()) { return safeAssign<StatusFindAllRequest>(request)->targetRequestParams().database; }
    if ("REQUEST_STOP:REPLICA_FIND_ALL"   == request->type()) { return safeAssign<StopFindAllRequest>(  request)->targetRequestParams().database; }

    throw std::logic_error(context + "operation is not supported for request id: " +
                           request->id() + ", type: " + request->type());
}

/**
 * Return a collection of the replica info data from eligible requests
 *
 * @param request - a request to be analyzed
 * @return        - a reference to the replica info collection object
 *
 * @throw std::logic_error for unsupported requsts
 */
ReplicaInfoCollection const& replicaInfoCollection(Request::Ptr const& request) {

    static std::string const context = "DatabaseServicesMySQL::replicaInfoCollection  ";

    if ("REPLICA_FIND_ALL"                == request->type()) { return safeAssign<FindAllRequest>(      request)->responseData(); }
    if ("REQUEST_STATUS:REPLICA_FIND_ALL" == request->type()) { return safeAssign<StatusFindAllRequest>(request)->responseData(); }
    if ("REQUEST_STOP:REPLICA_FIND_ALL"   == request->type()) { return safeAssign<StopFindAllRequest>(  request)->responseData(); }

    throw std::logic_error(context + "operation is not supported for request id: " +
                           request->id() + ", type: " + request->type());
}

template <typename T> bool isEmpty(T const& val) { return !val; }
template <>           bool isEmpty<std::string>(std::string const& val) { return val.empty(); }

/**
 * Update a value of a file attribute in the 'replica_file' table
 * for the corresponding replica.
 *
 * @param conn      - a database connector
 * @param replicaId - a replica of a file
 * @param col       - the column name (teh attribue to be updated)
 * @param val       - a new value of the attribute
 */
template <typename T>
void updateFileAttr(database::mysql::Connection::Ptr const& conn,
                    uint64_t replicaId,
                    std::string const& file,
                    std::string const& col,
                    T const& val) {
    if (isEmpty(val)) { return; }
    conn->execute(
        "UPDATE "  + conn->sqlId("replica_file") +
        "    SET " + conn->sqlEqual(col, val) +
        "  WHERE " + conn->sqlEqual("replica_id", replicaId) +
        "    AND " + conn->sqlEqual("name", file));
}

} /// namespace


namespace lsst {
namespace qserv {
namespace replica {

DatabaseServicesMySQL::DatabaseServicesMySQL(Configuration::Ptr const& configuration)
    :   DatabaseServices(configuration) {

    // Pull database info from the configuration and prepare
    // the connection objects.

    database::mysql::ConnectionParams params;

    params.host     = configuration->databaseHost();
    params.port     = configuration->databasePort();
    params.user     = configuration->databaseUser();
    params.password = configuration->databasePassword();
    params.database = configuration->databaseName();

    _conn  = database::mysql::Connection::open(params);
    _conn2 = database::mysql::Connection::open(params);
}

void DatabaseServicesMySQL::saveState(ControllerIdentity const& identity,
                                      uint64_t startTime) {

    static std::string const context = "DatabaseServicesMySQL::saveState[Controller]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    try {
        _conn->begin();
        _conn->executeInsertQuery(
            "controller",
            identity.id,
            identity.host,
            identity.pid,
            startTime);
        _conn->commit ();

    } catch (database::mysql::DuplicateKeyError const&) {
        _conn->rollback();
        throw std::logic_error(context + "the state is already in the database");
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Job::Ptr const& job) {

    std::string const context = "DatabaseServicesMySQL::saveState[Job::" + job->type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (Job id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        Job::Options const& options = job->options();
        _conn->begin();
        _conn->executeInsertQuery(
            "job",
            job->id(),
            job->controller()->identity().id,
            _conn->nullIfEmpty(job->parentJobId()),
            job->type(),
            Job::state2string(job->state()),
            Job::state2string(job->extendedState()),
                              job->beginTime(),
                              job->endTime(),
            PerformanceUtils::now(),    // heartbeat
            options.priority,
            options.exclusive,
            options.preemptable
        );

        // Extended state (if any provided by a specific job class) is recorded
        // in a job-specific table whose name is based on a value of the job's
        // 'type' parameter.

        std::string extendedTableName = "job_" + job->type();
        std::transform(extendedTableName.begin(),
                       extendedTableName.end(),
                       extendedTableName.begin(),
                       [] (unsigned char c) {
                           return std::tolower(c);
                       });

        std::string const extendedPersistentState = job->extendedPersistentState(_conn);
        if (not extendedPersistentState.empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: " << extendedPersistentState);
            _conn->execute("INSERT INTO " + _conn->sqlId(extendedTableName) +
                           " VALUES " + extendedPersistentState);
        }
        _conn->commit ();

    } catch (database::mysql::DuplicateKeyError const&) {

        try {
            _conn->rollback();
            _conn->begin();
            _conn->executeSimpleUpdateQuery(
                "job",
                _conn->sqlEqual("id",                            job->id()),
                std::make_pair( "state",      Job::state2string (job->state())),
                std::make_pair( "ext_state",  Job::state2string (job->extendedState())),
                std::make_pair( "begin_time",                    job->beginTime()),
                std::make_pair( "end_time",                      job->endTime())
            );
            _conn->commit ();

        } catch (database::mysql::Error const& ex) {
            if (_conn->inTransaction()) _conn->rollback();
            throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::updateHeartbeatTime(Job::Ptr const& job) {

    std::string const context = "DatabaseServicesMySQL::updateHeartbeatTime[Job::" + job->type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);
    try {
        _conn->begin();
        _conn->executeSimpleUpdateQuery(
            "job",
            _conn->sqlEqual("id", job->id()),
            std::make_pair( "heartbeat_time", PerformanceUtils::now())
        );
        _conn->commit ();

    } catch (database::mysql::Error const& ex) {
        if (_conn->inTransaction()) _conn->rollback();
        throw std::runtime_error(context + "failed to update the heartbeat, exception: " + ex.what());
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(QservMgtRequest::Ptr const& request) {

    std::string const context = "DatabaseServicesMySQL::saveState[QservMgtRequest::" + request->type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    // The implementation of the procedure varies quite significally depending on
    // a family of a request.

    // The original (target) requests are processed normally, via the usual
    // protocol: try-insert-if-duplicate-then-update.
    //
    // The extended request-specific persistent state (if available) is extracted
    // from each request and stored in the corresponding table.

    // Requests which haven't started yet or the ones which aren't associated
    // with any job should be ignored.
    try {
        if (request->jobId().empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request with no job set, id=" << request->id());
            return;
        }
    } catch (std::logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request which hasn't yet started, id=" << request->id());
        return;
    }

    Performance const& performance = request->performance();
    try {
        _conn->begin();
        _conn->executeInsertQuery(
            "request",
            request->id(),
            request->jobId(),
            request->type(),
            request->worker(),
            0,
            QservMgtRequest::state2string(request->state()),
            QservMgtRequest::state2string(request->extendedState()),
            request->serverError(),
            performance.c_create_time,
            performance.c_start_time,
            performance.w_receive_time,
            performance.w_start_time,
            performance.w_finish_time,
            performance.c_finish_time);

        // Extended state (if any provided by a specific request class) is recorded
        // in a request-specific table whose name is based on a value of the request's
        // 'type' parameter.

        std::string extendedTableName = "request_" + request->type();
        std::transform(extendedTableName.begin(),
                       extendedTableName.end(),
                       extendedTableName.begin(),
                       [] (unsigned char c) {
                           return std::tolower(c);
                       });

        std::string const extendedPersistentState = request->extendedPersistentState(_conn);
        if (not extendedPersistentState.empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: " << extendedPersistentState);
            _conn->execute("INSERT INTO " + _conn->sqlId(extendedTableName) +
                           " VALUES " + extendedPersistentState);
        }
        _conn->commit ();

    } catch (database::mysql::DuplicateKeyError const&) {

        try {
            _conn->rollback();
            _conn->begin();
            _conn->executeSimpleUpdateQuery(
                "request",
                _conn->sqlEqual("id",                                           request->id()),
                std::make_pair( "state",          QservMgtRequest::state2string(request->state())),
                std::make_pair( "ext_state",      QservMgtRequest::state2string(request->extendedState())),
                std::make_pair( "server_status",                                request->serverError()),
                std::make_pair( "c_create_time",  performance.c_create_time),
                std::make_pair( "c_start_time",   performance.c_start_time),
                std::make_pair( "w_receive_time", performance.w_receive_time),
                std::make_pair( "w_start_time",   performance.w_start_time),
                std::make_pair( "w_finish_time",  performance.w_finish_time),
                std::make_pair( "c_finish_time",  performance.c_finish_time));

            _conn->commit();

        } catch (database::mysql::Error const& ex) {
            if (_conn->inTransaction()) _conn->rollback();
            throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
        }
    }

    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Request::Ptr const& request) {

    std::string const context = "DatabaseServicesMySQL::saveState[Request::" + request->type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    // The implementation of the procedure varies quite significally depending on
    // a family of a request.

    // The original (target) requests are processed normally, via the usual
    // protocol: try-insert-if-duplicate-then-update.


    if (::in(request->type(), {"REPLICA_CREATE",
                               "REPLICA_DELETE"})) {

        // Requests which haven't started yet or the ones which aren't associated
        // with any job should be ignored.
        try {
            if (request->jobId().empty()) {
                LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request with no job set, id=" << request->id());
                return;
            }
        } catch (std::logic_error const&) {
            LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request which hasn't yet started, id=" << request->id());
            return;
        }

        Performance const& performance = request->performance();
        try {
            _conn->begin();
            _conn->executeInsertQuery(
                "request",
                request->id(),
                request->jobId(),
                request->type(),
                request->worker(),
                request->priority(),
                Request::state2string(request->state()),
                Request::state2string(request->extendedState()),
                status2string(request->extendedServerStatus()),
                performance.c_create_time,
                performance.c_start_time,
                performance.w_receive_time,
                performance.w_start_time,
                performance.w_finish_time,
                performance.c_finish_time);

            if (request->type() == "REPLICA_CREATE") {
                auto ptr = safeAssign<ReplicationRequest>(request);
                _conn->executeInsertQuery(
                    "request_replica_create",
                    ptr->id(),
                    ptr->database(),
                    ptr->chunk(),
                    ptr->sourceWorker());
            }
            if (request->type() == "REPLICA_DELETE") {
                auto ptr = safeAssign<DeleteRequest>(request);
                _conn->executeInsertQuery(
                    "request_replica_delete",
                    ptr->id(),
                    ptr->database(),
                    ptr->chunk());
            }
            if (request->extendedState() == Request::ExtendedState::SUCCESS) {
                saveReplicaInfo(::replicaInfo (request));
            }
            _conn->commit ();

        } catch (database::mysql::DuplicateKeyError const&) {

            try {
                _conn->rollback();
                _conn->begin();
                _conn->executeSimpleUpdateQuery(
                    "request",
                    _conn->sqlEqual("id",                                   request->id()),
                    std::make_pair( "state",          Request::state2string(request->state())),
                    std::make_pair( "ext_state",      Request::state2string(request->extendedState())),
                    std::make_pair( "server_status",          status2string(request->extendedServerStatus())),
                    std::make_pair( "c_create_time",  performance.c_create_time),
                    std::make_pair( "c_start_time",   performance.c_start_time),
                    std::make_pair( "w_receive_time", performance.w_receive_time),
                    std::make_pair( "w_start_time",   performance.w_start_time),
                    std::make_pair( "w_finish_time",  performance.w_finish_time),
                    std::make_pair( "c_finish_time",  performance.c_finish_time));

                if (request->extendedState() == Request::ExtendedState::SUCCESS) {
                    saveReplicaInfo(::replicaInfo(request));
                }
                _conn->commit();

            } catch (database::mysql::Error const& ex) {
                if (_conn->inTransaction()) _conn->rollback();
                throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
            }
        }
        return;
    }

    // The Status* or Stop* families of request classes are processed via
    // the limiter protocol: update-if-exists. Nost importantly, the updates
    // would refer to the request's 'targetId' (the one which is being tracked or
    // stopped) rather than the one which is passed into the method as the parameter.
    // The same aplies to the performance counters of the request

    if (::in(request->type(), {"REQUEST_STATUS:REPLICA_CREATE",
                               "REQUEST_STATUS:REPLICA_DELETE",
                               "REQUEST_STOP:REPLICA_CREATE",
                               "REQUEST_STOP:REPLICA_DELETE"})) {

        // Note that according to the current implementation of the requests
        // processing pipeline for both State* and Stop* families of request, these
        // states refer to the target request

        if ((request->state() == Request::State::FINISHED) and
            ::in(request->extendedState(), {Request::ExtendedState::SUCCESS,
                                            Request::ExtendedState::SERVER_QUEUED,
                                            Request::ExtendedState::SERVER_IN_PROGRESS,
                                            Request::ExtendedState::SERVER_IS_CANCELLING,
                                            Request::ExtendedState::SERVER_ERROR,
                                            Request::ExtendedState::SERVER_CANCELLED})) {
            std::string targetRequestId;
            Performance targetPerformance;

            ::targetRequestData(request,
                                targetRequestId,
                                targetPerformance);
            try {
                _conn->begin();
                _conn->executeSimpleUpdateQuery(
                    "request",
                    _conn->sqlEqual("id",                                   targetRequestId),
                    std::make_pair( "state",          Request::state2string(request->state())),
                    std::make_pair( "ext_state",      Request::state2string(request->extendedState())),
                    std::make_pair( "server_status",          status2string(request->extendedServerStatus())),
                    std::make_pair( "w_receive_time", targetPerformance.w_receive_time),
                    std::make_pair( "w_start_time",   targetPerformance.w_start_time),
                    std::make_pair( "w_finish_time",  targetPerformance.w_finish_time));

                if (request->extendedState() == Request::ExtendedState::SUCCESS) {
                    saveReplicaInfo(::replicaInfo(request));
                }
                _conn->commit();

            } catch (database::mysql::Error const& ex) {
                if (_conn->inTransaction()) _conn->rollback();
                throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
            }
        }
    }

    // Save, update or delete replica info according to a report stored within
    // these requests.

    if (::in(request->type(), {"REPLICA_FIND",
                               "REQUEST_STATUS:REPLICA_FIND",
                               "REQUEST_STOP:REPLICA_FIND"})) {

        if ((request->state()         == Request::State::FINISHED) and
            (request->extendedState() == Request::ExtendedState::SUCCESS)) {
            try {
                _conn->begin();
                saveReplicaInfo(::replicaInfo(request));
                _conn->commit();
            } catch (database::mysql::Error const& ex) {
                if (_conn->inTransaction()) _conn->rollback();
                throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
            }
        }
    }

    // Save, update or delete replica info according to a report stored within
    // these requests.

    if (::in(request->type(), {"REPLICA_FIND_ALL",
                               "REQUEST_STATUS:REPLICA_FIND_ALL",
                               "REQUEST_STOP:REPLICA_FIND_ALL"})) {

        if ((request->state()         == Request::State::FINISHED) and
            (request->extendedState() == Request::ExtendedState::SUCCESS)) {
            try {
                _conn->begin();
                saveReplicaInfoCollection(
                    request->worker(),
                    ::databaseNameOfRequest(request),
                    ::replicaInfoCollection(request));
                _conn->commit();
            } catch (database::mysql::Error const& ex) {
                if (_conn->inTransaction()) _conn->rollback();
                throw std::runtime_error(context + "failed to save the state, exception: " + ex.what());
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfo(ReplicaInfo const& info) {

    static std::string const context = "DatabaseServicesMySQL::saveReplicaInfo  ";

    try {

        // Try inserting if the replica is complete. Delete otherwise.

        if (info.status() == ReplicaInfo::Status::COMPLETE) {

            _conn->executeInsertQuery(
                "replica",
                "NULL",                         /* the auto-incremented PK */
                info.worker(),
                info.database(),
                info.chunk(),
                info.verifyTime());

            for (auto&& f: info.fileInfo()) {
                _conn->executeInsertQuery(
                    "replica_file",
                    database::mysql::Function::LAST_INSERT_ID,  /* FK -> PK of the above insert row */
                    f.name,
                    f.size,
                    f.mtime,
                    f.cs,
                    f.beginTransferTime,
                    f.endTransferTime);
            }

        } else {

            // This query will also cascade delete the relevant file entries
            // See details in the schema.
            _conn->execute(
                "DELETE FROM " + _conn->sqlId(   "replica") +
                "  WHERE " +     _conn->sqlEqual("worker",   info.worker()) +
                "    AND " +     _conn->sqlEqual("database", info.database()) +
                "    AND " +     _conn->sqlEqual("chunk",    info.chunk()));
        }

    } catch (database::mysql::DuplicateKeyError const&) {

        // Update the info in case if something has changed (recomputed control/check sum,
        // changed file size, the files migrated, etc.)

        // Get the PK
        //
        // NOTES: in theory this method may throw exceptions. Though, this shouldn't
        //        be happening in this context. We got here because the PK in question
        //        already exists.

        uint64_t replicaId;
        if (not _conn->executeSingleValueSelect(
                "SELECT id FROM " + _conn->sqlId(   "replica") +
                "  WHERE " +        _conn->sqlEqual("worker",   info.worker()) +
                "    AND " +        _conn->sqlEqual("database", info.database()) +
                "    AND " +        _conn->sqlEqual("chunk",    info.chunk()),
                "id",
                replicaId)) {

            throw std::logic_error(context + "NULL value is not allowed in this context");
        }

        // --------------------------------------------------------
        // Completelly replace the replica using the recursive call
        // --------------------------------------------------------

        _conn->execute(
            "DELETE FROM " + _conn->sqlId(   "replica") +
            "  WHERE " +     _conn->sqlEqual("id",replicaId));

        saveReplicaInfo(info);

        return;

        // --------------------------------------------------------------------------
        // ATTENTION: The alternative approach (presented below) would be to update
        // existing records. In practice this method is approximatelly 2 times slower
        // then the previous one (deleting replicas and staring them from scratch).
        // --------------------------------------------------------------------------

        uint64_t const verifyTime = info.verifyTime();
        if (verifyTime) {
            _conn->executeSimpleUpdateQuery(
                "replica",
                _conn->sqlEqual("id",          replicaId),
                std::make_pair( "verify_time", verifyTime));
        }
        for (auto&& f: info.fileInfo()) {
            ::updateFileAttr(_conn, replicaId, f.name, "begin_create_time", f.beginTransferTime);
            ::updateFileAttr(_conn, replicaId, f.name, "end_create_time",   f.endTransferTime);
            ::updateFileAttr(_conn, replicaId, f.name, "size",              f.size);
            ::updateFileAttr(_conn, replicaId, f.name, "mtime",             f.mtime);
            ::updateFileAttr(_conn, replicaId, f.name, "cs",                f.cs);
        }
    }
}

void DatabaseServicesMySQL::saveReplicaInfoCollection(std::string const& worker,
                                                      std::string const& database,
                                                      ReplicaInfoCollection const& infoCollection) {

    static std::string const context = "DatabaseServicesMySQL::saveReplicaInfoCollection  ";

    LOGS(_log, LOG_LVL_DEBUG, context << "infoCollection.size(): " << infoCollection.size());

    // Group new replicas by categories
    std::map<std::string,                       // worker
             std::map<std::string,              // database
                      std::map<unsigned int,    // chunk
                               bool>>> newReplicas;

    for (auto&& info: infoCollection) {
        newReplicas[info.worker()][info.database()][info.chunk()] = true;
    }

    // Note that this algorithm will also work if the input collection has
    // multiple contexts. The algorithm will affect database stored replicas
    // in the requested ('worker' and 'database' parameters of the method)
    // context only.

    if (newReplicas.count( worker) and
        newReplicas.at(    worker).count(database) and
        not newReplicas.at(worker).at(   database).empty()) {

        // Check each old replicas to see if it's present in the new collection
        std::vector<ReplicaInfo> oldReplicas;
        if (findWorkerReplicasNoLock(oldReplicas, worker, database)) {

            for (ReplicaInfo const& replica: oldReplicas) {
                unsigned int const chunk = replica.chunk();

                // Eliminate the 'dead' chunks entry from the database
                if (not newReplicas.at(worker).at(database).count(chunk)) {
                    _conn->execute(
                        "DELETE FROM " + _conn->sqlId(   "replica") +
                        "  WHERE " +     _conn->sqlEqual("worker",   worker) +
                        "    AND " +     _conn->sqlEqual("database", database) +
                        "    AND " +     _conn->sqlEqual("chunk",    chunk));
                }
            }
        }

    } else {

        // Bulk delete if the input collection is empty or has no context
        _conn->execute(
            "DELETE FROM " + _conn->sqlId(   "replica") +
            "  WHERE " +     _conn->sqlEqual("worker",   worker) +
            "    AND " +     _conn->sqlEqual("database", database));
    }

    // Finally push new (or update existing) replicas info into the database
    // (some of those replicas will be brand new, others - will need to be updated)
    for (auto&& info: infoCollection) {
        saveReplicaInfo(info);
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
}

bool DatabaseServicesMySQL::findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                               size_t maxReplicas,
                                               bool enabledWorkersOnly) const {

    static std::string const context = "DatabaseServicesMySQL::findOldestReplicas  ";

    LOCK(_mtx);

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not maxReplicas) {
        throw std::invalid_argument(context + "maxReplicas is not allowed to be 0");
    }
    if (not findReplicas(
                replicas,
                "SELECT * FROM " + _conn->sqlId("replica") +
                (enabledWorkersOnly ?
                " WHERE " +        _conn->sqlIn("worker", _configuration->workers(true)) : "") +
                " ORDER BY "     + _conn->sqlId("verify_time") + " ASC LIMIT " + std::to_string(maxReplicas)) or
        not replicas.size()) {

        LOGS(_log, LOG_LVL_ERROR, context << "failed to find the oldest replica(s)");
        return false;
    }
    return true;
}

bool DatabaseServicesMySQL::findReplicas(std::vector<ReplicaInfo>& replicas,
                                         unsigned int chunk,
                                         std::string const& database,
                                         bool enabledWorkersOnly) const {
    std::string const context =
         "DatabaseServicesMySQL::findReplicas  chunk: " + std::to_string(chunk) +
         "  database: " + database + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    if (not _configuration->isKnownDatabase(database)) {
        throw std::invalid_argument(context + "unknow database");
    }
    if (not findReplicas(
                replicas,
                "SELECT * FROM " +  _conn->sqlId(   "replica") +
                "  WHERE " +        _conn->sqlEqual("chunk",    chunk) +
                "    AND " +        _conn->sqlEqual("database", database) +
                (enabledWorkersOnly ?
                 "   AND " +        _conn->sqlIn(   "worker",   _configuration->workers(true)) :""))) {

        LOGS(_log, LOG_LVL_ERROR, context << "failed to find replicas");
        return false;
    }
    return true;
}

bool DatabaseServicesMySQL::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                               std::string const& worker,
                                               std::string const& database) const {
    LOCK(_mtx);
    return findWorkerReplicasNoLock(replicas,
                                    worker,
                                    database);
}

bool DatabaseServicesMySQL::findWorkerReplicasNoLock(std::vector<ReplicaInfo>& replicas,
                                                     std::string const& worker,
                                                     std::string const& database) const {
    std::string const context =
         "DatabaseServicesMySQL::findWorkerReplicasNoLock  worker: " + worker +
         " database: " + database + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownWorker(worker)) {
        throw std::invalid_argument(context + "unknow worker");
    }
    if (not _configuration->isKnownDatabase(database)) {
        throw std::invalid_argument(context + "unknow database");
    }
    if (not findReplicas(
                replicas,
                "SELECT * FROM " + _conn->sqlId(   "replica") +
                "  WHERE " +       _conn->sqlEqual("worker",   worker) +
                (database.empty() ? "" :
                "  AND "   +       _conn->sqlEqual( "database", database)))) {

        LOGS(_log, LOG_LVL_ERROR, context << "failed to find replicas");
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "replicas.size(): " << replicas.size());
    return true;
}

bool DatabaseServicesMySQL::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                               unsigned int chunk,
                                               std::string const& worker,
                                               std::string const& databaseFamily) const {
    std::string const context =
         "DatabaseServicesMySQL::findWorkerReplicas  worker: " + worker +
         " chunk: " + std::to_string(chunk) + "  database family: " + databaseFamily;

    LOGS(_log, LOG_LVL_DEBUG, context);

    LOCK(_mtx);

    if (not _configuration->isKnownWorker(worker)) {
        throw std::invalid_argument(context + "unknow worker");
    }
    if (not databaseFamily.empty() and not _configuration->isKnownDatabaseFamily(databaseFamily)) {
        throw std::invalid_argument(context + "unknow databaseFamily");
    }
    if (not findReplicas(
                replicas,
                "SELECT * FROM " + _conn->sqlId(   "replica") +
                "  WHERE " +       _conn->sqlEqual("worker", worker) +
                "  AND "   +       _conn->sqlEqual("chunk",  chunk) +
                (databaseFamily.empty() ? "" :
                "  AND " + _conn->sqlIn("database", _configuration->databases(databaseFamily))))) {

        LOGS(_log, LOG_LVL_ERROR, context << "failed to find replicas");
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "num replicas found: " << replicas.size());
    return true;
}

bool DatabaseServicesMySQL::findReplicas(std::vector<ReplicaInfo>& replicas,
                                         std::string const& query) const {

    static std::string const context = "DatabaseServicesMySQL::findReplicas(replicas,query)  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    bool result = false;

    bool startedTransaction  = false;
    bool startedTransaction2 = false;
    try {
        if (not _conn->inTransaction()) {
            _conn->begin();
            startedTransaction = true;
        }
        _conn->execute(query);

        if (not _conn2->inTransaction()) {
            _conn2->begin();
            startedTransaction2 = true;
        }
        if (_conn->hasResult()) {

            database::mysql::Row row;
            while (_conn->next(row)) {

                // Extract general attributes of the replica

                uint64_t     id;
                std::string  worker;
                std::string  database;
                unsigned int chunk;
                uint64_t     verifyTime;

                row.get("id",          id);
                row.get("worker",      worker);
                row.get("database",    database);
                row.get("chunk",       chunk);
                row.get("verify_time", verifyTime);

                // Pull a list of files associated with the replica
                //
                // ATTENTION: using a separate connector to avoid any interference
                //            with a context of the upper loop iterator.

                ReplicaInfo::FileInfoCollection files;

                _conn2->execute(
                    "SELECT * FROM " + _conn->sqlId(   "replica_file") +
                    "  WHERE "       + _conn->sqlEqual("replica_id", id));

                if (_conn2->hasResult()) {

                    database::mysql::Row row2;
                    while (_conn2->next(row2)) {

                        // Extract attributes of the file

                        std::string name;
                        uint64_t    size;
                        std::time_t mtime;
                        std::string cs;
                        uint64_t    beginCreateTime;
                        uint64_t    endCreateTime;

                        row2.get("name",              name);
                        row2.get("size",              size);
                        row2.get("mtime",             mtime);
                        row2.get("cs",                cs);
                        row2.get("begin_create_time", beginCreateTime);
                        row2.get("end_create_time",   endCreateTime);

                        files.emplace_back(
                            ReplicaInfo::FileInfo{
                                name,
                                size,
                                mtime,
                                cs,
                                beginCreateTime,
                                endCreateTime,
                                size
                            }
                        );
                    }
                }
                replicas.emplace_back(
                    ReplicaInfo(
                        ReplicaInfo::Status::COMPLETE,
                        worker,
                        database,
                        chunk,
                        verifyTime,
                        files
                    )
                );
            }
        }
        result = true;

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context
             << "database operation failed due to: " << ex.what());
    }
    if (startedTransaction  and _conn ->inTransaction()) _conn ->rollback();
    if (startedTransaction2 and _conn2->inTransaction()) _conn2->rollback();

    return result;
}

}}} // namespace lsst::qserv::replica
