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
#include <cstdint>
#include <ctime>
#include <stdexcept>

// Qserv headers
#include "lsst/log/Log.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/Job.h"
#include "replica/Performance.h"
#include "replica/QservMgtRequest.h"
#include "replica/ReplicaInfo.h"
#include "replica/Request.h"
#include "replica/SemanticMaps.h"

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesMySQL");

using namespace lsst::qserv::replica;

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

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

DatabaseServicesMySQL::DatabaseServicesMySQL(Configuration::Ptr const& configuration)
    :   DatabaseServices(),
        _configuration(configuration) {

    // Pull database info from the configuration and prepare
    // the connection objects.

    database::mysql::ConnectionParams params;

    params.host     = configuration->databaseHost();
    params.port     = configuration->databasePort();
    params.user     = configuration->databaseUser();
    params.password = configuration->databasePassword();
    params.database = configuration->databaseName();

    _conn = database::mysql::Connection::open(params);
}

void DatabaseServicesMySQL::saveState(ControllerIdentity const& identity,
                                      uint64_t startTime) {

    std::string const context = "DatabaseServicesMySQL::saveState[Controller]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "controller",
                    identity.id,
                    identity.host,
                    identity.pid,
                    startTime);
                conn->commit ();
            }
        );

    } catch (database::mysql::DuplicateKeyError const&) {
        LOGS(_log, LOG_LVL_ERROR, context << "the state is already in the database");
        _conn->rollback();
        throw std::logic_error(context + "the state is already in the database");

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Job const& job,
                                      Job::Options const& options) {

    std::string const context = "DatabaseServicesMySQL::saveState[Job::" + job.type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (Job id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        try {
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->begin();
                    conn->executeInsertQuery(
                        "job",
                        job.id(),
                        job.controller()->identity().id,
                        conn->nullIfEmpty(job.parentJobId()),
                        job.type(),
                        Job::state2string(job.state()),
                        Job::state2string(job.extendedState()),
                                          job.beginTime(),
                                          job.endTime(),
                        PerformanceUtils::now(),    // heartbeat
                        options.priority,
                        options.exclusive,
                        options.preemptable
                    );
            
                    // Extended state (if any provided by a specific job class) is recorded
                    // in a separate table.
            
                    for (auto&& entry: job.extendedPersistentState()) {
                        std::string const& param = entry.first;
                        std::string const& value = entry.second;
                        LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: ('" << param << "','" << value << "')");
                        conn->executeInsertQuery(
                            "job_ext",
                            job.id(),
                            param,value
                        );
                    }
                    conn->commit ();
                }
            );
    
        } catch (database::mysql::DuplicateKeyError const&) {

           _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->rollback();
                    conn->begin();
                    conn->executeSimpleUpdateQuery(
                        "job",
                        _conn->sqlEqual("id",                            job.id()),
                        std::make_pair( "state",      Job::state2string (job.state())),
                        std::make_pair( "ext_state",  Job::state2string (job.extendedState())),
                        std::make_pair( "begin_time",                    job.beginTime()),
                        std::make_pair( "end_time",                      job.endTime())
                    );
                    conn->commit ();
                }
            );
        }

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::updateHeartbeatTime(Job const& job) {

    std::string const context = "DatabaseServicesMySQL::updateHeartbeatTime[Job::" + job.type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    try {
        _conn->execute(
             [&](decltype(_conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "job",
                    _conn->sqlEqual("id", job.id()),
                    std::make_pair( "heartbeat_time", PerformanceUtils::now())
                );
                conn->commit ();
             }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(QservMgtRequest const& request,
                                      Performance const& performance,
                                      std::string const& serverError) {

    std::string const context =
        "DatabaseServicesMySQL::saveState[QservMgtRequest::" + request.type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    // Requests which haven't started yet or the ones which aren't associated
    // with any job should be ignored.
    try {
        if (request.jobId().empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context
                 << "ignoring the request with no job set, id=" << request.id());
            return;
        }
    } catch (std::logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG, context
             << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (QservMgtRequest id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        try {
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->begin();
                    conn->executeInsertQuery(
                        "request",
                        request.id(),
                        request.jobId(),
                        request.type(),
                        request.worker(),
                        0,
                        QservMgtRequest::state2string(request.state()),
                        QservMgtRequest::state2string(request.extendedState()),
                        serverError,
                        performance.c_create_time,
                        performance.c_start_time,
                        performance.w_receive_time,
                        performance.w_start_time,
                        performance.w_finish_time,
                        performance.c_finish_time);
            
                    // Extended state (if any provided by a specific request class) is recorded
                    // in a separate table.
            
                    for (auto&& entry: request.extendedPersistentState()) {
                        std::string const& param = entry.first;
                        std::string const& value = entry.second;
                        LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: ('" << param << "','" << value << "')");
                        conn->executeInsertQuery(
                            "request_ext",
                            request.id(),
                            param,value
                        );
                    }
                    conn->commit ();
                 }
            );
    
        } catch (database::mysql::DuplicateKeyError const&) {
    
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->rollback();
                    conn->begin();
                    conn->executeSimpleUpdateQuery(
                        "request",
                        _conn->sqlEqual("id",                                          request.id()),
                        std::make_pair("state",          QservMgtRequest::state2string(request.state())),
                        std::make_pair("ext_state",      QservMgtRequest::state2string(request.extendedState())),
                        std::make_pair("server_status",                                serverError),
                        std::make_pair("c_create_time",  performance.c_create_time),
                        std::make_pair("c_start_time",   performance.c_start_time),
                        std::make_pair("w_receive_time", performance.w_receive_time),
                        std::make_pair("w_start_time",   performance.w_start_time),
                        std::make_pair("w_finish_time",  performance.w_finish_time),
                        std::make_pair("c_finish_time",  performance.c_finish_time));
                    conn->commit();
                }
            );
        }

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Request const& request,
                                      Performance const& performance) {

    std::string const context =
        "DatabaseServicesMySQL::saveState[Request::" + request.type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    // Requests which haven't started yet or the ones which aren't associated
    // with any job should be ignored.
    try {
        if (request.jobId().empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context
                 << "ignoring the request with no job set, id=" << request.id());
            return;
        }
    } catch (std::logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG, context
             << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (QservMgtRequest id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        try {
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->begin();

                    // The primary state of the request
                    conn->executeInsertQuery(
                        "request",
                        request.id(),
                        request.jobId(),
                        request.type(),
                        request.worker(),
                        request.priority(),
                        Request::state2string(request.state()),
                        Request::state2string(request.extendedState()),
                        status2string(request.extendedServerStatus()),
                        performance.c_create_time,
                        performance.c_start_time,
                        performance.w_receive_time,
                        performance.w_start_time,
                        performance.w_finish_time,
                        performance.c_finish_time);
            
                    // Extended state (if any provided by a specific request class) is recorded
                    // in a separate table.
                    for (auto&& entry: request.extendedPersistentState()) {

                        std::string const& param = entry.first;
                        std::string const& value = entry.second;

                        LOGS(_log, LOG_LVL_DEBUG, context
                             << "extendedPersistentState: ('" << param << "','" << value << "')");

                        conn->executeInsertQuery(
                            "request_ext",
                            request.id(),
                            param,value
                        );
                    }
                    conn->commit ();
                 }
            );

        } catch (database::mysql::DuplicateKeyError const&) {
    
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->rollback();
                    conn->begin();
                    conn->executeSimpleUpdateQuery(
                        "request",
                        _conn->sqlEqual("id", request.id()),
                        std::make_pair("state",          Request::state2string(request.state())),
                        std::make_pair("ext_state",      Request::state2string(request.extendedState())),
                        std::make_pair("server_status",          status2string(request.extendedServerStatus())),
                        std::make_pair("c_create_time",  performance.c_create_time),
                        std::make_pair("c_start_time",   performance.c_start_time),
                        std::make_pair("w_receive_time", performance.w_receive_time),
                        std::make_pair("w_start_time",   performance.w_start_time),
                        std::make_pair("w_finish_time",  performance.w_finish_time),
                        std::make_pair("c_finish_time",  performance.c_finish_time));
                    conn->commit();
                }
            );
        }

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::updateRequestState(Request const& request,
                                               std::string const& targetRequestId,
                                               Performance const& targetRequestPerformance) {

    std::string const context = "DatabaseServicesMySQL::upateRequestState[Request::" + request.type() + "]  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    // According to the current implementation of the requests processing pipeline
    // for the request management (including State* and Stop* families of requests),
    // these states refer to the corresponding target request. Therefore only those
    // states are allowed to be considered for the updates.
    //
    // IMPLEMENTATION NOTE: the request state filter is placed in this method
    // to avoid code duplication in each monitoring request.

    if ((request.state() == Request::State::FINISHED) and
        ::in(request.extendedState(), {Request::ExtendedState::SUCCESS,
                                       Request::ExtendedState::SERVER_QUEUED,
                                       Request::ExtendedState::SERVER_IN_PROGRESS,
                                       Request::ExtendedState::SERVER_IS_CANCELLING,
                                       Request::ExtendedState::SERVER_ERROR,
                                       Request::ExtendedState::SERVER_CANCELLED})) {
        try {
            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->begin();
                    conn->executeSimpleUpdateQuery(
                        "request",
                        _conn->sqlEqual("id",                                   targetRequestId),
                        std::make_pair("state",          Request::state2string(request.state())),
                        std::make_pair("ext_state",      Request::state2string(request.extendedState())),
                        std::make_pair("server_status",          status2string(request.extendedServerStatus())),
                        std::make_pair("w_receive_time", targetRequestPerformance.w_receive_time),
                        std::make_pair("w_start_time",   targetRequestPerformance.w_start_time),
                        std::make_pair("w_finish_time",  targetRequestPerformance.w_finish_time));
                    conn->commit();
                }
            );

        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
            if (_conn->inTransaction()) _conn->rollback();
            throw;
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfo(ReplicaInfo const& info) {

    std::string const context = "DatabaseServicesMySQL::saveReplicaInfo  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                saveReplicaInfoImpl(lock, info);
                conn->commit();
            }
        );

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfoCollection(std::string const& worker,
                                                      std::string const& database,
                                                      ReplicaInfoCollection const& newReplicaInfoCollection) {

    std::string const context = "DatabaseServicesMySQL::saveReplicaInfoCollection  ";

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                saveReplicaInfoCollectionImpl(
                    lock,
                    worker,
                    database,
                    newReplicaInfoCollection);
                conn->commit();
            }
        );

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfoImpl(util::Lock const& lock,
                                                ReplicaInfo const& info) {

    std::string const context = "DatabaseServicesMySQL::saveReplicaInfoImpl  ";

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
                "DELETE FROM " + _conn->sqlId("replica") +
                "  WHERE "     + _conn->sqlEqual("worker",   info.worker()) +
                "    AND "     + _conn->sqlEqual("database", info.database()) +
                "    AND "     + _conn->sqlEqual("chunk",    info.chunk()));
        }

    } catch (database::mysql::DuplicateKeyError const&) {

        // Replace the replica with a newer version

        deleteReplicaInfoImpl(lock, info.worker(), info.database(), info.chunk());
        saveReplicaInfoImpl(lock, info);
    }
}

void DatabaseServicesMySQL::saveReplicaInfoCollectionImpl(util::Lock const& lock,
                                                          std::string const& worker,
                                                          std::string const& database,
                                                          ReplicaInfoCollection const& newReplicaInfoCollection) {

    std::string const context = "DatabaseServicesMySQL::saveReplicaInfoCollectionImpl  ";

    LOGS(_log, LOG_LVL_DEBUG, context
         << "worker: " << worker
         << " database: " << database
         << " num.replicas: " << newReplicaInfoCollection.size());

    // Group new replicas by contexts

    LOGS(_log, LOG_LVL_DEBUG, context << "new replicas group: 1");

    WorkerDatabaseChunkMap<ReplicaInfo const*> newReplicas;
    for (auto&& replica: newReplicaInfoCollection) {
        
        // Ignore replicas which are not in the specified context
        if (replica.worker() == worker and replica.database() == database) {
            newReplicas.atWorker(replica.worker())
                       .atDatabase(replica.database())
                       .atChunk(replica.chunk()) = &replica;
        }
    }
    
    // Obtain old replicas and group them by contexts

    std::vector<ReplicaInfo> oldReplicaInfoCollection;
    findWorkerReplicasImpl(lock, oldReplicaInfoCollection, worker, database);

    WorkerDatabaseChunkMap<ReplicaInfo const*> oldReplicas;
    for (auto&& replica: oldReplicaInfoCollection) {
        oldReplicas.atWorker(replica.worker())
                   .atDatabase(replica.database())
                   .atChunk(replica.chunk()) = &replica;
    }

    // Find differences between the collections

    WorkerDatabaseChunkMap<ReplicaInfo const*> inBoth;
    SemanticMaps::intersect(newReplicas,
                            oldReplicas,
                            inBoth);

    WorkerDatabaseChunkMap<ReplicaInfo const*> inNewReplicasOnly;
    WorkerDatabaseChunkMap<ReplicaInfo const*> inOldReplicasOnly;
    SemanticMaps::diff2(newReplicas,
                        oldReplicas,
                        inNewReplicasOnly,
                        inOldReplicasOnly);

    LOGS(_log, LOG_LVL_DEBUG, context << "*** replicas comparision summary *** "
         << " #new: " << newReplicaInfoCollection.size()
         << " #old: " << oldReplicaInfoCollection.size()
         << " #in-both: " << SemanticMaps::count(inBoth)
         << " #new-only: " << SemanticMaps::count(inNewReplicasOnly)
         << " #old-only: " << SemanticMaps::count(inOldReplicasOnly));

    // Eiminate outdated replicas
    
    for (auto&& worker: inOldReplicasOnly.workerNames()) {

        auto const& databases = inOldReplicasOnly.worker(worker);
        for (auto&& database: databases.databaseNames()) {

            auto const& chunks = databases.database(database);
            for (auto&& chunk: chunks.chunkNumbers()) {
                deleteReplicaInfoImpl(lock, worker, database, chunk);
            }
        }
    }

    // Insert new replicas not present in the old collection

    for (auto&& worker: inNewReplicasOnly.workerNames()) {

        auto const& databases = inNewReplicasOnly.worker(worker);
        for (auto&& database: databases.databaseNames()) {

            auto const& chunks = databases.database(database);
            for (auto&& chunk: chunks.chunkNumbers()) {
                ReplicaInfo const* ptr = chunks.chunk(chunk);
                saveReplicaInfoImpl(lock, *ptr);
            }
        }
    }

    // Deep comparision of the replicas in the intersect area to see
    // which of those need to be updated.

   for (auto&& worker: inBoth.workerNames()) {

        auto const& newDatabases = newReplicas.worker(worker);
        auto const& oldDatabases = oldReplicas.worker(worker);

        auto const& databases = inBoth.worker(worker);
        for (auto&& database: databases.databaseNames()) {

            auto const& newChunks = newDatabases.database(database);
            auto const& oldChunks = oldDatabases.database(database);

            auto const& chunks = databases.database(database);
            for (auto&& chunk: chunks.chunkNumbers()) {

                ReplicaInfo const* newPtr = newChunks.chunk(chunk);
                ReplicaInfo const* oldPtr = oldChunks.chunk(chunk);

                if (*newPtr != *oldPtr) {
                    deleteReplicaInfoImpl(lock, worker, database, chunk);
                    saveReplicaInfoImpl(lock, *newPtr);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
}

void DatabaseServicesMySQL::deleteReplicaInfoImpl(util::Lock const& lock,
                                                  std::string const& worker,
                                                  std::string const& database,
                                                  unsigned int chunk) {
    _conn->execute("DELETE FROM " + _conn->sqlId("replica") +
                   "  WHERE "     + _conn->sqlEqual("worker",   worker) +
                   "    AND "     + _conn->sqlEqual("database", database) +
                   "    AND "     + _conn->sqlEqual("chunk",    chunk));
}

void DatabaseServicesMySQL::findOldestReplicas(std::vector<ReplicaInfo>& replicas,
                                               size_t maxReplicas,
                                               bool enabledWorkersOnly) {

    std::string const context = "DatabaseServicesMySQL::findOldestReplicas  ";

    util::Lock lock(_mtx, context);

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not maxReplicas) {
        throw std::invalid_argument(context + "maxReplicas is not allowed to be 0");
    }
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                findReplicasImpl(
                    lock,
                    replicas,
                    "SELECT * FROM " + conn->sqlId("replica") +
                    (enabledWorkersOnly ?
                    " WHERE " + conn->sqlIn("worker", _configuration->workers(true)) : "") +
                    " ORDER BY "     + conn->sqlId("verify_time") +
                    " ASC LIMIT "    + std::to_string(maxReplicas)
                );
                conn->rollback();
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findReplicas(std::vector<ReplicaInfo>& replicas,
                                         unsigned int chunk,
                                         std::string const& database,
                                         bool enabledWorkersOnly) {
    std::string const context =
         "DatabaseServicesMySQL::findReplicas  chunk: " + std::to_string(chunk) +
         "  database: " + database + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    if (not _configuration->isKnownDatabase(database)) {
        throw std::invalid_argument(context + "unknow database");
    }
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                findReplicasImpl(
                    lock,
                    replicas,
                    "SELECT * FROM " +  conn->sqlId("replica") +
                    "  WHERE "       +  conn->sqlEqual("chunk",    chunk) +
                    "    AND "       +  conn->sqlEqual("database", database) +
                    (enabledWorkersOnly ?
                     "   AND "       +  conn->sqlIn("worker", _configuration->workers(true)) :""));
                conn->rollback();
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                               std::string const& worker,
                                               std::string const& database) {

    std::string const context = "DatabaseServicesMySQL::findWorkerReplicas  ";

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                findWorkerReplicasImpl(
                    lock,
                    replicas,
                    worker,
                    database);
                conn->rollback();
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findWorkerReplicasImpl(util::Lock const& lock,
                                                   std::vector<ReplicaInfo>& replicas,
                                                   std::string const& worker,
                                                   std::string const& database) {
    std::string const context =
         "DatabaseServicesMySQL::findWorkerReplicasImpl  worker: " + worker +
         " database: " + database + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownWorker(worker)) {
        throw std::invalid_argument(context + "unknow worker");
    }
    if (not database.empty()) {
        if (not _configuration->isKnownDatabase(database)) {
            throw std::invalid_argument(context + "unknow database");
        }
    }
    findReplicasImpl(
        lock,
        replicas,
        "SELECT * FROM " + _conn->sqlId("replica") +
        "  WHERE "       + _conn->sqlEqual("worker", worker) +
        (database.empty() ? "" :
        "  AND "         + _conn->sqlEqual( "database", database)));

    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findWorkerReplicas(std::vector<ReplicaInfo>& replicas,
                                               unsigned int chunk,
                                               std::string const& worker,
                                               std::string const& databaseFamily) {
    std::string const context =
         "DatabaseServicesMySQL::findWorkerReplicas  worker: " + worker +
         " chunk: " + std::to_string(chunk) + " database family: " + databaseFamily;

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    if (not _configuration->isKnownWorker(worker)) {
        throw std::invalid_argument(context + "unknow worker");
    }
    if (not databaseFamily.empty() and not _configuration->isKnownDatabaseFamily(databaseFamily)) {
        throw std::invalid_argument(context + "unknow databaseFamily");
    }
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                findReplicasImpl(
                    lock,
                    replicas,
                    "SELECT * FROM " + conn->sqlId("replica") +
                    "  WHERE "       + conn->sqlEqual("worker", worker) +
                    "  AND "         + conn->sqlEqual("chunk",  chunk) +
                    (databaseFamily.empty() ? "" :
                    "  AND "         + conn->sqlIn("database", _configuration->databases(databaseFamily))));
                conn->rollback();
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}


void DatabaseServicesMySQL::findReplicasImpl(util::Lock const& lock,
                                             std::vector<ReplicaInfo>& replicas,
                                             std::string const& query) {

    std::string const context = "DatabaseServicesMySQL::findReplicasImpl(replicas,query)  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    _conn->execute(query);

    if (_conn->hasResult()) {

        // Temporarily store incomplete (w/o files) replica in the map by their
        // database identifiers. Replicas will get extended on the next step and
        // put into the resulting collection.

        std::map<uint64_t, ReplicaInfo> id2replica;

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

            id2replica[id] = ReplicaInfo(
                ReplicaInfo::Status::COMPLETE,
                worker,
                database,
                chunk,
                verifyTime
            );
        }
        
        // Extract files for each replica using identifiers of the replicas,
        // update replicas and copy them over into the output collection.
        
        findReplicaFilesImpl(lock, id2replica, replicas);
    }
}

void DatabaseServicesMySQL::findReplicaFilesImpl(util::Lock const& lock,
                                                 std::map<uint64_t, ReplicaInfo> const& id2replica,
                                                 std::vector<ReplicaInfo>& replicas) {

    std::string const context = "DatabaseServicesMySQL::findReplicaFilesImpl  ";

    if (0 == id2replica.size()) return;

    // The collection of replica identifirs will be split into batches to ensure
    // that a length of the query string (for pulling files for each batch) would
    // not exceed the corresponidng MySQL limit.

    std::vector<uint64_t> ids;
    for (auto&& entry: id2replica) {
        ids.push_back(entry.first);
    }

    // Reserving 1024 for the rest of the query. Also assuming the worst case
    // scenario of the highest values of identifiers. Adding one extra byte for
    // a separator.
    //
    // TODO: come up with a more reliable algorithm which will avoid using
    // the fixed correction (of 1024 bytes).

   size_t const batchSize =
        (_conn->max_allowed_packet() - 1024) / (1 + std::to_string(UINT64_MAX).size());

    if ((_conn->max_allowed_packet() < 1024) or (0 == batchSize)) {
        throw std::runtime_error(
                context + "value of 'max_allowed_packet' set for the MySQL session is too small: " +
                std::to_string(_conn->max_allowed_packet()));
    }
 
    // Compute sizes of batches. This will be needed on the next step to iterate
    // over a collection of replica identifiers.

    std::vector<size_t> batches(ids.size() / batchSize, batchSize);     // complete batches
    size_t const lastBatchSize = ids.size() % batchSize;                // and the last one (if any)
    if (0 != lastBatchSize) {
        batches.push_back(lastBatchSize);
    }

    // Iterate over batches, submit a query per batch, harvest and process
    // results.
    //
    // IMPORTANT: the algorithm assumes that there will be at least one file
    // per replica. This assumprion will be enfoced wyen the loop will end.

    auto itr = ids.begin();         // points to the first replica identifier of a batch
    for (size_t size: batches) {

        _conn->execute(
            "SELECT * FROM " + _conn->sqlId("replica_file") +
            "  WHERE "       + _conn->sqlIn("replica_id", std::vector<uint64_t>(itr, itr + size)) +
            "  ORDER BY "    + _conn->sqlId("replica_id"));
    
        if (_conn->hasResult()) {
    
            uint64_t currentReplicaId = 0;
            ReplicaInfo::FileInfoCollection files;  // for accumulating files of the current
                                                    // replica

            auto copyExtendMove = [&id2replica,
                                   &replicas,
                                   &files] (uint64_t replicaId) {

                // Copy an incomplete replica from the input collection, extend it
                // then move it into the output (complete) collection.

                auto replica = id2replica.at(replicaId);
                replica.setFileInfo(files);
                replicas.push_back(std::move(replica));

                files.clear();
            };

            database::mysql::Row row;
            while (_conn->next(row)) {
    
                // Extract attributes of the file
    
                uint64_t    replicaId;
                std::string name;
                uint64_t    size;
                std::time_t mtime;
                std::string cs;
                uint64_t    beginCreateTime;
                uint64_t    endCreateTime;
    
                row.get("replica_id",        replicaId);
                row.get("name",              name);
                row.get("size",              size);
                row.get("mtime",             mtime);
                row.get("cs",                cs);
                row.get("begin_create_time", beginCreateTime);
                row.get("end_create_time",   endCreateTime);

                // Save files to the current replica if a change in the replica identifier
                // has been detected (unless just started iterating over the result set).

                if (replicaId != currentReplicaId) {
                    if (0 != currentReplicaId) {
                        copyExtendMove(currentReplicaId);
                    }
                    currentReplicaId = replicaId;
                }

                // Adding this file to the curent replica

                files.push_back(
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
            
            // Save files of the last replica processed before the 'while' loop above
            // ended. We need to do it here because the algorithm saves files only
            // when it detects changes in the replica identifier.

            if (0 != currentReplicaId) {
                copyExtendMove(currentReplicaId);
            }
        }

        // Advance iterator to the first identifier of the next
        // batch (if any).
        itr += size;
    }
    
    // Sanity check to ensure a collection of files has been found for each input
    // replica. Note that this is a requirements for a persistent collection
    // of reolicas stored by the Replication system.

    if (replicas.size() != id2replica.size()) {
        throw std::runtime_error(context + "database content may be corrupt");
    }
}

}}} // namespace lsst::qserv::replica
