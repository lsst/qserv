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

// Class header
#include "replica/DatabaseServicesMySQL.h"

// System headers
#include <algorithm>
#include <cstdint>
#include <ctime>
#include <limits>
#include <stdexcept>
#include <list>

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

using namespace std;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesMySQL");

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
        vector<Request::ExtendedState> const& col) {
    return col.end() != find(col.begin(), col.end(), val);
}

} /// namespace

namespace lsst {
namespace qserv {
namespace replica {

DatabaseServicesMySQL::DatabaseServicesMySQL(Configuration::Ptr const& configuration)
    :   DatabaseServices(),
        _configuration(configuration),
        _conn(database::mysql::Connection::open(
            database::mysql::ConnectionParams(
                configuration->databaseHost(),
                configuration->databasePort(),
                configuration->databaseUser(),
                configuration->databasePassword(),
                configuration->databaseName()))) {
}


void DatabaseServicesMySQL::saveState(ControllerIdentity const& identity,
                                      uint64_t startTime) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Controller] ";

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
        throw logic_error(context + "the state is already in the database");

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}


void DatabaseServicesMySQL::saveState(Job const& job,
                                      Job::Options const& options) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Job::" + job.type() + "] ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (Job id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        _conn->executeInsertOrUpdate(

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
                    string const& param = entry.first;
                    string const& value = entry.second;
                    LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: ('" << param << "','" << value << "')");
                    conn->executeInsertQuery(
                        "job_ext",
                        job.id(),
                        param,value
                    );
                }
                conn->commit ();
            },

            [&](decltype(_conn) conn) {
                conn->rollback();
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "job",
                    _conn->sqlEqual("id",                      job.id()),
                    make_pair( "state",      Job::state2string(job.state())),
                    make_pair( "ext_state",  Job::state2string(job.extendedState())),
                    make_pair( "begin_time",                   job.beginTime()),
                    make_pair( "end_time",                     job.endTime())
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


void DatabaseServicesMySQL::updateHeartbeatTime(Job const& job) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Job::" + job.type() + "] ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    try {
        _conn->execute(
             [&](decltype(_conn) conn) {
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "job",
                    _conn->sqlEqual("id", job.id()),
                    make_pair( "heartbeat_time", PerformanceUtils::now())
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
                                      string const& serverError) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + "[QservMgtRequest::" + request.type() + "] ";

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
    } catch (logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG, context
             << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (QservMgtRequest id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        _conn->executeInsertOrUpdate(

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
                    string const& param = entry.first;
                    string const& value = entry.second;
                    LOGS(_log, LOG_LVL_DEBUG, context << "extendedPersistentState: ('" << param << "','" << value << "')");
                    conn->executeInsertQuery(
                        "request_ext",
                        request.id(),
                        param,value
                    );
                }
                conn->commit ();
            },

            [&](decltype(_conn) conn) {
                conn->rollback();
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "request",
                    _conn->sqlEqual("id",                                     request.id()),
                    make_pair("state",          QservMgtRequest::state2string(request.state())),
                    make_pair("ext_state",      QservMgtRequest::state2string(request.extendedState())),
                    make_pair("server_status",                                serverError),
                    make_pair("c_create_time",  performance.c_create_time),
                    make_pair("c_start_time",   performance.c_start_time),
                    make_pair("w_receive_time", performance.w_receive_time),
                    make_pair("w_start_time",   performance.w_start_time),
                    make_pair("w_finish_time",  performance.w_finish_time),
                    make_pair("c_finish_time",  performance.c_finish_time));
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


void DatabaseServicesMySQL::saveState(Request const& request,
                                      Performance const& performance) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + "[Request::" + request.type() + "] ";

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
    } catch (logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG, context
             << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (QservMgtRequest id) already exists in the table
    // then the UPDATE query will be executed.

    try {
        _conn->executeInsertOrUpdate(
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

                    string const& param = entry.first;
                    string const& value = entry.second;

                    LOGS(_log, LOG_LVL_DEBUG, context
                         << "extendedPersistentState: ('" << param << "','" << value << "')");

                    conn->executeInsertQuery(
                        "request_ext",
                        request.id(),
                        param,value
                    );
                }
                conn->commit ();
            },
            [&](decltype(_conn) conn) {
                conn->rollback();
                conn->begin();
                conn->executeSimpleUpdateQuery(
                    "request",
                    _conn->sqlEqual("id",                             request.id()),
                    make_pair("state",          Request::state2string(request.state())),
                    make_pair("ext_state",      Request::state2string(request.extendedState())),
                    make_pair("server_status",          status2string(request.extendedServerStatus())),
                    make_pair("c_create_time",  performance.c_create_time),
                    make_pair("c_start_time",   performance.c_start_time),
                    make_pair("w_receive_time", performance.w_receive_time),
                    make_pair("w_start_time",   performance.w_start_time),
                    make_pair("w_finish_time",  performance.w_finish_time),
                    make_pair("c_finish_time",  performance.c_finish_time));
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


void DatabaseServicesMySQL::updateRequestState(Request const& request,
                                               string const& targetRequestId,
                                               Performance const& targetRequestPerformance) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Request::" + request.type() + "] ";

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
                        _conn->sqlEqual("id",                             targetRequestId),
                        make_pair("state",          Request::state2string(request.state())),
                        make_pair("ext_state",      Request::state2string(request.extendedState())),
                        make_pair("server_status",          status2string(request.extendedServerStatus())),
                        make_pair("w_receive_time", targetRequestPerformance.w_receive_time),
                        make_pair("w_start_time",   targetRequestPerformance.w_start_time),
                        make_pair("w_finish_time",  targetRequestPerformance.w_finish_time));
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

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _saveReplicaInfoImpl(lock, info);
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


void DatabaseServicesMySQL::saveReplicaInfoCollection(string const& worker,
                                                      string const& database,
                                                      ReplicaInfoCollection const& newReplicaInfoCollection) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _saveReplicaInfoCollectionImpl(
                    lock,
                    worker,
                    database,
                    newReplicaInfoCollection);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}


void DatabaseServicesMySQL::_saveReplicaInfoImpl(util::Lock const& lock,
                                                 ReplicaInfo const& info) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    try {

        // Try inserting if the replica is complete. Delete otherwise.

        if (info.status() == ReplicaInfo::Status::COMPLETE) {

            _conn->executeInsertQuery(
                "replica",
                database::mysql::Keyword::SQL_NULL,             /* the auto-incremented PK */
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

        _deleteReplicaInfoImpl(lock, info.worker(), info.database(), info.chunk());
        _saveReplicaInfoImpl(lock, info);
    }
}


void DatabaseServicesMySQL::_saveReplicaInfoCollectionImpl(util::Lock const& lock,
                                                           string const& worker,
                                                           string const& database,
                                                           ReplicaInfoCollection const& newReplicaInfoCollection) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context
         << "worker: " << worker
         << " database: " << database
         << " num.replicas: " << newReplicaInfoCollection.size());

    if (worker.empty()) {
        throw invalid_argument(worker + "worker name can't be empty");
    }
    if (database.empty()) {
        throw invalid_argument(context + "database name can't be empty");
    }

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

    vector<ReplicaInfo> oldReplicaInfoCollection;
    _findWorkerReplicasImpl(lock, oldReplicaInfoCollection, worker, database);

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

    LOGS(_log, LOG_LVL_DEBUG, context << "*** replicas comparison summary *** "
         << " #new: " << newReplicaInfoCollection.size()
         << " #old: " << oldReplicaInfoCollection.size()
         << " #in-both: " << SemanticMaps::count(inBoth)
         << " #new-only: " << SemanticMaps::count(inNewReplicasOnly)
         << " #old-only: " << SemanticMaps::count(inOldReplicasOnly));

    // Eliminate outdated replicas
    
    for (auto&& worker: inOldReplicasOnly.workerNames()) {

        auto const& databases = inOldReplicasOnly.worker(worker);
        for (auto&& database: databases.databaseNames()) {

            auto const& chunks = databases.database(database);
            for (auto&& chunk: chunks.chunkNumbers()) {
                _deleteReplicaInfoImpl(lock, worker, database, chunk);
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
                _saveReplicaInfoImpl(lock, *ptr);
            }
        }
    }

    // Deep comparison of the replicas in the intersect area to see
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
                    _deleteReplicaInfoImpl(lock, worker, database, chunk);
                    _saveReplicaInfoImpl(lock, *newPtr);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
}


void DatabaseServicesMySQL::_deleteReplicaInfoImpl(util::Lock const& lock,
                                                   string const& worker,
                                                   string const& database,
                                                   unsigned int chunk) {
    _conn->execute(
        "DELETE FROM " + _conn->sqlId("replica") +
        "  WHERE "     + _conn->sqlEqual("worker",   worker) +
        "    AND "     + _conn->sqlEqual("database", database) +
        "    AND "     + _conn->sqlEqual("chunk",    chunk)
    );
}


void DatabaseServicesMySQL::findOldestReplicas(vector<ReplicaInfo>& replicas,
                                               size_t maxReplicas,
                                               bool enabledWorkersOnly,
                                               bool allDatabases,
                                               bool isPublished) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not maxReplicas) {
        throw invalid_argument(context + "maxReplicas is not allowed to be 0");
    }

    util::Lock lock(_mtx, context);

    try {
        string const noSpecificFamily;
        auto const databases = _configuration->databases(noSpecificFamily, allDatabases, isPublished);
        string const query =
            "SELECT * FROM " + _conn->sqlId("replica") +
            " WHERE "        + _conn->sqlIn("database", databases) +
            (enabledWorkersOnly ?
            "   AND "        + _conn->sqlIn("worker", _configuration->workers(true)) : "") +
            " ORDER BY "     + _conn->sqlId("verify_time") +
            " ASC LIMIT "    + to_string(maxReplicas);
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findReplicasImpl(lock, replicas, query);
                conn->rollback();
            }
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}


void DatabaseServicesMySQL::findReplicas(vector<ReplicaInfo>& replicas,
                                         unsigned int chunk,
                                         string const& database,
                                         bool enabledWorkersOnly) {
    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + " chunk=" + to_string(chunk) +
        "  database=" + database +
        " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownDatabase(database)) {
        throw invalid_argument(context + "unknown database");
    }

    util::Lock lock(_mtx, context);

    try {
        string const query =
            "SELECT * FROM " +  _conn->sqlId("replica") +
            "  WHERE "       +  _conn->sqlEqual("chunk",    chunk) +
            "    AND "       +  _conn->sqlEqual("database", database) +
            (enabledWorkersOnly ?
             "   AND "       +  _conn->sqlIn("worker", _configuration->workers(true)) : "");
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findReplicasImpl(lock, replicas, query);
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


void DatabaseServicesMySQL::findWorkerReplicas(vector<ReplicaInfo>& replicas,
                                               string const& worker,
                                               string const& database,
                                               bool allDatabases,
                                               bool isPublished,
                                               bool includeFileInfo) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) +
        " worker=" + worker + " database=" + database +
        " allDatabases=" + string(allDatabases ? "1" : "0") +
        " isPublished=" + string(isPublished ? "1" : "0") + "  ";

    util::Lock lock(_mtx, context);

    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findWorkerReplicasImpl(
                    lock,
                    replicas,
                    worker,
                    database,
                    allDatabases,
                    isPublished,
                    includeFileInfo);
                conn->rollback();
            }
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}


uint64_t DatabaseServicesMySQL::numWorkerReplicas(string const& worker,
                                                  string const& database,
                                                  bool allDatabases,
                                                  bool isPublished) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) +
        " worker=" + worker + " database=" + database +
        " allDatabases=" + string(allDatabases ? "1" : "0") +
        " isPublished=" + string(isPublished ? "1" : "0") + "  ";

    util::Lock lock(_mtx, context);

    uint64_t num;
    try {
        string query =
            "SELECT COUNT(*) AS num FROM " + _conn->sqlId("replica") +
            "  WHERE " + _conn->sqlEqual("worker", worker) +
            "  AND ";
        if (database.empty()) {
            string const noSpecificFamily;
            query += _conn->sqlIn("database",
                                  _configuration->databases(noSpecificFamily,
                                                            allDatabases,
                                                            isPublished));
        } else {
            if (not _configuration->isKnownDatabase(database)) {
                throw invalid_argument(context + "unknown database: '" + database + "'");
            }
            query += _conn->sqlEqual("database", database);
        }
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                conn->executeSingleValueSelect<uint64_t>(query, "num", num, false);
                conn->rollback();
            }
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** num: " << num);
    return num;
}


void DatabaseServicesMySQL::_findWorkerReplicasImpl(util::Lock const& lock,
                                                    vector<ReplicaInfo>& replicas,
                                                    string const& worker,
                                                    string const& database,
                                                    bool allDatabases,
                                                    bool isPublished,
                                                    bool includeFileInfo) {
    string const context =
        "DatabaseServicesMySQL::" + string(__func__) +
        " worker=" + worker + " database=" + database +
        " allDatabases=" + string(allDatabases ? "1" : "0") +
        " isPublished=" + string(isPublished ? "1" : "0") + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownWorker(worker)) {
        throw invalid_argument(context + "unknown worker");
    }
    string query =
        "SELECT * FROM " + _conn->sqlId("replica") +
        "  WHERE "       + _conn->sqlEqual("worker", worker) +
        "    AND ";
    if (database.empty()) {
        string const noSpecificFamily;
        query += _conn->sqlIn("database",
                              _configuration->databases(noSpecificFamily,
                                                        allDatabases,
                                                        isPublished));
    } else {
        if (not _configuration->isKnownDatabase(database)) {
            throw invalid_argument(context + "unknown database: '" + database + "'");
        }
        query += _conn->sqlEqual("database", database);
    }
    _findReplicasImpl(lock, replicas, query, includeFileInfo);

    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}


void DatabaseServicesMySQL::findWorkerReplicas(vector<ReplicaInfo>& replicas,
                                               unsigned int chunk,
                                               string const& worker,
                                               string const& databaseFamily,
                                               bool allDatabases,
                                               bool isPublished) {
    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + " worker=" + worker +
        " chunk=" + to_string(chunk) + " family=" + databaseFamily +
        " allDatabases=" + string(allDatabases ? "1" : "0") +
        " isPublished=" + string(isPublished ? "1" : "0") + "  ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownWorker(worker)) {
        throw invalid_argument(context + "unknown worker");
    }
    if (not databaseFamily.empty() and not _configuration->isKnownDatabaseFamily(databaseFamily)) {
        throw invalid_argument(context + "unknown databaseFamily");
    }

    util::Lock lock(_mtx, context);

    try {
        string const query =
            "SELECT * FROM " + _conn->sqlId("replica") +
            "  WHERE "       + _conn->sqlEqual("worker", worker) +
            "  AND "         + _conn->sqlEqual("chunk",  chunk) +
            "  AND "         + _conn->sqlIn("database",
                                            _configuration->databases(databaseFamily,
                                                                      allDatabases,
                                                                      isPublished));
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findReplicasImpl(lock, replicas, query);
                conn->rollback();
            }
        );
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}


void DatabaseServicesMySQL::findDatabaseReplicas(
                                vector<ReplicaInfo>& replicas,
                                string const& database,
                                bool enabledWorkersOnly) {
    string const context =
        "DatabaseServicesMySQL::" + string(__func__) +
        "  database=" + database + " enabledWorkersOnly=" + string(enabledWorkersOnly ? "1" : "0") +
        " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownDatabase(database)) {
        throw invalid_argument(context + "unknown database");
    }

    util::Lock lock(_mtx, context);

    try {
        string const query =
            "SELECT * FROM " + _conn->sqlId("replica") +
            "  WHERE "       + _conn->sqlEqual("database", database) +
            (enabledWorkersOnly ?
             "   AND "       + _conn->sqlIn("worker", _configuration->workers(true)) : "");
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findReplicasImpl(lock, replicas, query);
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


void DatabaseServicesMySQL::findDatabaseChunks(
                                vector<unsigned int>& chunks,
                                string const& database,
                                bool enabledWorkersOnly) {
    string const context =
        "DatabaseServicesMySQL::" + string(__func__) +
        "  database=" + database + " enabledWorkersOnly=" + string(enabledWorkersOnly ? "1" : "0") +
        " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (not _configuration->isKnownDatabase(database)) {
        throw invalid_argument(context + "unknown database");
    }

    util::Lock lock(_mtx, context);

    try {
        string const query =
            "SELECT DISTINCT " + _conn->sqlId("chunk") + " FROM " + _conn->sqlId("replica") +
            "  WHERE "         + _conn->sqlEqual("database", database) +
            (enabledWorkersOnly ?
             "   AND "         + _conn->sqlIn("worker", _configuration->workers(true)) : "") +
            " ORDER BY "       + _conn->sqlId("chunk");
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _findChunksImpl(lock, chunks, query);
                conn->rollback();
            }
        );
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << chunks.size());
}


map<unsigned int, size_t> DatabaseServicesMySQL::actualReplicationLevel(
                                    string const& database,
                                    vector<string> const& workersToExclude) {
    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " database=" + database + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (database.empty()) {
        throw invalid_argument(context + "database name can't be empty");
    }
    if (not _configuration->isKnownDatabase(database)) {
        throw invalid_argument(context + "unknown database");
    }
    if (not workersToExclude.empty()) {
        for (auto&& worker: workersToExclude) {
            if (not _configuration->isKnownWorker(worker)) {
                throw invalid_argument(context + "unknown worker: " + worker);
            }
        }
    }

    util::Lock lock(_mtx, context);

    try {
        map<unsigned int, size_t> result;

        string const query =
            "SELECT " + _conn->sqlId("level") + ",COUNT(*) AS " + _conn->sqlId("num_chunks") +
            "  FROM (" +
            "    SELECT  "     + _conn->sqlId("chunk")   + ",COUNT(*) AS " + _conn->sqlId("level") +
            "      FROM  "     + _conn->sqlId("replica") +
            "      WHERE "     + _conn->sqlEqual("database", database) +
                   (workersToExclude.empty() ? "" :
            "        AND NOT " + _conn->sqlIn("worker", workersToExclude)) +
            "        AND     " + _conn->sqlId("chunk") + " != 1234567890" +
            "      GROUP BY  " + _conn->sqlId("chunk") +
            "  )"              + _conn->sqlId("chunks") +
            "  GROUP BY "      + _conn->sqlId("level");

        LOGS(_log, LOG_LVL_DEBUG, context + "query: " + query);

        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                conn->execute(query);

                // Always do this before extracting results in case of this lambda
                // function gets executed more than once due to reconnects.
                result.clear();
        
                database::mysql::Row row;
                while (conn->next(row)) {

                    unsigned int level;
                    size_t numChunks;

                    row.get("level", level);
                    row.get("num_chunks", numChunks);

                    result[level] = numChunks;
                }
                conn->rollback();
            }
        );
        LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
        return result;
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
}


size_t DatabaseServicesMySQL::numOrphanChunks(string const& database,
                                              vector<string> const& uniqueOnWorkers) {

    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " database=" + database + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (database.empty()) {
        throw invalid_argument(context + "database name can't be empty");
    }
    if (not _configuration->isKnownDatabase(database)) {
        throw invalid_argument(context + "unknown database");
    }
    if (not uniqueOnWorkers.empty()) {
        for (auto&& worker: uniqueOnWorkers) {
            if (not _configuration->isKnownWorker(worker)) {
                throw invalid_argument(context + "unknown worker: " + worker);
            }
        }
    }

    util::Lock lock(_mtx, context);

    try {
        size_t result = 0;

        if (not uniqueOnWorkers.empty()) {

            vector<string> workersToExclude;
            for (auto&& worker: _configuration->allWorkers()) {
                if (uniqueOnWorkers.end() == find(uniqueOnWorkers.begin(),
                                                   uniqueOnWorkers.end(),
                                                   worker)) {
                    workersToExclude.push_back(worker);
                }
            }
            string const query =
                "SELECT COUNT(*) AS " + _conn->sqlId("num_chunks") +
                "  FROM "             + _conn->sqlId("replica") +
                "  WHERE "            + _conn->sqlEqual("database", database) +
                "    AND "            + _conn->sqlIn("worker", uniqueOnWorkers) +
                "    AND "            + _conn->sqlId("chunk") + " != 1234567890" +
                "    AND "            + _conn->sqlId("chunk") + " NOT IN" +
                "    (SELECT  "       + _conn->sqlId("chunk")   +
                "       FROM  "       + _conn->sqlId("replica") +
                "       WHERE "       + _conn->sqlEqual("database", database) +
                (workersToExclude.empty() ? "" :
                "         AND "       + _conn->sqlIn("worker", workersToExclude)) +
                "    )";

            LOGS(_log, LOG_LVL_DEBUG, context + "query: " + query);

            _conn->execute(
                [&](decltype(_conn) conn) {
                    conn->begin();
                    conn->executeSingleValueSelect(query, "num_chunks", result);
                    conn->rollback();
                }
            );
        }
        LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
        return result;
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }

}


void DatabaseServicesMySQL::logControllerEvent(ControllerEvent const& event) {

    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " " +
         " controllerId=" + event.controllerId +
         " timeStamp="    + to_string(event.timeStamp) +
         " task="         + event.task +
         " operation="    + event.operation +
         " status="       + event.status +
         " requestId="    + event.requestId +
         " jobId="        + event.jobId +
         " kvInfo.size="  + to_string(event.kvInfo.size()) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                _logControllerEvent(
                    lock,
                    event);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}


void DatabaseServicesMySQL::_logControllerEvent(util::Lock const& lock,
                                                ControllerEvent const& event) {

    _conn->executeInsertQuery(
        "controller_log",
        database::mysql::Keyword::SQL_NULL,
        event.controllerId,
        event.timeStamp,
        event.task,
        event.operation,
        event.status,
        _conn->nullIfEmpty(event.requestId),
        _conn->nullIfEmpty(event.jobId)
    );
    for (auto&& kv: event.kvInfo) {
        _conn->executeInsertQuery(
            "controller_log_ext",
                database::mysql::Function::LAST_INSERT_ID,
                kv.first,
                kv.second
        );
    }
}


list<ControllerEvent> DatabaseServicesMySQL::readControllerEvents(
                                                    string const& controllerId,
                                                    uint64_t fromTimeStamp,
                                                    uint64_t toTimeStamp,
                                                    size_t maxEntries) {
    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " " +
         " controllerId="  + controllerId +
         " fromTimeStamp=" + to_string(fromTimeStamp) +
         " toTimeStamp="   + to_string(toTimeStamp) +
         " maxEntries="    + to_string(maxEntries) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    
    list<ControllerEvent> events;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                events = _readControllerEvents(
                    lock,
                    controllerId,
                    fromTimeStamp,
                    toTimeStamp,
                    maxEntries);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return events;
}


list<ControllerEvent> DatabaseServicesMySQL::_readControllerEvents(
                                                    util::Lock const& lock,
                                                    string const& controllerId,
                                                    uint64_t fromTimeStamp,
                                                    uint64_t toTimeStamp,
                                                    size_t maxEntries) {
    if (controllerId.empty()) {
        throw invalid_argument(
                "DatabaseServicesMySQL::" + string(__func__) + " parameter"
                " controllerId can't be empty");
    }
    if (fromTimeStamp > toTimeStamp) {
        throw invalid_argument(
                "DatabaseServicesMySQL::" + string(__func__) + " illegal time range"
                " for events: [" + to_string(fromTimeStamp) + "," + to_string(toTimeStamp) + "]");
    }

    list<ControllerEvent> events;

    string const query =
        "SELECT * FROM " + _conn->sqlId("controller_log") +
        "  WHERE "       + _conn->sqlEqual("controller_id", controllerId) +
        "    AND "       + _conn->sqlGreaterOrEqual("time", fromTimeStamp) +
        "    AND "       + _conn->sqlLessOrEqual("time", toTimeStamp != 0 ? toTimeStamp : numeric_limits<uint64_t>::max()) +
        "  ORDER BY "    + _conn->sqlId("time") + " DESC" + (maxEntries == 0 ? "" :
        "  LIMIT "       + to_string(maxEntries));

    _conn->execute(query);
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            ControllerEvent event;

            row.get("id",            event.id);
            row.get("controller_id", event.controllerId);
            row.get("time",          event.timeStamp);
            row.get("task",          event.task);
            row.get("operation",     event.operation);
            row.get("status",        event.status);
            if (not row.isNull("request_id")) row.get("request_id", event.requestId);
            if (not row.isNull("job_id"))     row.get("job_id",     event.jobId);

            events.push_back(event);
        }
        for (auto&& event: events) {
            string const query =
                "SELECT * FROM " + _conn->sqlId("controller_log_ext") +
                "  WHERE "       + _conn->sqlEqual("controller_log_id", event.id);

            _conn->execute(query);
            if (_conn->hasResult()) {

                database::mysql::Row row;
                while (_conn->next(row)) {
                    string key;
                    string val;
                    row.get("key", key);
                    row.get("val", val);
                    event.kvInfo.emplace_back(key, val);
                }
            }
        }
    }
    return events;
}


ControllerInfo DatabaseServicesMySQL::controller(string const& id) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) {
        throw invalid_argument(context + ", controller identifier can't be empty");
    }
    util::Lock lock(_mtx, context);
    
    ControllerInfo info;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                info = _controller(lock, id);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


ControllerInfo DatabaseServicesMySQL::_controller(util::Lock const& lock,
                                                  string const& id) {
    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("controller") +
        "  WHERE "       + _conn->sqlEqual("id", id)
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            ControllerInfo info;

            row.get("id",         info.id);
            row.get("start_time", info.started);
            row.get("hostname",   info.hostname);
            row.get("pid",        info.pid);

            return info;
        }
    }
    throw DatabaseServicesNotFound("no Controller found for id: " + id);
}


list<ControllerInfo> DatabaseServicesMySQL::controllers(uint64_t fromTimeStamp,
                                                        uint64_t toTimeStamp,
                                                        size_t maxEntries) {
    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " " +
         " fromTimeStamp=" + to_string(fromTimeStamp) +
         " toTimeStamp="   + to_string(toTimeStamp) +
         " maxEntries="    + to_string(maxEntries) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    
    list<ControllerInfo> collection;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                collection = _controllers(
                    lock,
                    fromTimeStamp,
                    toTimeStamp,
                    maxEntries);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}


list<ControllerInfo> DatabaseServicesMySQL::_controllers(util::Lock const& lock,
                                                         uint64_t fromTimeStamp,
                                                         uint64_t toTimeStamp,
                                                         size_t maxEntries) {
    list<ControllerInfo> collection;
    string const limitOpt = maxEntries == 0 ? "" : "  LIMIT " + to_string(maxEntries);
    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("controller") +
        "  WHERE "       + _conn->sqlGreaterOrEqual("start_time", fromTimeStamp) +
        "    AND "       + _conn->sqlLessOrEqual("start_time", toTimeStamp != 0
                                    ? toTimeStamp
                                    : numeric_limits<uint64_t>::max()) +
        "  ORDER BY "    + _conn->sqlId("start_time") + " DESC" +
        limitOpt
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            ControllerInfo info;

            row.get("id",         info.id);
            row.get("start_time", info.started);
            row.get("hostname",   info.hostname);
            row.get("pid",        info.pid);

            collection.push_back(info);
        }
    }
    return collection;
}


RequestInfo DatabaseServicesMySQL::request(string const& id) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) {
        throw invalid_argument(context + ", request identifier can't be empty");
    }
    util::Lock lock(_mtx, context);
    
    RequestInfo info;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                info = _request(lock, id);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


RequestInfo DatabaseServicesMySQL::_request(util::Lock const& lock,
                                            string const& id) {
    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("request") +
        "  WHERE "       + _conn->sqlEqual("id", id)
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            RequestInfo info;

            row.get("id",             info.id);
            row.get("job_id",         info.jobId);
            row.get("name",           info.name);
            row.get("worker",         info.worker);
            row.get("priority",       info.priority);
            row.get("state",          info.state);
            row.get("ext_state",      info.extendedState);
            row.get("server_status",  info.serverStatus);
            row.get("c_create_time",  info.controllerCreateTime);
            row.get("c_start_time",   info.controllerStartTime);
            row.get("c_finish_time",  info.controllerFinishTime);
            row.get("w_receive_time", info.workerReceiveTime);
            row.get("w_start_time",   info.workerStartTime);
            row.get("w_finish_time",  info.workerFinishTime);

            _conn->execute(
                "SELECT * FROM " + _conn->sqlId("request_ext") +
                "  WHERE "       + _conn->sqlEqual("request_id", id)
            );
            if (_conn->hasResult()) {

                database::mysql::Row row;
                while (_conn->next(row)) {

                    string param;
                    string value;

                    row.get("param", param);
                    row.get("value", value);

                    info.kvInfo.emplace_back(param, value);
                }
            }
            return info;
        }
    }
    throw DatabaseServicesNotFound("no Request found for id: " + id);
}


list<RequestInfo> DatabaseServicesMySQL::requests(string const& jobId,
                                                  uint64_t fromTimeStamp,
                                                  uint64_t toTimeStamp,
                                                  size_t maxEntries) {
    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " " +
         " jobId="         + jobId +
         " fromTimeStamp=" + to_string(fromTimeStamp) +
         " toTimeStamp="   + to_string(toTimeStamp) +
         " maxEntries="    + to_string(maxEntries) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    
    list<RequestInfo> collection;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                collection = _requests(
                    lock,
                    jobId,
                    fromTimeStamp,
                    toTimeStamp,
                    maxEntries);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}


list<RequestInfo> DatabaseServicesMySQL::_requests(util::Lock const& lock,
                                                   string const& jobId,
                                                   uint64_t fromTimeStamp,
                                                   uint64_t toTimeStamp,
                                                   size_t maxEntries) {

    list<RequestInfo> collection;

    string const jobIdOpt = jobId.empty()   ? "" : " AND "   + _conn->sqlEqual("job_id", jobId);
    string const limitOpt = maxEntries == 0 ? "" : " LIMIT " + to_string(maxEntries);

    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("request") +
        "  WHERE "       + _conn->sqlGreaterOrEqual("c_create_time", fromTimeStamp) +
        "    AND "       + _conn->sqlLessOrEqual("c_create_time", toTimeStamp != 0
                                    ? toTimeStamp
                                    : numeric_limits<uint64_t>::max()) +
        jobIdOpt +
        "  ORDER BY "    + _conn->sqlId("c_create_time") + " DESC" +
        limitOpt
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            RequestInfo info;

            row.get("id",             info.id);
            row.get("job_id",         info.jobId);
            row.get("name",           info.name);
            row.get("worker",         info.worker);
            row.get("priority",       info.priority);
            row.get("state",          info.state);
            row.get("ext_state",      info.extendedState);
            row.get("server_status",  info.serverStatus);
            row.get("c_create_time",  info.controllerCreateTime);
            row.get("c_start_time",   info.controllerStartTime);
            row.get("c_finish_time",  info.controllerFinishTime);
            row.get("w_receive_time", info.workerReceiveTime);
            row.get("w_start_time",   info.workerStartTime);
            row.get("w_finish_time",  info.workerFinishTime);

            collection.push_back(info);
        }
    }
    for (auto&& info: collection) {
        _conn->execute(
            "SELECT * FROM " + _conn->sqlId("request_ext") +
            "  WHERE "       + _conn->sqlEqual("request_id", info.id)
        );
        if (_conn->hasResult()) {

            database::mysql::Row row;
            while (_conn->next(row)) {

                string param;
                string value;

                row.get("param", param);
                row.get("value", value);

                info.kvInfo.emplace_back(param, value);
            }
        }
    }
    return collection;
}


JobInfo DatabaseServicesMySQL::job(string const& id) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) {
        throw invalid_argument(context + ", job identifier can't be empty");
    }
    util::Lock lock(_mtx, context);
    
    JobInfo info;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                info = _job(lock, id);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


JobInfo DatabaseServicesMySQL::_job(util::Lock const& lock,
                                    string const& id) {
    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("job") +
        "  WHERE "       + _conn->sqlEqual("id", id)
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            JobInfo info;

            row.get("id",             info.id);
            row.get("controller_id",  info.controllerId);
            row.get("parent_job_id",  info.parentJobId);
            row.get("type",           info.type);
            row.get("state",          info.state);
            row.get("ext_state",      info.extendedState);

            row.get("begin_time",     info.beginTime);
            row.get("end_time",       info.endTime);
            row.get("heartbeat_time", info.heartbeatTime);

            row.get("priority",       info.priority);
            row.get("exclusive",      info.exclusive);
            row.get("preemptable",    info.preemptable);

            _conn->execute(
                "SELECT * FROM " + _conn->sqlId("job_ext") +
                "  WHERE "       + _conn->sqlEqual("job_id", id)
            );
            if (_conn->hasResult()) {

                database::mysql::Row row;
                while (_conn->next(row)) {

                    string param;
                    string value;

                    row.get("param", param);
                    row.get("value", value);

                    info.kvInfo.emplace_back(param, value);
                }
            }
            return info;
        }
    }
    throw DatabaseServicesNotFound("no Job found for id: " + id);
}


list<JobInfo> DatabaseServicesMySQL::jobs(string const& controllerId,
                                          string const& parentJobId,
                                          uint64_t fromTimeStamp,
                                          uint64_t toTimeStamp,
                                          size_t maxEntries) {
    string const context =
         "DatabaseServicesMySQL::" + string(__func__) + " " +
         " controllerId="  + controllerId +
         " parentJobId="   + parentJobId +
         " fromTimeStamp=" + to_string(fromTimeStamp) +
         " toTimeStamp="   + to_string(toTimeStamp) +
         " maxEntries="    + to_string(maxEntries) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    
    list<JobInfo> collection;
    try {
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                collection = _jobs(
                    lock,
                    controllerId,
                    parentJobId,
                    fromTimeStamp,
                    toTimeStamp,
                    maxEntries);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}


list<JobInfo> DatabaseServicesMySQL::_jobs(util::Lock const& lock,
                                           string const& controllerId,
                                           string const& parentJobId,
                                           uint64_t fromTimeStamp,
                                           uint64_t toTimeStamp,
                                           size_t maxEntries) {
    list<JobInfo> collection;

    string const controllerIdOpt =
        controllerId.empty() ? "" : " AND " + _conn->sqlEqual("controller_id", controllerId);

    string const parentJobIdOpt =
        parentJobId.empty() ? "" : " AND " + _conn->sqlEqual("parent_job_id", parentJobId);

    string const limitOpt =
        maxEntries == 0 ? "" : " LIMIT " + to_string(maxEntries);

    _conn->execute(
        "SELECT * FROM " + _conn->sqlId("job") +
        "  WHERE "       + _conn->sqlGreaterOrEqual("begin_time", fromTimeStamp) +
        "    AND "       + _conn->sqlLessOrEqual("begin_time", toTimeStamp != 0
                                    ? toTimeStamp
                                    : numeric_limits<uint64_t>::max()) +
        controllerIdOpt +
        parentJobIdOpt +
        "  ORDER BY "    + _conn->sqlId("begin_time") + " DESC" +
        limitOpt
    );
    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            JobInfo info;

            row.get("id",             info.id);
            row.get("controller_id",  info.controllerId);
            row.get("parent_job_id",  info.parentJobId);
            row.get("type",           info.type);
            row.get("state",          info.state);
            row.get("ext_state",      info.extendedState);

            row.get("begin_time",     info.beginTime);
            row.get("end_time",       info.endTime);
            row.get("heartbeat_time", info.heartbeatTime);

            row.get("priority",       info.priority);
            row.get("exclusive",      info.exclusive);
            row.get("preemptable",    info.preemptable);

            collection.push_back(info);
        }
    }
    for (auto&& info: collection) {
        _conn->execute(
            "SELECT * FROM " + _conn->sqlId("job_ext") +
            "  WHERE "       + _conn->sqlEqual("job_id", info.id)
        );
        if (_conn->hasResult()) {

            database::mysql::Row row;
            while (_conn->next(row)) {

                string param;
                string value;

                row.get("param", param);
                row.get("value", value);

                info.kvInfo.emplace_back(param, value);
            }
        }
    }
    return collection;
}


TransactionInfo DatabaseServicesMySQL::transaction(uint32_t id) {

    string const context = _context(__func__) + "id="  + to_string(id) + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    TransactionInfo info;
    try {
        auto const predicate = _conn->sqlEqual("id", id);
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                info = _findTransactionImpl(lock, predicate);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


vector<TransactionInfo> DatabaseServicesMySQL::transactions(string const& databaseName) {

    string const context = _context(__func__) + "database="  + databaseName + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    vector<TransactionInfo> collection;
    try {
        auto const predicate = databaseName.empty() ? "" : _conn->sqlEqual("database", databaseName);
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                collection = _findTransactionsImpl(lock, predicate);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}


TransactionInfo DatabaseServicesMySQL::beginTransaction(string const& databaseName) {

    string const context = _context(__func__) + "database="  + databaseName + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);
    
    uint64_t const beginTime = PerformanceUtils::now();
    uint64_t const endTime   = 0;
    string   const state     = "STARTED";

    TransactionInfo info;
    try {
        auto const predicate = _conn->sqlEqual("id", database::mysql::Function::LAST_INSERT_ID);
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                conn->executeInsertQuery(
                    "transaction",
                    database::mysql::Keyword::SQL_NULL,
                    databaseName,
                    state,
                    beginTime,
                    endTime
                );
                info = _findTransactionImpl(lock, predicate);
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


TransactionInfo DatabaseServicesMySQL::endTransaction(uint32_t id, bool abort) {

    string const context = _context(__func__) +
            "id="  + to_string(id) + " abort=" + string(abort ? "true" : "false") + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    util::Lock lock(_mtx, context);

    uint64_t const endTime = PerformanceUtils::now();
    string   const state   = abort ? "ABORTED" : "FINISHED";

    TransactionInfo info;
    try {
        auto const predicate = _conn->sqlEqual("id", id);
        _conn->execute(
            [&](decltype(_conn) conn) {
                conn->begin();
                info = _findTransactionImpl(lock, predicate);
                if (info.endTime != 0) {
                    throw logic_error(context + "transaction " + to_string(id) + " is not active");
                }
                conn->executeSimpleUpdateQuery(
                    "transaction",
                    predicate,
                    make_pair("state",    state),
                    make_pair("end_time", endTime)
                );
                info.state   = state;
                info.endTime = endTime;
                conn->commit();
            }
        );

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        if (_conn->inTransaction()) _conn->rollback();
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}


TransactionInfo DatabaseServicesMySQL::_findTransactionImpl(util::Lock const& lock,
                                                            string const& predicate) {

    string const context = _context(__func__) + "predicate=" + predicate + " ";
    auto   const collection = _findTransactionsImpl(lock, predicate);
    size_t const num = collection.size();

    if (num == 1) return collection[0];
    if (num == 0) throw DatabaseServicesNotFound(context + "no such transaction");
    throw DatabaseServicesError(context + "two many transactions found: " + to_string(num));
}


vector<TransactionInfo> DatabaseServicesMySQL::_findTransactionsImpl(util::Lock const& lock,
                                                                     string const& predicate) {

    string const context = _context(__func__) + "predicate=" + predicate + " ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    vector<TransactionInfo> collection;

    string const query =
            "SELECT * FROM " + _conn->sqlId("transaction") +
            (predicate.empty() ? "" : " WHERE " + predicate) +
            " ORDER BY begin_time DESC";

    _conn->execute(query);

    if (_conn->hasResult()) {

        database::mysql::Row row;
        while (_conn->next(row)) {

            TransactionInfo info;

            row.get("id",         info.id);
            row.get("database",   info.database);
            row.get("state",      info.state);
            row.get("begin_time", info.beginTime);
            row.get("end_time",   info.endTime);

            collection.push_back(info);
        }
    }
    return collection;
}


void DatabaseServicesMySQL::_findReplicasImpl(util::Lock const& lock,
                                              vector<ReplicaInfo>& replicas,
                                              string const& query,
                                              bool includeFileInfo) {

    string const context =
        "DatabaseServicesMySQL::" + string(__func__) + "(replicas,query) ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    replicas.clear();

    _conn->execute(query);

    if (_conn->hasResult()) {

        // Temporarily store incomplete (w/o files) replica in the map by their
        // database identifiers. Replicas will get extended on the next step and
        // put into the resulting collection.

        map<uint64_t, ReplicaInfo> id2replica;

        database::mysql::Row row;
        while (_conn->next(row)) {

            // Extract general attributes of the replica

            uint64_t     id;
            string       worker;
            string       database;
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
        
        // Extract files for each replica using identifiers of the replicas
        // update replicas in the dictionary.
        if (includeFileInfo) _findReplicaFilesImpl(lock, id2replica);

        // Copy replicas from the dictionary into the output collection
        for (auto&& entry: id2replica) {
            replicas.push_back(entry.second);
        }
    }
}


void DatabaseServicesMySQL::_findReplicaFilesImpl(util::Lock const& lock,
                                                  map<uint64_t, ReplicaInfo>& id2replica) {

    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    if (0 == id2replica.size()) return;

    // The collection of replica identifiers will be split into batches to ensure
    // that a length of the query string (for pulling files for each batch) would
    // not exceed the corresponding MySQL limit.

    vector<uint64_t> ids;
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
        (_conn->max_allowed_packet() - 1024) /
        (1 + to_string(numeric_limits<unsigned long long>::max()).size());

    if ((_conn->max_allowed_packet() < 1024) or (0 == batchSize)) {
        throw runtime_error(
                context + "value of 'max_allowed_packet' set for the MySQL session is too small: " +
                to_string(_conn->max_allowed_packet()));
    }
 
    // Compute sizes of batches. This will be needed on the next step to iterate
    // over a collection of replica identifiers.

    vector<size_t> batches(ids.size() / batchSize, batchSize);  // complete batches
    size_t const lastBatchSize = ids.size() % batchSize;        // and the last one (if any)
    if (0 != lastBatchSize) {
        batches.push_back(lastBatchSize);
    }

    // Iterate over batches, submit a query per batch, harvest and process
    // results.
    //
    // IMPORTANT: the algorithm does NOT require replicas to have files because some
    // replicas may still be in a process of being ingested.

    auto itr = ids.begin();         // points to the first replica identifier of a batch
    for (size_t size: batches) {

        _conn->execute(
            "SELECT * FROM " + _conn->sqlId("replica_file") +
            "  WHERE "       + _conn->sqlIn("replica_id", vector<uint64_t>(itr, itr + size)) +
            "  ORDER BY "    + _conn->sqlId("replica_id"));
    
        if (_conn->hasResult()) {
    
            uint64_t currentReplicaId = 0;
            ReplicaInfo::FileInfoCollection files;  // for accumulating files of the current
                                                    // replica

            // Extend a replica in place
            auto extendReplicaThenClearFiles = [&id2replica,&files] (uint64_t replicaId) {
                id2replica.at(replicaId).setFileInfo(files);
                files.clear();
            };

            database::mysql::Row row;
            while (_conn->next(row)) {
    
                // Extract attributes of the file
    
                uint64_t replicaId;
                string   name;
                uint64_t size;
                time_t   mtime;
                string   cs;
                uint64_t beginCreateTime;
                uint64_t endCreateTime;
    
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
                        extendReplicaThenClearFiles(currentReplicaId);
                    }
                    currentReplicaId = replicaId;
                }

                // Adding this file to the current replica

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
                extendReplicaThenClearFiles(currentReplicaId);
            }
        }

        // Advance iterator to the first identifier of the next
        // batch (if any).
        itr += size;
    }
}


void DatabaseServicesMySQL::_findChunksImpl(util::Lock const& lock,
                                            vector<unsigned int>& chunks,
                                            string const& query) {
    auto const context = "DatabaseServicesMySQL::" + string(__func__) + "(chunks,query) ";

    LOGS(_log, LOG_LVL_DEBUG, context);

    chunks.clear();

    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            unsigned int chunk;
            row.get("chunk", chunk);
            chunks.push_back(chunk);
        }
    }
}


string DatabaseServicesMySQL::_context(string const& func) const {
    return "DatabaseServicesMySQL::" + func + " ";
}

}}} // namespace lsst::qserv::replica
