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
#include "global/stringUtil.h"
#include "http/Method.h"
#include "replica/Common.h"
#include "replica/Configuration.h"
#include "replica/Controller.h"
#include "replica/DatabaseMySQLUtils.h"
#include "replica/Job.h"
#include "replica/NamedMutexRegistry.h"
#include "replica/QservWorkerMgtRequest.h"
#include "replica/ReplicaInfo.h"
#include "replica/Request.h"
#include "replica/SemanticMaps.h"
#include "util/TimeUtils.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;
namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.DatabaseServicesMySQL");

/**
 * Typical use:
 * @code
 * bool yesFound = in(Request::ExtendedState::SUCCESS, {
 *                          Request::ExtendedState::SUCCESS,
 *                          Request::ExtendedState::SERVER_ERROR,
 *                          Request::ExtendedState::SERVER_CANCELLED});
 * @code
 * @return 'true' if the specified state is found in a collection.
 */
bool in(Request::ExtendedState val, vector<Request::ExtendedState> const& col) {
    return col.end() != find(col.begin(), col.end(), val);
}

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

DatabaseServicesMySQL::DatabaseServicesMySQL(Configuration::Ptr const& configuration)
        : DatabaseServices(),
          _configuration(configuration),
          _conn(Connection::open(ConnectionParams(configuration->get<string>("database", "host"),
                                                  configuration->get<uint16_t>("database", "port"),
                                                  configuration->get<string>("database", "user"),
                                                  configuration->get<string>("database", "password"),
                                                  configuration->get<string>("database", "name")))),
          _g(_conn) {}

void DatabaseServicesMySQL::saveState(ControllerIdentity const& identity, uint64_t startTime) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Controller] ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    replica::Lock lock(_mtx, context);
    try {
        string const query = _g.insert("controller", identity.id, identity.host, identity.pid, startTime);
        _conn->executeInOwnTransaction([&query](decltype(_conn) conn) { conn->execute(query); });
    } catch (database::mysql::ER_DUP_ENTRY_ const&) {
        LOGS(_log, LOG_LVL_ERROR, context << "the state is already in the database");
        throw logic_error(context + "the state is already in the database");

    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Job const& job) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Job::" + job.type() + "] ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    replica::Lock lock(_mtx, context);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (Job id) already exists in the table
    // then the UPDATE query will be executed.
    try {
        auto const insert = [&](decltype(_conn) conn) {
            auto const heartbeatTime = util::TimeUtils::now();
            string const query = _g.insert(
                    "job", job.id(), job.controller()->identity().id, _g.nullIfEmpty(job.parentJobId()),
                    job.type(), Job::state2string(job.state()), Job::state2string(job.extendedState()),
                    job.beginTime(), job.endTime(), heartbeatTime, job.priority());
            conn->execute(query);
            for (auto&& entry : job.extendedPersistentState()) {
                string const& param = entry.first;
                string const& value = entry.second;
                string const query = _g.insert("job_ext", job.id(), param, value);
                conn->execute(query);
            }
        };
        auto const update = [&](decltype(_conn) conn) {
            string const query = _g.update("job", make_pair("state", Job::state2string(job.state())),
                                           make_pair("ext_state", Job::state2string(job.extendedState())),
                                           make_pair("begin_time", job.beginTime()),
                                           make_pair("end_time", job.endTime())) +
                                 _g.where(_g.eq("id", job.id()));
            conn->execute(query);
        };
        _conn->executeInsertOrUpdate(insert, update);
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::updateHeartbeatTime(Job const& job) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "[Job::" + job.type() + "] ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    replica::Lock lock(_mtx, context);
    try {
        string const query = _g.update("job", make_pair("heartbeat_time", util::TimeUtils::now())) +
                             _g.where(_g.eq("id", job.id()));
        _conn->executeInOwnTransaction([&query](decltype(_conn) conn) { conn->execute(query); });
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(QservWorkerMgtRequest const& request, Performance const& performance,
                                      string const& serverError) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + "[QservWorkerMgtRequest::" + request.type() + "] ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    // Requests which haven't started yet or the ones which aren't associated
    // with any job should be ignored.
    try {
        if (request.jobId().empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request with no job set, id=" << request.id());
            return;
        }
    } catch (logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG,
             context << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    replica::Lock lock(_mtx, context);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (id) already exists in the table
    // then the UPDATE query will be executed.
    try {
        auto const insert = [&](decltype(_conn) conn) {
            string const query =
                    _g.insert("request", request.id(), request.jobId(), request.type(), request.workerName(),
                              0, QservWorkerMgtRequest::state2string(request.state()),
                              QservWorkerMgtRequest::state2string(request.extendedState()), serverError,
                              performance.c_create_time, performance.c_start_time, performance.w_receive_time,
                              performance.w_start_time, performance.w_finish_time, performance.c_finish_time);
            conn->execute(query);
            for (auto&& entry : request.extendedPersistentState()) {
                string const& param = entry.first;
                string const& value = entry.second;
                string const query = _g.insert("request_ext", request.id(), param, value);
                conn->execute(query);
            }
        };
        auto const update = [&](decltype(_conn) conn) {
            string const query =
                    _g.update("request",
                              make_pair("state", QservWorkerMgtRequest::state2string(request.state())),
                              make_pair("ext_state",
                                        QservWorkerMgtRequest::state2string(request.extendedState())),
                              make_pair("server_status", serverError),
                              make_pair("c_create_time", performance.c_create_time),
                              make_pair("c_start_time", performance.c_start_time),
                              make_pair("w_receive_time", performance.w_receive_time),
                              make_pair("w_start_time", performance.w_start_time),
                              make_pair("w_finish_time", performance.w_finish_time),
                              make_pair("c_finish_time", performance.c_finish_time)) +
                    _g.where(_g.eq("id", request.id()));
            conn->execute(query);
        };
        _conn->executeInsertOrUpdate(insert, update);
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveState(Request const& request, Performance const& performance) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + "[Request::" + request.type() + "] ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    // Requests which haven't started yet or the ones which aren't associated
    // with any job should be ignored.
    try {
        if (request.jobId().empty()) {
            LOGS(_log, LOG_LVL_DEBUG, context << "ignoring the request with no job set, id=" << request.id());
            return;
        }
    } catch (logic_error const&) {
        LOGS(_log, LOG_LVL_DEBUG,
             context << "ignoring the request which hasn't yet started, id=" << request.id());
        return;
    }

    replica::Lock lock(_mtx, context);

    // The algorithm will first try the INSERT query into the base table.
    // If a row with the same primary key (request id) already exists in the table
    // then the UPDATE query will be executed.
    try {
        auto const insert = [&](decltype(_conn) conn) {
            string const query =
                    _g.insert("request", request.id(), request.jobId(), request.type(), request.workerName(),
                              request.priority(), Request::state2string(request.state()),
                              Request::state2string(request.extendedState()),
                              status2string(request.extendedServerStatus()), performance.c_create_time,
                              performance.c_start_time, performance.w_receive_time, performance.w_start_time,
                              performance.w_finish_time, performance.c_finish_time);
            conn->execute(query);
            for (auto&& entry : request.extendedPersistentState()) {
                string const& param = entry.first;
                string const& value = entry.second;
                string const query = _g.insert("request_ext", request.id(), param, value);
                conn->execute(query);
            }
        };
        auto const update = [&](decltype(_conn) conn) {
            string const query =
                    _g.update("request", make_pair("state", Request::state2string(request.state())),
                              make_pair("ext_state", Request::state2string(request.extendedState())),
                              make_pair("server_status", status2string(request.extendedServerStatus())),
                              make_pair("c_create_time", performance.c_create_time),
                              make_pair("c_start_time", performance.c_start_time),
                              make_pair("w_receive_time", performance.w_receive_time),
                              make_pair("w_start_time", performance.w_start_time),
                              make_pair("w_finish_time", performance.w_finish_time),
                              make_pair("c_finish_time", performance.c_finish_time)) +
                    _g.where(_g.eq("id", request.id()));
            conn->execute(query);
        };
        _conn->executeInsertOrUpdate(insert, update);
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::updateRequestState(Request const& request, string const& targetRequestId,
                                               Performance const& targetRequestPerformance) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + "[Request::" + request.type() + "] ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    replica::Lock lock(_mtx, context);

    // According to the current implementation of the requests processing pipeline
    // for the request management (including State* and Stop* families of requests),
    // these states refer to the corresponding target request. Therefore only those
    // states are allowed to be considered for the updates.
    //
    // IMPLEMENTATION NOTE: the request state filter is placed in this method
    // to avoid code duplication in each monitoring request.
    if ((request.state() == Request::State::FINISHED) and
        ::in(request.extendedState(),
             {Request::ExtendedState::SUCCESS, Request::ExtendedState::SERVER_QUEUED,
              Request::ExtendedState::SERVER_IN_PROGRESS, Request::ExtendedState::SERVER_IS_CANCELLING,
              Request::ExtendedState::SERVER_ERROR, Request::ExtendedState::SERVER_CANCELLED})) {
        try {
            string const query =
                    _g.update("request", make_pair("state", Request::state2string(request.state())),
                              make_pair("ext_state", Request::state2string(request.extendedState())),
                              make_pair("server_status", status2string(request.extendedServerStatus())),
                              make_pair("w_receive_time", targetRequestPerformance.w_receive_time),
                              make_pair("w_start_time", targetRequestPerformance.w_start_time),
                              make_pair("w_finish_time", targetRequestPerformance.w_finish_time)) +
                    _g.where(_g.eq("id", targetRequestId));
            _conn->executeInOwnTransaction([&query](decltype(_conn) conn) { conn->execute(query); });
        } catch (database::mysql::Error const& ex) {
            LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
            throw;
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfo(ReplicaInfo const& info) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    unsigned int maxReconnects = 0;  // pull the default value from the Configuration
    unsigned int timeoutSec = 0;     // pull the default value from the Configuration
    unsigned int maxRetriesOnDeadLock = 1;

    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { _saveReplicaInfoImpl(lock, info); },
                                       maxReconnects, timeoutSec, maxRetriesOnDeadLock);
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::saveReplicaInfoCollection(string const& workerName, string const& database,
                                                      ReplicaInfoCollection const& newReplicaInfoCollection) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertWorkerIsValid(context, workerName);
    _assertDatabaseIsValid(context, database);

    unsigned int maxReconnects = 0;  // pull the default value from the Configuration
    unsigned int timeoutSec = 0;     // pull the default value from the Configuration
    unsigned int maxRetriesOnDeadLock = 1;

    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) {
                    _saveReplicaInfoCollectionImpl(lock, workerName, database, newReplicaInfoCollection);
                },
                maxReconnects, timeoutSec, maxRetriesOnDeadLock);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::_saveReplicaInfoImpl(replica::Lock const& lock, ReplicaInfo const& info) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    try {
        // Try inserting if the replica is complete. Delete otherwise.
        if (info.status() == ReplicaInfo::Status::COMPLETE) {
            string const query = _g.insert("replica", Sql::NULL_, info.worker(), info.database(),
                                           info.chunk(), info.verifyTime());
            _conn->execute(query);
            for (auto&& f : info.fileInfo()) {
                string const query = _g.insert("replica_file", Sql::LAST_INSERT_ID, f.name, f.size, f.mtime,
                                               f.cs, f.beginTransferTime, f.endTransferTime);
                _conn->execute(query);
            }
        } else {
            // This query will also cascade delete the relevant file entries
            // See details in the schema.
            string const query = _g.delete_("replica") + _g.where(_g.eq("worker", info.worker()),
                                                                  _g.eq("database", info.database()),
                                                                  _g.eq("chunk", info.chunk()));
            _conn->execute(query);
        }

    } catch (database::mysql::ER_DUP_ENTRY_ const&) {
        // Replace the replica with a newer version
        _deleteReplicaInfoImpl(lock, info.worker(), info.database(), info.chunk());
        _saveReplicaInfoImpl(lock, info);
    }
}

void DatabaseServicesMySQL::_saveReplicaInfoCollectionImpl(
        replica::Lock const& lock, string const& workerName, string const& database,
        ReplicaInfoCollection const& newReplicaInfoCollection) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG,
         context << "worker: " << workerName << " database: " << database
                 << " num.replicas: " << newReplicaInfoCollection.size());

    // Group new replicas by contexts
    LOGS(_log, LOG_LVL_DEBUG, context << "new replicas group: 1");
    WorkerDatabaseChunkMap<ReplicaInfo const*> newReplicas;
    for (auto&& replica : newReplicaInfoCollection) {
        // Ignore replicas which are not in the specified context
        if (replica.worker() == workerName and replica.database() == database) {
            newReplicas.atWorker(replica.worker()).atDatabase(replica.database()).atChunk(replica.chunk()) =
                    &replica;
        }
    }

    // Obtain old replicas and group them by contexts
    vector<ReplicaInfo> oldReplicaInfoCollection;
    _findWorkerReplicasImpl(lock, oldReplicaInfoCollection, workerName, database);
    WorkerDatabaseChunkMap<ReplicaInfo const*> oldReplicas;
    for (auto&& replica : oldReplicaInfoCollection) {
        oldReplicas.atWorker(replica.worker()).atDatabase(replica.database()).atChunk(replica.chunk()) =
                &replica;
    }

    // Find differences between the collections
    WorkerDatabaseChunkMap<ReplicaInfo const*> inBoth;
    SemanticMaps::intersect(newReplicas, oldReplicas, inBoth);

    WorkerDatabaseChunkMap<ReplicaInfo const*> inNewReplicasOnly;
    WorkerDatabaseChunkMap<ReplicaInfo const*> inOldReplicasOnly;
    SemanticMaps::diff2(newReplicas, oldReplicas, inNewReplicasOnly, inOldReplicasOnly);

    LOGS(_log, LOG_LVL_DEBUG,
         context << "*** replicas comparison summary *** "
                 << " #new: " << newReplicaInfoCollection.size() << " #old: "
                 << oldReplicaInfoCollection.size() << " #in-both: " << SemanticMaps::count(inBoth)
                 << " #new-only: " << SemanticMaps::count(inNewReplicasOnly)
                 << " #old-only: " << SemanticMaps::count(inOldReplicasOnly));

    // Eliminate outdated replicas

    for (auto&& workerName : inOldReplicasOnly.workerNames()) {
        auto const& databases = inOldReplicasOnly.worker(workerName);
        for (auto&& database : databases.databaseNames()) {
            auto const& chunks = databases.database(database);
            for (auto&& chunk : chunks.chunkNumbers()) {
                _deleteReplicaInfoImpl(lock, workerName, database, chunk);
            }
        }
    }

    // Insert new replicas not present in the old collection
    for (auto&& workerName : inNewReplicasOnly.workerNames()) {
        auto const& databases = inNewReplicasOnly.worker(workerName);
        for (auto&& database : databases.databaseNames()) {
            auto const& chunks = databases.database(database);
            for (auto&& chunk : chunks.chunkNumbers()) {
                ReplicaInfo const* ptr = chunks.chunk(chunk);
                _saveReplicaInfoImpl(lock, *ptr);
            }
        }
    }

    // Deep comparison of the replicas in the intersect area to see
    // which of those need to be updated.
    for (auto&& workerName : inBoth.workerNames()) {
        auto const& newDatabases = newReplicas.worker(workerName);
        auto const& oldDatabases = oldReplicas.worker(workerName);
        auto const& databases = inBoth.worker(workerName);
        for (auto&& database : databases.databaseNames()) {
            auto const& newChunks = newDatabases.database(database);
            auto const& oldChunks = oldDatabases.database(database);
            auto const& chunks = databases.database(database);
            for (auto&& chunk : chunks.chunkNumbers()) {
                ReplicaInfo const* newPtr = newChunks.chunk(chunk);
                ReplicaInfo const* oldPtr = oldChunks.chunk(chunk);
                if (*newPtr != *oldPtr) {
                    _deleteReplicaInfoImpl(lock, workerName, database, chunk);
                    _saveReplicaInfoImpl(lock, *newPtr);
                }
            }
        }
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
}

void DatabaseServicesMySQL::_deleteReplicaInfoImpl(replica::Lock const& lock, string const& workerName,
                                                   string const& database, unsigned int chunk) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    _assertWorkerIsValid(context, workerName);
    _assertDatabaseIsValid(context, database);

    string const query = _g.delete_("replica") + _g.where(_g.eq("worker", workerName),
                                                          _g.eq("database", database), _g.eq("chunk", chunk));
    _conn->execute(query);
}

void DatabaseServicesMySQL::findOldestReplicas(vector<ReplicaInfo>& replicas, size_t maxReplicas,
                                               bool enabledWorkersOnly, bool allDatabases, bool isPublished) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (maxReplicas == 0) throw invalid_argument(context + "maxReplicas is not allowed to be 0");

    replica::Lock lock(_mtx, context);
    try {
        string const noSpecificFamily;
        auto const databases = _configuration->databases(noSpecificFamily, allDatabases, isPublished);
        string const query =
                _g.select(Sql::STAR) + _g.from("replica") +
                _g.where(_g.in("database", databases),
                         enabledWorkersOnly ? _g.in("worker", _configuration->workers(true)) : "") +
                _g.orderBy(make_pair("verify_time", "ASC")) + _g.limit(maxReplicas);
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { _findReplicasImpl(lock, replicas, query); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findReplicas(vector<ReplicaInfo>& replicas, unsigned int chunk,
                                         string const& database, bool enabledWorkersOnly,
                                         bool includeFileInfo) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " chunk=" + to_string(chunk) +
                           "  database=" + database + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    try {
        string const query =
                _g.select(Sql::STAR) + _g.from("replica") +
                _g.where(_g.eq("chunk", chunk), _g.eq("database", database),
                         enabledWorkersOnly ? _g.in("worker", _configuration->workers(true)) : "");
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { _findReplicasImpl(lock, replicas, query, includeFileInfo); });
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findReplicas(vector<ReplicaInfo>& replicas, vector<unsigned int> const& chunks,
                                         string const& database, bool enabledWorkersOnly,
                                         bool includeFileInfo) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) +
                           " chunks.size()=" + to_string(chunks.size()) + "  database=" + database + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    // The trivial optimization for the trivial input
    if (chunks.empty()) {
        replicas.clear();
        return;
    }

    replica::Lock lock(_mtx, context);
    try {
        string const query =
                _g.select(Sql::STAR) + _g.from("replica") +
                _g.where(_g.in("chunk", chunks), _g.eq("database", database),
                         enabledWorkersOnly ? _g.in("worker", _configuration->workers(true)) : "");
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { _findReplicasImpl(lock, replicas, query, includeFileInfo); });
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findWorkerReplicas(vector<ReplicaInfo>& replicas, string const& workerName,
                                               string const& database, bool allDatabases, bool isPublished,
                                               bool includeFileInfo) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " worker=" + workerName +
                           " database=" + database + " allDatabases=" + bool2str(allDatabases) +
                           " isPublished=" + bool2str(isPublished) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertWorkerIsValid(context, workerName);

    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            _findWorkerReplicasImpl(lock, replicas, workerName, database, allDatabases, isPublished,
                                    includeFileInfo);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

uint64_t DatabaseServicesMySQL::numWorkerReplicas(string const& workerName, string const& database,
                                                  bool allDatabases, bool isPublished) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " worker=" + workerName +
                           " database=" + database + " allDatabases=" + bool2str(allDatabases) +
                           " isPublished=" + bool2str(isPublished) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertWorkerIsValid(context, workerName);

    uint64_t num;
    replica::Lock lock(_mtx, context);
    try {
        string query = _g.select(Sql::COUNT_STAR) + _g.from("replica");
        if (database.empty()) {
            string const noSpecificFamily;
            auto const databases = _configuration->databases(noSpecificFamily, allDatabases, isPublished);
            query += _g.where(_g.in("database", databases), _g.eq("worker", workerName));
        } else {
            if (!_configuration->isKnownDatabase(database))
                throw invalid_argument(context + "unknown database: " + database);
            query += _g.where(_g.eq("database", database), _g.eq("worker", workerName));
        }
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { selectSingleValue(conn, query, num, 0, false); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** num: " << num);
    return num;
}

void DatabaseServicesMySQL::_findWorkerReplicasImpl(replica::Lock const& lock, vector<ReplicaInfo>& replicas,
                                                    string const& workerName, string const& database,
                                                    bool allDatabases, bool isPublished,
                                                    bool includeFileInfo) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " worker=" + workerName +
                           " database=" + database + " allDatabases=" + bool2str(allDatabases) +
                           " isPublished=" + bool2str(isPublished) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertWorkerIsValid(context, workerName);

    auto query = _g.select(Sql::STAR) + _g.from("replica");
    if (database.empty()) {
        string const noSpecificFamily;
        auto const databases = _configuration->databases(noSpecificFamily, allDatabases, isPublished);
        query += _g.where(_g.in("database", databases), _g.eq("worker", workerName));
    } else {
        if (!_configuration->isKnownDatabase(database))
            throw invalid_argument(context + "unknown database: " + database);
        query += _g.where(_g.eq("database", database), _g.eq("worker", workerName));
    }
    _findReplicasImpl(lock, replicas, query, includeFileInfo);
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findWorkerReplicas(vector<ReplicaInfo>& replicas, unsigned int chunk,
                                               string const& workerName, string const& databaseFamily,
                                               bool allDatabases, bool isPublished) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " worker=" + workerName +
                           " chunk=" + to_string(chunk) + " family=" + databaseFamily +
                           " allDatabases=" + bool2str(allDatabases) +
                           " isPublished=" + bool2str(isPublished) + "  ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertWorkerIsValid(context, workerName);
    _assertDatabaseFamilyIsValid(context, databaseFamily);

    replica::Lock lock(_mtx, context);
    try {
        auto const databases = _configuration->databases(databaseFamily, allDatabases, isPublished);
        auto const query =
                _g.select(Sql::STAR) + _g.from("replica") +
                _g.where(_g.eq("worker", workerName), _g.eq("chunk", chunk), _g.in("database", databases));
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { _findReplicasImpl(lock, replicas, query); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findDatabaseReplicas(vector<ReplicaInfo>& replicas, string const& database,
                                                 bool enabledWorkersOnly) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "  database=" + database +
                           " enabledWorkersOnly=" + bool2str(enabledWorkersOnly) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    try {
        string const query =
                _g.select(Sql::STAR) + _g.from("replica") +
                _g.where(_g.eq("database", database),
                         enabledWorkersOnly ? _g.in("worker", _configuration->workers(true)) : "");
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { _findReplicasImpl(lock, replicas, query); });
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << replicas.size());
}

void DatabaseServicesMySQL::findDatabaseChunks(vector<unsigned int>& chunks, string const& database,
                                               bool enabledWorkersOnly) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "  database=" + database +
                           " enabledWorkersOnly=" + bool2str(enabledWorkersOnly) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    try {
        string query = _g.select(_g.distinctId("chunk")) + _g.from("replica");
        if (enabledWorkersOnly) {
            query += _g.where(_g.eq("database", database), _g.in("worker", _configuration->workers(true)));
        } else {
            query += _g.where(_g.eq("database", database));
        }
        query += _g.orderBy(make_pair("chunk", ""));
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { _findChunksImpl(lock, chunks, query); });
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context << "** DONE ** replicas.size(): " << chunks.size());
}

map<unsigned int, size_t> DatabaseServicesMySQL::actualReplicationLevel(
        string const& database, vector<string> const& workersToExclude) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " database=" + database + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    try {
        auto const subQuery =
                _g.subQuery(_g.select("chunk", _g.as(Sql::COUNT_STAR, "level")) + _g.from("replica") +
                            _g.where(_g.eq("database", database), _g.notIn("worker", workersToExclude),
                                     _g.neq("chunk", 1234567890)) +
                            _g.groupBy("chunk"));
        string const query = _g.select("level", _g.as(Sql::COUNT_STAR, "num_chunks")) +
                             _g.from(_g.as(subQuery, "chunks")) + _g.groupBy("level");

        LOGS(_log, LOG_LVL_DEBUG, context + "query: " + query);

        map<unsigned int, size_t> result;
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
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
        });
        LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
        return result;
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
}

size_t DatabaseServicesMySQL::numOrphanChunks(string const& database, vector<string> const& uniqueOnWorkers) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " database=" + database + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    try {
        size_t result = 0;
        if (!uniqueOnWorkers.empty()) {
            vector<string> workersToExclude;
            for (auto&& workerName : _configuration->allWorkers()) {
                if (uniqueOnWorkers.end() ==
                    find(uniqueOnWorkers.begin(), uniqueOnWorkers.end(), workerName)) {
                    workersToExclude.push_back(workerName);
                }
            }
            string const subQuery = _g.select("chunk") + _g.from("replica") +
                                    _g.where(_g.eq("database", database), _g.in("worker", workersToExclude));

            string const query = _g.select(Sql::COUNT_STAR) + _g.from("replica") +
                                 _g.where(_g.eq("database", database), _g.in("worker", uniqueOnWorkers),
                                          _g.neq("chunk", 1234567890), _g.notInSubQuery("chunk", subQuery));

            LOGS(_log, LOG_LVL_DEBUG, context + "query: " + query);

            _conn->executeInOwnTransaction(
                    [&](decltype(_conn) conn) { selectSingleValue(conn, query, result); });
        }
        LOGS(_log, LOG_LVL_DEBUG, context << "** DONE **");
        return result;
    } catch (database::mysql::Error const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
}

void DatabaseServicesMySQL::logControllerEvent(ControllerEvent const& event) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + " " + " controllerId=" + event.controllerId +
            " timeStamp=" + to_string(event.timeStamp) + " task=" + event.task +
            " operation=" + event.operation + " status=" + event.status + " requestId=" + event.requestId +
            " jobId=" + event.jobId + " kvInfo.size=" + to_string(event.kvInfo.size()) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { _logControllerEvent(lock, event); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::_logControllerEvent(replica::Lock const& lock, ControllerEvent const& event) {
    string const query = _g.insert("controller_log", Sql::NULL_, event.controllerId, event.timeStamp,
                                   event.task, event.operation, event.status, _g.nullIfEmpty(event.requestId),
                                   _g.nullIfEmpty(event.jobId));
    _conn->execute(query);

    for (auto&& kv : event.kvInfo) {
        string const query = _g.insert("controller_log_ext", Sql::LAST_INSERT_ID, kv.first, kv.second);
        _conn->execute(query);
    }
}

list<ControllerEvent> DatabaseServicesMySQL::readControllerEvents(string const& controllerId,
                                                                  uint64_t fromTimeStamp,
                                                                  uint64_t toTimeStamp, size_t maxEntries,
                                                                  string const& task, string const& operation,
                                                                  string const& operationStatus) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " " +
                           " controllerId=" + controllerId + " fromTimeStamp=" + to_string(fromTimeStamp) +
                           " toTimeStamp=" + to_string(toTimeStamp) + " maxEntries=" + to_string(maxEntries) +
                           " task=" + task + " maxEntries=" + operation + " maxEntries=" + operationStatus +
                           " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    list<ControllerEvent> events;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            events = _readControllerEvents(lock, controllerId, fromTimeStamp, toTimeStamp, maxEntries, task,
                                           operation, operationStatus);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return events;
}

list<ControllerEvent> DatabaseServicesMySQL::_readControllerEvents(
        replica::Lock const& lock, string const& controllerId, uint64_t fromTimeStamp, uint64_t toTimeStamp,
        size_t maxEntries, string const& task, string const& operation, string const& operationStatus) {
    if (fromTimeStamp > toTimeStamp) {
        throw invalid_argument("DatabaseServicesMySQL::" + string(__func__) +
                               " illegal time range"
                               " for events: [" +
                               to_string(fromTimeStamp) + "," + to_string(toTimeStamp) + "]");
    }
    string const query =
            _g.select(Sql::STAR) + _g.from("controller_log") +
            _g.where(_g.geq("time", fromTimeStamp),
                     _g.leq("time", toTimeStamp != 0 ? toTimeStamp : numeric_limits<uint64_t>::max()),
                     controllerId.empty() ? "" : _g.eq("controller_id", controllerId),
                     task.empty() ? "" : _g.eq("task", task),
                     operation.empty() ? "" : _g.eq("operation", operation),
                     operationStatus.empty() ? "" : _g.eq("status", operationStatus)) +
            _g.orderBy(make_pair("time", "DESC")) + _g.limit(maxEntries);

    list<ControllerEvent> events;
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            ControllerEvent event;
            row.get("id", event.id);
            row.get("controller_id", event.controllerId);
            row.get("time", event.timeStamp);
            row.get("task", event.task);
            row.get("operation", event.operation);
            row.get("status", event.status);
            if (not row.isNull("request_id")) row.get("request_id", event.requestId);
            if (not row.isNull("job_id")) row.get("job_id", event.jobId);
            events.push_back(event);
        }
        for (auto&& event : events) {
            string const query = _g.select(Sql::STAR) + _g.from("controller_log_ext") +
                                 _g.where(_g.eq("controller_log_id", event.id));
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

json DatabaseServicesMySQL::readControllerEventDict(string const& controllerId) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + " " + " controllerId=" + controllerId + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (controllerId.empty()) throw invalid_argument(context + "controllerId can't be empty");

    json dict;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { dict = _readControllerEventDict(lock, controllerId); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return dict;
}

json DatabaseServicesMySQL::_readControllerEventDict(replica::Lock const& lock, string const& controllerId) {
    json dict;
    for (const char* col : {"task", "operation", "status"}) {
        string const query = _g.select(_g.distinctId(col)) + _g.from("controller_log") +
                             _g.where(_g.eq("controller_id", controllerId));
        _conn->execute(query);
        json& colValues = dict[col];
        database::mysql::Row row;
        while (_conn->next(row)) {
            string val;
            row.get(col, val);
            // Skip both NULL and the empty string
            if (!val.empty()) colValues.push_back(val);
        }
    }
    return dict;
}

ControllerInfo DatabaseServicesMySQL::controller(string const& id) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) throw invalid_argument(context + ", controller identifier can't be empty");

    ControllerInfo info;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { info = _controller(lock, id); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

ControllerInfo DatabaseServicesMySQL::_controller(replica::Lock const& lock, string const& id) {
    string const query = _g.select(Sql::STAR) + _g.from("controller") + _g.where(_g.eq("id", id));
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            ControllerInfo info;
            row.get("id", info.id);
            row.get("start_time", info.started);
            row.get("hostname", info.hostname);
            row.get("pid", info.pid);
            return info;
        }
    }
    throw DatabaseServicesNotFound("no Controller found for id: " + id);
}

list<ControllerInfo> DatabaseServicesMySQL::controllers(uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                                        size_t maxEntries) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " " +
                           " fromTimeStamp=" + to_string(fromTimeStamp) +
                           " toTimeStamp=" + to_string(toTimeStamp) + " maxEntries=" + to_string(maxEntries) +
                           " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    list<ControllerInfo> collection;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _controllers(lock, fromTimeStamp, toTimeStamp, maxEntries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

list<ControllerInfo> DatabaseServicesMySQL::_controllers(replica::Lock const& lock, uint64_t fromTimeStamp,
                                                         uint64_t toTimeStamp, size_t maxEntries) {
    list<ControllerInfo> collection;
    string const query =
            _g.select(Sql::STAR) + _g.from("controller") +
            _g.where(_g.geq("start_time", fromTimeStamp),
                     _g.leq("start_time", toTimeStamp != 0 ? toTimeStamp : numeric_limits<uint64_t>::max())) +
            _g.orderBy(make_pair("start_time", "DESC")) + _g.limit(maxEntries);

    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            ControllerInfo info;
            row.get("id", info.id);
            row.get("start_time", info.started);
            row.get("hostname", info.hostname);
            row.get("pid", info.pid);
            collection.push_back(info);
        }
    }
    return collection;
}

RequestInfo DatabaseServicesMySQL::request(string const& id) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) throw invalid_argument(context + ", request identifier can't be empty");

    RequestInfo info;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { info = _request(lock, id); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

RequestInfo DatabaseServicesMySQL::_request(replica::Lock const& lock, string const& id) {
    string const query = _g.select(Sql::STAR) + _g.from("request") + _g.where(_g.eq("id", id));
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            RequestInfo info;
            row.get("id", info.id);
            row.get("job_id", info.jobId);
            row.get("name", info.name);
            row.get("worker", info.worker);
            row.get("priority", info.priority);
            row.get("state", info.state);
            row.get("ext_state", info.extendedState);
            row.get("server_status", info.serverStatus);
            row.get("c_create_time", info.controllerCreateTime);
            row.get("c_start_time", info.controllerStartTime);
            row.get("c_finish_time", info.controllerFinishTime);
            row.get("w_receive_time", info.workerReceiveTime);
            row.get("w_start_time", info.workerStartTime);
            row.get("w_finish_time", info.workerFinishTime);
            string const query =
                    _g.select(Sql::STAR) + _g.from("request_ext") + _g.where(_g.eq("request_id", id));
            _conn->execute(query);
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

list<RequestInfo> DatabaseServicesMySQL::requests(string const& jobId, uint64_t fromTimeStamp,
                                                  uint64_t toTimeStamp, size_t maxEntries) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " " + " jobId=" + jobId +
                           " fromTimeStamp=" + to_string(fromTimeStamp) +
                           " toTimeStamp=" + to_string(toTimeStamp) + " maxEntries=" + to_string(maxEntries) +
                           " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    list<RequestInfo> collection;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _requests(lock, jobId, fromTimeStamp, toTimeStamp, maxEntries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

list<RequestInfo> DatabaseServicesMySQL::_requests(replica::Lock const& lock, string const& jobId,
                                                   uint64_t fromTimeStamp, uint64_t toTimeStamp,
                                                   size_t maxEntries) {
    list<RequestInfo> collection;
    string const query = _g.select(Sql::STAR) + _g.from("request") +
                         _g.where(_g.geq("c_create_time", fromTimeStamp),
                                  _g.leq("c_create_time",
                                         toTimeStamp != 0 ? toTimeStamp : numeric_limits<uint64_t>::max()),
                                  jobId.empty() ? "" : _g.eq("job_id", jobId)) +
                         _g.orderBy(make_pair("c_create_time", "DESC")) + _g.limit(maxEntries);

    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            RequestInfo info;
            row.get("id", info.id);
            row.get("job_id", info.jobId);
            row.get("name", info.name);
            row.get("worker", info.worker);
            row.get("priority", info.priority);
            row.get("state", info.state);
            row.get("ext_state", info.extendedState);
            row.get("server_status", info.serverStatus);
            row.get("c_create_time", info.controllerCreateTime);
            row.get("c_start_time", info.controllerStartTime);
            row.get("c_finish_time", info.controllerFinishTime);
            row.get("w_receive_time", info.workerReceiveTime);
            row.get("w_start_time", info.workerStartTime);
            row.get("w_finish_time", info.workerFinishTime);
            collection.push_back(info);
        }
    }
    for (auto&& info : collection) {
        string const query =
                _g.select(Sql::STAR) + _g.from("request_ext") + _g.where(_g.eq("request_id", info.id));
        _conn->execute(query);
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
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " id=" + id + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (id.empty()) throw invalid_argument(context + ", job identifier can't be empty");

    JobInfo info;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) { info = _job(lock, id); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

JobInfo DatabaseServicesMySQL::_job(replica::Lock const& lock, string const& id) {
    string const query = _g.select(Sql::STAR) + _g.from("job") + _g.where(_g.eq("id", id));
    _conn->execute(query);
    if (_conn->hasResult()) {
        JobInfo info;
        database::mysql::Row row;
        while (_conn->next(row)) {
            row.get("id", info.id);
            row.get("controller_id", info.controllerId);
            row.get("parent_job_id", info.parentJobId);
            row.get("type", info.type);
            row.get("state", info.state);
            row.get("ext_state", info.extendedState);
            row.get("begin_time", info.beginTime);
            row.get("end_time", info.endTime);
            row.get("heartbeat_time", info.heartbeatTime);
            row.get("priority", info.priority);
        }
        string const query = _g.select(Sql::STAR) + _g.from("job_ext") + _g.where(_g.eq("job_id", id));
        _conn->execute(query);
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
    throw DatabaseServicesNotFound("no Job found for id: " + id);
}

list<JobInfo> DatabaseServicesMySQL::jobs(string const& controllerId, string const& parentJobId,
                                          uint64_t fromTimeStamp, uint64_t toTimeStamp, size_t maxEntries) {
    string const context =
            "DatabaseServicesMySQL::" + string(__func__) + " " + " controllerId=" + controllerId +
            " parentJobId=" + parentJobId + " fromTimeStamp=" + to_string(fromTimeStamp) +
            " toTimeStamp=" + to_string(toTimeStamp) + " maxEntries=" + to_string(maxEntries) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (controllerId.empty()) throw invalid_argument(context + "controllerId can't be empty");

    list<JobInfo> collection;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _jobs(lock, controllerId, parentJobId, fromTimeStamp, toTimeStamp, maxEntries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

list<JobInfo> DatabaseServicesMySQL::_jobs(replica::Lock const& lock, string const& controllerId,
                                           string const& parentJobId, uint64_t fromTimeStamp,
                                           uint64_t toTimeStamp, size_t maxEntries) {
    list<JobInfo> collection;
    string const query =
            _g.select(Sql::STAR) + _g.from("job") +
            _g.where(_g.geq("begin_time", fromTimeStamp),
                     _g.leq("begin_time", toTimeStamp != 0 ? toTimeStamp : numeric_limits<uint64_t>::max()),
                     controllerId.empty() ? "" : _g.eq("controller_id", controllerId),
                     parentJobId.empty() ? "" : _g.eq("parent_job_id", parentJobId)) +
            _g.orderBy(make_pair("begin_time", "DESC")) + _g.limit(maxEntries);
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            JobInfo info;
            row.get("id", info.id);
            row.get("controller_id", info.controllerId);
            row.get("parent_job_id", info.parentJobId);
            row.get("type", info.type);
            row.get("state", info.state);
            row.get("ext_state", info.extendedState);
            row.get("begin_time", info.beginTime);
            row.get("end_time", info.endTime);
            row.get("heartbeat_time", info.heartbeatTime);
            row.get("priority", info.priority);
            collection.push_back(info);
        }
    }
    for (auto&& info : collection) {
        string const query = _g.select(Sql::STAR) + _g.from("job_ext") + _g.where(_g.eq("job_id", info.id));
        _conn->execute(query);
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

TransactionInfo DatabaseServicesMySQL::transaction(TransactionId id, bool includeContext, bool includeLog) {
    string const context = _context(__func__) + "id=" + to_string(id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    TransactionInfo info;
    replica::Lock lock(_mtx, context);
    try {
        string const predicate = _g.eq("id", id);
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

vector<TransactionInfo> DatabaseServicesMySQL::transactions(string const& databaseName, bool includeContext,
                                                            bool includeLog) {
    string const context = _context(__func__) + "databaseName=" + databaseName + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    _assertDatabaseIsValid(context, databaseName);
    replica::Lock lock(_mtx, context);
    string const predicate = databaseName.empty() ? "" : _g.eq("database", databaseName);
    return _transactions(lock, predicate, includeContext, includeLog);
}

vector<TransactionInfo> DatabaseServicesMySQL::transactions(TransactionInfo::State state, bool includeContext,
                                                            bool includeLog) {
    string const context = _context(__func__) + "state=" + TransactionInfo::state2string(state) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    replica::Lock lock(_mtx, context);
    string const predicate = _g.eq("state", TransactionInfo::state2string(state));
    return _transactions(lock, predicate, includeContext, includeLog);
}

TransactionInfo DatabaseServicesMySQL::createTransaction(string const& databaseName,
                                                         NamedMutexRegistry& namedMutexRegistry,
                                                         unique_ptr<replica::Lock>& namedMutexLock,
                                                         json const& transactionContext) {
    string const context = _context(__func__) + "database=" + databaseName + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, databaseName);

    if (!transactionContext.is_object())
        throw invalid_argument(context +
                               "a value of the parameter 'transactionContext is not a valid JSON object");

    uint64_t const beginTime = util::TimeUtils::now();
    uint64_t const startTime = 0;
    uint64_t const transitionTime = 0;
    uint64_t const endTime = 0;
    string const state = "IS_STARTING";
    TransactionInfo info;

    replica::Lock lock(_mtx, context);
    try {
        string const predicate = _g.eq("id", Sql::LAST_INSERT_ID);
        string const query = _g.insert("transaction", Sql::NULL_, databaseName, state, beginTime, startTime,
                                       endTime, transitionTime, transactionContext.dump());
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            conn->execute(query);
            bool const includeContext = true;
            bool const includeLog = true;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
            namedMutexLock.reset(
                    new replica::Lock(namedMutexRegistry.get("transaction:" + to_string(info.id))));
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

TransactionInfo DatabaseServicesMySQL::updateTransaction(TransactionId id, TransactionInfo::State newState) {
    string const context = _context(__func__) + "id=" + to_string(id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    // Figure out which timestamp needs to be updated along with the requested
    // state transition.
    string timeStampName;
    switch (newState) {
        case TransactionInfo::State::STARTED:
            timeStampName = "start_time";
            break;
        case TransactionInfo::State::IS_FINISHING:
        case TransactionInfo::State::IS_ABORTING:
            timeStampName = "transition_time";
            break;
        default:
            timeStampName = "end_time";
            break;
    }
    TransactionInfo info;

    replica::Lock lock(_mtx, context);
    try {
        auto const predicate = _g.eq("id", id);
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            bool includeContext = false;
            bool includeLog = false;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
            if (!TransactionInfo::stateTransitionIsAllowed(info.state, newState)) {
                throw invalid_argument(context + "state transition from " +
                                       TransactionInfo::state2string(newState) + "into " +
                                       TransactionInfo::state2string(newState) + " is not allowed.");
            }
            string const query =
                    _g.update("transaction", make_pair("state", TransactionInfo::state2string(newState)),
                              make_pair(timeStampName, util::TimeUtils::now())) +
                    _g.where(predicate);
            conn->execute(query);
            includeContext = true;
            includeLog = true;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

TransactionInfo DatabaseServicesMySQL::updateTransaction(TransactionId id, json const& transactionContext) {
    string const context = _context(__func__) + "id=" + to_string(id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    if (!transactionContext.is_object())
        throw invalid_argument(context +
                               "a value of the parameter 'transactionContext is not a valid JSON object");

    TransactionInfo info;
    replica::Lock lock(_mtx, context);
    try {
        string const predicate = _g.eq("id", id);
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            bool includeContext = false;
            bool includeLog = false;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
            string const query = _g.update("transaction", make_pair("context", transactionContext.dump())) +
                                 _g.where(predicate);
            conn->execute(query);
            includeContext = true;
            includeLog = true;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

TransactionInfo DatabaseServicesMySQL::updateTransaction(TransactionId id,
                                                         unordered_map<string, json> const& events) {
    string const context = _context(__func__) + "id=" + to_string(id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    TransactionInfo info;
    replica::Lock lock(_mtx, context);
    try {
        string const predicate = _g.eq("id", id);
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            bool includeContext = false;
            bool includeLog = false;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
            for (auto&& elem : events) {
                string const& eventName = elem.first;
                json const& eventData = elem.second;
                uint64_t const eventTime = util::TimeUtils::now();
                string const query = _g.insert("transaction_log", Sql::NULL_, info.id,
                                               TransactionInfo::state2string(info.state), eventTime,
                                               eventName, eventData.dump());
                conn->execute(query);
            }
            includeContext = true;
            includeLog = true;
            info = _findTransactionImpl(lock, predicate, includeContext, includeLog);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

vector<TransactionInfo> DatabaseServicesMySQL::_transactions(replica::Lock const& lock,
                                                             string const& predicate, bool includeContext,
                                                             bool includeLog) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    vector<TransactionInfo> collection;
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _findTransactionsImpl(lock, predicate, includeContext, includeLog);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

TransactionInfo DatabaseServicesMySQL::_findTransactionImpl(replica::Lock const& lock,
                                                            string const& predicate, bool includeContext,
                                                            bool includeLog) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    auto const collection = _findTransactionsImpl(lock, predicate, includeContext, includeLog);
    size_t const num = collection.size();
    if (num == 1) return collection[0];
    if (num == 0) throw DatabaseServicesNotFound(context + "no such transaction");
    throw DatabaseServicesError(context + "two many transactions found: " + to_string(num));
}

vector<TransactionInfo> DatabaseServicesMySQL::_findTransactionsImpl(replica::Lock const& lock,
                                                                     string const& predicate,
                                                                     bool includeContext, bool includeLog) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    vector<TransactionInfo> collection;
    string query;
    if (includeContext) {
        query = _g.select(Sql::STAR);
    } else {
        query = _g.select("id", "database", "state", "begin_time", "start_time", "transition_time",
                          "end_time");
    }
    query += _g.from("transaction") + _g.where(predicate) + _g.orderBy(make_pair("begin_time", "DESC"));

    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            TransactionInfo info;
            row.get("id", info.id);
            row.get("database", info.database);
            string stateStr;
            row.get("state", stateStr);
            info.state = TransactionInfo::string2state(stateStr);
            row.get("begin_time", info.beginTime);
            row.get("start_time", info.startTime);
            row.get("transition_time", info.transitionTime);
            row.get("end_time", info.endTime);
            if (includeContext) {
                string contextStr;
                row.get("context", contextStr);
                if (!contextStr.empty()) {
                    info.context = json::parse(contextStr);
                }
            }
            collection.push_back(info);
        }
    }
    if (includeLog) {
        for (auto& info : collection) {
            string const query = _g.select(Sql::STAR) + _g.from("transaction_log") +
                                 _g.where(_g.eq("transaction_id", info.id)) +
                                 _g.orderBy(make_pair("time", "ASC"));
            _conn->execute(query);
            if (_conn->hasResult()) {
                database::mysql::Row row;
                while (_conn->next(row)) {
                    TransactionInfo::Event event;
                    row.get("id", event.id);
                    string transactionStateStr;
                    row.get("transaction_state", transactionStateStr);
                    event.transactionState = TransactionInfo::string2state(transactionStateStr);
                    row.get("name", event.name);
                    row.get("time", event.time);
                    string data;
                    row.get("data", data);
                    if (!data.empty()) event.data = json::parse(data);
                    info.log.push_back(move(event));
                }
            }
        }
    }
    return collection;
}

TransactionContribInfo DatabaseServicesMySQL::transactionContrib(unsigned int id, bool includeWarnings,
                                                                 bool includeRetries) {
    string const context = _context(__func__) + "id=" + to_string(id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    vector<TransactionContribInfo> collection;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _transactionContribsImpl(lock, _g.eq("id", id), includeWarnings, includeRetries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    if (collection.size() == 0) {
        throw DatabaseServicesNotFound(context + "no such contribution");
    } else if (collection.size() != 1) {
        string const msg = context + "database schema problem - contribution identifiers aren't unique.";
        LOGS(_log, LOG_LVL_FATAL, msg);
        throw runtime_error(msg);
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection[0];
}

string DatabaseServicesMySQL::_typeSelectorPredicate(
        TransactionContribInfo::TypeSelector typeSelector) const {
    switch (typeSelector) {
        case TransactionContribInfo::TypeSelector::SYNC:
            return _g.eq("type", "SYNC");
        case TransactionContribInfo::TypeSelector::ASYNC:
            return _g.eq("type", "ASYNC");
        case TransactionContribInfo::TypeSelector::SYNC_OR_ASYNC:
            return string();
    }
    return string();
}

vector<TransactionContribInfo> DatabaseServicesMySQL::transactionContribs(
        TransactionId transactionId, string const& table, string const& workerName,
        TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings, bool includeRetries) {
    string const context = _context(__func__) + "transactionId=" + to_string(transactionId) +
                           " table=" + table + " worker=" + workerName + " " +
                           " typeSelector=" + TransactionContribInfo::typeSelector2str(typeSelector) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    replica::Lock lock(_mtx, context);

    string const predicate = _g.packConds(
            _g.eq("transaction_id", transactionId), table.empty() ? "" : _g.eq("table", table),
            workerName.empty() ? "" : _g.eq("worker", workerName), _typeSelectorPredicate(typeSelector));
    return _transactionContribs(lock, predicate, includeWarnings, includeRetries);
}

vector<TransactionContribInfo> DatabaseServicesMySQL::transactionContribs(
        TransactionId transactionId, TransactionContribInfo::Status status, string const& table,
        string const& workerName, TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings,
        bool includeRetries) {
    string const context = _context(__func__) + "transactionId=" + to_string(transactionId) +
                           " status=" + TransactionContribInfo::status2str(status) + " table=" + table +
                           " worker=" + workerName + " " +
                           " typeSelector=" + TransactionContribInfo::typeSelector2str(typeSelector) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    replica::Lock lock(_mtx, context);
    string const predicate = _g.packConds(_g.eq("transaction_id", transactionId),
                                          _g.eq("status", TransactionContribInfo::status2str(status)),
                                          table.empty() ? "" : _g.eq("table", table),
                                          workerName.empty() ? "" : _g.eq("worker", workerName),
                                          _typeSelectorPredicate(typeSelector));
    return _transactionContribs(lock, predicate, includeWarnings, includeRetries);
}

vector<TransactionContribInfo> DatabaseServicesMySQL::transactionContribs(
        string const& database, string const& table, string const& workerName,
        TransactionContribInfo::TypeSelector typeSelector, bool includeWarnings, bool includeRetries) {
    string const context = _context(__func__) + "database=" + database + " table=" + table +
                           " worker=" + workerName + " " +
                           " typeSelector=" + TransactionContribInfo::typeSelector2str(typeSelector) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    replica::Lock lock(_mtx, context);
    string const predicate = _g.packConds(
            _g.eq("database", database), table.empty() ? "" : _g.eq("table", table),
            workerName.empty() ? "" : _g.eq("worker", workerName), _typeSelectorPredicate(typeSelector));
    return _transactionContribs(lock, predicate, includeWarnings, includeRetries);
}

TransactionContribInfo DatabaseServicesMySQL::createdTransactionContrib(
        TransactionContribInfo const& info, bool failed, TransactionContribInfo::Status statusOnFailed) {
    string const context = _context(__func__) + "transactionId=" + to_string(info.transactionId) +
                           " table=" + info.table + " chunk=" + to_string(info.chunk) +
                           " isOverlap=" + bool2str(info.isOverlap) + " worker=" + info.worker + " " +
                           " url=" + info.url + " charsetName=" + info.charsetName +
                           " failed=" + bool2str(failed) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    uint64_t const numBytes = 0;
    uint64_t const numRows = 0;

    uint64_t const createTime = util::TimeUtils::now();
    uint64_t const startTime = 0;
    uint64_t const readTime = 0;
    uint64_t const loadTime = 0;

    unsigned int const numFailedRetries = 0;
    unsigned int const numWarnings = 0;
    uint64_t const numRowsLoaded = 0;

    TransactionContribInfo::Status const status =
            failed ? statusOnFailed : TransactionContribInfo::Status::IN_PROGRESS;

    TransactionContribInfo updatedInfo;

    replica::Lock lock(_mtx, context);
    try {
        vector<string> queries;
        queries.emplace_back(_g.insert(
                "transaction_contrib", Sql::NULL_, info.transactionId, info.worker, info.database, info.table,
                info.chunk, info.isOverlap ? 1 : 0, info.url, info.async ? "ASYNC" : "SYNC", info.maxRetries,
                numFailedRetries, numBytes, numRows, createTime, startTime, readTime, loadTime,
                TransactionContribInfo::status2str(status), info.tmpFile, numWarnings, numRowsLoaded,
                failed ? info.httpError : 0, failed ? info.systemError : 0, failed ? info.error : string(),
                (failed && info.retryAllowed) ? 1 : 0));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "max_num_warnings",
                                       info.maxNumWarnings));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "fields_terminated_by",
                                       info.dialectInput.fieldsTerminatedBy));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "fields_enclosed_by",
                                       info.dialectInput.fieldsEnclosedBy));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "fields_escaped_by",
                                       info.dialectInput.fieldsEscapedBy));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "lines_terminated_by",
                                       info.dialectInput.linesTerminatedBy));
        queries.emplace_back(_g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "http_method",
                                       http::method2string(info.httpMethod)));
        queries.emplace_back(
                _g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "http_data", info.httpData));
        for (string const& header : info.httpHeaders) {
            queries.emplace_back(
                    _g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "http_headers", header));
        }
        queries.emplace_back(
                _g.insert("transaction_contrib_ext", Sql::LAST_INSERT_ID, "charset_name", info.charsetName));
        string const predicate = _g.eq("id", Sql::LAST_INSERT_ID);
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            for (auto&& query : queries) conn->execute(query);
            updatedInfo = _transactionContribImpl(lock, predicate);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return updatedInfo;
}

TransactionContribInfo DatabaseServicesMySQL::updateTransactionContrib(TransactionContribInfo const& info) {
    string const context = _context(__func__) + "id=" + to_string(info.id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    string const predicate = _g.eq("id", info.id);
    // These flags should be set to retain the full state of the contribution descriptor
    // upter reloading it from the database.
    bool const includeWarnings = true;
    bool const includeRetries = true;
    TransactionContribInfo updatedInfo;
    replica::Lock lock(_mtx, context);
    try {
        list<string> queries;
        string const query =
                _g.update("transaction_contrib", make_pair("type", info.async ? "ASYNC" : "SYNC"),
                          make_pair("num_failed_retries", info.numFailedRetries),
                          make_pair("num_bytes", info.numBytes), make_pair("num_rows", info.numRows),
                          make_pair("start_time", info.startTime), make_pair("read_time", info.readTime),
                          make_pair("load_time", info.loadTime),
                          make_pair("status", TransactionContribInfo::status2str(info.status)),
                          make_pair("tmp_file", info.tmpFile), make_pair("num_warnings", info.numWarnings),
                          make_pair("num_rows_loaded", info.numRowsLoaded),
                          make_pair("http_error", info.httpError),
                          make_pair("system_error", info.systemError), make_pair("error", info.error),
                          make_pair("retry_allowed", info.retryAllowed)) +
                _g.where(predicate);
        queries.push_back(query);
        // Warnings are saved only for the (formally) successfully ingested contributions.
        // These contributions may still be analyzed for potential problems with
        // the input data. The key indicator here is the total number of warnings
        // recorded in the contribution descriptor's attrubute 'numWarnings'.
        if (info.status == TransactionContribInfo::Status::FINISHED) {
            // The position number plays two roles here. Firstly, it allows to preserve
            // the original order of the warnings exactly how they were reported by MySQL.
            // Secondly, the combined unique index (contrib_id,pos) would prevent the method
            // from inserting duplicates.
            size_t pos = 0;
            for (auto&& w : info.warnings) {
                string const query =
                        _g.insert("transaction_contrib_warn", info.id, pos++, w.level, w.code, w.message);
                queries.emplace_back(query);
            }
        }
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            for (auto&& query : queries) {
                conn->execute(query);
            }
            updatedInfo = _transactionContribImpl(lock, predicate, includeWarnings, includeRetries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return updatedInfo;
}

TransactionContribInfo DatabaseServicesMySQL::saveLastTransactionContribRetry(
        TransactionContribInfo const& info) {
    string const context = _context(__func__) + "id=" + to_string(info.id) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    if ((info.numFailedRetries == 0) || (info.numFailedRetries != info.failedRetries.size())) {
        throw invalid_argument(context + "inconsistent retries");
    }
    auto const& lastRetry = info.failedRetries.back();
    // These flags should be set to retain the full state of the contribution descriptor
    // upter reloading it from the database.
    bool const includeWarnings = true;
    bool const includeRetries = true;
    TransactionContribInfo updatedInfo;
    list<string> queries;
    replica::Lock lock(_mtx, context);
    string const predicate = _g.eq("id", info.id);
    string query = _g.update("transaction_contrib", make_pair("num_failed_retries", info.numFailedRetries)) +
                   _g.where(predicate);
    queries.push_back(query);
    query = _g.insert("transaction_contrib_retry", info.id, lastRetry.numBytes, lastRetry.numRows,
                      lastRetry.startTime, lastRetry.readTime, lastRetry.tmpFile, lastRetry.httpError,
                      lastRetry.systemError, lastRetry.error);
    queries.push_back(query);
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            for (auto&& query : queries) {
                conn->execute(query);
            }
            updatedInfo = _transactionContribImpl(lock, predicate, includeWarnings, includeRetries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return updatedInfo;
}

vector<TransactionContribInfo> DatabaseServicesMySQL::_transactionContribs(replica::Lock const& lock,
                                                                           string const& predicate,
                                                                           bool includeWarnings,
                                                                           bool includeRetries) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    vector<TransactionContribInfo> collection;
    try {
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            collection = _transactionContribsImpl(lock, predicate, includeWarnings, includeRetries);
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

TransactionContribInfo DatabaseServicesMySQL::_transactionContribImpl(replica::Lock const& lock,
                                                                      string const& predicate,
                                                                      bool includeWarnings,
                                                                      bool includeRetries) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    auto const collection = _transactionContribsImpl(lock, predicate, includeWarnings, includeRetries);
    size_t const num = collection.size();
    if (num == 1) return collection[0];
    if (num == 0) throw DatabaseServicesNotFound(context + "no such transaction contribution");
    throw DatabaseServicesError(context + "two many transaction contributions found: " + to_string(num));
}

vector<TransactionContribInfo> DatabaseServicesMySQL::_transactionContribsImpl(replica::Lock const& lock,
                                                                               string const& predicate,
                                                                               bool includeWarnings,
                                                                               bool includeRetries) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    vector<TransactionContribInfo> collection;
    string const query = _g.select(Sql::STAR) + _g.from("transaction_contrib") + _g.where(predicate);
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            TransactionContribInfo info;
            row.get("id", info.id);
            row.get("transaction_id", info.transactionId);
            row.get("worker", info.worker);
            row.get("database", info.database);
            row.get("table", info.table);
            row.get("chunk", info.chunk);
            row.get("is_overlap", info.isOverlap);
            row.get("url", info.url);
            string type;
            row.get("type", type);
            info.async = (type == "ASYNC");
            row.get("max_retries", info.maxRetries);
            row.get("num_failed_retries", info.numFailedRetries);
            row.get("num_bytes", info.numBytes);
            row.get("num_rows", info.numRows);
            row.get("create_time", info.createTime);
            row.get("start_time", info.startTime);
            row.get("read_time", info.readTime);
            row.get("load_time", info.loadTime);
            string str;
            row.get("status", str);
            info.status = TransactionContribInfo::str2status(str);
            row.get("tmp_file", info.tmpFile);
            row.get("num_warnings", info.numWarnings);
            row.get("num_rows_loaded", info.numRowsLoaded);
            row.get("http_error", info.httpError);
            row.get("system_error", info.systemError);
            row.get("error", info.error);
            row.get("retry_allowed", info.retryAllowed);
            collection.push_back(info);
        }
    }
    for (auto& contrib : collection) {
        string const query = _g.select("key", "val") + _g.from("transaction_contrib_ext") +
                             _g.where(_g.eq("contrib_id", contrib.id));
        _conn->execute(query);
        if (_conn->hasResult()) {
            database::mysql::Row row;
            while (_conn->next(row)) {
                string key;
                row.get("key", key);
                if (key.empty()) continue;
                string val;
                row.get("val", val);
                if (val.empty()) continue;
                if (key == "max_num_warnings")
                    contrib.maxNumWarnings = lsst::qserv::stoui(val);
                else if (key == "fields_terminated_by")
                    contrib.dialectInput.fieldsTerminatedBy = val;
                else if (key == "fields_enclosed_by")
                    contrib.dialectInput.fieldsEnclosedBy = val;
                else if (key == "fields_escaped_by")
                    contrib.dialectInput.fieldsEscapedBy = val;
                else if (key == "lines_terminated_by")
                    contrib.dialectInput.linesTerminatedBy = val;
                else if (key == "http_method")
                    contrib.httpMethod = http::string2method(val);
                else if (key == "http_data")
                    contrib.httpData = val;
                else if (key == "http_headers")
                    contrib.httpHeaders.emplace_back(val);
                else if (key == "charset_name")
                    contrib.charsetName = val;
                else {
                    throw DatabaseServicesError(context + "unexpected extended parameter '" + key +
                                                "' for contribution id=" + to_string(contrib.id));
                }
            }
        }
    }
    if (includeWarnings) {
        for (auto& contrib : collection) {
            string const query = _g.select("level", "code", "message") + _g.from("transaction_contrib_warn") +
                                 _g.where(_g.eq("contrib_id", contrib.id)) +
                                 _g.orderBy(make_pair("pos", "ASC"));
            _conn->execute(query);
            if (_conn->hasResult()) {
                database::mysql::Row row;
                while (_conn->next(row)) {
                    database::mysql::Warning w;
                    row.get("level", w.level);
                    row.get("code", w.code);
                    row.get("message", w.message);
                    contrib.warnings.push_back(w);
                }
            }
        }
    }
    if (includeRetries) {
        for (auto& contrib : collection) {
            if (contrib.numFailedRetries == 0) continue;
            // Sorting is needed to recover the original ordering of the retries.
            string const query = _g.select(Sql::STAR) + _g.from("transaction_contrib_retry") +
                                 _g.where(_g.eq("contrib_id", contrib.id)) +
                                 _g.orderBy(make_pair("start_time", "ASC"), make_pair("read_time", "ASC"));
            _conn->execute(query);
            if (_conn->hasResult()) {
                database::mysql::Row row;
                while (_conn->next(row)) {
                    TransactionContribInfo::FailedRetry r;
                    row.get("num_bytes", r.numBytes);
                    row.get("num_rows", r.numRows);
                    row.get("start_time", r.startTime);
                    row.get("read_time", r.readTime);
                    row.get("tmp_file", r.tmpFile);
                    row.get("http_error", r.httpError);
                    row.get("system_error", r.systemError);
                    row.get("error", r.error);
                    contrib.failedRetries.push_back(r);
                }
            }
        }
    }
    return collection;
}

DatabaseIngestParam DatabaseServicesMySQL::ingestParam(string const& database, string const& category,
                                                       string const& param) {
    string const context =
            _context(__func__) + "database=" + database + " category=" + category + " param=" + param;
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    if (category.empty()) throw invalid_argument(context + "category can't be empty");
    if (param.empty()) throw invalid_argument(param + "category can't be empty");

    DatabaseIngestParam info;
    replica::Lock lock(_mtx, context);
    try {
        string const predicate =
                _g.packConds(_g.eq("database", database), _g.eq("category", category), _g.eq("param", param));
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { info = _ingestParamImpl(lock, predicate); });
    } catch (DatabaseServicesNotFound const&) {
        // Nothing to report here because parameters are optional. Pass the exception
        // to the caller.
        throw;
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return info;
}

vector<DatabaseIngestParam> DatabaseServicesMySQL::ingestParams(string const& database,
                                                                string const& category) {
    string const context = _context(__func__) + "database=" + database + " category=" + category;
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    vector<DatabaseIngestParam> collection;
    replica::Lock lock(_mtx, context);
    try {
        string const predicate = _g.packConds(_g.eq("database", database),
                                              category.empty() ? "" : _g.eq("category", category));
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { collection = _ingestParamsImpl(lock, predicate); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return collection;
}

void DatabaseServicesMySQL::saveIngestParam(string const& database, string const& category,
                                            string const& param, string const& value) {
    string const context =
            _context(__func__) + "database=" + database + " category=" + category + " param=" + param;
    LOGS(_log, LOG_LVL_DEBUG, context);

    _assertDatabaseIsValid(context, database);

    if (category.empty()) throw invalid_argument(context + "category can't be empty");
    if (param.empty()) throw invalid_argument(context + "param can't be empty");

    replica::Lock lock(_mtx, context);
    try {
        auto const insert = [&](decltype(_conn) conn) {
            string const query = _g.insert("database_ingest", database, category, param, value);
            conn->execute(query);
        };
        auto const update = [&](decltype(_conn) conn) {
            string const query =
                    _g.update("database_ingest", make_pair("value", value)) +
                    _g.where(_g.eq("database", database), _g.eq("category", category), _g.eq("param", param));
            conn->execute(query);
        };
        _conn->executeInsertOrUpdate(insert, update);
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

void DatabaseServicesMySQL::_assertDatabaseFamilyIsValid(string const& context,
                                                         string const& familyName) const {
    if (familyName.empty()) throw invalid_argument(context + "database family name can't be empty");
    if (!_configuration->isKnownDatabaseFamily(familyName))
        throw invalid_argument(context + "unknown database fanily: " + familyName);
}

void DatabaseServicesMySQL::_assertDatabaseIsValid(string const& context, string const& databaseName) const {
    if (databaseName.empty()) throw invalid_argument(context + "database name can't be empty");
    if (!_configuration->isKnownDatabase(databaseName))
        throw invalid_argument(context + "unknown database: " + databaseName);
}

void DatabaseServicesMySQL::_assertWorkerIsValid(string const& context, string const& workerName) const {
    if (workerName.empty()) throw invalid_argument(context + "worker name can't be empty");
    if (!_configuration->isKnownWorker(workerName))
        throw invalid_argument(context + "unknown worker: " + workerName);
}

DatabaseIngestParam DatabaseServicesMySQL::_ingestParamImpl(replica::Lock const& lock,
                                                            string const& predicate) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    auto const collection = _ingestParamsImpl(lock, predicate);
    size_t const num = collection.size();
    if (num == 1) return collection[0];
    if (num == 0) throw DatabaseServicesNotFound(context + "no such parameter");
    throw DatabaseServicesError(context + "two many parameters found: " + to_string(num));
}

vector<DatabaseIngestParam> DatabaseServicesMySQL::_ingestParamsImpl(replica::Lock const& lock,
                                                                     string const& predicate) {
    string const context = _context(__func__) + "predicate=" + predicate + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    vector<DatabaseIngestParam> collection;
    string const query = _g.select(Sql::STAR) + _g.from("database_ingest") + _g.where(predicate);
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            DatabaseIngestParam info;
            row.get("database", info.database);
            row.get("category", info.category);
            row.get("param", info.param);
            row.get("value", info.value);
            collection.push_back(info);
        }
    }
    return collection;
}

void DatabaseServicesMySQL::_findReplicasImpl(replica::Lock const& lock, vector<ReplicaInfo>& replicas,
                                              string const& query, bool includeFileInfo) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + "(replicas,query) ";
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
            uint64_t id;
            string workerName;
            string database;
            unsigned int chunk;
            uint64_t verifyTime;
            row.get("id", id);
            row.get("worker", workerName);
            row.get("database", database);
            row.get("chunk", chunk);
            row.get("verify_time", verifyTime);
            id2replica[id] =
                    ReplicaInfo(ReplicaInfo::Status::COMPLETE, workerName, database, chunk, verifyTime);
        }

        // Extract files for each replica using identifiers of the replicas
        // update replicas in the dictionary.
        if (includeFileInfo) _findReplicaFilesImpl(lock, id2replica);

        // Copy replicas from the dictionary into the output collection
        for (auto&& entry : id2replica) {
            replicas.push_back(entry.second);
        }
    }
}

void DatabaseServicesMySQL::_findReplicaFilesImpl(replica::Lock const& lock,
                                                  map<uint64_t, ReplicaInfo>& id2replica) {
    string const context = "DatabaseServicesMySQL::" + string(__func__) + " ";

    if (0 == id2replica.size()) return;

    // The collection of replica identifiers will be split into batches to ensure
    // that a length of the query string (for pulling files for each batch) would
    // not exceed the corresponding MySQL limit.
    vector<uint64_t> ids;
    for (auto&& entry : id2replica) {
        ids.push_back(entry.first);
    }

    // Reserving 1024 for the rest of the query. Also assuming the worst case
    // scenario of the highest values of identifiers. Adding one extra byte for
    // a separator.
    //
    // TODO: come up with a more reliable algorithm which will avoid using
    // the fixed correction (of 1024 bytes).
    size_t const batchSize = (_conn->max_allowed_packet() - 1024) /
                             (1 + to_string(numeric_limits<unsigned long long>::max()).size());

    if ((_conn->max_allowed_packet() < 1024) or (0 == batchSize)) {
        throw runtime_error(context +
                            "value of 'max_allowed_packet' set for the MySQL session is too small: " +
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
    auto itr = ids.begin();  // points to the first replica identifier of a batch
    for (size_t size : batches) {
        string const query = _g.select(Sql::STAR) + _g.from("replica_file") +
                             _g.where(_g.in("replica_id", vector<uint64_t>(itr, itr + size))) +
                             _g.orderBy(make_pair("replica_id", ""));
        _conn->execute(query);
        if (_conn->hasResult()) {
            uint64_t currentReplicaId = 0;
            ReplicaInfo::FileInfoCollection files;  // for accumulating files of the current
                                                    // replica
            // Extend a replica in place
            auto extendReplicaThenClearFiles = [&id2replica, &files](uint64_t replicaId) {
                id2replica.at(replicaId).setFileInfo(files);
                files.clear();
            };

            database::mysql::Row row;
            while (_conn->next(row)) {
                // Extract attributes of the file
                uint64_t replicaId;
                string name;
                uint64_t size;
                time_t mtime;
                string cs;
                uint64_t beginCreateTime;
                uint64_t endCreateTime;
                row.get("replica_id", replicaId);
                row.get("name", name);
                row.get("size", size);
                row.get("mtime", mtime);
                row.get("cs", cs);
                row.get("begin_create_time", beginCreateTime);
                row.get("end_create_time", endCreateTime);

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
                        ReplicaInfo::FileInfo{name, size, mtime, cs, beginCreateTime, endCreateTime, size});
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

void DatabaseServicesMySQL::_findChunksImpl(replica::Lock const& lock, vector<unsigned int>& chunks,
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

TableRowStats DatabaseServicesMySQL::tableRowStats(string const& database, string const& table,
                                                   TransactionId transactionId) {
    auto const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    TableRowStats stats;
    replica::Lock lock(_mtx, context);
    try {
        _conn->executeInOwnTransaction(
                [&](decltype(_conn) conn) { stats = _tableRowStats(lock, database, table, transactionId); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
    return stats;
}

void DatabaseServicesMySQL::saveTableRowStats(TableRowStats const& stats) {
    auto const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);
    replica::Lock lock(_mtx, context);
    try {
        string const columns = _g.packIds("database", "table", "transaction_id", "chunk", "is_overlap",
                                          "num_rows", "update_time");
        _conn->executeInOwnTransaction([&](decltype(_conn) conn) {
            for (auto&& e : stats.entries) {
                string values2insert = _g.packVals(stats.database, stats.table);
                if (e.transactionId == 0) {
                    _g.packVal(values2insert, Sql::NULL_);
                } else {
                    _g.packVal(values2insert, e.transactionId);
                }
                _g.packVal(values2insert, e.chunk, e.isOverlap, e.numRows, e.updateTime);
                string const values2update = _g.packPairs(make_pair("num_rows", e.numRows),
                                                          make_pair("update_time", e.updateTime));
                string const query =
                        _g.insertPacked("stats_table_rows", columns, values2insert, values2update);
                conn->execute(query);
            }
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

TableRowStats DatabaseServicesMySQL::_tableRowStats(replica::Lock const& lock, string const& database,
                                                    string const& table, TransactionId transactionId) {
    auto const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    TableRowStats stats(database, table);
    string const query = _g.select(Sql::STAR) + _g.from("stats_table_rows") +
                         _g.where(_g.eq("database", database), _g.eq("table", table),
                                  transactionId == 0 ? "" : _g.eq("transaction_id", transactionId));
    _conn->execute(query);
    if (_conn->hasResult()) {
        database::mysql::Row row;
        while (_conn->next(row)) {
            TransactionId rowTransactionId;
            unsigned int chunk;
            bool isOverlap;
            size_t numRows;
            uint64_t updateTime;
            row.get("transaction_id", rowTransactionId);
            row.get("chunk", chunk);
            row.get("is_overlap", isOverlap);
            row.get("num_rows", numRows);
            row.get("update_time", updateTime);
            stats.entries.emplace_back(
                    TableRowStatsEntry(rowTransactionId, chunk, isOverlap, numRows, updateTime));
        }
    }
    return stats;
}

void DatabaseServicesMySQL::deleteTableRowStats(string const& databaseName, string const& tableName,
                                                ChunkOverlapSelector overlapSelector) {
    auto const context = "DatabaseServicesMySQL::" + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context);

    replica::Lock lock(_mtx, context);
    try {
        TableInfo const table = _configuration->databaseInfo(databaseName).findTable(tableName);
        string predicate = _g.packConds(_g.eq("database", table.database), _g.eq("table", table.name));
        if (table.isPartitioned && overlapSelector != ChunkOverlapSelector::CHUNK_AND_OVERLAP) {
            bool const isOverlap = overlapSelector == ChunkOverlapSelector::OVERLAP;
            _g.packCond(predicate, _g.eq("is_overlap", isOverlap));
        }
        string const query = _g.delete_("stats_table_rows") + _g.where(predicate);
        _conn->executeInOwnTransaction([&query](decltype(_conn) conn) { conn->execute(query); });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context << "failed, exception: " << ex.what());
        throw;
    }
    LOGS(_log, LOG_LVL_DEBUG, context + "** DONE **");
}

string DatabaseServicesMySQL::_context(string const& func) { return "DatabaseServicesMySQL::" + func + " "; }

}  // namespace lsst::qserv::replica
