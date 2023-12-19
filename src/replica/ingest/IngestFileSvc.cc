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
#include "replica/ingest/IngestFileSvc.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <thread>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "http/Exceptions.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLTypes.h"
#include "replica/services/DatabaseServices.h"
#include "replica/util/ChunkedTable.h"
#include "replica/util/FileUtils.h"
#include "replica/util/Mutex.h"
#include "replica/util/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestFileSvc");

/// The context for diagnostic & debug printouts
string const context = "INGEST-FILE-SVC ";
}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

IngestFileSvc::IngestFileSvc(ServiceProvider::Ptr const& serviceProvider, string const& workerName)
        : _serviceProvider(serviceProvider), _workerName(workerName) {}

IngestFileSvc::~IngestFileSvc() { closeFile(); }

string const& IngestFileSvc::openFile(TransactionId transactionId, string const& tableName,
                                      csv::Dialect const& dialect, std::string const& charsetName,
                                      unsigned int chunk, bool isOverlap) {
    string const context_ = context + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    _transactionId = transactionId;
    if (charsetName.empty()) {
        _charsetName = _serviceProvider->config()->get<string>("worker", "ingest-charset-name");
    } else {
        _charsetName = charsetName;
    }
    _dialect = dialect;
    _chunk = chunk;
    _isOverlap = isOverlap;

    // Construct and cache the transaction identifier field to be prepended at
    // the beginning of each row. Note that the prefix will be the same for each
    // row of the file.
    stringstream ss;
    if (dialect.fieldsEnclosedBy() == '\0') {
        ss << transactionId;
    } else {
        ss << dialect.fieldsEnclosedBy() << transactionId << dialect.fieldsEnclosedBy();
    }
    ss << dialect.fieldsTerminatedBy();
    _transactionIdField = ss.str();

    // Check if a context of the request is valid
    try {
        auto transaction = _serviceProvider->databaseServices()->transaction(_transactionId);
        if (transaction.state != TransactionInfo::State::STARTED) {
            throw logic_error(context_ + "transaction " + to_string(_transactionId) + " is not active");
        }
        _database = _serviceProvider->config()->databaseInfo(transaction.database);
        if (_database.isPublished) {
            throw logic_error(context_ + "database '" + _database.name + "' is already PUBLISHED");
        }
        _table = _database.findTable(tableName);
    } catch (DatabaseServicesNotFound const& ex) {
        throw invalid_argument(context_ + "invalid transaction identifier: " + to_string(_transactionId));
    }

    // The next test is for the partitioned tables only, and it's meant to check if
    // the chunk number is valid and it's allocated to this worker. The test will
    // also ensure that the database is in the UNPUBLISHED state.
    if (_table.isPartitioned) {
        vector<ReplicaInfo> replicas;  // Chunk replicas at the current worker found
                                       // among the unpublished databases only
        bool const allDatabases = false;
        bool const isPublished = false;

        _serviceProvider->databaseServices()->findWorkerReplicas(replicas, _chunk, _workerName,
                                                                 _database.family, allDatabases, isPublished);
        if (replicas.cend() == find_if(replicas.cbegin(), replicas.cend(), [&](ReplicaInfo const& replica) {
                return replica.database() == _database.name;
            })) {
            throw invalid_argument(context_ + "chunk " + to_string(_chunk) +
                                   " of the UNPUBLISHED database '" + _database.name +
                                   "' is not allocated to worker '" + _workerName + "'");
        }
    }
    try {
        _fileName = FileUtils::createTemporaryFile(
                _serviceProvider->config()->get<string>("worker", "loader-tmp-dir"),
                _database.name + "-" + _table.name + "-" + to_string(_chunk) + "-" +
                        to_string(_transactionId),
                "-%%%%-%%%%-%%%%-%%%%", ".csv");
    } catch (exception const& ex) {
        lsst::qserv::http::raiseRetryAllowedError(
                context_, "failed to generate a unique name for a temporary file, ex: " + string(ex.what()));
    }
    _file.open(_fileName, ios::out | ios::trunc | ios::binary);
    if (not _file.is_open()) {
        lsst::qserv::http::raiseRetryAllowedError(
                context_, "failed to create a temporary file '" + _fileName + "', error: '" +
                                  strerror(errno) + "', errno: " + to_string(errno));
    }
    return _fileName;
}

void IngestFileSvc::writeRowIntoFile(char const* buf, size_t size) {
    if (!_file.write(_transactionIdField.data(), _transactionIdField.size()) || !_file.write(buf, size)) {
        string const context_ = context + string(__func__) + " ";
        lsst::qserv::http::raiseRetryAllowedError(
                context_, "failed to write into the temporary file '" + _fileName + "', error: '" +
                                  strerror(errno) + "', errno: " + to_string(errno) + ".");
    }
    ++_totalNumRows;
}

void IngestFileSvc::loadDataIntoTable(unsigned int maxNumWarnings) {
    string const context_ = context + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_ << "_totalNumRows: " << _totalNumRows);

    // Make sure no unsaved rows were staying in memory before proceeding
    // to the loading phase.
    _file.flush();

    // Make sure no change in the state of the current transaction happened
    // while the input file was being prepared for the ingest.
    auto const transactionInfo = _serviceProvider->databaseServices()->transaction(_transactionId);
    if (transactionInfo.state != TransactionInfo::State::STARTED) {
        throw logic_error(context_ + "transaction " + to_string(_transactionId) + " changed state to " +
                          TransactionInfo::state2string(transactionInfo.state) +
                          " while the input file was being prepared for the ingest.");
    }

    // ATTENTION: the data loading method used in this implementation requires
    // that the MySQL server has (at least) the read-only access to files in
    // a folder in which the CSV file will be stored by this server. So, make
    // proper adjustments to a configuration of the Replication system.

    try {
        // The RAII connection handler automatically aborts the active transaction
        // should an exception be thrown within the block.
        ConnectionHandler h(Connection::open(Configuration::qservWorkerDbParams(_database.name)));
        QueryGenerator const g(h.conn);
        vector<Query> tableMgtStatements;

        // Make sure no outstanding table locks exist from prior operations
        // on persistent database connections.
        tableMgtStatements.push_back(Query("UNLOCK TABLES"));

        string dataLoadQuery;

        // The query to be executed after ingesting data into the table if the current
        // (super-)transaction gets aborted during the ingest.
        // The query will remove the corresponding MySQL partition.
        Query partitionRemovalQuery;

        if (_table.isPartitioned) {
            // Note, that the algorithm will create chunked tables for _ALL_ partitioned
            // tables (not just for the current one) to ensure they have representations
            // in all chunks touched by the ingest workflows. Missing representations would
            // cause Qserv to fail when processing queries involving these tables.
            for (auto&& tableName : _database.partitionedTables()) {
                auto const table = _database.findTable(tableName);
                // Chunked tables are created from the prototype table which is expected
                // to exist in the database before attempting data loading.
                // Note that this algorithm won't create MySQL partitions in the DUMMY chunk
                // tables since these tables are not supposed to store any data.
                bool const overlap = true;
                SqlId const sqlProtoTable = g.id(_database.name, table.name);
                SqlId const sqlTable =
                        g.id(_database.name, ChunkedTable(table.name, _chunk, !overlap).name());
                SqlId const sqlFullOverlapTable =
                        g.id(_database.name, ChunkedTable(table.name, _chunk, overlap).name());
                SqlId const tablesToBeCreated[] = {
                        sqlTable, sqlFullOverlapTable,
                        g.id(_database.name,
                             ChunkedTable(table.name, lsst::qserv::DUMMY_CHUNK, !overlap).name()),
                        g.id(_database.name,
                             ChunkedTable(table.name, lsst::qserv::DUMMY_CHUNK, overlap).name())};
                for (auto&& sqlTable : tablesToBeCreated) {
                    bool const ifNotExists = true;
                    string const query = g.createTableLike(sqlTable, sqlProtoTable, ifNotExists);
                    tableMgtStatements.push_back(Query(query, sqlTable.str));
                }
                // Skip this operation for tables that have already been been published. Note that published
                // tables do not have MySQL partitions. Any attempts to add a partition to those tables will
                // result in the MySQL failures.
                if (!table.isPublished) {
                    SqlId const tablesToBePartitioned[] = {sqlTable, sqlFullOverlapTable};
                    for (auto&& sqlTable : tablesToBePartitioned) {
                        bool const ifNotExists = true;
                        string const query =
                                g.alterTable(sqlTable) + g.addPartition(_transactionId, ifNotExists);
                        tableMgtStatements.push_back(Query(query, sqlTable.str));
                    }
                }
                // An additional step for the current request's table
                if (table.name == _table.name) {
                    SqlId const sqlDestinationTable = _isOverlap ? sqlFullOverlapTable : sqlTable;
                    bool const local = false;
                    dataLoadQuery =
                            g.loadDataInfile(_fileName, sqlDestinationTable, _charsetName, local, _dialect);
                    bool const ifExists = true;
                    partitionRemovalQuery = Query(
                            g.alterTable(sqlDestinationTable) + g.dropPartition(_transactionId, ifExists),
                            sqlDestinationTable.str);
                }
            }
        } else {
            // Regular tables are expected to exist in the database before
            // attempting data loading.
            SqlId const sqlTable = g.id(_database.name, _table.name);
            bool const ifNotExists = true;
            tableMgtStatements.push_back(Query(
                    g.alterTable(sqlTable) + g.addPartition(_transactionId, ifNotExists), sqlTable.str));
            bool const local = false;
            dataLoadQuery = g.loadDataInfile(_fileName, sqlTable, _charsetName, local, _dialect);
            bool const ifExists = true;
            partitionRemovalQuery =
                    Query(g.alterTable(sqlTable) + g.dropPartition(_transactionId, ifExists), sqlTable.str);
        }
        for (auto&& statement : tableMgtStatements) {
            LOGS(_log, LOG_LVL_DEBUG, context_ << "query: " << statement.query);
        }
        LOGS(_log, LOG_LVL_DEBUG, context_ << "query: " << dataLoadQuery);

        unsigned int maxReconnects = 0;  // pull the default value from the Configuration
        unsigned int timeoutSec = 0;     // pull the default value from the Configuration

        // Allow retries for the table management statements in case of deadlocks.
        // Deadlocks may happen when two or many threads are attempting to create
        // or modify partitioned tables, or at a presence of other threads loading
        // data into these tables.
        //
        // TODO: the experimental limit for the maximum number of retries may need
        //       to be made unlimited, or be limited by some configurable timeout.
        unsigned int maxRetriesOnDeadLock = 1;

        h.conn->executeInOwnTransaction(
                [&](decltype(h.conn) const& conn_) {
                    for (auto&& statement : tableMgtStatements) {
                        if (statement.mutexName.empty()) {
                            conn_->execute(statement.query);
                        } else {
                            replica::Lock const lock(_serviceProvider->getNamedMutex(statement.mutexName),
                                                     context_);
                            conn_->execute(statement.query);
                        }
                    }
                },
                maxReconnects, timeoutSec, maxRetriesOnDeadLock);

        // Load table contribution
        if (dataLoadQuery.empty()) {
            throw runtime_error(context_ + "no data loading query generated");
        }
        unsigned int effectiveMaxNumWarnings = maxNumWarnings;
        if (effectiveMaxNumWarnings == 0) {
            effectiveMaxNumWarnings =
                    _serviceProvider->config()->get<unsigned int>("worker", "loader-max-warnings");
        }
        string const setErrorCountQuery =
                g.setVars(SqlVarScope::SESSION, make_pair("max_error_count", effectiveMaxNumWarnings));
        h.conn->executeInOwnTransaction([&](decltype(h.conn) const& conn_) {
            conn_->execute(setErrorCountQuery);
            conn_->execute(dataLoadQuery);
            // ATTENTION: it's important to obtain the number of loaded rows before
            // checking for the warnings. Otherwise, if the collection of warnings won't
            // be found empty then MariaDB will reset the counter of the loaded rows to -1.
            _numRowsLoaded = conn_->affectedRows();
            _numWarnings = conn_->warningCount();
            if (_numWarnings != 0) {
                _warnings = conn_->warnings(effectiveMaxNumWarnings);
            }
        });

        // Make the final check to ensure the current transaction wasn't aborted
        // while the input file was being ingested into the table. If it was
        // then make the best attempt to remove the partition.
        auto const transactionInfo = _serviceProvider->databaseServices()->transaction(_transactionId);
        if (transactionInfo.state == TransactionInfo::State::ABORTED) {
            LOGS(_log, LOG_LVL_WARN,
                 context_ << "transaction " << _transactionId
                          << " was aborted during ingest. Removing the MySQL partition, query: "
                          << partitionRemovalQuery.query);
            try {
                h.conn->executeInOwnTransaction(
                        [&](decltype(h.conn) const& conn_) {
                            replica::Lock const lock(
                                    _serviceProvider->getNamedMutex(partitionRemovalQuery.mutexName),
                                    context_);
                            conn_->execute(partitionRemovalQuery.query);
                        },
                        maxReconnects, timeoutSec, maxRetriesOnDeadLock);
            } catch (exception const& ex) {
                // Just report the error and take no further actions.
                LOGS(_log, LOG_LVL_ERROR,
                     context_ << "partition removal query failed: " << partitionRemovalQuery.query
                              << ", exception: " << ex.what());
            }
            throw logic_error(context_ + "transaction " + to_string(_transactionId) +
                              " got aborted while the file was being ingested into the table.");
        }

    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_ERROR, context_ << "exception: " << ex.what());
        throw;
    }
}

void IngestFileSvc::closeFile() {
    string const context_ = context + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);
    if (_file.is_open()) {
        _file.close();
        boost::system::error_code ec;
        fs::remove(_fileName, ec);
        if (ec.value() != 0) {
            LOGS(_log, LOG_LVL_WARN,
                 context_ << "file removal failed, error: '" << ec.message()
                          << "', ec: " + to_string(ec.value()));
        }
    }
}

}  // namespace lsst::qserv::replica
