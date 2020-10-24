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
#include "replica/IngestFileSvc.h"

// System headers
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <functional>
#include <thread>
#include <stdexcept>

// Third party headers
#include "boost/filesystem.hpp"

// Qserv headers
#include "global/constants.h"
#include "replica/ChunkedTable.h"
#include "replica/DatabaseMySQL.h"
#include "replica/DatabaseServices.h"
#include "replica/FileUtils.h"
#include "replica/ReplicaInfo.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;
namespace fs = boost::filesystem;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.IngestFileSvc");

/// The context for diagnostic & debug printouts
string const context = "INGEST-FILE-SVC ";

}   // namespace

namespace lsst {
namespace qserv {
namespace replica {

IngestFileSvc::IngestFileSvc(ServiceProvider::Ptr const& serviceProvider,
                             string const& workerName)
    :   _serviceProvider(serviceProvider),
        _workerName(workerName),
        _workerInfo(serviceProvider->config()->workerInfo(workerName)) {
}


IngestFileSvc::~IngestFileSvc() {
    closeFile();
}


void IngestFileSvc::openFile(TransactionId transactionId,
                             string const& table,
                             char columnSeparator,
                             unsigned int chunk,
                             bool isOverlap) {

    string const context_ = context + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_);

    _transactionId   = transactionId;
    _table           = table;
    _columnSeparator = columnSeparator;
    _chunk           = chunk;
    _isOverlap       = isOverlap;

    // Check if a context of the request is valid
    try {
        auto transactionInfo = _serviceProvider->databaseServices()->transaction(_transactionId);
        if (transactionInfo.state != TransactionInfo::STARTED) {
            throw logic_error(context_ + "transaction " + to_string(_transactionId) + " is not active");
        }
        _databaseInfo = _serviceProvider->config()->databaseInfo(transactionInfo.database);
        if (_databaseInfo.isPublished) {
            throw invalid_argument(context_ + "database '" + _databaseInfo.name + "' is already PUBLISHED");
        }
        _isPartitioned = _databaseInfo.partitionedTables.end() != find(
                _databaseInfo.partitionedTables.begin(),
                _databaseInfo.partitionedTables.end(),
                _table);
        if (not _isPartitioned) {
            if (_databaseInfo.regularTables.end() == find(
                    _databaseInfo.regularTables.begin(),
                    _databaseInfo.regularTables.end(),
                    _table)) {
                throw invalid_argument(
                        context_ + "no such table '" + _table + "' in a scope of database '" +
                        _databaseInfo.name + "'");
            }
        }
    } catch (DatabaseServicesNotFound const& ex) {
        throw invalid_argument(context_ + "invalid transaction identifier: " + to_string(_transactionId));
    }

    // The next test is for the partitioned tables only, and it's meant to check if
    // the chunk number is valid and it's allocated to this worker. The test will
    // also ensure that the database is in the UNPUBLISHED state.
    if (_isPartitioned) {

        vector<ReplicaInfo> replicas;       // Chunk replicas at the current worker found
                                            // among the unpublished databases only
        bool const allDatabases = false;
        bool const isPublished = false;

        _serviceProvider->databaseServices()->findWorkerReplicas(
            replicas,
            _chunk,
            _workerName,
            _databaseInfo.family,
            allDatabases,
            isPublished
        );
        if (replicas.cend() == find_if(replicas.cbegin(), replicas.cend(),
                [&](ReplicaInfo const& replica) {
                    return replica.database() == _databaseInfo.name;
                })) {
            throw invalid_argument(
                    context_ + "chunk " + to_string(_chunk) + " of the UNPUBLISHED database '" +
                    _databaseInfo.name + "' is not allocated to worker '" + _workerName + "'");
        }
    }
    try {
        _fileName = FileUtils::createTemporaryFile(
            _workerInfo.loaderTmpDir,
            _databaseInfo.name + "-" + _table + "-" + to_string(_chunk) + "-" + to_string(_transactionId),
            "-%%%%-%%%%-%%%%-%%%%",
            ".csv"
        );
    } catch (exception const& ex) {
        throw runtime_error(
                context_ + "failed to generate a unique name for a temporary file, ex: " + string(ex.what()));
    }
    _file.open(_fileName, ofstream::out);
    if (not _file.is_open()) {
        throw runtime_error(context_ + "failed to create a temporary file: " + _fileName);
    }
}


void IngestFileSvc::writeRowIntoFile(string const& row) {
    _file << _transactionId << _columnSeparator << row << "\n";
    ++_totalNumRows;
}


void IngestFileSvc::loadDataIntoTable() {

    string const context_ = context + string(__func__) + " ";
    LOGS(_log, LOG_LVL_DEBUG, context_ << "_totalNumRows: " << _totalNumRows);

    // Make sure no unsaved rows were staying in memory before proceeding
    // to the loading phase.
    _file.flush();

    // ATTENTION: the data loading method used in this implementation requires
    // that the MySQL server has (at least) the read-only access to files in
    // a folder in which the CSV file will be stored by this server. So, make
    // proper adjustments to a configuration of the Replication system.

    try {
        // The RAII connection handler automatically aborts the active transaction
        // should an exception be thrown within the block.
        database::mysql::ConnectionHandler h(
            database::mysql::Connection::open(
                database::mysql::ConnectionParams(
                    _workerInfo.dbHost,
                    _workerInfo.dbPort,
                    _workerInfo.dbUser,
                    _serviceProvider->config()->qservWorkerDatabasePassword(),
                    ""
                )
            )
        );

        string const sqlDatabase = h.conn->sqlId(_databaseInfo.name);
        string const sqlPartition = h.conn->sqlPartitionId(_transactionId);

        vector<string> tableMgtStatements;

        // Make sure no outstanding table locks exist from prior operations
        // on persistent database connections.
        tableMgtStatements.push_back("UNLOCK TABLES");

        string dataLoadStatement;

        if (_isPartitioned) {
            
            // Note, that the algorithm will create chunked tables for _ALL_ partitioned
            // tables (not just for the current one) to ensure they have representations
            // in all chunks touched by the ingest workflows. Missing representations would
            // cause Qserv to fail when processing queries involving these tables.

            for (auto&& table: _databaseInfo.partitionedTables) {

                // Chunked tables are created from the prototype table which is expected
                // to exist in the database before attempting data loading.

                bool const overlap = true;
                string const sqlProtoTable       = sqlDatabase + "." + h.conn->sqlId(table);
                string const sqlTable            = sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, _chunk, not overlap).name());
                string const sqlFullOverlapTable = sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, _chunk, overlap).name());

                string const tablesToBeCreated[] = {
                    sqlTable,
                    sqlFullOverlapTable,
                    sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, lsst::qserv::DUMMY_CHUNK, not overlap).name()),
                    sqlDatabase + "." + h.conn->sqlId(ChunkedTable(table, lsst::qserv::DUMMY_CHUNK, overlap).name())
                };
                for (auto&& table: tablesToBeCreated) {
                    tableMgtStatements.push_back(
                        "CREATE TABLE IF NOT EXISTS " + table + " LIKE " + sqlProtoTable
                    );
                    tableMgtStatements.push_back(
                        "ALTER TABLE " + table + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                            " VALUES IN (" + to_string(_transactionId) + "))"
                    );
                }

                // An additional step for the current request's table
                if (table == _table) {
                    dataLoadStatement =
                        "LOAD DATA INFILE " + h.conn->sqlValue(_fileName) +
                            " INTO TABLE " + (_isOverlap ? sqlFullOverlapTable : sqlTable) +
                            " FIELDS TERMINATED BY " + h.conn->sqlValue(string() + _columnSeparator);
                }
            }
        } else {

            // Regular tables are expected to exist in the database before
            // attempting data loading.

            string const sqlTable = sqlDatabase + "." + h.conn->sqlId(_table);

            tableMgtStatements.push_back(
                "ALTER TABLE " + sqlTable + " ADD PARTITION IF NOT EXISTS (PARTITION " + sqlPartition +
                    " VALUES IN (" + to_string(_transactionId) + "))"
            );
            dataLoadStatement =
                "LOAD DATA INFILE " + h.conn->sqlValue(_fileName) +
                    " INTO TABLE " + sqlTable +
                    " FIELDS TERMINATED BY " + h.conn->sqlValue(string() + _columnSeparator);
        }
        for (auto&& statement: tableMgtStatements) {
            LOGS(_log, LOG_LVL_DEBUG, context_ << "statement: " << statement);
        }
        LOGS(_log, LOG_LVL_DEBUG, context_ << "statement: " << dataLoadStatement);

        // Allow retries for the table management statements in case of deadlocks.
        // Deadlocks may happen when two or many threads are attempting to create
        // or modify partitioned tables, or at a presence of other threads loading
        // data into these tables.
        //
        // TODO: the experimental limit for the maximum number of retries may need
        //       to be made unlimited, or be limited by some configurable timeout.
        int const maxRetries = 1;
        int numRetries = 0;
        while (true) {
            try {
                h.conn->execute([&tableMgtStatements](decltype(h.conn) const& conn_) {
                    conn_->begin();
                    for (auto&& statement: tableMgtStatements) {
                        conn_->execute(statement);
                    }
                    conn_->commit();
                });
                break;
            } catch (database::mysql::LockDeadlock const& ex) {
                if (h.conn->inTransaction()) h.conn->rollback();
                if (numRetries < maxRetries) {
                    LOGS(_log, LOG_LVL_WARN, context_ << "exception: " << ex.what());
                    ++numRetries;
                } else {
                    LOGS(_log, LOG_LVL_ERROR, context_ << "maximum number of retries "
                        << maxRetries << " for avoiding table management deadlocks has been reached."
                        << " Aborting the file loading operation.");
                    throw;
                }
            }
        }

        // Load table contribution
        if (dataLoadStatement.empty()) {
            throw runtime_error(context_ + "no data loading statement generated");
        }
        h.conn->execute([&dataLoadStatement](decltype(h.conn) const& conn_) {
            conn_->begin();
            conn_->execute(dataLoadStatement);
            conn_->commit();
        });

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
            LOGS(_log, LOG_LVL_WARN, context_ << "file removal failed: " << ec.message());
        }
    }
}

}}} // namespace lsst::qserv::replica