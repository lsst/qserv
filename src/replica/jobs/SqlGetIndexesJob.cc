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
#include "replica/jobs/SqlGetIndexesJob.h"

// Qserv headers
#include "replica/mysql/DatabaseMySQLRow.h"
#include "replica/requests/SqlGetIndexesRequest.h"

// LSST headers
#include "lsst/log/Log.h"

// System headers
#include <stdexcept>
#include <vector>

using namespace std;
using namespace nlohmann;
using namespace lsst::qserv::replica;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.SqlGetIndexesJob");

void parseRow2index(database::mysql::Row const& row, map<string, SqlIndexes::Index>& name2index) {
    string name;
    row.get("Key_name", name);
    SqlIndexes::Index& index = name2index[name];
    index.name = name;

    int nonUnique;
    row.get("Non_unique", nonUnique);
    index.unique = nonUnique == 0;

    row.get("Index_type", index.type);
    row.get("Index_comment", index.comment);

    SqlIndexes::Column column;
    row.get("Column_name", column.name);
    row.get("Seq_in_index", column.seq);
    if (!row.get("Sub_part", column.subPart)) {
        column.subPart = 0;
    }
    string collation;
    if (row.get("Collation", collation)) {
        if (collation == "A") {
            column.collation = SqlIndexes::Column::Collation::ASC;
        } else {
            column.collation = SqlIndexes::Column::Collation::DESC;
        }
    } else {
        column.collation = SqlIndexes::Column::Collation::NOT_SORTED;
    }
    index.columns.push_back(column);
}
}  // namespace

namespace lsst::qserv::replica {

string SqlIndexes::Column::collation2string(SqlIndexes::Column::Collation collation) {
    switch (collation) {
        case SqlIndexes::Column::Collation::ASC:
            return "ASC";
        case SqlIndexes::Column::Collation::DESC:
            return "DESC";
        case SqlIndexes::Column::Collation::NOT_SORTED:
            return "NOT_SORTED";
    }
    throw invalid_argument("SqlIndexes::Column::" + string(__func__) +
                           " invalid collation: " + to_string(static_cast<int>(collation)) + ".");
}

bool SqlIndexes::Index::equalIndexDef(SqlIndexes::Index const& other) const {
    return (name == other.name) && (unique == other.unique) && (type == other.type) &&
           (columns == other.columns);
}

string SqlIndexes::Index::status2string(SqlIndexes::Index::Status status) {
    switch (status) {
        case SqlIndexes::Index::Status::COMPLETE:
            return "COMPLETE";
        case SqlIndexes::Index::Status::INCOMPLETE:
            return "INCOMPLETE";
        case SqlIndexes::Index::Status::INCONSISTENT:
            return "INCONSISTENT";
    }
    throw invalid_argument("SqlIndexes::Index::" + string(__func__) +
                           " invalid status: " + to_string(static_cast<int>(status)) + ".");
}

json SqlIndexes::toJson() const {
    json result = json::object();
    result["database"] = database;
    result["table"] = table;
    result["overlap"] = overlap ? 1 : 0;
    result["indexes"] = json::array();
    for (auto&& index : indexes) {
        json indexJson = json::object();
        indexJson["name"] = index.name;
        indexJson["unique"] = index.unique ? 1 : 0;
        indexJson["type"] = index.type;
        indexJson["comment"] = index.comment;
        indexJson["columns"] = json::array();
        for (auto&& column : index.columns) {
            json columnJson = json::object();
            columnJson["name"] = column.name;
            columnJson["seq"] = column.seq;
            columnJson["sub_part"] = column.subPart;
            columnJson["collation"] = Column::collation2string(column.collation);
            indexJson["columns"].push_back(columnJson);
        }
        indexJson["status"] = Index::status2string(index.status);
        indexJson["num_replicas_total"] = index.numReplicasTotal;
        indexJson["num_replicas"] = index.numReplicas;
        result["indexes"].push_back(indexJson);
    }
    return result;
}

string SqlGetIndexesJob::typeName() { return "SqlGetIndexesJob"; }

SqlGetIndexesJob::Ptr SqlGetIndexesJob::create(string const& database, string const& table, bool overlap,
                                               bool allWorkers, Controller::Ptr const& controller,
                                               string const& parentJobId, CallbackType const& onFinish,
                                               int priority) {
    return Ptr(new SqlGetIndexesJob(database, table, overlap, allWorkers, controller, parentJobId, onFinish,
                                    priority));
}

SqlGetIndexesJob::SqlGetIndexesJob(string const& database, string const& table, bool overlap, bool allWorkers,
                                   Controller::Ptr const& controller, string const& parentJobId,
                                   CallbackType const& onFinish, int priority)
        : SqlJob(0, allWorkers, controller, parentJobId, "SQL_GET_TABLE_INDEXES", priority),
          _database(database),
          _table(table),
          _overlap(overlap),
          _onFinish(onFinish) {}

list<pair<string, string>> SqlGetIndexesJob::extendedPersistentState() const {
    list<pair<string, string>> result;
    result.emplace_back("database", database());
    result.emplace_back("table", table());
    result.emplace_back("overlap", bool2str(overlap()));
    result.emplace_back("all_workers", bool2str(allWorkers()));
    return result;
}

SqlIndexes SqlGetIndexesJob::indexes() const {
    string const context_ = context() + string(__func__) + "[" + id() + "] ";
    if (state() != Job::State::FINISHED) throw logic_error(context_ + "is not finished yet.");
    if (extendedState() != Job::ExtendedState::SUCCESS) throw logic_error(context_ + "has failed.");

    // Count all tables ispected by the job across all workers.
    size_t numReplicasTotal = 0;
    for (auto&& itr : _workers2tables) {
        numReplicasTotal += itr.second.size();
    }

    // The nested dictionary of index definitions extracted from
    // the result set of the job (all indexes of all replicas
    // across all workers).
    map<string, map<string, map<string, SqlIndexes::Index>>> worker2table2index2def;
    getResultData().iterate([&worker2table2index2def](string const& worker, string const& table,
                                                      SqlResultSet::ResultSet const& resultSet) {
        // The dictionary of index definitions extracted from
        // the result set of the table (all indexes of
        // a single replica).
        map<string, SqlIndexes::Index> name2index;
        SqlResultSet::iterate(resultSet, [&name2index](database::mysql::Row const& row) {
            ::parseRow2index(row, name2index);
        });
        for (auto&& itr : name2index) {
            worker2table2index2def[worker][table][itr.first] = itr.second;
        }
    });

    // Analyze the above-discovered index definitions for completeness and
    // consistency. Register each index in the final collection. Also count
    // the number of replicas for each such index.
    //
    // Note that if the index won't be consistent across all tables then the very
    // first definition encountered by the algoritm will be assumed as the reference
    // index. In reality, an ambiguity of this random choice won't matter since
    // the only pieces of information that matter in this scenario would be the name
    // of the index and its INCOMPLETE status. It will be up to the data administrators
    // to investigate why the index turned into such state.
    map<string, SqlIndexes::Index> name2finalIndex;
    for (auto workrItr : worker2table2index2def) {
        for (auto tableItr : workrItr.second) {
            for (auto indexItr : tableItr.second) {
                SqlIndexes::Index const& index = indexItr.second;
                auto finalIndexItr = name2finalIndex.find(index.name);
                if (finalIndexItr == name2finalIndex.end()) {
                    // First time seeing this index. Register in the final collection.
                    name2finalIndex[index.name] = index;
                    // Initialize values the attributes that were not set in the partial
                    // index definition.
                    name2finalIndex[index.name].status = SqlIndexes::Index::Status::COMPLETE;
                    name2finalIndex[index.name].numReplicasTotal = numReplicasTotal;
                    name2finalIndex[index.name].numReplicas = 1;
                } else {
                    SqlIndexes::Index& finalIndex = finalIndexItr->second;
                    finalIndex.numReplicas++;
                    if (!index.equalIndexDef(finalIndex)) {
                        finalIndex.status = SqlIndexes::Index::Status::INCONSISTENT;
                    }
                }
            }
        }
    }

    // Pack the findings into the final result object. And while doing so verify
    // consistent indexes for completeness. Mark the incomplete ones.
    SqlIndexes result;
    result.database = _database;
    result.table = _table;
    result.overlap = _overlap;
    for (auto itr : name2finalIndex) {
        SqlIndexes::Index& index = itr.second;
        if (index.status != SqlIndexes::Index::Status::INCONSISTENT) {
            if (index.numReplicasTotal != index.numReplicas) {
                index.status = SqlIndexes::Index::Status::INCOMPLETE;
            }
        }
        result.indexes.push_back(index);
    }
    return result;
}

list<SqlRequest::Ptr> SqlGetIndexesJob::launchRequests(replica::Lock const& lock, string const& worker,
                                                       size_t maxRequestsPerWorker) {
    list<SqlRequest::Ptr> requests;
    if (maxRequestsPerWorker == 0) return requests;

    // Make sure this worker has already been served
    if (_workers2tables.count(worker) != 0) return requests;

    // Only the requested subset of tables is going to be processed at the worker.
    bool const allTables = false;
    _workers2tables[worker] = workerTables(worker, database(), table(), allTables, overlap());

    // Divide tables into subsets allocated to the "batch" requests. Then launch
    // the requests for the current worker.
    for (auto&& tables : distributeTables(_workers2tables[worker], maxRequestsPerWorker)) {
        bool const keepTracking = true;
        requests.push_back(SqlGetIndexesRequest::createAndStart(
                controller(), worker, database(), tables,
                [self = shared_from_base<SqlGetIndexesJob>()](SqlGetIndexesRequest::Ptr const& request) {
                    self->onRequestFinish(request);
                },
                priority(), keepTracking, id()));
    }
    return requests;
}

void SqlGetIndexesJob::notify(replica::Lock const& lock) {
    LOGS(_log, LOG_LVL_DEBUG, context() << __func__ << "[" << typeName() << "]");
    notifyDefaultImpl<SqlGetIndexesJob>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
