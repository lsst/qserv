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
#include "replica/SqlJobResult.h"

// Qserv headers
#include "replica/Common.h"

// System headers
#include <vector>

using namespace std;
using namespace nlohmann;

namespace lsst::qserv::replica {

void SqlJobResult::merge(SqlJobResult const& other) {
    for (auto&& otherWorkerItr : other.resultSets) {
        auto&& worker = otherWorkerItr.first;
        auto&& otherColl = otherWorkerItr.second;
        auto&& thisColl = resultSets[worker];
        thisColl.insert(thisColl.cend(), otherColl.cbegin(), otherColl.cend());
    }
}

void SqlJobResult::iterate(OnResultVisitCallback const& onResultVisitCallback) const {
    for (auto&& workerItr : resultSets) {
        auto&& worker = workerItr.first;
        for (auto&& requestResultSets : workerItr.second) {
            for (auto&& scopeItr : requestResultSets.queryResultSet) {
                auto&& scope = scopeItr.first;
                auto&& resultSet = scopeItr.second;
                onResultVisitCallback(worker, scope, resultSet);
            }
        }
    }
}

json SqlJobResult::toJson() const {
    json result = json::object();
    iterate([&result](Worker const& worker, Scope const& scope, SqlResultSet::ResultSet const& resultSet) {
        result["completed"][worker][scope] = resultSet.extendedStatus == ProtocolStatusExt::NONE ? 1 : 0;
        result["error"][worker][scope] = resultSet.error;
    });
    return result;
}

util::ColumnTablePrinter SqlJobResult::toColumnTable(string const& caption, string const& indent,
                                                     bool verticalSeparator, bool reportAll,
                                                     string const& scopeName) const {
    vector<string> workers;
    vector<string> scopes;
    vector<string> statuses;
    vector<string> errors;

    iterate([&](Worker const& worker, Scope const& scope, SqlResultSet::ResultSet const& resultSet) {
        if (reportAll or resultSet.extendedStatus != ProtocolStatusExt::NONE) {
            workers.push_back(worker);
            scopes.push_back(scope);
            statuses.push_back(status2string(resultSet.extendedStatus));
            errors.push_back(resultSet.error);
        }
    });

    util::ColumnTablePrinter table(caption, indent, verticalSeparator);
    table.addColumn("worker", workers, util::ColumnTablePrinter::LEFT);
    table.addColumn(scopeName, scopes, util::ColumnTablePrinter::LEFT);
    table.addColumn("status", statuses, util::ColumnTablePrinter::LEFT);
    table.addColumn("error", errors, util::ColumnTablePrinter::LEFT);

    return table;
}

util::ColumnTablePrinter SqlJobResult::summaryToColumnTable(string const& caption, std::string const& indent,
                                                            bool verticalSeparator) const {
    vector<string> workers;
    vector<size_t> succeeded;
    vector<size_t> failed;
    vector<double> performance;

    for (auto&& itr : resultSets) {
        auto&& worker = itr.first;
        for (auto&& workerResultSet : itr.second) {
            size_t numSucceeded = 0;
            size_t numFailed = 0;
            for (auto&& queryResultSetItr : workerResultSet.queryResultSet) {
                auto&& resultSet = queryResultSetItr.second;
                if (resultSet.extendedStatus == ProtocolStatusExt::NONE) {
                    numSucceeded++;
                } else {
                    numFailed++;
                }
            }
            workers.push_back(worker);
            succeeded.push_back(numSucceeded);
            failed.push_back(numFailed);
            performance.push_back(workerResultSet.performanceSec);
        }
    }

    util::ColumnTablePrinter table(caption, indent, verticalSeparator);
    table.addColumn("worker", workers, util::ColumnTablePrinter::LEFT);
    table.addColumn("#succeeded", succeeded);
    table.addColumn("#failed", failed);
    table.addColumn("performance [sec]", performance);

    return table;
}

}  // namespace lsst::qserv::replica
