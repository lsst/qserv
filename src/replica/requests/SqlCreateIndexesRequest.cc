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
#include "replica/requests/SqlCreateIndexesRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

using namespace std;

namespace {
const uint64_t unlimitedMaxRows = 0;
}  // namespace

namespace lsst::qserv::replica {

shared_ptr<SqlCreateIndexesRequest> SqlCreateIndexesRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, SqlIndexSpec const& indexSpec, string const& indexName,
        string const& indexComment, vector<SqlIndexColumn> const& indexColumns, CallbackType const& onFinish,
        int priority, bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<SqlCreateIndexesRequest>(
            new SqlCreateIndexesRequest(controller, workerName, database, tables, indexSpec, indexName,
                                        indexComment, indexColumns, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

SqlCreateIndexesRequest::SqlCreateIndexesRequest(shared_ptr<Controller> const& controller,
                                                 string const& workerName, string const& database,
                                                 vector<string> const& tables, SqlIndexSpec const& indexSpec,
                                                 string const& indexName, string const& indexComment,
                                                 vector<SqlIndexColumn> const& indexColumns,
                                                 CallbackType const& onFinish, int priority,
                                                 bool keepTracking)
        : SqlRequest(controller, "SQL_CREATE_TABLE_INDEXES", protocol::SqlRequestType::CREATE_TABLE_INDEX,
                     workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {
    // Finish initializing the request body's content
    requestBody.set_database(database);
    requestBody.clear_tables();
    for (auto&& table : tables) {
        requestBody.add_tables(table);
    }
    requestBody.set_index_spec(indexSpec.protocol());
    requestBody.set_index_name(indexName);
    requestBody.set_index_comment(indexComment);
    requestBody.clear_index_columns();
    for (auto&& column : indexColumns) {
        auto out = requestBody.add_index_columns();
        out->set_name(column.name);
        out->set_length(column.length);
        out->set_ascending(column.ascending);
    }
    requestBody.set_batch_mode(true);
}

void SqlCreateIndexesRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<SqlCreateIndexesRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
