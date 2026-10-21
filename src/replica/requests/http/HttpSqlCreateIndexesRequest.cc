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
#include "replica/requests/http/HttpSqlCreateIndexesRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

// Third-party headers
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

namespace {

using namespace lsst::qserv::replica;

json buildIndexSpec(SqlIndexSpec const& indexSpec, string const& indexName, string const& indexComment,
                    vector<SqlIndexColumn> const& indexColumns) {
    json result = json::object({{"spec", indexSpec.str()},
                                {"name", indexName},
                                {"comment", indexComment},
                                {"keys", json::array()}});
    for (auto const& column : indexColumns) {
        result["keys"].push_back(json::object(
                {{"name", column.name}, {"length", column.length}, {"ascending", column.ascending ? 1 : 0}}));
    }
    return result;
}

bool const batchMode = true;
bool const ifNotExists = true;
uint64_t const unlimitedMaxRows = 0;

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlCreateIndexesRequest> HttpSqlCreateIndexesRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, SqlIndexSpec const& indexSpec, string const& indexName,
        string const& indexComment, vector<SqlIndexColumn> const& indexColumns, CallbackType const& onFinish,
        int priority, bool keepTracking, string const& jobId, unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlCreateIndexesRequest>(
            new HttpSqlCreateIndexesRequest(controller, workerName, database, tables, indexSpec, indexName,
                                            indexComment, indexColumns, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlCreateIndexesRequest::HttpSqlCreateIndexesRequest(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        vector<string> const& tables, SqlIndexSpec const& indexSpec, string const& indexName,
        string const& indexComment, vector<SqlIndexColumn> const& indexColumns, CallbackType const& onFinish,
        int priority, bool keepTracking)
        : HttpSqlRequest(controller, "SQL_CREATE_TABLE_INDEXES", protocol::SqlRequestType::CREATE_TABLE_INDEX,
                         database, ::batchMode,
                         {{"index", ::buildIndexSpec(indexSpec, indexName, indexComment, indexColumns)},
                          {"tables", tables},
                          {"if_not_exists", ::ifNotExists ? 1 : 0}},
                         workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlCreateIndexesRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlCreateIndexesRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
