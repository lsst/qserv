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
#include "replica/requests/http/HttpSqlCreateTableRequest.h"

// Qserv headers
#include "replica/proto/Protocol.h"

// Third-party headers
#include "nlohmann/json.hpp"

using namespace std;
using json = nlohmann::json;

namespace {

using namespace lsst::qserv::replica;

json makeColumns(list<SqlColDef> const& columns) {
    json result = json::array();
    for (auto&& column : columns) {
        result.push_back({{"name", column.name}, {"type", column.type}});
    }
    return result;
}

bool const batchMode = false;
uint64_t const unlimitedMaxRows = 0;

}  // namespace

namespace lsst::qserv::replica {

shared_ptr<HttpSqlCreateTableRequest> HttpSqlCreateTableRequest::createAndStart(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        string const& table, string const& engine, string const& partitionByColumn,
        list<SqlColDef> const& columns, string const& charsetName, string const& collationName,
        CallbackType const& onFinish, int priority, bool keepTracking, string const& jobId,
        unsigned int requestExpirationIvalSec) {
    auto ptr = shared_ptr<HttpSqlCreateTableRequest>(new HttpSqlCreateTableRequest(
            controller, workerName, database, table, engine, partitionByColumn, columns, charsetName,
            collationName, onFinish, priority, keepTracking));
    ptr->start(jobId, requestExpirationIvalSec);
    return ptr;
}

HttpSqlCreateTableRequest::HttpSqlCreateTableRequest(
        shared_ptr<Controller> const& controller, string const& workerName, string const& database,
        string const& table, string const& engine, string const& partitionByColumn,
        list<SqlColDef> const& columns, string const& charsetName, string const& collationName,
        CallbackType const& onFinish, int priority, bool keepTracking)
        : HttpSqlRequest(controller, "SQL_CREATE_TABLE", protocol::SqlRequestType::CREATE_TABLE, database,
                         ::batchMode,
                         {{"table", table},
                          {"engine", engine},
                          {"comment", ""},
                          {"charset_name", charsetName},
                          {"collation_name", collationName},
                          {"columns", ::makeColumns(columns)},
                          {"partition_by_column", partitionByColumn}},
                         workerName, ::unlimitedMaxRows, priority, keepTracking),
          _onFinish(onFinish) {}

void HttpSqlCreateTableRequest::notify(replica::Lock const& lock) {
    notifyDefaultImpl<HttpSqlCreateTableRequest>(lock, _onFinish);
}

}  // namespace lsst::qserv::replica
