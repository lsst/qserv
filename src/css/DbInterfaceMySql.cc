// -*- LSST-C++ -*-

/*
 * This file is part of qserv.
 *
 * Developed for the LSST Data Management System.
 * This product includes software developed by the LSST Project
 * (https://www.lsst.org).
 * See the COPYRIGHT file at the top-level directory of this distribution
 * for details of code ownership.
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
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

// Class header
#include "css/DbInterfaceMySql.h"

// System headers
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "css/CssError.h"
#include "css/DbInterfaceMySql.h"
#include "sql/SqlConnectionFactory.h"
#include "sql/SqlResults.h"
#include "sql/SqlTransactionScope.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.css.DbInterfaceMySql");
}  // namespace

namespace lsst::qserv {

namespace {

class DbITransaction : public sql::SqlTransactionScope {
public:
    using Ptr = shared_ptr<DbITransaction>;

    ~DbITransaction() override = default;

    /// Override to throw an appropriate exception.
    void throwException(util::Issue::Context const& ctx, string const& msg) override {
        throw css::CssError(ctx, msg + " mysql(" + to_string(errObj.errNo()) + " " + errObj.errMsg() + ")");
    }

    friend sql::SqlTransactionScope;

protected:
    /// Constructor - create with sql::SqlTransactionScope::create<DbITransaction>
    explicit DbITransaction(sql::SqlConnection& conn) : sql::SqlTransactionScope(conn) {}
};

}  // namespace

namespace css {

DbInterfaceMySql::DbInterfaceMySql(mysql::MySqlConfig const& mysqlConf)
        : _conn(sql::SqlConnectionFactory::make(mysqlConf)) {}

set<int> DbInterfaceMySql::getEmptyChunks(string const& dbName) {
    string const funcName(__func__);
    lock_guard<mutex> sync(_dbMutex);

    auto trans = sql::SqlTransactionScope::create<DbITransaction>(*_conn);

    // run query
    sql::SqlErrorObject errObj;
    sql::SqlResults results;
    string const query = "SELECT chunkId FROM `" + getEmptyChunksTableName(dbName) + "`";
    LOGS(_log, LOG_LVL_DEBUG, "Executing query: " << query);
    if (not _conn->runQuery(query, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, funcName << " SQL query failed: " << query);
        throw CssError(ERR_LOC, errObj);
    }

    // get results of the query
    vector<string> emptyChunks;
    if (not results.extractFirstColumn(emptyChunks, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, funcName << "Failed to extract empty chunks from query result");
        throw CssError(ERR_LOC, errObj);
    }

    trans->commit();

    // Create the set
    set<int> emptyChunkSet;
    for (auto const& j : emptyChunks) {
        try {
            emptyChunkSet.insert(std::stoi(j));
        } catch (std::logic_error const& e) {
            string eMsg(funcName + " failed conversion " + j + " " + e.what());
            LOGS(_log, LOG_LVL_ERROR, eMsg);
            throw CssError(ERR_LOC, eMsg);
        }
    }
    return emptyChunkSet;
}

}  // namespace css
}  // namespace lsst::qserv
