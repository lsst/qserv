/*
 * LSST Data Management System
 * Copyright 2019 LSST.
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
 * see <https://www.lsstcorp.org/LegalNotices/>.
 */

// Class header
#include "qproc/DatabaseModels.h"

// System headers

// Third-party headers

// LSST headers
#include "lsst/log/Log.h"

// Qserv headers
#include "mysql/MySqlConfig.h"
#include "sql/SqlConnectionFactory.h"
#include "util/ConfigStore.h"
#include "util/Issue.h"

using namespace std;

namespace {
LOG_LOGGER _log = LOG_GET("lsst.qserv.qproc.DatabaseModels");
}

namespace lsst {
namespace qserv {
namespace qproc {

DatabaseModels::Ptr DatabaseModels::create(map<string, string> const& configMap) {
    util::ConfigStore cfgStore(configMap);
    /// Use the CSS config for now. The CSS database is not used but sql::SqlConnection wants a database name.
    mysql::MySqlConfig mySqlConfig(cfgStore.get("username"),
                                   cfgStore.get("password"),
                                   cfgStore.get("hostname"),
                                   cfgStore.getInt("port"),
                                   cfgStore.get("socket"),
                                   cfgStore.get("db"));


    Ptr dbModels(new DatabaseModels(mySqlConfig));
    return dbModels;
}


DatabaseModels::Ptr DatabaseModels::create(sql::SqlConfig const& cfg) {
    Ptr dbModels(new DatabaseModels(cfg));
    return dbModels;
}


DatabaseModels::DatabaseModels(sql::SqlConfig const& sqlConfig)
    : _conn(sql::SqlConnectionFactory::make(sqlConfig)) {}


bool DatabaseModels::applySql(string const& sql, sql::SqlResults& results, sql::SqlErrorObject& errObj) {
    lock_guard<mutex> lg(_sqlMutex);

    if (not _conn->connectToDb(errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "DatabaseModels could not connect " << errObj.printErrMsg());
        return false;
    }
    if (not _conn->runQuery(sql, results, errObj)) {
        LOGS(_log, LOG_LVL_ERROR, "DatabaseModels applySql error: " << errObj.printErrMsg());
        return false;
    }
    LOGS(_log, LOG_LVL_DEBUG, "DatabaseModels query success: " << sql);
    return true;
}


vector<string> DatabaseModels::listColumns(string const& dbName, string const& tableName) {
    lock_guard<mutex> lg(_sqlMutex);
    return _conn->listColumns(dbName, tableName);
}

}}} // namespace lsst::qserv::qproc
