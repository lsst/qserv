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
#include "replica/worker/WorkerUtils.h"

// System headers
#include <stdexcept>
#include <vector>

// Qserv headers
#include "replica/config/Configuration.h"
#include "replica/mysql/DatabaseMySQL.h"
#include "replica/mysql/DatabaseMySQLGenerator.h"

// LSST headers
#include "lsst/log/Log.h"

using namespace std;

namespace {

LOG_LOGGER _log = LOG_GET("lsst.qserv.replica.WorkerUtils");

}  // namespace

namespace lsst::qserv::replica {

using namespace database::mysql;

void WorkerUtils::createMissingDatabase(string const& context, string const& databaseName) {
    LOGS(_log, LOG_LVL_DEBUG, context << "  create database: " << databaseName);
    try {
        ConnectionHandler const h(Connection::open(Configuration::qservWorkerDbParams()));
        QueryGenerator const g(h.conn);
        vector<string> queries;
        bool const ifNotExists = true;
        queries.push_back(g.createDb(databaseName, ifNotExists));
        queries.push_back(g.grant("ALL", databaseName, "qsmaster", "localhost"));
        queries.push_back(g.replace("qservw_worker", "Dbs", databaseName));
        h.conn->executeInOwnTransaction([&](decltype(h.conn) const& conn_) {
            for (auto const& query : queries) {
                conn_->execute(query);
            }
        });
    } catch (exception const& ex) {
        LOGS(_log, LOG_LVL_WARN,
             context << "  database: " << databaseName
                     << "  failed to create the missing database, error: " << ex.what());
    }
}

}  // namespace lsst::qserv::replica
