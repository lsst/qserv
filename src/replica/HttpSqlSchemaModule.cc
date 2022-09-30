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
#include "replica/HttpSqlSchemaModule.h"

// System headers
#include <algorithm>
#include <limits>
#include <map>
#include <stdexcept>

// Qserv headers
#include "css/CssAccess.h"
#include "css/DbInterfaceMySql.h"
#include "replica/Configuration.h"
#include "replica/DatabaseMySQL.h"
#include "replica/HttpExceptions.h"
#include "replica/SqlAlterTablesJob.h"
#include "replica/SqlResultSet.h"
#include "replica/HttpRequestBody.h"

using namespace std;
using json = nlohmann::json;

namespace lsst::qserv::replica {

using namespace database::mysql;

void HttpSqlSchemaModule::process(Controller::Ptr const& controller, string const& taskName,
                                  HttpProcessorConfig const& processorConfig, qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp, string const& subModuleName,
                                  HttpAuthType const authType) {
    HttpSqlSchemaModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}

HttpSqlSchemaModule::HttpSqlSchemaModule(Controller::Ptr const& controller, string const& taskName,
                                         HttpProcessorConfig const& processorConfig,
                                         qhttp::Request::Ptr const& req, qhttp::Response::Ptr const& resp)
        : HttpModule(controller, taskName, processorConfig, req, resp) {}

json HttpSqlSchemaModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "GET-TABLE-SCHEMA")
        return _getTableSchema();
    else if (subModuleName == "ALTER-TABLE-SCHEMA")
        return _alterTableSchema();
    throw invalid_argument(context() + "::" + string(__func__) + "  unsupported sub-module: '" +
                           subModuleName + "'");
}

json HttpSqlSchemaModule::_getTableSchema() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = params().at("database");
    string const tableName = params().at("table");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    json schemaJson;
    if (database.isPublished) {
        // Extract schema from czar's MySQL database.
        ConnectionHandler const h(qservMasterDbConnection(database.name));
        QueryGenerator const g(h.conn);
        string const query = g.select(Sql::STAR) + g.from(g.id("information_schema", "columns")) +
                             g.where(g.eq("TABLE_SCHEMA", database.name), g.eq("TABLE_NAME", table.name));
        h.conn->executeInOwnTransaction([&query, &schemaJson](decltype(h.conn) const& conn) {
            conn->execute(query);
            Row row;
            while (conn->next(row)) {
                size_t precision;
                bool const hasPrecision = row.get("NUMERIC_PRECISION", precision);
                size_t charMaxLength;
                bool const hasCharMaxLength = row.get("CHARACTER_MAXIMUM_LENGTH", charMaxLength);
                schemaJson.push_back(json::object(
                        {{"ORDINAL_POSITION", row.getAs<size_t>("ORDINAL_POSITION")},
                         {"COLUMN_NAME", row.getAs<string>("COLUMN_NAME")},
                         {"COLUMN_TYPE", row.getAs<string>("COLUMN_TYPE")},
                         {"DATA_TYPE", row.getAs<string>("DATA_TYPE")},
                         {"NUMERIC_PRECISION", hasPrecision ? to_string(precision) : "NULL"},
                         {"CHARACTER_MAXIMUM_LENGTH", hasCharMaxLength ? to_string(charMaxLength) : "NULL"},
                         {"IS_NULLABLE", row.getAs<string>("IS_NULLABLE")},
                         {"COLUMN_DEFAULT", row.getAs<string>("COLUMN_DEFAULT", "NULL")},
                         {"COLUMN_COMMENT", row.getAs<string>("COLUMN_COMMENT")}}));
            }
        });
    } else {
        // Pull schema info from the Replication/Ingest system's database.
        size_t ordinalPosition = 1;
        for (auto const& column : table.columns) {
            schemaJson.push_back(json::object({{"ORDINAL_POSITION", ordinalPosition++},
                                               {"COLUMN_NAME", column.name},
                                               {"COLUMN_TYPE", column.type},
                                               {"DATA_TYPE", string()},
                                               {"NUMERIC_PRECISION", string()},
                                               {"CHARACTER_MAXIMUM_LENGTH", string()},
                                               {"IS_NULLABLE", string()},
                                               {"COLUMN_DEFAULT", string()},
                                               {"COLUMN_COMMENT", string()}}));
        }
    }
    json result;
    result["schema"][database.name][table.name] = schemaJson;
    return result;
}

json HttpSqlSchemaModule::_alterTableSchema() {
    debug(__func__);
    checkApiVersion(__func__, 12);

    string const databaseName = params().at("database");
    string const tableName = params().at("table");
    string const spec = body().required<string>("spec");

    debug(__func__, "database=" + databaseName);
    debug(__func__, "table=" + tableName);
    debug(__func__, "spec=" + spec);

    auto const config = controller()->serviceProvider()->config();
    auto const database = config->databaseInfo(databaseName);
    auto const table = database.findTable(tableName);

    // This safeguard is needed since database/table definition doesn't exist in Qserv
    // master until the catalog is published. It's unsafe to modifying table schema while
    // they data are still being ingested as it would reault in all sorts of data corruptions
    // or inconsistencies.
    if (not database.isPublished) {
        throw HttpError(__func__, "database '" + database.name + "' is not published");
    }

    // Update table definition at Qserv master database. Note this step will
    // also validate the query.
    ConnectionHandler const h(qservMasterDbConnection(database.name));
    QueryGenerator const g(h.conn);
    string query = g.alterTable(g.id(database.name, table.name), spec);
    h.conn->executeInOwnTransaction([&query](decltype(h.conn) const& conn) { conn->execute(query); });

    // Update CSS based on the new table schema at the Qserv master database
    string newCssTableSchema;
    query = g.select(Sql::STAR) + g.from(g.id("information_schema", "columns")) +
            g.where(g.eq("TABLE_SCHEMA", database.name), g.eq("TABLE_NAME", table.name));
    h.conn->executeInOwnTransaction([&query, &newCssTableSchema, &g](decltype(h.conn) const& conn) {
        conn->execute(query);
        Row row;
        while (conn->next(row)) {
            if (!newCssTableSchema.empty()) newCssTableSchema += ",";
            // ATTENTION: in the current implementation of the Qserv Ingest System, default values
            // other than NULL aren't supported in the column definitions. All table contributions
            // are required to explicitly provide values for all fields or NULL. The only exception
            // allowed here is to either restrict teh values to be NULL or have NULL as the default
            // value.
            newCssTableSchema += g.id(row.getAs<string>("COLUMN_NAME")).str + " " +
                                 row.getAs<string>("COLUMN_TYPE") +
                                 (row.getAs<string>("IS_NULLABLE") != "YES" ? " DEFAULT NULL" : " NOT NULL");
        }
    });
    auto const cssAccess = qservCssAccess();
    if (!cssAccess->containsDb(database.name)) {
        throw HttpError(__func__, "Database '" + database.name + "' is not in CSS.");
    }
    if (!cssAccess->containsTable(database.name, table.name)) {
        throw HttpError(__func__, "Table '" + database.name + "'.'" + table.name + "' is not in CSS.");
    }
    string const oldCssTableSchema = cssAccess->getTableSchema(database.name, table.name);
    try {
        cssAccess->setTableSchema(database.name, table.name, newCssTableSchema);
    } catch (exception const& ex) {
        throw HttpError(__func__,
                        "Failed to update CSS table schema of '" + database.name + "'.'" + table.name + "'.",
                        json({{"css_error", ex.what()},
                              {"css_old_schema", oldCssTableSchema},
                              {"css_new_schema", newCssTableSchema}}));
    }

    // Modify all relevant tables at all Qserv workers
    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlAlterTablesJob::create(
            database.name, table.name, spec, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog-management-priority-level"));
    job->start();
    logJobStartedEvent(SqlAlterTablesJob::typeName(), job, database.family);
    job->wait();
    logJobFinishedEvent(SqlAlterTablesJob::typeName(), job, database.family);

    auto const extendedErrorReport = job->getExtendedErrorReport();
    if (not extendedErrorReport.is_null()) {
        throw HttpError(__func__, "The operation failed. See details in the extended report.",
                        extendedErrorReport);
    }
    return json::object();
}

}  // namespace lsst::qserv::replica
