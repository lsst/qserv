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

namespace lsst {
namespace qserv {
namespace replica {

void HttpSqlSchemaModule::process(Controller::Ptr const& controller,
                                  string const& taskName,
                                  HttpProcessorConfig const& processorConfig,
                                  qhttp::Request::Ptr const& req,
                                  qhttp::Response::Ptr const& resp,
                                  string const& subModuleName,
                                  HttpModule::AuthType const authType) {
    HttpSqlSchemaModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpSqlSchemaModule::HttpSqlSchemaModule(
            Controller::Ptr const& controller,
            string const& taskName,
            HttpProcessorConfig const& processorConfig,
            qhttp::Request::Ptr const& req,
            qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpSqlSchemaModule::executeImpl(string const& subModuleName) {
    if (subModuleName == "GET-TABLE-SCHEMA") return _getTableSchema();
    else if (subModuleName == "ALTER-TABLE-SCHEMA") return _alterTableSchema();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpSqlSchemaModule::_getTableSchema() {
    debug(__func__);

    string const database = params().at("database");
    string const table = params().at("table");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);

    json schemaJson;
    if (databaseInfo.isPublished) {
        // Extract schema from czar's MySQL database.
        database::mysql::ConnectionHandler const h(qservMasterDbConnection(database));
        h.conn->executeInOwnTransaction([&](decltype(h.conn) const& conn) {
            conn->execute("SELECT * FROM " + conn->sqlId("information_schema", "columns") +
                        " WHERE " + conn->sqlEqual("TABLE_SCHEMA", database) +
                        "   AND " + conn->sqlEqual("TABLE_NAME", table));
            database::mysql::Row row;
            while (conn->next(row)) {
                size_t precision;
                bool const hasPrecision = row.get("NUMERIC_PRECISION", precision);
                size_t charMaxLength;
                bool const hasCharMaxLength = row.get("CHARACTER_MAXIMUM_LENGTH", charMaxLength);
                schemaJson.push_back(json::object({
                    {"ORDINAL_POSITION",         row.getAs<size_t>("ORDINAL_POSITION")},
                    {"COLUMN_NAME",              row.getAs<string>("COLUMN_NAME")},
                    {"COLUMN_TYPE",              row.getAs<string>("COLUMN_TYPE")},
                    {"DATA_TYPE",                row.getAs<string>("DATA_TYPE")},
                    {"NUMERIC_PRECISION",        hasPrecision     ? to_string(precision)     : "NULL"},
                    {"CHARACTER_MAXIMUM_LENGTH", hasCharMaxLength ? to_string(charMaxLength) : "NULL"},
                    {"IS_NULLABLE",              row.getAs<string>("IS_NULLABLE")},
                    {"COLUMN_DEFAULT",           row.getAs<string>("COLUMN_DEFAULT", "NULL")},
                    {"COLUMN_COMMENT",           row.getAs<string>("COLUMN_COMMENT")}
                }));
            }
        });
    } else {
        // Pull schema info from the Replication/Ingest system's database.
        size_t ordinalPosition = 1;
        for (auto const& coldef: databaseInfo.columns.at(table)) {
            schemaJson.push_back(json::object({
                {"ORDINAL_POSITION",         ordinalPosition++},
                {"COLUMN_NAME",              coldef.name},
                {"COLUMN_TYPE",              coldef.type},
                {"DATA_TYPE",                string()},
                {"NUMERIC_PRECISION",        string()},
                {"CHARACTER_MAXIMUM_LENGTH", string()},
                {"IS_NULLABLE",              string()},
                {"COLUMN_DEFAULT",           string()},
                {"COLUMN_COMMENT",           string()}
            }));
        }
    }
    json result;
    result["schema"][database][table] = schemaJson;
    return result;
}


json HttpSqlSchemaModule::_alterTableSchema() {
    debug(__func__);

    string const database = params().at("database");
    string const table = params().at("table");
    string const spec = body().required<string>("spec");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "spec=" + spec);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);

    // This safeguard is needed since database/table definition doesn't exist in Qserv
    // master until the catalog is published. It's unsafe to modifying table schema while
    // they data are still being ingested as it would reault in all sorts of data corruptions
    // or inconsistencies.
    if (not databaseInfo.isPublished) {
        throw HttpError(__func__, "database '" + databaseInfo.name + "' is not published");
    }

    // Update table definition at Qserv master database. Note this step will
    // also validate the query.
    database::mysql::ConnectionHandler const h(qservMasterDbConnection(databaseInfo.name));
    h.conn->executeInOwnTransaction([&](decltype(h.conn) const& conn) {
        conn->execute("ALTER TABLE " + conn->sqlId(databaseInfo.name, table) + " " + spec);
    });

    // Update CSS based on the new table schema at the Qserv master database
    string newCssTableSchema;
    h.conn->executeInOwnTransaction([&](decltype(h.conn) const& conn) {
        conn->execute("SELECT * FROM " + conn->sqlId("information_schema", "columns") +
                      " WHERE " + conn->sqlEqual("TABLE_SCHEMA", databaseInfo.name) +
                      "   AND " + conn->sqlEqual("TABLE_NAME", table));
        database::mysql::Row row;
        while (conn->next(row)) {
            if (!newCssTableSchema.empty()) newCssTableSchema += ",";
            // ATTENTION: in the current implementation of the Qserv Ingest System, default values
            // other than NULL aren't supported in the column definitions. All table contributions
            // are required to explicitly provide values for all fields or NULL. The only exception
            // allowed here is to either restrict teh values to be NULL or have NULL as the default
            // value.
            newCssTableSchema +=
                conn->sqlId(row.getAs<string>("COLUMN_NAME")) + " " + row.getAs<string>("COLUMN_TYPE") +
                (row.getAs<string>("IS_NULLABLE") != "YES" ? " DEFAULT NULL" : " NOT NULL");
        }
    });
    auto const cssAccess = qservCssAccess();
    if (!cssAccess->containsDb(databaseInfo.name)) {
        throw HttpError(__func__, "Database '" + databaseInfo.name + "' is not in CSS.");
    }
    if (!cssAccess->containsTable(databaseInfo.name, table)) {
        throw HttpError(__func__, "Table '" + databaseInfo.name + "'.'" + table + "' is not in CSS.");
    }
    string const oldCssTableSchema = cssAccess->getTableSchema(databaseInfo.name, table);
    try {
        cssAccess->setTableSchema(databaseInfo.name, table, newCssTableSchema);
    } catch (exception const& ex) {
        throw HttpError(
            __func__,
            "Failed to update CSS table schema of '" + databaseInfo.name + "'.'" + table + "'.",
            json({{"css_error", ex.what()},
                  {"css_old_schema", oldCssTableSchema},
                  {"css_new_schema", newCssTableSchema}})
        );
    }

    // Modify all relevant tables at all Qserv workers
    bool const allWorkers = true;
    string const noParentJobId;
    auto const job = SqlAlterTablesJob::create(
            databaseInfo.name, table, spec, allWorkers, controller(), noParentJobId, nullptr,
            config->get<int>("controller", "catalog_management_priority_level"));
    job->start();
    logJobStartedEvent(SqlAlterTablesJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlAlterTablesJob::typeName(), job, databaseInfo.family);

    auto const extendedErrorReport = job->getExtendedErrorReport();
    if (not extendedErrorReport.is_null()) {
        throw HttpError(__func__, "The operation failed. See details in the extended report.",
                extendedErrorReport);
    }
    return json::object();
}

}}}  // namespace lsst::qserv::replica
