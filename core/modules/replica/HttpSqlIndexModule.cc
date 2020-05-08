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
#include "replica/HttpSqlIndexModule.h"

// System headers
#include <algorithm>
#include <limits>
#include <map>
#include <stdexcept>

// Qserv headers
#include "replica/Configuration.h"
#include "replica/HttpExceptions.h"
#include "replica/SqlCreateIndexesJob.h"
#include "replica/SqlDropIndexesJob.h"
#include "replica/SqlGetIndexesJob.h"
#include "replica/SqlResultSet.h"
#include "HttpRequestBody.h"

using namespace std;
using json = nlohmann::json;
using namespace lsst::qserv::replica;

namespace {
/**
 * Analyze completion status of a job and return an error string to be
 * reported back to a client if any such error was reported by the job.
 *
 * @param job  A pointer to a job
 * @return A summary of the error (if any). Otherwise an empty string is returned.
 */
string getErrorIfAny(SqlJob::Ptr const& job) {
    if (job->extendedState() == Job::ExtendedState::SUCCESS) return string();

    // Extract counters for specific errors. The counters will be reported
    // to a client.
    auto&& resultData = job->getResultData();
    size_t numSucceeded = 0;
    map<ExtendedCompletionStatus,size_t> numFailed;
    resultData.iterate(
        [&numFailed, &numSucceeded](SqlJobResult::Worker const& worker,
                                    SqlJobResult::Scope const& object,
                                    SqlResultSet::ResultSet const& resultSet) {
            if (resultSet.extendedStatus == ExtendedCompletionStatus::EXT_STATUS_NONE) {
                numSucceeded++;
            } else {
                numFailed[resultSet.extendedStatus]++;
            }
        }
    );

    string error = "job failure code: " + Job::state2string(job->extendedState())
            + ", success counter: " + to_string(numSucceeded) + ", error counters:";
    for (auto&& elem: numFailed) {
        string const extendedStatus = status2string(elem.first);
        size_t const num = elem.second;
        error += " " + extendedStatus + ":" + to_string(num);
    }
    return error;
}


/**
 * Translate a result set of a job into a JSON object. 
 * 
 * @note ignore errors reported in the input set for now. Only consider
 * successful result sets.
 * 
 * The output JSON object has the following schema:
 * @code
 *   <worker>:{
 *     <table>:{
 *       <index-key>:{
 *         "columns":{
 *           <column>:<number>
 *         },
 *         "comment":<string>
 *       }
 *     }
 *   }
 * @code
 *
 * @param jobResultSet The result set to analyze.
 * @param context The context string for error reporting.
 * @return The JSON representation of the result.
 */
json result2json(SqlJobResult const& jobResultSet, string const& context) {
    // The cached locations of the fields are computed once during the very first
    // iteration over result sets. It's assumed all result sets have the same set
    // of fields.
    map<string,size_t> field;

    json result;
    jobResultSet.iterate([&](SqlJobResult::Worker const& worker,
                             SqlJobResult::Scope const& scope,
                             SqlResultSet::ResultSet const& resultSet) {
        // TODO: Ignoring failed or empty results for now. Will decide
        // what to do about them later.
        if (resultSet.extendedStatus != ExtendedCompletionStatus::EXT_STATUS_NONE) return;
        if (not resultSet.hasResult) return;

        // Compute indexes just once and save it for analyzing this and other result sets.
        if (field.empty()) {
            for (size_t idx = 0, num = resultSet.fields.size(); idx < num; ++idx) {
                field[resultSet.fields[idx].name] = idx;
            }
        }
        
        for (auto&& row:resultSet.rows) {
            auto const& cells = row.cells;
            string const& tableName  = cells[field.at("Table")];
            string const& keyName    = cells[field.at("Key_name")];
            string const& columnName = cells[field.at("Column_name")];
            string const& sequence   = cells[field.at("Seq_in_index")];
            string const& comment    = cells[field.at("Index_comment")];
            json& key = result[worker][tableName][keyName];
            key["columns"][columnName] = sequence;
            key["comment"] = comment;
        }
    });
    return result;
}
}

namespace lsst {
namespace qserv {
namespace replica {

void HttpSqlIndexModule::process(Controller::Ptr const& controller,
                                 string const& taskName,
                                 HttpProcessorConfig const& processorConfig,
                                 qhttp::Request::Ptr const& req,
                                 qhttp::Response::Ptr const& resp,
                                 string const& subModuleName,
                                 HttpModule::AuthType const authType) {
    HttpSqlIndexModule module(controller, taskName, processorConfig, req, resp);
    module.execute(subModuleName, authType);
}


HttpSqlIndexModule::HttpSqlIndexModule(
            Controller::Ptr const& controller,
            string const& taskName,
            HttpProcessorConfig const& processorConfig,
            qhttp::Request::Ptr const& req,
            qhttp::Response::Ptr const& resp)
    :   HttpModule(controller, taskName, processorConfig, req, resp) {
}


json HttpSqlIndexModule::executeImpl(string const& subModuleName) {
    if (subModuleName.empty()) return _getIndexes();
    else if (subModuleName == "CREATE-INDEXES") return _createIndexes();
    else if (subModuleName == "DROP-INDEXES") return _dropIndexes();
    throw invalid_argument(
            context() + "::" + string(__func__) +
            "  unsupported sub-module: '" + subModuleName + "'");
}


json HttpSqlIndexModule::_getIndexes() {
    debug(__func__);

    string const database = body().required<string>("database");
    string const table = body().required<string>("table");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);

    bool const allWorkers = true;
    auto const job = SqlGetIndexesJob::create(database, table, allWorkers, controller());
    job->start();
    logJobStartedEvent(SqlGetIndexesJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlGetIndexesJob::typeName(), job, databaseInfo.family);

    string const error = ::getErrorIfAny(job);
    if (not error.empty()) throw HttpError(__func__, error);

    json result;
    result["workers"] = ::result2json(job->getResultData(), context());
    return result;
}


json HttpSqlIndexModule::_createIndexes() {
    debug(__func__);

    string const database = body().required<string>("database");
    string const table = body().required<string>("table");
    string const index = body().required<string>("index");
    string const comment = body().optional<string>("comment", string());
    SqlRequestParams::IndexSpec const spec = SqlRequestParams::IndexSpec(
        body().optional<string>("spec", "DEFAULT", {"DEFAULT", "UNIQUE", "FULLTEXT", "SPATIAL"})
    );
    json const columnsJson = body().required<json>("columns");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "index=" + index);
    debug(__func__, "comment=" + comment);
    debug(__func__, "spec=" + spec.str());
    debug(__func__, "columns.size()=" + columnsJson.size());

    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);

    // This safeguard is needed here because the index management job launched
    // doesn't have this restriction.
    if (databaseInfo.isPublished) throw HttpError(__func__, "database is not published");

    // Process the input collection of the column specifications.
    // 
    // At this step an optional (if the table schema is available) effort
    // to evaluate the column specification will be made to ensure the columns
    // are present in the schema.
    //
    // TODO: another possibility would be to either pull the schema from
    // the information schema of the Qserv czar's database or to "pre-flight"
    // the index creation against the table instance. Though, the later idea
    // has potential complications - the index may already exist in that table.

    auto const schema = databaseInfo.columns.count(table) == 0 ? list<SqlColDef>() : databaseInfo.columns.at(table);
    if (not columnsJson.is_array()) {
        throw invalid_argument(
                context() + "::" + string(__func__) + "  parameter 'columns' is not a simple JSON array.");
    }
    vector<SqlIndexColumn> columns;
    for (auto&& columnJson: columnsJson) {
        string const column = HttpRequestBody::required<string>(columnJson, "column");
        if (not schema.empty() and schema.cend() == find_if(
                schema.cbegin(), schema.cend(), [&column](auto&& c) { return c.name == column; })) {
            throw invalid_argument(
                    context() + "::" + string(__func__) + "  requested column '" + column
                    + "' has not been found in the table schema.");
        }        
        columns.emplace_back(
            column,
            HttpRequestBody::required<size_t>(columnJson, "length"),
            HttpRequestBody::required<int>(columnJson, "ascending")
        );
    }

    bool const allWorkers = true;
    auto const job = SqlCreateIndexesJob::create(database, table, spec, index, comment, columns,
                                                 allWorkers, controller());
    job->start();
    logJobStartedEvent(SqlCreateIndexesJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlCreateIndexesJob::typeName(), job, databaseInfo.family);

    string const error = ::getErrorIfAny(job);
    if (not error.empty()) throw HttpError(__func__, error);

    return json::object();
}


json HttpSqlIndexModule::_dropIndexes() {
    debug(__func__);

    string const database = body().required<string>("database");
    string const table = body().required<string>("table");
    string const index = body().required<string>("index");

    debug(__func__, "database=" + database);
    debug(__func__, "table=" + table);
    debug(__func__, "index=" + index);

    auto const config = controller()->serviceProvider()->config();
    auto const databaseInfo = config->databaseInfo(database);

    bool const allWorkers = true;
    auto const job = SqlDropIndexesJob::create(database, table, index, allWorkers, controller());
    job->start();
    logJobStartedEvent(SqlDropIndexesJob::typeName(), job, databaseInfo.family);
    job->wait();
    logJobFinishedEvent(SqlDropIndexesJob::typeName(), job, databaseInfo.family);

    string const error = ::getErrorIfAny(job);
    if (not error.empty()) throw HttpError(__func__, error);

    return json::object();
}

}}}  // namespace lsst::qserv::replica
